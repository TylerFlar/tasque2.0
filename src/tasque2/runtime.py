from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from tasque2 import result_inbox
from tasque2.extensions import registry as extension_registry
from tasque2.models import ProviderRun, WorkAttempt, WorkItem, utc_now
from tasque2.providers import ProviderRequest, ProviderRuntime
from tasque2.queue import ClaimedWork, WorkQueue

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerResult:
    summary: str
    produces: dict[str, Any] = field(default_factory=dict)
    report_artifact_id: str | None = None


@dataclass(frozen=True)
class RunOutcome:
    work_item_id: str
    attempt_id: str
    status: str
    summary: str


WorkerFunction = Callable[[WorkItem], WorkerResult | dict[str, Any] | str | None]


class WorkerNotFoundError(KeyError):
    pass


class FunctionWorkerRegistry:
    def __init__(self) -> None:
        self._workers: dict[str, WorkerFunction] = {}

    def register(self, worker_kind: str, function: WorkerFunction) -> None:
        self._workers[worker_kind] = function

    def run(self, work_item: WorkItem) -> WorkerResult:
        function = self._workers.get(work_item.worker_kind)
        if function is None:
            raise WorkerNotFoundError(f"No function worker registered for {work_item.worker_kind!r}.")
        return normalize_worker_result(function(work_item))


def normalize_worker_result(result: WorkerResult | dict[str, Any] | str | None) -> WorkerResult:
    if isinstance(result, WorkerResult):
        return result
    if isinstance(result, str):
        return WorkerResult(summary=result)
    if result is None:
        return WorkerResult(summary="Function worker completed.")

    summary = str(result.get("summary", "Function worker completed."))
    produces = result.get("produces")
    if produces is None:
        produces = {key: value for key, value in result.items() if key != "summary"}
    if not isinstance(produces, dict):
        produces = {"value": produces}
    report_artifact_id = result.get("report_artifact_id")
    if report_artifact_id is not None:
        report_artifact_id = str(report_artifact_id)
    return WorkerResult(
        summary=summary,
        produces=produces,
        report_artifact_id=report_artifact_id,
    )


def default_function_registry() -> FunctionWorkerRegistry:
    registry = FunctionWorkerRegistry()
    registry.register("manual", _manual_worker)
    registry.register("noop", _noop_worker)
    registry.register("function.noop", _noop_worker)
    registry.register("echo", _echo_worker)
    registry.register("function.echo", _echo_worker)
    return registry


def _manual_worker(work_item: WorkItem) -> WorkerResult:
    return WorkerResult(
        summary="Manual work item acknowledged by the local function runtime.",
        produces={"title": work_item.title, "worker_kind": work_item.worker_kind},
    )


def _noop_worker(work_item: WorkItem) -> WorkerResult:
    return WorkerResult(
        summary=f"No-op worker completed: {work_item.title}",
        produces={"work_item_id": work_item.id},
    )


def _echo_worker(work_item: WorkItem) -> WorkerResult:
    return WorkerResult(
        summary=work_item.task_instruction,
        produces={
            "title": work_item.title,
            "task_instruction": work_item.task_instruction,
            "context": work_item.context,
        },
    )


class WorkRunner:
    def __init__(
        self,
        session: Session,
        *,
        registry: FunctionWorkerRegistry | None = None,
        provider_runtime: ProviderRuntime | None = None,
        lease_owner: str = "local-runner",
        lease_seconds: int | None = None,
    ) -> None:
        self.session = session
        self.registry = registry or default_function_registry()
        self.provider_runtime = provider_runtime or ProviderRuntime()
        self.lease_owner = lease_owner
        self.lease_seconds = lease_seconds

    def recover_deposited_results(self, *, orphaned_before: datetime) -> int:
        """Finish attempts whose worker deposited a result after the daemon died.

        A provider subprocess is spawned into its own process group, so it
        outlives the daemon that started it and can still submit its result --
        but the in-memory result_token that the runtime would have consumed died
        with the parent. Without this, restart requeues work that already
        completed, which for a career apply means submitting the same
        application twice.

        Scoped by the same ``orphaned_before`` predicate that orphan recovery
        uses, so it can only touch attempts from a previous daemon process and
        never races the live poll loop for a result.
        """
        queue = WorkQueue(self.session)
        attempts = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.status == "running",
                WorkAttempt.lease_owner == self.lease_owner,
                or_(
                    WorkAttempt.heartbeat_at.is_(None),
                    WorkAttempt.heartbeat_at < orphaned_before,
                ),
            )
        ).all()

        recovered = 0
        for attempt in attempts:
            work_item = attempt.work_item
            payload = result_inbox.consume_for_work_item(self.session, work_item.id)
            if payload is None:
                continue
            provider_run = self.session.get(ProviderRun, attempt.provider_run_id or "")
            if provider_run is None:
                continue
            try:
                result = self.provider_runtime.finalize_submitted_payload(
                    self.session,
                    work_item=work_item,
                    attempt=attempt,
                    provider_run=provider_run,
                    request=ProviderRequest(
                        provider=provider_run.provider,
                        prompt="",
                        cwd=provider_run.cwd,
                    ),
                    payload=payload,
                    provider_name=provider_run.provider,
                )
            except Exception as exc:  # noqa: BLE001 - a bad payload must not block recovery
                queue.fail_attempt(
                    attempt.id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                recovered += 1
                continue

            provider_run.status = "succeeded"
            provider_run.ended_at = utc_now()
            provider_run.usage = {
                **(provider_run.usage or {}),
                "recovered_deposited_result": True,
            }
            queue.complete_attempt(
                attempt.id,
                summary=result.summary,
                produces=result.produces,
                report_artifact_id=result.report_artifact_id,
            )
            recovered += 1

        self.session.flush()
        return recovered

    def claim(self) -> ClaimedWork | None:
        """Claim the next ready work item for this runner's lease owner.

        Split out from :meth:`run_next` so a concurrent daemon can serialize the
        claim (the only writer-vs-writer step) while running the claimed work --
        the long part -- in parallel. Callers that want to publish the claim
        before doing anything slow should ``session.commit()`` after this returns.
        """
        queue = WorkQueue(self.session)
        return queue.claim_next_ready_work(
            lease_owner=self.lease_owner,
            lease_seconds=self.lease_seconds,
        )

    def execute(self, claimed: ClaimedWork) -> RunOutcome:
        """Run an already-claimed work item and record its outcome."""
        queue = WorkQueue(self.session)
        try:
            if self.provider_runtime.can_run(claimed.work_item.worker_kind):
                result = self.provider_runtime.run(
                    self.session,
                    claimed.work_item,
                    claimed.attempt,
                )
            else:
                result = self.registry.run(claimed.work_item)
        except Exception as exc:
            queue.fail_attempt(
                claimed.attempt.id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return RunOutcome(
                work_item_id=claimed.work_item.id,
                attempt_id=claimed.attempt.id,
                status=claimed.work_item.status,
                summary=str(exc),
            )

        queue.complete_attempt(
            claimed.attempt.id,
            summary=result.summary,
            produces=result.produces,
            report_artifact_id=result.report_artifact_id,
        )
        # Fallback recorders (extension-registered): a run that emitted
        # machine-readable produces but skipped its logging tool still leaves
        # ledger rows, so the computed digests stay complete.
        for ingestor_name, ingest_attempt in extension_registry().attempt_ingestors:
            try:
                ingest_attempt(self.session, claimed.work_item, claimed.attempt)
            except Exception:  # noqa: BLE001 - history capture must never fail the run
                logger.exception(
                    "Attempt ingestor %s failed for attempt %s",
                    ingestor_name,
                    claimed.attempt.id,
                )
        return RunOutcome(
            work_item_id=claimed.work_item.id,
            attempt_id=claimed.attempt.id,
            status=claimed.work_item.status,
            summary=result.summary,
        )

    def run_next(self) -> RunOutcome | None:
        claimed = self.claim()
        if claimed is None:
            return None
        return self.execute(claimed)
