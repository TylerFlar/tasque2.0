"""Run claimed work items: provider-backed workers or in-process function workers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from opentelemetry.trace import SpanKind
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from tasque2.extensions import registry as extension_registry
from tasque2.models import ProviderRun, WorkAttempt, WorkItem, utc_now
from tasque2.telemetry import context_from_traceparent, record_exception, span
from tasque2.work.queue import ClaimedWork, WorkQueue

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


class WorkerNotFoundError(LookupError):
    pass


class FunctionWorkerRegistry:
    """In-process workers keyed by ``worker_kind`` (used for smoke runs and tests)."""

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
    produces = result.get("produces")
    if produces is None:
        produces = {key: value for key, value in result.items() if key != "summary"}
    if not isinstance(produces, dict):
        produces = {"value": produces}
    report_artifact_id = result.get("report_artifact_id")
    return WorkerResult(
        summary=str(result.get("summary", "Function worker completed.")),
        produces=produces,
        report_artifact_id=str(report_artifact_id) if report_artifact_id is not None else None,
    )


def default_function_registry() -> FunctionWorkerRegistry:
    registry = FunctionWorkerRegistry()
    registry.register("manual", _manual_worker)
    registry.register("function.noop", _noop_worker)
    registry.register("function.echo", _echo_worker)
    registry.register("function.notify", _notify_worker)
    return registry


def _manual_worker(work_item: WorkItem) -> WorkerResult:
    return WorkerResult(
        summary="Manual work item acknowledged.",
        produces={"title": work_item.title, "worker_kind": work_item.worker_kind},
    )


def _noop_worker(work_item: WorkItem) -> WorkerResult:
    return WorkerResult(summary=f"No-op worker completed: {work_item.title}", produces={"work_item_id": work_item.id})


def _notify_worker(work_item: WorkItem) -> WorkerResult:
    """Post the work's own text as its message: reminders and other notices that need no model."""
    return WorkerResult(summary=work_item.task_instruction.strip(), produces={"notice": True})


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
        provider_runtime=None,
        lease_owner: str = "local-runner",
        lease_seconds: int | None = None,
    ) -> None:
        from tasque2.worker.runtime import ProviderRuntime

        self.session = session
        self.registry = registry or default_function_registry()
        self.provider_runtime = provider_runtime or ProviderRuntime()
        self.lease_owner = lease_owner
        self.lease_seconds = lease_seconds

    def claim(self) -> ClaimedWork | None:
        """Claim the next ready item; callers publish the claim with ``session.commit()``."""
        return WorkQueue(self.session).claim_next_ready_work(
            lease_owner=self.lease_owner,
            lease_seconds=self.lease_seconds,
        )

    def execute(self, claimed: ClaimedWork) -> RunOutcome:
        """Run an already-claimed work item and record its outcome."""
        work_item, attempt = claimed.work_item, claimed.attempt
        queue = WorkQueue(self.session)
        with span(
            "tasque.work.run",
            kind=SpanKind.CONSUMER,
            context=context_from_traceparent(work_item.traceparent),
            attributes=work_span_attributes(work_item, attempt),
        ) as current:
            try:
                if self.provider_runtime.can_run(work_item.worker_kind):
                    result = self.provider_runtime.run(self.session, work_item, attempt)
                else:
                    result = self.registry.run(work_item)
            except Exception as exc:
                record_exception(current, exc)
                queue.fail_attempt(attempt.id, error_type=type(exc).__name__, error_message=str(exc))
                current.set_attribute("tasque.work.status", work_item.status)
                return RunOutcome(work_item.id, attempt.id, work_item.status, str(exc))

            queue.complete_attempt(
                attempt.id,
                summary=result.summary,
                produces=result.produces,
                report_artifact_id=result.report_artifact_id,
            )
            current.set_attribute("tasque.work.status", work_item.status)
            self._run_attempt_ingestors(work_item, attempt)
            return RunOutcome(work_item.id, attempt.id, work_item.status, result.summary)

    def run_next(self) -> RunOutcome | None:
        claimed = self.claim()
        if claimed is None:
            return None
        return self.execute(claimed)

    def recover_deposited_results(self, *, orphaned_before: datetime) -> int:
        """Finish attempts whose worker submitted a result after the previous daemon died.

        A provider subprocess outlives the daemon that started it and can still submit its
        result; requeueing that work would redo it (and could resubmit an application).
        Only attempts from before ``orphaned_before`` are considered, so this never races
        the live result polling.
        """
        from tasque2.worker import results as result_inbox

        queue = WorkQueue(self.session)
        attempts = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.status == "running",
                WorkAttempt.lease_owner == self.lease_owner,
                or_(WorkAttempt.heartbeat_at.is_(None), WorkAttempt.heartbeat_at < orphaned_before),
            )
        ).all()
        recovered = 0
        for attempt in attempts:
            provider_run = self.session.get(ProviderRun, attempt.provider_run_id or "")
            if provider_run is None:
                continue
            payload = result_inbox.consume_for_work_item(self.session, attempt.work_item_id)
            if payload is None:
                continue
            provider_run.status = "succeeded"
            provider_run.ended_at = utc_now()
            provider_run.usage = {**(provider_run.usage or {}), "recovered_deposited_result": True}
            try:
                result = self.provider_runtime.finalize(
                    self.session,
                    work_item=attempt.work_item,
                    attempt=attempt,
                    provider_run=provider_run,
                    payload=payload,
                )
            except Exception as exc:  # noqa: BLE001 - one bad payload must not block recovery
                queue.fail_attempt(attempt.id, error_type=type(exc).__name__, error_message=str(exc))
            else:
                queue.complete_attempt(
                    attempt.id,
                    summary=result.summary,
                    produces=result.produces,
                    report_artifact_id=result.report_artifact_id,
                )
                self._run_attempt_ingestors(attempt.work_item, attempt)
            recovered += 1
        self.session.flush()
        return recovered

    def _run_attempt_ingestors(self, work_item: WorkItem, attempt: WorkAttempt) -> None:
        for name, ingest in extension_registry().attempt_ingestors:
            try:
                ingest(self.session, work_item, attempt)
            except Exception:  # noqa: BLE001 - history capture must never fail the run
                logger.exception("Attempt ingestor %s failed for attempt %s", name, attempt.id)


def work_span_attributes(work_item: WorkItem, attempt: WorkAttempt | None = None) -> dict[str, Any]:
    return {
        "tasque.work.id": work_item.id,
        "tasque.work.title": work_item.title,
        "tasque.work.lane": work_item.lane,
        "tasque.worker.kind": work_item.worker_kind,
        "tasque.work.source_kind": work_item.source_kind,
        "tasque.work.attempt": attempt.attempt_number if attempt is not None else None,
        "tasque.schedule.id": work_item.schedule_id,
        "tasque.workflow.run.id": work_item.workflow_run_id,
    }
