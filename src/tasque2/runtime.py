from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.orm import Session

from tasque2.models import WorkItem
from tasque2.providers import ProviderRuntime
from tasque2.queue import WorkQueue


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

    def run_next(self) -> RunOutcome | None:
        queue = WorkQueue(self.session)
        claimed = queue.claim_next_ready_work(
            lease_owner=self.lease_owner,
            lease_seconds=self.lease_seconds,
        )
        if claimed is None:
            return None

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
        return RunOutcome(
            work_item_id=claimed.work_item.id,
            attempt_id=claimed.attempt.id,
            status=claimed.work_item.status,
            summary=result.summary,
        )
