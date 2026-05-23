from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.daemon import DaemonTickResult, TasqueDaemon
from tasque2.models import Artifact, ProviderRun, Schedule, WorkflowRun, WorkItem
from tasque2.repo import WorkRepository
from tasque2.reports import ReportService
from tasque2.scheduler import ScheduleService
from tasque2.workflows import WorkflowService

LOCAL_SMOKE_WORKFLOW_DEFINITION: dict[str, Any] = {
    "nodes": [
        {
            "key": "prepare",
            "kind": "work",
            "title": "Prepare local smoke context",
            "task_instruction": "Prepare the local smoke workflow context.",
            "worker_kind": "function.echo",
        },
        {
            "key": "provider",
            "kind": "work",
            "title": "Run test provider smoke",
            "task_instruction": (
                "Submit a provider smoke result with ok true, provider \"test\", "
                "and message \"local smoke passed\"."
            ),
            "worker_kind": "provider.fake",
            "runtime_contract": {},
            "depends_on": ["prepare"],
            "max_attempts": 1,
        },
        {
            "key": "join",
            "kind": "join",
            "depends_on": ["provider"],
        },
    ]
}


@dataclass(frozen=True)
class RunbookSmokeResult:
    schedule_id: str
    scheduled_work_item_id: str
    workflow_definition_id: str
    workflow_run_id: str
    workflow_status: str
    provider_work_item_id: str
    provider_run_id: str
    report_artifact_id: str
    tick_count: int
    tick_results: tuple[DaemonTickResult, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schedule_id": self.schedule_id,
            "scheduled_work_item_id": self.scheduled_work_item_id,
            "workflow_definition_id": self.workflow_definition_id,
            "workflow_run_id": self.workflow_run_id,
            "workflow_status": self.workflow_status,
            "provider_work_item_id": self.provider_work_item_id,
            "provider_run_id": self.provider_run_id,
            "report_artifact_id": self.report_artifact_id,
            "tick_count": self.tick_count,
            "tick_results": [
                {
                    "recovered_leases": tick.recovered_leases,
                    "scheduled_work": tick.scheduled_work,
                    "workflow_runs_changed": tick.workflow_runs_changed,
                    "work_items_ran": tick.work_items_ran,
                }
                for tick in self.tick_results
            ],
        }


def run_local_smoke(
    session: Session,
    *,
    title: str = "Local runbook smoke",
    max_ticks: int = 10,
) -> RunbookSmokeResult:
    schedule = _create_due_smoke_schedule(session, title=title)
    first_tick = TasqueDaemon(session).run_once(max_work_items=10)
    scheduled_work = _scheduled_work_item(session, schedule)

    workflow_definition = WorkflowService(session).create_definition(
        name="tasque.local_smoke",
        version="1",
        definition=LOCAL_SMOKE_WORKFLOW_DEFINITION,
    )
    workflow_run = WorkflowService(session).start_run(
        workflow_definition_id=workflow_definition.id,
        name=title,
        input={
            "goal": "Exercise schedule, workflow, provider, artifact, and report paths locally.",
        },
    )

    tick_results = [first_tick]
    for _ in range(max_ticks):
        tick_results.append(TasqueDaemon(session).run_once(max_work_items=10))
        session.refresh(workflow_run)
        if workflow_run.status in {"completed", "failed", "canceled"}:
            break
    else:
        raise RuntimeError(f"Local smoke did not finish after {max_ticks} daemon ticks.")

    if workflow_run.status != "completed":
        raise RuntimeError(f"Local smoke workflow ended with status {workflow_run.status!r}.")

    provider_work = _provider_work_item(session, workflow_run)
    provider_run = _provider_run(session, provider_work)
    report_artifact = _write_workflow_report_artifact(session, workflow_run)
    WorkRepository(session).emit_event(
        event_type="runbook.smoke_completed",
        entity_kind="workflow_run",
        entity_id=workflow_run.id,
        workflow_run_id=workflow_run.id,
        source="runbook",
        summary=f"Completed local runbook smoke: {title}",
        payload={
            "schedule_id": schedule.id,
            "scheduled_work_item_id": scheduled_work.id,
            "provider_work_item_id": provider_work.id,
            "provider_run_id": provider_run.id,
            "report_artifact_id": report_artifact.id,
        },
    )
    session.flush()
    return RunbookSmokeResult(
        schedule_id=schedule.id,
        scheduled_work_item_id=scheduled_work.id,
        workflow_definition_id=workflow_definition.id,
        workflow_run_id=workflow_run.id,
        workflow_status=workflow_run.status,
        provider_work_item_id=provider_work.id,
        provider_run_id=provider_run.id,
        report_artifact_id=report_artifact.id,
        tick_count=len(tick_results),
        tick_results=tuple(tick_results),
    )


def _create_due_smoke_schedule(session: Session, *, title: str) -> Schedule:
    now = datetime.now(UTC)
    return ScheduleService(session).create_schedule(
        name=f"{title}: scheduled trigger",
        schedule_type="date",
        expression=(now - timedelta(seconds=1)).isoformat(),
        worker_kind="function.echo",
        payload={
            "title": f"{title}: scheduled trigger",
            "task_instruction": "Scheduled trigger ran before workflow orchestration.",
        },
        timezone_name="UTC",
        catchup_policy="coalesce",
    )


def _scheduled_work_item(session: Session, schedule: Schedule) -> WorkItem:
    work = session.scalar(select(WorkItem).where(WorkItem.schedule_id == schedule.id))
    if work is None:
        raise RuntimeError("Local smoke schedule did not enqueue work.")
    if work.status != "succeeded":
        raise RuntimeError(f"Local smoke scheduled work ended with status {work.status!r}.")
    return work


def _provider_work_item(session: Session, workflow_run: WorkflowRun) -> WorkItem:
    work = session.scalar(
        select(WorkItem).where(
            WorkItem.workflow_run_id == workflow_run.id,
            WorkItem.worker_kind == "provider.fake",
        )
    )
    if work is None:
        raise RuntimeError("Local smoke test-provider work was not created.")
    if work.status != "succeeded":
        raise RuntimeError(f"Local smoke test-provider work ended with status {work.status!r}.")
    return work


def _provider_run(session: Session, work_item: WorkItem) -> ProviderRun:
    run = session.scalar(
        select(ProviderRun).where(ProviderRun.attempt.has(work_item_id=work_item.id))
    )
    if run is None:
        raise RuntimeError("Local smoke provider run was not recorded.")
    if run.status != "succeeded":
        raise RuntimeError(f"Local smoke provider run ended with status {run.status!r}.")
    return run


def _write_workflow_report_artifact(session: Session, workflow_run: WorkflowRun) -> Artifact:
    report = ReportService(session).workflow_report(workflow_run.id)
    return ArtifactStore().write_text(
        session,
        kind="report",
        title=report.title,
        content=report.body,
        suffix=".md",
        workflow_run_id=workflow_run.id,
        tags=["runbook", "smoke", "workflow-report"],
        source_kind="runbook",
        source_id=workflow_run.id,
    )
