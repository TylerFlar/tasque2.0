"""A deterministic end-to-end run: schedule, workflow, fake provider, artifacts, report."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.daemon.tick import DaemonTick
from tasque2.models import ProviderRun, WorkAttempt, WorkItem
from tasque2.ops.reports import ReportService
from tasque2.schedules import ScheduleService
from tasque2.workflows import WorkflowService

SMOKE_WORKFLOW: dict[str, Any] = {
    "nodes": [
        {
            "key": "prepare",
            "title": "Prepare smoke context",
            "task_instruction": "Prepare the smoke workflow context.",
            "worker_kind": "function.echo",
        },
        {
            "key": "provider",
            "title": "Run the fake provider",
            "task_instruction": "Submit a smoke result.",
            "worker_kind": "provider.fake",
            "depends_on": ["prepare"],
        },
        {"key": "join", "kind": "join", "depends_on": ["provider"]},
    ]
}


@dataclass(frozen=True)
class SmokeResult:
    schedule_id: str
    scheduled_work_item_id: str
    workflow_run_id: str
    workflow_status: str
    provider_run_id: str
    report_artifact_id: str
    ticks: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def run_smoke(session: Session, *, title: str = "Local smoke", max_ticks: int = 10) -> SmokeResult:
    schedule = ScheduleService(session).create_schedule(
        name=f"{title}: scheduled trigger",
        schedule_type="date",
        expression=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        worker_kind="function.echo",
        payload={"title": f"{title}: scheduled trigger", "task_instruction": "Scheduled trigger ran."},
        timezone_name="UTC",
    )
    ticker = DaemonTick()
    ticker.run(session)
    scheduled = session.scalar(select(WorkItem).where(WorkItem.schedule_id == schedule.id))
    if scheduled is None or scheduled.status != "succeeded":
        raise RuntimeError("The smoke schedule did not run its work.")

    workflows = WorkflowService(session)
    definition = workflows.create_definition(name="tasque.smoke", version="1", definition=SMOKE_WORKFLOW)
    run = workflows.start_run(workflow_definition_id=definition.id, name=title)
    ticks = 1
    for _ in range(max_ticks):
        ticker.run(session)
        ticks += 1
        session.refresh(run)
        if run.status in {"completed", "failed", "canceled"}:
            break
    if run.status != "completed":
        raise RuntimeError(f"The smoke workflow ended with status {run.status!r}.")

    provider_run = session.scalar(
        select(ProviderRun)
        .join(WorkAttempt, WorkAttempt.id == ProviderRun.attempt_id)
        .join(WorkItem, WorkItem.id == WorkAttempt.work_item_id)
        .where(WorkItem.workflow_run_id == run.id, WorkItem.worker_kind == "provider.fake")
    )
    if provider_run is None or provider_run.status != "succeeded":
        raise RuntimeError("The fake provider run was not recorded as succeeded.")
    report = ReportService(session).workflow_report(run.id)
    artifact = ArtifactStore().write_text(
        session,
        kind="report",
        title=report.title,
        content=report.body,
        suffix=".md",
        workflow_run_id=run.id,
        tags=["smoke", "workflow-report"],
        source_kind="smoke",
        source_id=run.id,
    )
    session.flush()
    return SmokeResult(
        schedule_id=schedule.id,
        scheduled_work_item_id=scheduled.id,
        workflow_run_id=run.id,
        workflow_status=run.status,
        provider_run_id=provider_run.id,
        report_artifact_id=artifact.id,
        ticks=ticks,
    )
