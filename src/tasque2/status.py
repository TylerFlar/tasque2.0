from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.models import FailedWork, Schedule, WorkAttempt, WorkflowRun, WorkItem


@dataclass(frozen=True)
class SystemStatus:
    work_items: dict[str, int]
    work_attempts: dict[str, int]
    failed_work_unresolved: int
    schedules_enabled: int
    workflow_runs: dict[str, int]
    ready_work: int
    running_work: int


def get_system_status(session: Session) -> SystemStatus:
    return SystemStatus(
        work_items=_count_by(session, WorkItem.status),
        work_attempts=_count_by(session, WorkAttempt.status),
        failed_work_unresolved=session.scalar(
            select(func.count()).select_from(FailedWork).where(FailedWork.status == "unresolved")
        )
        or 0,
        schedules_enabled=session.scalar(
            select(func.count()).select_from(Schedule).where(Schedule.enabled.is_(True))
        )
        or 0,
        workflow_runs=_count_by(session, WorkflowRun.status),
        ready_work=session.scalar(
            select(func.count()).select_from(WorkItem).where(WorkItem.status == "ready")
        )
        or 0,
        running_work=session.scalar(
            select(func.count()).select_from(WorkItem).where(WorkItem.status == "running")
        )
        or 0,
    )


def _count_by(session: Session, column) -> dict[str, int]:
    rows = session.execute(select(column, func.count()).group_by(column)).all()
    return {str(status): int(count) for status, count in rows}
