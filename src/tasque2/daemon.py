from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.orm import Session

from tasque2.memory_ingest import MemoryIngestService
from tasque2.models import utc_now
from tasque2.queue import WorkQueue
from tasque2.runtime import WorkRunner
from tasque2.scheduler import ScheduleService
from tasque2.workflows import WorkflowService


@dataclass(frozen=True)
class DaemonTickResult:
    recovered_leases: int
    recovered_orphans: int
    scheduled_work: int
    workflow_runs_changed: int
    work_items_ran: int
    memory_ingested: int = 0

    @property
    def has_activity(self) -> bool:
        return any(
            (
                self.recovered_leases,
                self.recovered_orphans,
                self.scheduled_work,
                self.workflow_runs_changed,
                self.work_items_ran,
                self.memory_ingested,
            )
        )


class TasqueDaemon:
    def __init__(self, session: Session) -> None:
        self.session = session

    def run_once(
        self,
        *,
        max_work_items: int = 10,
        recover_orphaned_lease_owner: str | None = None,
        orphaned_before: datetime | None = None,
    ) -> DaemonTickResult:
        queue = WorkQueue(self.session)
        scheduler = ScheduleService(self.session)
        workflows = WorkflowService(self.session)

        recovered = queue.recover_expired_leases()
        recovered_orphans = 0
        if recover_orphaned_lease_owner is not None:
            recovered_orphans = queue.recover_orphaned_attempts(
                lease_owner=recover_orphaned_lease_owner,
                orphaned_before=orphaned_before or utc_now(),
            )
        scheduled = scheduler.poll_due_schedules()
        workflow_changes = workflows.tick_runs()

        ran = 0
        runner = WorkRunner(self.session, lease_owner="daemon")
        while ran < max_work_items:
            outcome = runner.run_next()
            if outcome is None:
                break
            ran += 1

        workflow_changes += workflows.tick_runs()
        memory_ingested = MemoryIngestService(self.session).auto_ingest_pending().ingested_sources
        self.session.flush()
        return DaemonTickResult(
            recovered_leases=recovered,
            recovered_orphans=recovered_orphans,
            scheduled_work=scheduled,
            workflow_runs_changed=workflow_changes,
            work_items_ran=ran,
            memory_ingested=memory_ingested,
        )
