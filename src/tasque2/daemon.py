from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.memory_ingest import MemoryIngestService
from tasque2.models import utc_now
from tasque2.queue import WorkQueue
from tasque2.runtime import WorkRunner
from tasque2.scheduler import ScheduleService
from tasque2.workflows import WorkflowService

# Sentinel returned by the concurrent worker when nothing was claimable.
_NO_WORK = object()


def _run_work_concurrently(
    *,
    max_work_items: int,
    concurrency: int,
    lease_owner: str = "daemon",
    lease_seconds: int | None = None,
) -> int:
    """Drain up to ``max_work_items`` ready work items using ``concurrency`` threads.

    Each worker thread runs in its own ``session_scope`` so the long provider
    subprocesses execute in parallel. SQLite allows a single writer, so the
    *claim* step (the only place two threads would fight for the write lock) is
    serialized with a process-local lock and committed before the lock is
    released -- no two workers can claim the same item, and no write lock is held
    across a subprocess (``ProviderRuntime.run`` commits before it spawns one).
    """
    claim_lock = threading.Lock()
    counter_lock = threading.Lock()
    counter = {"ran": 0}

    def claim_and_run_one() -> object:
        with session_scope() as session:
            runner = WorkRunner(
                session,
                lease_owner=lease_owner,
                lease_seconds=lease_seconds,
            )
            with claim_lock:
                claimed = runner.claim()
                if claimed is None:
                    return _NO_WORK
                # Publish the claim (status=running) before releasing the lock so
                # a sibling worker can't re-claim the same item against a stale
                # pre-commit snapshot.
                session.commit()
            return runner.execute(claimed)

    def worker(index: int) -> int:
        ran = 0
        while True:
            with counter_lock:
                if counter["ran"] >= max_work_items:
                    break
                counter["ran"] += 1  # reserve a slot before claiming
            try:
                outcome: object = claim_and_run_one()
            except Exception as exc:  # noqa: BLE001 - isolate one worker from the pool
                print(f"Tasque daemon worker {index} failed: {exc}")
                outcome = _NO_WORK
            if outcome is _NO_WORK:
                with counter_lock:
                    counter["ran"] -= 1  # refund: nothing was available to run
                break
            ran += 1
        return ran

    workers = max(1, int(concurrency))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        totals = list(pool.map(worker, range(workers)))
    return sum(totals)


@dataclass(frozen=True)
class DaemonTickResult:
    recovered_leases: int
    recovered_orphans: int
    scheduled_work: int
    workflow_runs_changed: int
    work_items_ran: int
    memory_ingested: int = 0
    expired_overdue: int = 0

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
                self.expired_overdue,
            )
        )


class TasqueDaemon:
    def __init__(self, session: Session) -> None:
        self.session = session

    def run_once(
        self,
        *,
        max_work_items: int = 10,
        concurrency: int | None = None,
        recover_orphaned_lease_owner: str | None = None,
        orphaned_before: datetime | None = None,
    ) -> DaemonTickResult:
        if concurrency is None:
            concurrency = get_settings().daemon_concurrency
        concurrency = max(1, int(concurrency))

        queue = WorkQueue(self.session)
        scheduler = ScheduleService(self.session)
        workflows = WorkflowService(self.session)

        recovered = queue.recover_expired_leases()
        expired_overdue = queue.expire_overdue_work()
        recovered_orphans = 0
        if recover_orphaned_lease_owner is not None:
            recovered_orphans = queue.recover_orphaned_attempts(
                lease_owner=recover_orphaned_lease_owner,
                orphaned_before=orphaned_before or utc_now(),
            )
        scheduled = scheduler.poll_due_schedules()
        workflow_changes = workflows.tick_runs()

        if concurrency > 1:
            # Worker threads run in their own sessions, so they can only see work
            # that's already committed. Publish what this tick just scheduled and
            # fanned out, run the claims in parallel, then drop our now-stale
            # snapshot so the follow-up workflow tick sees the workers' results.
            self.session.commit()
            ran = _run_work_concurrently(
                max_work_items=max_work_items,
                concurrency=concurrency,
            )
            self.session.expire_all()
        else:
            ran = 0
            runner = WorkRunner(self.session, lease_owner="daemon")
            while ran < max_work_items:
                outcome = runner.run_next()
                if outcome is None:
                    break
                ran += 1

        workflow_changes += workflows.tick_runs()
        memory_ingested = (
            MemoryIngestService(self.session).auto_ingest_pending().ingested_sources
            if get_settings().memory_auto_ingest
            else 0
        )
        self.session.flush()
        return DaemonTickResult(
            recovered_leases=recovered,
            recovered_orphans=recovered_orphans,
            scheduled_work=scheduled,
            workflow_runs_changed=workflow_changes,
            work_items_ran=ran,
            memory_ingested=memory_ingested,
            expired_overdue=expired_overdue,
        )
