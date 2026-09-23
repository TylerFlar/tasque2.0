"""One daemon tick: recover, schedule, advance workflows, dispatch work, keep house."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, fields
from datetime import datetime

from sqlalchemy.orm import Session

from tasque2.artifacts import prune_artifacts
from tasque2.config import get_settings
from tasque2.daemon.pool import WorkPool, drain_synchronously
from tasque2.memory import expire_ttl_memories
from tasque2.models import utc_now
from tasque2.reminders import ReminderService
from tasque2.schedules import ScheduleService
from tasque2.scratch import prune_scratch_dirs
from tasque2.telemetry import instruments
from tasque2.work.queue import WorkQueue
from tasque2.work.runner import WorkRunner
from tasque2.worker import results
from tasque2.workflows import WorkflowService

logger = logging.getLogger(__name__)

LEASE_OWNER = "daemon"
RESULT_REAP_AGE_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class TickResult:
    recovered_results: int = 0
    recovered_orphans: int = 0
    recovered_leases: int = 0
    expired_overdue: int = 0
    scheduled: int = 0
    workflow_changes: int = 0
    dispatched: int = 0
    finished: int = 0
    artifacts_pruned: int = 0
    memories_expired: int = 0
    scratch_pruned: int = 0

    @property
    def has_activity(self) -> bool:
        return any(getattr(self, item.name) for item in fields(self))

    def describe(self) -> str:
        return ", ".join(f"{item.name}={getattr(self, item.name)}" for item in fields(self) if getattr(self, item.name))


class IntervalGate:
    """Process-local "is this pass due" clock for bookkeeping that should not run every tick."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last: datetime | None = None

    def claim(self, now: datetime, interval_seconds: int) -> bool:
        with self._lock:
            if self._last is not None and (now - self._last).total_seconds() < interval_seconds:
                return False
            self._last = now
            return True

    def reset(self) -> None:
        with self._lock:
            self._last = None


class DaemonTick:
    """Runs ticks; ``pool`` is None for synchronous one-shot runs (CLI, tests)."""

    def __init__(self, *, pool: WorkPool | None = None, recover_before: datetime | None = None) -> None:
        self.pool = pool
        self._recover_before = recover_before
        self._retention_gate = IntervalGate()
        self._ttl_gate = IntervalGate()

    def run(self, session: Session, *, max_claims: int | None = None, claim: bool = True) -> TickResult:
        started = time.perf_counter()
        settings = get_settings()
        max_claims = settings.daemon_max_claims_per_tick if max_claims is None else max_claims
        queue = WorkQueue(session)
        lease_seconds = settings.daemon_lease_seconds if self.pool is not None else None
        if self.pool is not None:
            queue.heartbeat_running_attempts(self.pool.in_flight_attempt_ids(), lease_seconds=lease_seconds)

        recovered_results = recovered_orphans = 0
        if self._recover_before is not None:
            recovered_results = WorkRunner(session, lease_owner=LEASE_OWNER).recover_deposited_results(
                orphaned_before=self._recover_before
            )
            recovered_orphans = queue.recover_orphaned_attempts(
                lease_owner=LEASE_OWNER, orphaned_before=self._recover_before
            )
            self._recover_before = None

        recovered_leases = queue.recover_expired_leases()
        expired_overdue = queue.expire_overdue_work()
        scheduled = ScheduleService(session).poll_due_schedules()
        workflows = WorkflowService(session)
        workflow_changes = workflows.tick_runs()
        session.commit()

        dispatched = finished = 0
        if self.pool is not None:
            finished = self.pool.reap()
            ready = queue.ready_count() if claim else 0
            if ready:
                dispatched = self.pool.dispatch(
                    limit=min(max_claims, ready), lease_owner=LEASE_OWNER, lease_seconds=lease_seconds
                )
        elif claim:
            finished = drain_synchronously(
                max_items=max_claims, concurrency=settings.daemon_concurrency, lease_owner=LEASE_OWNER
            )
        session.expire_all()
        workflow_changes += workflows.tick_runs()

        artifacts_pruned, scratch_pruned = self._retention(session, settings)
        memories_expired = self._expire_memories(session, settings)
        session.flush()
        instruments().tick_duration.record(time.perf_counter() - started)
        return TickResult(
            recovered_results=recovered_results,
            recovered_orphans=recovered_orphans,
            recovered_leases=recovered_leases,
            expired_overdue=expired_overdue,
            scheduled=scheduled,
            workflow_changes=workflow_changes,
            dispatched=dispatched,
            finished=finished,
            artifacts_pruned=artifacts_pruned,
            memories_expired=memories_expired,
            scratch_pruned=scratch_pruned,
        )

    def _retention(self, session: Session, settings) -> tuple[int, int]:
        if not self._retention_gate.claim(utc_now(), settings.artifact_retention_interval_seconds):
            return 0, 0
        # The inbox reap writes through its own session, so this one must not hold SQLite's write lock.
        session.commit()
        try:
            results.reap_stale(max_age_seconds=RESULT_REAP_AGE_SECONDS)
        except Exception:  # noqa: BLE001 - bookkeeping never breaks a tick
            logger.exception("Result inbox cleanup failed")
        artifacts = scratch = 0
        metrics = instruments()
        try:
            pruned = prune_artifacts(session)
            artifacts = pruned.pruned
            if artifacts:
                logger.info("Artifact retention: pruned %s files (%.1f MB)", artifacts, pruned.megabytes_freed)
                metrics.retention_pruned.add(artifacts, {"tasque.retention.kind": "artifact"})
        except Exception:  # noqa: BLE001 - bookkeeping never breaks a tick
            logger.exception("Artifact retention failed")
        try:
            pruned_dirs = prune_scratch_dirs()
            scratch = pruned_dirs.pruned
            if scratch:
                logger.info(
                    "Scratch retention: removed %s run directories (%.1f MB)", scratch, pruned_dirs.megabytes_freed
                )
                metrics.retention_pruned.add(scratch, {"tasque.retention.kind": "scratch"})
        except Exception:  # noqa: BLE001
            logger.exception("Scratch retention failed")
        try:
            reminders = ReminderService(session).prune()
            if reminders:
                logger.info("Reminder retention: removed %s past reminders", reminders)
                metrics.retention_pruned.add(reminders, {"tasque.retention.kind": "reminder"})
        except Exception:  # noqa: BLE001
            logger.exception("Reminder retention failed")
        return artifacts, scratch

    def _expire_memories(self, session: Session, settings) -> int:
        interval = settings.memory_ttl_interval_seconds
        if interval <= 0 or not self._ttl_gate.claim(utc_now(), interval):
            return 0
        try:
            expired = expire_ttl_memories(session)
        except Exception:  # noqa: BLE001
            logger.exception("Memory TTL expiry failed")
            return 0
        if expired:
            logger.info("Memory TTL expiry: archived %s memories", expired)
            instruments().retention_pruned.add(expired, {"tasque.retention.kind": "memory"})
        return expired
