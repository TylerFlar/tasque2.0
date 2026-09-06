from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.orm import Session

from tasque2.artifacts import prune_artifacts
from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.memory import expire_ttl_memories
from tasque2.memory_ingest import MemoryIngestService
from tasque2.models import utc_now
from tasque2.queue import WorkQueue
from tasque2.runtime import WorkRunner
from tasque2.scheduler import ScheduleService
from tasque2.workflows import WorkflowService

# Sentinel returned by the concurrent worker when nothing was claimable.
_NO_WORK = object()


class _IntervalGate:
    """Process-local "is this pass due yet" clock for tick-time bookkeeping.

    Retention-style sweeps are bookkeeping, not work: running them on every tick
    would rescan the store every few seconds for nothing. Process-local like the
    limit gate -- a restarted daemon simply runs each pass once on its first tick.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_at: datetime | None = None

    def claim(self, now: datetime, interval_seconds: int) -> bool:
        """True when the pass is due, reserving the slot if so."""
        with self._lock:
            if self._last_at is not None and (now - self._last_at).total_seconds() < interval_seconds:
                return False
            self._last_at = now
            return True

    def reset(self) -> None:
        with self._lock:
            self._last_at = None


_artifact_retention_gate = _IntervalGate()
_memory_ttl_gate = _IntervalGate()


def reset_bookkeeping_clocks() -> None:
    """Forget when the bookkeeping passes last ran (used by tests)."""
    _artifact_retention_gate.reset()
    _memory_ttl_gate.reset()


def _claim_and_run_one(
    *,
    claim_lock: threading.Lock,
    lease_owner: str,
    lease_seconds: int | None,
    holder: dict[str, str | None] | None = None,
) -> object:
    """Claim the next ready item under ``claim_lock`` and run it in its own session.

    SQLite allows a single writer, so the *claim* step (the only place two
    threads would fight for the write lock) is serialized and committed before
    the lock is released -- no two workers can claim the same item, and no write
    lock is held across a subprocess (``ProviderRuntime.run`` commits before it
    spawns one). ``holder`` receives the attempt id once claimed so a caller can
    keep the lease fresh while the run is in flight. Returns ``_NO_WORK`` when
    nothing was claimable.
    """
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
            # Publish the claim (status=running) before releasing the lock so a
            # sibling worker can't re-claim the same item against a stale
            # pre-commit snapshot.
            session.commit()
        if holder is not None:
            holder["attempt_id"] = claimed.attempt.id
        return runner.execute(claimed)


def _run_work_concurrently(
    *,
    max_work_items: int,
    concurrency: int,
    lease_owner: str = "daemon",
    lease_seconds: int | None = None,
) -> int:
    """Drain up to ``max_work_items`` ready work items using ``concurrency`` threads.

    Each worker thread runs in its own ``session_scope`` so the long provider
    subprocesses execute in parallel; see ``_claim_and_run_one`` for why the
    claim itself is serialized.
    """
    claim_lock = threading.Lock()
    counter_lock = threading.Lock()
    counter = {"ran": 0}

    def worker(index: int) -> int:
        ran = 0
        while True:
            with counter_lock:
                if counter["ran"] >= max_work_items:
                    break
                counter["ran"] += 1  # reserve a slot before claiming
            try:
                outcome: object = _claim_and_run_one(
                    claim_lock=claim_lock,
                    lease_owner=lease_owner,
                    lease_seconds=lease_seconds,
                )
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


class _BackgroundWorkPool:
    """Long-lived worker pool that runs claims without blocking the tick.

    ``_run_work_concurrently`` drains synchronously: the tick does not return
    until every claimed item finishes. That is fine for one-shot CLI runs, but
    in the service loop it means a 30-minute provider run also freezes schedule
    polling, workflow ticks and lease recovery for 30 minutes -- on 2026-08-03
    every schedule sat at the same ``last_evaluated_at`` for the whole afternoon
    because two career applies were still executing inside one tick.

    Here the executor outlives the tick: each tick reaps whatever finished,
    refreshes leases for what is still in flight, tops the pool back up to
    ``concurrency``, and returns immediately.
    """

    def __init__(self, concurrency: int) -> None:
        self.concurrency = max(1, int(concurrency))
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency,
            thread_name_prefix="tasque-work",
        )
        self._claim_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._in_flight: dict[Future[object], dict[str, str | None]] = {}

    def in_flight_count(self) -> int:
        """Count work still executing, ignoring finished futures awaiting reap."""
        with self._state_lock:
            return sum(1 for future in self._in_flight if not future.done())

    def in_flight_attempt_ids(self) -> list[str]:
        with self._state_lock:
            return [
                attempt_id
                for future, holder in self._in_flight.items()
                if not future.done() and (attempt_id := holder.get("attempt_id")) is not None
            ]

    def reap(self) -> int:
        """Drop finished futures and return how many actually ran work."""
        with self._state_lock:
            done = [future for future in self._in_flight if future.done()]
            for future in done:
                del self._in_flight[future]
        ran = 0
        for future in done:
            try:
                if future.result() is not _NO_WORK:
                    ran += 1
            except Exception as exc:  # noqa: BLE001 - isolate the pool from one bad run
                print(f"Tasque daemon worker failed: {exc}")
        return ran

    def dispatch(self, *, limit: int, lease_owner: str, lease_seconds: int | None) -> int:
        """Submit up to ``limit`` new claims, respecting the concurrency cap."""
        capacity = self.concurrency - self.in_flight_count()
        slots = max(0, min(int(limit), capacity))
        for _ in range(slots):
            holder: dict[str, str | None] = {"attempt_id": None}
            future = self._executor.submit(
                self._claim_and_run,
                holder,
                lease_owner,
                lease_seconds,
            )
            with self._state_lock:
                self._in_flight[future] = holder
        return slots

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _claim_and_run(
        self,
        holder: dict[str, str | None],
        lease_owner: str,
        lease_seconds: int | None,
    ) -> object:
        return _claim_and_run_one(
            claim_lock=self._claim_lock,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
            holder=holder,
        )


_background_pool: _BackgroundWorkPool | None = None
_background_pool_lock = threading.Lock()


def _get_background_pool(concurrency: int) -> _BackgroundWorkPool:
    global _background_pool
    with _background_pool_lock:
        if _background_pool is None or _background_pool.concurrency != max(1, int(concurrency)):
            _background_pool = _BackgroundWorkPool(concurrency)
        return _background_pool


def reset_background_pool() -> None:
    """Drop the shared pool (used by tests; a real daemon keeps one for its life)."""
    global _background_pool
    with _background_pool_lock:
        pool = _background_pool
        _background_pool = None
    if pool is not None:
        pool.shutdown()


def background_pool_is_idle() -> bool:
    """True when no dispatched work is still running."""
    with _background_pool_lock:
        pool = _background_pool
    return pool is None or pool.in_flight_count() == 0


@dataclass(frozen=True)
class DaemonTickResult:
    recovered_leases: int
    recovered_orphans: int
    scheduled_work: int
    workflow_runs_changed: int
    work_items_ran: int
    memory_ingested: int = 0
    expired_overdue: int = 0
    recovered_results: int = 0
    artifacts_pruned: int = 0
    memories_expired: int = 0

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
                self.recovered_results,
                self.artifacts_pruned,
                self.memories_expired,
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
        wait: bool = True,
    ) -> DaemonTickResult:
        """Run one daemon tick.

        ``wait=True`` (the default, used by ``daemon-once`` and tests) drains
        claimed work synchronously. The service loop passes ``wait=False`` so a
        long provider run cannot stall schedule polling and workflow ticks.
        """
        if concurrency is None:
            concurrency = get_settings().daemon_concurrency
        concurrency = max(1, int(concurrency))
        settings = get_settings()

        queue = WorkQueue(self.session)
        scheduler = ScheduleService(self.session)
        workflows = WorkflowService(self.session)

        pool = None if wait else _get_background_pool(concurrency)
        lease_seconds = settings.daemon_lease_seconds if pool is not None else None
        if pool is not None:
            # Refresh leases before recovery so in-flight work is never mistaken
            # for the leavings of a dead daemon.
            queue.heartbeat_running_attempts(
                pool.in_flight_attempt_ids(),
                lease_seconds=lease_seconds,
            )

        recovered_results = 0
        if recover_orphaned_lease_owner is not None:
            # Before requeueing anything, adopt results that a subprocess
            # deposited after its parent daemon died -- re-running those would
            # redo already-completed work (and resubmit real applications).
            recovered_results = WorkRunner(
                self.session,
                lease_owner=recover_orphaned_lease_owner,
            ).recover_deposited_results(orphaned_before=orphaned_before or utc_now())

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

        if pool is not None:
            self.session.commit()
            ran = pool.reap()
            pool.dispatch(
                limit=max_work_items,
                lease_owner="daemon",
                lease_seconds=lease_seconds,
            )
            self.session.expire_all()
        elif concurrency > 1:
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
        artifacts_pruned = self._prune_artifacts_if_due(settings)
        memories_expired = self._expire_memory_ttl_if_due(settings)
        self.session.flush()
        return DaemonTickResult(
            recovered_leases=recovered,
            recovered_orphans=recovered_orphans,
            scheduled_work=scheduled,
            workflow_runs_changed=workflow_changes,
            work_items_ran=ran,
            memory_ingested=memory_ingested,
            expired_overdue=expired_overdue,
            recovered_results=recovered_results,
            artifacts_pruned=artifacts_pruned,
            memories_expired=memories_expired,
        )

    def _expire_memory_ttl_if_due(self, settings) -> int:
        """Archive TTL-expired memories when the pass's interval has elapsed."""
        interval = settings.memory_ttl_interval_seconds
        if interval <= 0:
            return 0
        if not _memory_ttl_gate.claim(utc_now(), interval):
            return 0
        try:
            expired = expire_ttl_memories(self.session)
        except Exception as exc:  # noqa: BLE001 - bookkeeping must never break a tick
            print(f"Tasque memory TTL expiry failed: {exc}")
            return 0
        if expired:
            print(f"Tasque memory TTL expiry: archived {expired} memories")
        return expired

    def _prune_artifacts_if_due(self, settings) -> int:
        """Run the artifact retention pass when its interval has elapsed."""
        if settings.artifact_retention_days <= 0:
            return 0
        if not _artifact_retention_gate.claim(utc_now(), settings.artifact_retention_interval_seconds):
            return 0
        try:
            result = prune_artifacts(self.session)
        except Exception as exc:  # noqa: BLE001 - retention must never break a tick
            print(f"Tasque artifact retention failed: {exc}")
            return 0
        if result.pruned:
            print(
                f"Tasque artifact retention: pruned {result.pruned} artifacts "
                f"({result.megabytes_freed:.1f} MB)"
            )
        return result.pruned
