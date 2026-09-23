"""Worker threads that claim and run work items.

SQLite allows one writer, so the claim itself (the only step where two workers would
contend) is serialized and committed before the lock is released; the long provider run
happens outside the lock in each worker's own session.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor

from tasque2.db import session_scope
from tasque2.work.runner import WorkRunner

logger = logging.getLogger(__name__)

NO_WORK = object()


def claim_and_run(
    *,
    claim_lock: threading.Lock,
    lease_owner: str,
    lease_seconds: int | None,
    holder: dict[str, str | None] | None = None,
) -> object:
    """Claim one ready item and run it; returns ``NO_WORK`` when nothing was claimable."""
    with session_scope() as session:
        runner = WorkRunner(session, lease_owner=lease_owner, lease_seconds=lease_seconds)
        with claim_lock:
            claimed = runner.claim()
            if claimed is None:
                return NO_WORK
            session.commit()
        if holder is not None:
            holder["attempt_id"] = claimed.attempt.id
        return runner.execute(claimed)


def drain_synchronously(*, max_items: int, concurrency: int, lease_owner: str = "daemon") -> int:
    """Run up to ``max_items`` ready items to completion on ``concurrency`` threads."""
    claim_lock = threading.Lock()
    counter_lock = threading.Lock()
    budget = {"left": max_items}

    def worker(_index: int) -> int:
        ran = 0
        while True:
            with counter_lock:
                if budget["left"] <= 0:
                    return ran
                budget["left"] -= 1
            try:
                outcome = claim_and_run(claim_lock=claim_lock, lease_owner=lease_owner, lease_seconds=None)
            except Exception:  # noqa: BLE001 - isolate one worker from the others
                logger.exception("Worker thread failed")
                outcome = NO_WORK
            if outcome is NO_WORK:
                with counter_lock:
                    budget["left"] += 1
                return ran
            ran += 1

    workers = max(1, int(concurrency))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return sum(executor.map(worker, range(workers)))


class WorkPool:
    """A long-lived pool: each tick reaps finished runs and tops the pool back up."""

    def __init__(self, concurrency: int) -> None:
        self.concurrency = max(1, int(concurrency))
        self._executor = ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="tasque-work")
        self._claim_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._in_flight: dict[Future[object], dict[str, str | None]] = {}

    def in_flight_count(self) -> int:
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
        """Drop finished futures; returns how many of them actually ran work."""
        with self._state_lock:
            done = [future for future in self._in_flight if future.done()]
            for future in done:
                del self._in_flight[future]
        ran = 0
        for future in done:
            try:
                if future.result() is not NO_WORK:
                    ran += 1
            except Exception:  # noqa: BLE001 - one bad run must not break the pool
                logger.exception("Worker thread failed")
        return ran

    def dispatch(self, *, limit: int, lease_owner: str, lease_seconds: int | None) -> int:
        slots = max(0, min(int(limit), self.concurrency - self.in_flight_count()))
        for _ in range(slots):
            holder: dict[str, str | None] = {"attempt_id": None}
            future = self._executor.submit(
                claim_and_run,
                claim_lock=self._claim_lock,
                lease_owner=lease_owner,
                lease_seconds=lease_seconds,
                holder=holder,
            )
            with self._state_lock:
                self._in_flight[future] = holder
        return slots

    def shutdown(self, *, wait: bool = False) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=True)
