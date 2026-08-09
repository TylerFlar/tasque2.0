from __future__ import annotations

import time
from pathlib import Path

from sqlalchemy import select

from tasque2.daemon import TasqueDaemon, background_pool_is_idle
from tasque2.db import session_scope
from tasque2.models import WorkAttempt, WorkItem
from tasque2.repo import WorkRepository


def _make_ready_items(session, count: int) -> list[str]:
    repo = WorkRepository(session)
    ids = [
        repo.create_work_item(
            title=f"echo-{i}",
            task_instruction=f"echo {i}",
            worker_kind="function.echo",
        ).id
        for i in range(count)
    ]
    session.flush()
    return ids


def test_run_once_concurrent_runs_every_ready_item_exactly_once(fresh_db: Path) -> None:
    with session_scope() as session:
        ids = _make_ready_items(session, 8)

        result = TasqueDaemon(session).run_once(max_work_items=20, concurrency=4)

        assert result.work_items_ran == 8

        # Drop the stale snapshot so we read what the worker sessions committed.
        session.expire_all()
        for work_item_id in ids:
            work_item = session.get(WorkItem, work_item_id)
            assert work_item is not None
            assert work_item.status == "succeeded"
            attempts = session.scalars(
                select(WorkAttempt).where(WorkAttempt.work_item_id == work_item_id)
            ).all()
            # Exactly one attempt per item proves no two workers claimed the same one.
            assert len(attempts) == 1
            assert attempts[0].status == "succeeded"


def test_run_once_concurrent_respects_max_work_items_budget(fresh_db: Path) -> None:
    with session_scope() as session:
        _make_ready_items(session, 10)

        result = TasqueDaemon(session).run_once(max_work_items=4, concurrency=4)

        assert result.work_items_ran == 4

        session.expire_all()
        succeeded = session.scalars(
            select(WorkItem).where(WorkItem.status == "succeeded")
        ).all()
        ready = session.scalars(
            select(WorkItem).where(WorkItem.status == "ready")
        ).all()
        assert len(succeeded) == 4
        assert len(ready) == 6


def test_run_once_concurrent_idle_tick_is_noop(fresh_db: Path) -> None:
    with session_scope() as session:
        result = TasqueDaemon(session).run_once(max_work_items=10, concurrency=4)
        assert result.work_items_ran == 0
        assert not result.has_activity


def _drain_background_pool(timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while not background_pool_is_idle():
        if time.monotonic() > deadline:
            raise AssertionError("background work pool did not drain")
        time.sleep(0.02)


def test_non_blocking_tick_dispatches_without_waiting_for_the_work(fresh_db: Path) -> None:
    # The service loop must not block on provider runs: on 2026-08-03 two
    # 30-minute career applies executing inside one tick froze schedule
    # polling for the whole afternoon. wait=False dispatches and returns.
    with session_scope() as session:
        _make_ready_items(session, 4)

    with session_scope() as session:
        dispatch = TasqueDaemon(session).run_once(
            max_work_items=10,
            concurrency=2,
            wait=False,
        )
    # Nothing is reaped on the dispatching tick -- it did not wait for results.
    assert dispatch.work_items_ran == 0

    _drain_background_pool()

    with session_scope() as session:
        reaped = TasqueDaemon(session).run_once(
            max_work_items=10,
            concurrency=2,
            wait=False,
        )
        assert reaped.work_items_ran > 0

    _drain_background_pool()
    with session_scope() as session:
        TasqueDaemon(session).run_once(max_work_items=10, concurrency=2, wait=False)

    _drain_background_pool()
    with session_scope() as session:
        TasqueDaemon(session).run_once(max_work_items=10, concurrency=2, wait=False)
        session.expire_all()
        remaining = session.scalars(
            select(WorkItem).where(WorkItem.status.in_(("ready", "running")))
        ).all()
        assert remaining == []


def test_non_blocking_tick_keeps_leases_fresh_for_in_flight_work(fresh_db: Path) -> None:
    # In-flight attempts must keep heartbeating, or lease recovery would treat
    # live work as the leavings of a dead daemon and run it a second time.
    with session_scope() as session:
        _make_ready_items(session, 2)

    with session_scope() as session:
        TasqueDaemon(session).run_once(max_work_items=10, concurrency=2, wait=False)

    _drain_background_pool()

    with session_scope() as session:
        TasqueDaemon(session).run_once(max_work_items=10, concurrency=2, wait=False)
        session.expire_all()
        attempts = session.scalars(select(WorkAttempt)).all()
        assert attempts
        for attempt in attempts:
            # Every attempt carried a lease while it ran.
            assert attempt.heartbeat_at is not None
