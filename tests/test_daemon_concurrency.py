from __future__ import annotations

from pathlib import Path

from sqlalchemy import select

from tasque2.daemon import TasqueDaemon
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
