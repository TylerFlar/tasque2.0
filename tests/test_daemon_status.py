from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import select

from tasque2.daemon import TasqueDaemon
from tasque2.db import session_scope
from tasque2.models import WorkAttempt, WorkItem
from tasque2.queue import WorkQueue
from tasque2.repo import WorkRepository
from tasque2.scheduler import ScheduleService
from tasque2.status import get_system_status


def test_daemon_once_polls_schedule_and_runs_work(fresh_db: Path) -> None:
    now = datetime.now(UTC)
    with session_scope() as session:
        ScheduleService(session).create_schedule(
            name="Daemon one-shot",
            schedule_type="date",
            expression=(now - timedelta(seconds=1)).isoformat(),
            worker_kind="function.echo",
            payload={"task_instruction": "Daemon should run this."},
            timezone_name="UTC",
        )

        result = TasqueDaemon(session).run_once(max_work_items=5)

        assert result.scheduled_work == 1
        assert result.work_items_ran == 1
        assert result.has_activity
        work = session.scalar(select(WorkItem).where(WorkItem.title == "Daemon one-shot"))
        assert work is not None
        assert work.status == "succeeded"


def test_daemon_idle_result_has_no_activity(fresh_db: Path) -> None:
    with session_scope() as session:
        result = TasqueDaemon(session).run_once(max_work_items=5)

        assert not result.has_activity


def test_daemon_once_recovers_expired_leases_without_running_when_limited(fresh_db: Path) -> None:
    now = datetime.now(UTC)
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Recover me",
            task_instruction="Lease expires.",
            worker_kind="function.echo",
            max_attempts=2,
        )
        claimed = WorkQueue(session).claim_next_ready_work(
            lease_owner="lost",
            lease_seconds=1,
            now=now - timedelta(minutes=10),
        )
        assert claimed is not None

        result = TasqueDaemon(session).run_once(max_work_items=0)

        assert result.recovered_leases == 1
        assert session.get(WorkItem, work.id).status == "ready"
        assert session.get(WorkAttempt, claimed.attempt.id).status == "expired"


def test_daemon_once_recovers_orphaned_daemon_attempts_without_timeout(
    fresh_db: Path,
) -> None:
    now = datetime.now(UTC)
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Recover daemon orphan",
            task_instruction="Daemon restarted.",
            worker_kind="function.echo",
            max_attempts=1,
        )
        claimed = WorkQueue(session).claim_next_ready_work(
            lease_owner="daemon",
            now=now - timedelta(minutes=10),
        )
        assert claimed is not None

        result = TasqueDaemon(session).run_once(
            max_work_items=0,
            recover_orphaned_lease_owner="daemon",
            orphaned_before=now,
        )

        assert result.recovered_orphans == 1
        assert session.get(WorkItem, work.id).status == "ready"
        assert session.get(WorkAttempt, claimed.attempt.id).status == "orphaned"


def test_system_status_counts_core_entities(fresh_db: Path) -> None:
    with session_scope() as session:
        WorkRepository(session).create_work_item(
            title="Status",
            task_instruction="Count me.",
            worker_kind="manual",
        )

        snapshot = get_system_status(session)

        assert snapshot.work_items["ready"] == 1
        assert snapshot.failed_work_unresolved == 0
