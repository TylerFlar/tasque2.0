from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.artifacts import ArtifactStore
from tasque2.config import reset_settings
from tasque2.daemon import Daemon, DaemonTick, IntervalGate, TickResult, WorkPool, control, drain_synchronously
from tasque2.daemon.tick import LEASE_OWNER
from tasque2.db import session_scope
from tasque2.models import AgentResult, ProviderRun, WorkAttempt, WorkItem, utc_now
from tasque2.ops.status import get_system_status
from tasque2.schedules import ScheduleService
from tasque2.work import runner
from tasque2.work.queue import WorkQueue
from tasque2.work.repository import WorkRepository
from tasque2.worker import results


def _configure(monkeypatch: pytest.MonkeyPatch, **env: str) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    reset_settings()


def _use_function_worker(monkeypatch: pytest.MonkeyPatch, worker_kind: str, function: Callable) -> None:
    """Make every runner the daemon builds know one extra function worker."""
    default = runner.default_function_registry

    def registry() -> runner.FunctionWorkerRegistry:
        workers = default()
        workers.register(worker_kind, function)
        return workers

    monkeypatch.setattr(runner, "default_function_registry", registry)


def _blocking_worker(started: threading.Event, release: threading.Event) -> Callable:
    def block(_work_item: WorkItem) -> str:
        started.set()
        release.wait(timeout=30)
        return "Released."

    return block


def _echo_items(session, count: int, **fields) -> list[str]:
    repo = WorkRepository(session)
    return [
        repo.create_work_item(
            title=f"echo-{index}", task_instruction=f"echo {index}", worker_kind="function.echo", **fields
        ).id
        for index in range(count)
    ]


def _wait_for(condition: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition was not met in time")
        time.sleep(0.01)


async def _until(condition: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0.01)


def _write_foreign_state(*, pid: int, last_tick_at: datetime) -> None:
    path = control.state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "pid": pid,
                "started_at": (last_tick_at - timedelta(hours=1)).isoformat(),
                "last_tick_at": last_tick_at.isoformat(),
                "in_flight_attempt_ids": [],
                "draining": False,
                "version": "0.0.0",
            }
        ),
        encoding="utf-8",
    )


def test_state_file_round_trips(isolated: Path) -> None:
    started_at = utc_now() - timedelta(minutes=3)

    control.write_state(started_at=started_at, in_flight_attempt_ids=["a", "b"], draining=True, version="9.9.9")
    state = control.read_state()

    assert control.state_path() == isolated / "data" / "daemon.state.json"
    assert state.pid == os.getpid()
    assert state.started_at == started_at
    assert state.in_flight == 2
    assert state.draining is True
    assert state.version == "9.9.9"
    assert state.is_fresh()

    control.clear_state()
    assert control.read_state() is None
    control.clear_state()


def test_read_state_is_none_for_a_missing_or_corrupt_file(isolated: Path) -> None:
    assert control.read_state() is None
    control.state_path().parent.mkdir(parents=True)
    control.state_path().write_text("{not json", encoding="utf-8")

    assert control.read_state() is None


def test_state_freshness_follows_the_stale_window(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, TASQUE2_DAEMON_STALE_SECONDS="60")
    now = utc_now()
    _write_foreign_state(pid=1, last_tick_at=now - timedelta(seconds=30))
    state = control.read_state()

    assert state.is_fresh(now)
    assert not state.is_fresh(now + timedelta(seconds=31))
    assert not control.DaemonState(None, None, None, 0, False, None).is_fresh(now)


def test_drain_flag_file(isolated: Path) -> None:
    assert not control.drain_requested()

    path = control.request_drain()

    assert path == control.drain_path()
    assert control.drain_requested()
    control.clear_drain()
    assert not control.drain_requested()
    control.clear_drain()


def test_live_daemon_reason_is_none_without_signs_of_life(fresh_db: Path) -> None:
    assert control.live_daemon_reason() is None


def test_live_daemon_reason_reports_a_fresh_state_from_another_process(fresh_db: Path) -> None:
    other_pid = os.getpid() + 1
    _write_foreign_state(pid=other_pid, last_tick_at=utc_now() - timedelta(seconds=5))

    reason = control.live_daemon_reason()

    assert reason is not None
    assert f"pid {other_pid}" in reason


def test_live_daemon_reason_ignores_this_process_and_stale_state(fresh_db: Path) -> None:
    control.write_state(started_at=utc_now(), in_flight_attempt_ids=[], draining=False, version="test")
    assert control.live_daemon_reason() is None

    _write_foreign_state(pid=os.getpid() + 1, last_tick_at=utc_now() - timedelta(hours=1))
    assert control.live_daemon_reason() is None


def test_recently_evaluated_schedules_alone_are_not_a_live_daemon(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        service = ScheduleService(session)
        enabled = service.create_schedule(
            name="Hourly", schedule_type="interval", expression="hours=1", worker_kind="manual", payload={}
        )
        disabled = service.create_schedule(
            name="Off", schedule_type="interval", expression="hours=1", worker_kind="manual", payload={}, enabled=False
        )
        enabled.last_evaluated_at = now - timedelta(seconds=5)
        disabled.last_evaluated_at = now - timedelta(seconds=1)
        session.flush()

        assert control.latest_schedule_tick(session) == now - timedelta(seconds=5)
        assert control.live_daemon_reason(now=now) is None


def test_tick_fires_due_schedules_and_runs_their_work(fresh_db: Path) -> None:
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

        result = DaemonTick().run(session, max_claims=5)

        assert (result.scheduled, result.finished, result.dispatched) == (1, 1, 0)
        assert result.has_activity
        work = session.scalar(select(WorkItem).where(WorkItem.title == "Daemon one-shot"))
        assert work.status == "succeeded"
        assert work.lane == "Daemon one-shot"


def test_idle_tick_has_no_activity(fresh_db: Path) -> None:
    with session_scope() as session:
        result = DaemonTick().run(session, max_claims=5)

    assert not result.has_activity
    assert result.describe() == ""


def test_tick_result_describes_only_its_activity() -> None:
    result = TickResult(scheduled=2, finished=1)

    assert result.has_activity
    assert result.describe() == "scheduled=2, finished=1"


def test_tick_recovers_expired_leases_without_claiming(fresh_db: Path) -> None:
    now = datetime.now(UTC)
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Recover me", task_instruction="Lease expires.", worker_kind="function.echo", max_attempts=2
        )
        claimed = WorkQueue(session).claim_next_ready_work(
            lease_owner="lost", lease_seconds=1, now=now - timedelta(minutes=10)
        )

        result = DaemonTick().run(session, max_claims=0)

        assert (result.recovered_leases, result.finished) == (1, 0)
        assert session.get(WorkItem, work.id).status == "ready"
        assert session.get(WorkAttempt, claimed.attempt.id).status == "expired"


def test_first_tick_recovers_orphaned_attempts_once(fresh_db: Path) -> None:
    now = datetime.now(UTC)
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Recover daemon orphan", task_instruction="Daemon restarted.", worker_kind="function.echo"
        )
        queue = WorkQueue(session)
        orphan = queue.claim_next_ready_work(lease_owner=LEASE_OWNER, now=now - timedelta(minutes=10))
        tick = DaemonTick(recover_before=now)

        first = tick.run(session, max_claims=0)

        assert first.recovered_orphans == 1
        assert session.get(WorkItem, work.id).status == "ready"
        assert session.get(WorkAttempt, orphan.attempt.id).status == "orphaned"

        second_orphan = queue.claim_next_ready_work(lease_owner=LEASE_OWNER, now=now - timedelta(minutes=5))
        assert tick.run(session, max_claims=0).recovered_orphans == 0
        assert session.get(WorkAttempt, second_orphan.attempt.id).status == "running"


def test_first_tick_adopts_results_deposited_after_the_previous_daemon_died(fresh_db: Path) -> None:
    now = datetime.now(UTC)
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Apply", task_instruction="Apply.", worker_kind="provider.default"
        )
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner=LEASE_OWNER, now=now - timedelta(minutes=10))
        provider_run = ProviderRun(attempt_id=claimed.attempt.id, provider="claude", status="running")
        session.add(provider_run)
        session.flush()
        claimed.attempt.provider_run_id = provider_run.id
        work_id = work.id
    results.deposit(
        result_token="late-token",
        payload={"summary": "Applied", "report": "", "produces": {}, "work_item_id": work_id},
    )

    with session_scope() as session:
        result = DaemonTick(recover_before=now).run(session, max_claims=0)

        assert (result.recovered_results, result.recovered_orphans) == (1, 0)
        assert session.get(WorkItem, work_id).status == "succeeded"


def test_tick_without_claim_runs_nothing(fresh_db: Path) -> None:
    with session_scope() as session:
        ids = _echo_items(session, 2)

        result = DaemonTick().run(session, claim=False)

        assert result.finished == 0
        assert {session.get(WorkItem, work_id).status for work_id in ids} == {"ready"}


def test_tick_runs_every_ready_item_exactly_once_across_threads(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, TASQUE2_DAEMON_CONCURRENCY="4")
    with session_scope() as session:
        ids = _echo_items(session, 8)

    with session_scope() as session:
        result = DaemonTick().run(session, max_claims=20)

    assert result.finished == 8
    with session_scope() as session:
        for work_id in ids:
            attempts = session.scalars(select(WorkAttempt).where(WorkAttempt.work_item_id == work_id)).all()
            assert [attempt.status for attempt in attempts] == ["succeeded"]


def test_drain_synchronously_respects_the_item_budget(fresh_db: Path) -> None:
    with session_scope() as session:
        _echo_items(session, 10)

    assert drain_synchronously(max_items=4, concurrency=4) == 4

    with session_scope() as session:
        statuses = session.scalars(select(WorkItem.status)).all()
    assert sorted(statuses) == ["ready"] * 6 + ["succeeded"] * 4


def test_drain_synchronously_idle_queue_runs_nothing(fresh_db: Path) -> None:
    assert drain_synchronously(max_items=10, concurrency=4) == 0


def test_tick_reaps_stale_results_in_the_pass_that_prunes_artifacts(fresh_db: Path) -> None:
    with session_scope() as session:
        artifact = ArtifactStore().write_text(session, kind="provider_stream", title="old stream", content="x")
        artifact.created_at = utc_now() - timedelta(days=45)
    results.deposit(result_token="stale", payload={"work_item_id": "gone"})
    results.deposit(result_token="fresh", payload={"work_item_id": "running"})
    with session_scope() as session:
        session.get(AgentResult, "stale").created_at = utc_now() - timedelta(days=2)

    with session_scope() as session:
        result = DaemonTick().run(session, max_claims=0)

    assert result.artifacts_pruned == 1
    with session_scope() as session:
        assert session.get(AgentResult, "stale") is None
        assert session.get(AgentResult, "fresh") is not None


def test_interval_gate_allows_one_pass_per_interval() -> None:
    gate = IntervalGate()
    now = utc_now()

    assert gate.claim(now, 60)
    assert not gate.claim(now + timedelta(seconds=59), 60)
    assert gate.claim(now + timedelta(seconds=60), 60)
    gate.reset()
    assert gate.claim(now, 60)


def test_pool_tick_dispatches_without_waiting_for_the_work(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    release = threading.Event()
    started: list[str] = []

    def block(work_item: WorkItem) -> str:
        started.append(work_item.id)
        release.wait(timeout=30)
        return "Released."

    _use_function_worker(monkeypatch, "function.block", block)
    with session_scope() as session:
        repo = WorkRepository(session)
        ids = [
            repo.create_work_item(title=f"block-{index}", task_instruction="Block.", worker_kind="function.block").id
            for index in range(3)
        ]

    pool = WorkPool(2)
    tick = DaemonTick(pool=pool)
    try:
        with session_scope() as session:
            first = tick.run(session)
        assert (first.dispatched, first.finished) == (2, 0)
        _wait_for(lambda: len(started) == 2)

        with session_scope() as session:
            busy = tick.run(session)
        assert (busy.dispatched, busy.finished) == (0, 0)
        assert len(pool.in_flight_attempt_ids()) == 2

        release.set()
        _wait_for(lambda: pool.in_flight_count() == 0)
        with session_scope() as session:
            refill = tick.run(session)
        assert (refill.dispatched, refill.finished) == (1, 2)

        _wait_for(lambda: pool.in_flight_count() == 0)
        with session_scope() as session:
            last = tick.run(session)
        assert last.finished == 1
    finally:
        release.set()
        pool.shutdown(wait=True)

    with session_scope() as session:
        assert {session.get(WorkItem, work_id).status for work_id in ids} == {"succeeded"}


def test_pool_tick_keeps_leases_of_in_flight_work_fresh(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    started, release = threading.Event(), threading.Event()
    _use_function_worker(monkeypatch, "function.block", _blocking_worker(started, release))
    with session_scope() as session:
        work_id = (
            WorkRepository(session)
            .create_work_item(title="Long run", task_instruction="Block.", worker_kind="function.block")
            .id
        )

    pool = WorkPool(1)
    tick = DaemonTick(pool=pool)
    try:
        with session_scope() as session:
            assert tick.run(session).dispatched == 1
        assert started.wait(timeout=10)
        with session_scope() as session:
            attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work_id))
            assert attempt.lease_owner == LEASE_OWNER
            attempt.lease_expires_at = utc_now() - timedelta(minutes=1)
            attempt_id = attempt.id

        with session_scope() as session:
            result = tick.run(session)
            attempt = session.get(WorkAttempt, attempt_id)

            assert result.recovered_leases == 0
            assert attempt.status == "running"
            assert attempt.lease_expires_at > utc_now() + timedelta(seconds=500)
    finally:
        release.set()
        pool.shutdown(wait=True)

    with session_scope() as session:
        assert session.get(WorkItem, work_id).status == "succeeded"


def test_stop_request_drains_in_flight_work_then_clears_the_state_file(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, TASQUE2_DAEMON_TICK_SECONDS="0.05")
    started, release = threading.Event(), threading.Event()
    _use_function_worker(monkeypatch, "function.block", _blocking_worker(started, release))
    with session_scope() as session:
        repo = WorkRepository(session)
        in_flight_id = repo.create_work_item(
            title="In flight", task_instruction="Block.", worker_kind="function.block", priority=10
        ).id
        queued_id = repo.create_work_item(title="Queued", task_instruction="Wait.", worker_kind="function.echo").id

    daemon = Daemon(discord=False)

    def state_is(*, in_flight: int, draining: bool) -> bool:
        state = control.read_state()
        return state is not None and (state.in_flight, state.draining) == (in_flight, draining)

    async def scenario() -> None:
        task = asyncio.create_task(daemon.run())
        try:
            await _until(started.is_set)
            await _until(lambda: state_is(in_flight=1, draining=False))
            daemon.request_stop()
            await _until(lambda: state_is(in_flight=1, draining=True))
            assert not task.done()
            release.set()
            await asyncio.wait_for(task, timeout=10)
        finally:
            release.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())

    assert not control.state_path().exists()
    assert daemon.pool.in_flight_count() == 0
    with session_scope() as session:
        assert session.get(WorkItem, in_flight_id).status == "succeeded"
        queued = session.get(WorkItem, queued_id)
        assert (queued.status, queued.attempt_count) == ("ready", 0)


def test_drain_file_stops_an_idle_daemon(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, TASQUE2_DAEMON_TICK_SECONDS="0.05")
    daemon = Daemon(discord=False)

    async def scenario() -> None:
        task = asyncio.create_task(daemon.run())
        try:
            await _until(lambda: control.read_state() is not None)
            control.request_drain()
            await asyncio.wait_for(task, timeout=10)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())

    assert not control.state_path().exists()
    assert not control.drain_requested()


def test_system_status_counts_work_by_status(fresh_db: Path) -> None:
    with session_scope() as session:
        WorkRepository(session).create_work_item(title="Status", task_instruction="Count me.", worker_kind="manual")

        snapshot = get_system_status(session)

    assert snapshot.work_items == {"ready": 1}
    assert snapshot.ready_work == 1
    assert snapshot.failed_work_unresolved == 0


def test_idle_pool_tick_dispatches_nothing(fresh_db: Path) -> None:
    pool = WorkPool(2)
    try:
        with session_scope() as session:
            result = DaemonTick(pool=pool).run(session)
        assert result.dispatched == 0
        assert not result.has_activity
        assert pool.in_flight_count() == 0
    finally:
        pool.shutdown(wait=True)
