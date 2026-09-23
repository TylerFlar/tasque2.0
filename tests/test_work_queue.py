from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.models import FailedWork, ProviderRun, WorkAttempt, WorkDependency, WorkEvent, WorkItem, utc_now
from tasque2.telemetry import span
from tasque2.work.queue import WorkQueue
from tasque2.work.repository import WorkRepository
from tasque2.work.retry import (
    CAPACITY_GATE_MAX_SECONDS,
    LIMIT_RETRY_BUFFER_SECONDS,
    LIMIT_RETRY_FALLBACK_SECONDS,
    LIMIT_RETRY_FLOOR,
    TRANSIENT_RETRY_DELAY_SECONDS,
    TRANSIENT_RETRY_FLOOR,
    capacity_gate,
    decide_retry,
    limit_retry_delay_seconds,
)
from tasque2.work.runner import WorkRunner

SESSION_LIMIT = "You've hit your session limit · resets 11:40am (America/Los_Angeles)"


def _create(session, title: str = "Work", **fields) -> WorkItem:
    fields.setdefault("task_instruction", f"Do {title}.")
    fields.setdefault("worker_kind", "manual")
    return WorkRepository(session).create_work_item(title=title, **fields)


def _events(session, work_item_id: str, event_type: str) -> list[WorkEvent]:
    return list(
        session.scalars(
            select(WorkEvent).where(WorkEvent.work_item_id == work_item_id, WorkEvent.event_type == event_type)
        ).all()
    )


def _counted(points, **attributes) -> int:
    return sum(
        point.value for point in points if all(point.attributes.get(key) == value for key, value in attributes.items())
    )


def test_an_item_claimed_by_another_process_during_a_claim_is_skipped(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with session_scope() as session:
        work_id = (
            WorkRepository(session)
            .create_work_item(title="Contended", task_instruction="Run once.", worker_kind="function.noop")
            .id
        )
    original = WorkQueue._has_unsatisfied_dependency
    competed = []

    def claim_elsewhere_first(self, work_item):
        if not competed:
            competed.append(True)
            with session_scope() as other:
                assert WorkQueue(other).claim_next_ready_work(lease_owner="other") is not None
        return original(self, work_item)

    monkeypatch.setattr(WorkQueue, "_has_unsatisfied_dependency", claim_elsewhere_first)
    with session_scope() as session:
        assert WorkQueue(session).claim_next_ready_work(lease_owner="first") is None

    with session_scope() as session:
        attempts = session.scalars(select(WorkAttempt).where(WorkAttempt.work_item_id == work_id)).all()
        assert [attempt.lease_owner for attempt in attempts] == ["other"]
        assert session.get(WorkItem, work_id).attempt_count == 1


def test_explicit_lane_wins_over_context_and_parent(fresh_db: Path) -> None:
    with session_scope() as session:
        parent = _create(session, "Parent", lane="parent-lane")
        child = _create(
            session,
            "Child",
            lane="explicit-lane",
            context={"lane": "context-lane", "parent_work_item_id": parent.id},
        )

        assert child.lane == "explicit-lane"


def test_lane_comes_from_the_context_before_the_parent(fresh_db: Path) -> None:
    with session_scope() as session:
        parent = _create(session, "Parent", lane="parent-lane")
        child = _create(session, "Child", context={"lane": "  context-lane  ", "parent_work_item_id": parent.id})

        assert child.lane == "context-lane"


def test_lane_is_inherited_from_the_parent_work_item(fresh_db: Path) -> None:
    with session_scope() as session:
        parent = _create(session, "Parent", lane="finance-daily")
        child = _create(session, "Follow-up", context={"lane": "   ", "parent_work_item_id": parent.id})
        grandchild = _create(session, "Reply", context={"parent_work_item_id": child.id})

        assert child.lane == "finance-daily"
        assert grandchild.lane == "finance-daily"


def test_lane_is_empty_without_a_source(fresh_db: Path) -> None:
    with session_scope() as session:
        orphan = _create(session, "Orphan", context={"parent_work_item_id": "no-such-work-item"})
        plain = _create(session, "Plain")

        assert orphan.lane is None
        assert plain.lane is None


def test_created_event_records_the_lane(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Laned", lane="kitchen")

        [created] = _events(session, work.id, "work.created")
        assert created.payload["lane"] == "kitchen"


def test_traceparent_defaults_to_the_active_span(fresh_db: Path) -> None:
    with span("enqueue") as current, session_scope() as session:
        traceparent = _create(session, "Traced").traceparent

    context = current.get_span_context()
    version, trace_id, span_id, _flags = traceparent.split("-")
    assert (version, trace_id, span_id) == ("00", f"{context.trace_id:032x}", f"{context.span_id:016x}")


def test_traceparent_is_empty_outside_a_trace(fresh_db: Path) -> None:
    with session_scope() as session:
        assert _create(session, "Untraced").traceparent is None


def test_explicit_traceparent_is_kept(fresh_db: Path) -> None:
    stored = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    with span("enqueue"), session_scope() as session:
        assert _create(session, "Linked", traceparent=stored).traceparent == stored


def test_claim_takes_the_highest_priority_and_creates_a_leased_attempt(fresh_db: Path) -> None:
    with session_scope() as session:
        _create(session, "Low", priority=0)
        high = _create(session, "High", priority=10)

        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test-worker", lease_seconds=30)

        assert claimed is not None
        assert claimed.work_item.id == high.id
        assert claimed.work_item.status == "running"
        assert claimed.work_item.attempt_count == 1
        assert claimed.attempt.attempt_number == 1
        assert claimed.attempt.lease_owner == "test-worker"
        assert claimed.attempt.lease_expires_at == claimed.attempt.started_at + timedelta(seconds=30)
        assert len(_events(session, high.id, "work.claimed")) == 1


def test_claim_has_no_lease_expiry_by_default(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "No default lease timeout")

        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test-worker")

        assert claimed is not None
        assert claimed.work_item.id == work.id
        assert claimed.attempt.lease_expires_at is None


def test_claim_skips_work_that_is_not_due_yet(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        work = _create(session, "Later", not_before=now + timedelta(hours=1))
        queue = WorkQueue(session)

        assert queue.claim_next_ready_work(lease_owner="daemon", now=now) is None
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now + timedelta(hours=2))

        assert claimed is not None
        assert claimed.work_item.id == work.id


def test_claim_waits_for_unfinished_dependencies(fresh_db: Path) -> None:
    with session_scope() as session:
        upstream = _create(session, "Upstream")
        blocked = _create(session, "Blocked", priority=10)
        node_blocked = _create(session, "Waits on a workflow node", priority=5)
        session.add(WorkDependency(blocked_work_item_id=blocked.id, dependency_work_item_id=upstream.id))
        session.add(WorkDependency(blocked_work_item_id=node_blocked.id, dependency_workflow_node_id="node-1"))
        session.flush()
        queue = WorkQueue(session)

        first = queue.claim_next_ready_work(lease_owner="daemon")
        assert first is not None
        assert first.work_item.id == upstream.id
        assert queue.claim_next_ready_work(lease_owner="daemon") is None

        queue.complete_attempt(first.attempt.id, summary="Upstream done.")
        second = queue.claim_next_ready_work(lease_owner="daemon")

        assert second is not None
        assert second.work_item.id == blocked.id
        assert queue.claim_next_ready_work(lease_owner="daemon") is None
        assert session.get(WorkItem, node_blocked.id).status == "ready"


def test_claim_expires_overdue_work_instead_of_running_it(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        overdue = _create(
            session,
            "Stale",
            worker_kind="provider.default",
            deadline_at=now - timedelta(hours=1),
            priority=10,
        )
        fresh = _create(session, "Fresh", priority=0)

        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="daemon", now=now)

        assert claimed is not None
        assert claimed.work_item.id == fresh.id
        assert session.get(WorkItem, overdue.id).status == "dead_letter"
        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == overdue.id))
        assert failed is not None
        assert failed.error_type == "DeadlineExceeded"
        assert failed.attempt_id is None
        assert len(_events(session, overdue.id, "work.deadline_exceeded")) == 1


def test_expire_overdue_work_only_touches_passed_deadlines(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        overdue = _create(session, "Past deadline", deadline_at=now - timedelta(minutes=1))
        paused = _create(session, "Paused past deadline", deadline_at=now - timedelta(minutes=1))
        future = _create(session, "Future deadline", deadline_at=now + timedelta(hours=2))
        no_deadline = _create(session, "No deadline")
        queue = WorkQueue(session)
        queue.pause_work(paused.id)

        expired = queue.expire_overdue_work(now=now)

        assert expired == 2
        assert session.get(WorkItem, overdue.id).status == "dead_letter"
        assert session.get(WorkItem, paused.id).status == "dead_letter"
        assert session.get(WorkItem, future.id).status == "ready"
        assert session.get(WorkItem, no_deadline.id).status == "ready"


def test_transient_provider_error_retries_past_max_attempts(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Transient blip", worker_kind="provider.default", max_attempts=1)
        queue = WorkQueue(session)
        base = utc_now()
        statuses = []
        for index in range(TRANSIENT_RETRY_FLOOR):
            now = base + timedelta(minutes=index)
            claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
            assert claimed is not None
            queue.fail_attempt(
                claimed.attempt.id,
                error_type="TransientProviderError",
                error_message="API Error: The socket connection was closed unexpectedly.",
                now=now,
            )
            statuses.append(session.get(WorkItem, work.id).status)

        assert statuses == ["ready"] * (TRANSIENT_RETRY_FLOOR - 1) + ["dead_letter"]
        retries = _events(session, work.id, "work.retry_scheduled")
        assert len(retries) == TRANSIENT_RETRY_FLOOR - 1
        assert all(event.payload["transient"] is True for event in retries)
        assert all(event.payload["delay_seconds"] >= TRANSIENT_RETRY_DELAY_SECONDS for event in retries)


def test_reported_failure_dead_letters_on_the_first_attempt(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Genuine failure", worker_kind="provider.default", max_attempts=1)
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(lease_owner="daemon")
        assert claimed is not None

        queue.fail_attempt(
            claimed.attempt.id,
            error_type="ProviderExecutionError",
            error_message="Task is impossible as specified.",
        )

        refreshed = session.get(WorkItem, work.id)
        assert refreshed.status == "dead_letter"
        assert refreshed.attempt_count == 1
        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert failed.attempt_id == claimed.attempt.id
        assert failed.retry_count == 1


def test_retry_policy_delay_postpones_the_next_attempt(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        work = _create(session, "Backoff", max_attempts=2, retry_policy={"delay_seconds": 120})
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert claimed is not None

        queue.fail_attempt(claimed.attempt.id, error_type="RuntimeError", error_message="boom", now=now)

        refreshed = session.get(WorkItem, work.id)
        assert refreshed.status == "ready"
        assert refreshed.not_before == now + timedelta(seconds=120)
        assert queue.claim_next_ready_work(lease_owner="daemon", now=now + timedelta(seconds=60)) is None
        assert queue.claim_next_ready_work(lease_owner="daemon", now=now + timedelta(seconds=121)) is not None


def test_decide_retry_budgets_by_failure_kind() -> None:
    reported = decide_retry(error_type="RuntimeError", error_message="boom", attempt_number=1, max_attempts=1)
    assert (reported.retry, reported.transient, reported.limit_stop) == (False, False, False)

    reported_retry = decide_retry(
        error_type="RuntimeError", error_message="boom", attempt_number=1, max_attempts=3, base_delay_seconds=45
    )
    assert (reported_retry.retry, reported_retry.delay_seconds) == (True, 45)

    transient = decide_retry(
        error_type="TransientProviderError", error_message="socket closed", attempt_number=1, max_attempts=1
    )
    assert (transient.retry, transient.transient, transient.delay_seconds) == (
        True,
        True,
        TRANSIENT_RETRY_DELAY_SECONDS,
    )

    exhausted = decide_retry(
        error_type="TransientProviderError",
        error_message="socket closed",
        attempt_number=TRANSIENT_RETRY_FLOOR,
        max_attempts=1,
    )
    assert exhausted.retry is False

    limited = decide_retry(
        error_type="TransientProviderError",
        error_message="You've hit your session limit",
        attempt_number=TRANSIENT_RETRY_FLOOR,
        max_attempts=1,
    )
    assert (limited.retry, limited.limit_stop, limited.delay_seconds) == (True, True, LIMIT_RETRY_FALLBACK_SECONDS)

    limit_exhausted = decide_retry(
        error_type="TransientProviderError",
        error_message="session limit reached",
        attempt_number=LIMIT_RETRY_FLOOR,
        max_attempts=1,
    )
    assert (limit_exhausted.retry, limit_exhausted.limit_stop) == (False, True)

    reported_limit_text = decide_retry(
        error_type="ProviderExecutionError",
        error_message="You've hit your session limit",
        attempt_number=1,
        max_attempts=1,
    )
    assert (reported_limit_text.retry, reported_limit_text.limit_stop) == (False, False)


def test_limit_retry_delay_parses_the_stated_reset() -> None:
    tz = ZoneInfo("America/Los_Angeles")

    assert limit_retry_delay_seconds(SESSION_LIMIT, now=datetime(2026, 7, 8, 10, 7, tzinfo=tz)) == (
        93 * 60 + LIMIT_RETRY_BUFFER_SECONDS
    )
    assert (
        limit_retry_delay_seconds(
            "session limit reached - resets 3am",
            now=datetime(2026, 7, 8, 23, 30, tzinfo=tz),
            default_timezone="America/Los_Angeles",
        )
        == int(3.5 * 3600) + LIMIT_RETRY_BUFFER_SECONDS
    )
    assert (
        limit_retry_delay_seconds(
            "You've hit your weekly limit · resets Aug 19, 11pm (America/Los_Angeles)",
            now=datetime(2026, 8, 18, 15, 17, tzinfo=tz),
        )
        == 31 * 3600 + 43 * 60 + LIMIT_RETRY_BUFFER_SECONDS
    )
    assert (
        limit_retry_delay_seconds(
            "weekly limit · resets Jan 2, 9am (America/Los_Angeles)",
            now=datetime(2026, 12, 31, 12, 0, tzinfo=tz),
        )
        == 45 * 3600 + LIMIT_RETRY_BUFFER_SECONDS
    )


def test_limit_retry_delay_falls_back_by_the_window_scope() -> None:
    assert limit_retry_delay_seconds("You've hit your weekly limit - resets Jul 15, 2026") == 6 * 3600
    assert limit_retry_delay_seconds("You've hit your session limit") == LIMIT_RETRY_FALLBACK_SECONDS
    assert limit_retry_delay_seconds("You've hit your monthly spend limit · raise it at claude.ai/settings/usage") == (
        12 * 3600
    )


def test_limit_retry_delay_ignores_other_errors() -> None:
    assert limit_retry_delay_seconds("API Error: socket closed unexpectedly") is None
    assert limit_retry_delay_seconds("") is None
    assert limit_retry_delay_seconds(None) is None


def test_session_limit_failure_waits_for_the_stated_reset(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Limit-stopped apply", worker_kind="provider.default", max_attempts=1)
        queue = WorkQueue(session)
        now = utc_now()
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert claimed is not None

        queue.fail_attempt(
            claimed.attempt.id, error_type="TransientProviderError", error_message=SESSION_LIMIT, now=now
        )

        refreshed = session.get(WorkItem, work.id)
        assert refreshed.status == "ready"
        delay = (refreshed.not_before - now).total_seconds()
        assert LIMIT_RETRY_BUFFER_SECONDS <= delay <= 24 * 3600 + LIMIT_RETRY_BUFFER_SECONDS + 60


def test_limit_stops_get_their_own_retry_floor(fresh_db: Path) -> None:
    now = datetime(2026, 9, 1, 19, 0, tzinfo=UTC)
    with session_scope() as session:
        work = _create(session, "Weekly-limited apply", worker_kind="provider.default", max_attempts=1)
        queue = WorkQueue(session)
        for attempt_number in range(1, LIMIT_RETRY_FLOOR + 1):
            capacity_gate.reset()
            refreshed = session.get(WorkItem, work.id)
            refreshed.not_before = None
            session.flush()
            claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
            assert claimed is not None, f"not claimable on attempt {attempt_number}"
            queue.fail_attempt(
                claimed.attempt.id,
                error_type="TransientProviderError",
                error_message="You've hit your weekly limit · resets Sep 3, 11pm (America/Los_Angeles)",
                now=now,
            )
            refreshed = session.get(WorkItem, work.id)
            if attempt_number < LIMIT_RETRY_FLOOR:
                assert refreshed.status == "ready", f"dead-lettered on attempt {attempt_number}"
                assert refreshed.not_before == now + timedelta(hours=59, seconds=LIMIT_RETRY_BUFFER_SECONDS)

        assert refreshed.attempt_count == LIMIT_RETRY_FLOOR
        assert refreshed.status == "dead_letter"


def test_session_limit_gates_other_provider_claims(fresh_db: Path) -> None:
    with session_scope() as session:
        first = _create(session, "Apply 1", worker_kind="provider.default")
        _create(session, "Apply 2", worker_kind="provider.default")
        local = _create(session, "Local bookkeeping")
        queue = WorkQueue(session)
        now = utc_now()
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert claimed is not None
        assert claimed.work_item.id == first.id

        queue.fail_attempt(
            claimed.attempt.id, error_type="TransientProviderError", error_message=SESSION_LIMIT, now=now
        )

        assert capacity_gate.is_closed(now)
        nxt = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert nxt is not None
        assert nxt.work_item.id == local.id
        assert queue.claim_next_ready_work(lease_owner="daemon", now=now) is None


def test_limit_gate_releases_after_the_reset_passes(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Apply", worker_kind="provider.default")
        queue = WorkQueue(session)
        now = utc_now()
        capacity_gate.hold_until(now + timedelta(minutes=30))

        assert queue.claim_next_ready_work(lease_owner="daemon", now=now) is None
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now + timedelta(minutes=31))

        assert claimed is not None
        assert claimed.work_item.id == work.id


def test_capacity_gate_only_extends_and_is_capped() -> None:
    now = utc_now()
    capacity_gate.hold_until(now + timedelta(minutes=30))
    capacity_gate.hold_until(now + timedelta(minutes=10))
    assert capacity_gate.until() == now + timedelta(minutes=30)

    capacity_gate.hold_until(now + timedelta(days=3))
    until = capacity_gate.until()
    assert now + timedelta(hours=5) < until <= utc_now() + timedelta(seconds=CAPACITY_GATE_MAX_SECONDS)
    assert capacity_gate.is_closed(now)
    assert not capacity_gate.is_closed(until + timedelta(seconds=1))

    capacity_gate.reset()
    assert capacity_gate.until() is None
    assert not capacity_gate.is_closed()


def test_limit_stops_are_counted_per_lane(fresh_db: Path, metric_points) -> None:
    lane = "queue-test-limit-lane"
    with session_scope() as session:
        _create(session, "Limited", worker_kind="provider.default", lane=lane)
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(lease_owner="daemon")
        queue.fail_attempt(claimed.attempt.id, error_type="TransientProviderError", error_message=SESSION_LIMIT)

    assert _counted(metric_points("tasque.provider.limit_stops"), **{"tasque.work.lane": lane}) == 1


def test_failed_attempts_are_counted_by_outcome(fresh_db: Path, metric_points) -> None:
    lane = "queue-test-outcome-lane"
    with session_scope() as session:
        _create(session, "Fails twice", max_attempts=2, lane=lane)
        queue = WorkQueue(session)
        for _ in range(2):
            claimed = queue.claim_next_ready_work(lease_owner="daemon")
            queue.fail_attempt(claimed.attempt.id, error_type="RuntimeError", error_message="boom")

    points = metric_points("tasque.work.runs")
    assert _counted(points, **{"tasque.work.lane": lane, "tasque.work.outcome": "retry"}) == 1
    assert _counted(points, **{"tasque.work.lane": lane, "tasque.work.outcome": "dead_letter"}) == 1
    durations = [
        point for point in metric_points("tasque.work.duration") if point.attributes.get("tasque.work.lane") == lane
    ]
    assert sum(point.count for point in durations) == 2


def test_expired_lease_requeues_retryable_work(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        work = _create(session, "Recover lease", max_attempts=2)
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(
            lease_owner="lost-worker", lease_seconds=1, now=now - timedelta(minutes=5)
        )
        assert claimed is not None

        assert queue.recover_expired_leases(now=now) == 1

        assert session.get(WorkItem, work.id).status == "ready"
        attempt = session.get(WorkAttempt, claimed.attempt.id)
        assert attempt.status == "expired"
        assert attempt.error_type == "LeaseExpired"
        assert len(_events(session, work.id, "work.lease_expired")) == 1


def test_expired_lease_closes_its_provider_run(fresh_db: Path) -> None:
    with session_scope() as session:
        _create(session, "Leased apply", worker_kind="provider.default")
        queue = WorkQueue(session)
        now = utc_now()
        claimed = queue.claim_next_ready_work(lease_owner="daemon", lease_seconds=60, now=now)
        run = ProviderRun(attempt_id=claimed.attempt.id, provider="claude", status="running", started_at=now)
        session.add(run)
        session.flush()

        queue.recover_expired_leases(now=now + timedelta(seconds=120))

        refreshed = session.get(ProviderRun, run.id)
        assert refreshed.status == "orphaned"
        assert refreshed.ended_at is not None
        assert refreshed.usage["lease_expired"] is True


def test_heartbeat_refreshes_only_running_attempts(fresh_db: Path) -> None:
    now = utc_now()
    later = now + timedelta(minutes=5)
    with session_scope() as session:
        _create(session, "Long run")
        _create(session, "Finished run")
        queue = WorkQueue(session)
        running = queue.claim_next_ready_work(lease_owner="daemon", lease_seconds=60, now=now)
        finished = queue.claim_next_ready_work(lease_owner="daemon", lease_seconds=60, now=now)
        queue.complete_attempt(finished.attempt.id, summary="Done.")

        refreshed = queue.heartbeat_running_attempts(
            [running.attempt.id, finished.attempt.id, ""], lease_seconds=600, now=later
        )

        assert refreshed == 1
        assert running.attempt.heartbeat_at == later
        assert running.attempt.lease_expires_at == later + timedelta(seconds=600)
        assert finished.attempt.heartbeat_at == now
        assert queue.heartbeat_running_attempts([], lease_seconds=600) == 0


def test_orphaned_attempt_requeues_without_a_worker_timeout(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        work = _create(session, "Recover orphan", worker_kind="provider.default", max_attempts=1)
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now - timedelta(minutes=10))
        provider_run = ProviderRun(
            attempt_id=claimed.attempt.id, provider="codex", status="running", started_at=claimed.attempt.started_at
        )
        session.add(provider_run)
        session.flush()

        recovered = queue.recover_orphaned_attempts(lease_owner="daemon", orphaned_before=now, now=now)

        assert recovered == 1
        refreshed = session.get(WorkItem, work.id)
        assert refreshed.status == "ready"
        assert refreshed.max_attempts == 2
        assert session.get(WorkAttempt, claimed.attempt.id).status == "orphaned"
        assert session.get(ProviderRun, provider_run.id).status == "orphaned"
        [event] = _events(session, work.id, "work.orphaned_attempt_recovered")
        assert event.payload["provider_run_ids"] == [provider_run.id]


def test_orphan_recovery_leaves_leased_foreign_and_live_attempts_alone(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        for title in ("Leased", "Foreign", "Live"):
            _create(session, title)
        queue = WorkQueue(session)
        leased = queue.claim_next_ready_work(lease_owner="daemon", lease_seconds=3600, now=now - timedelta(minutes=10))
        foreign = queue.claim_next_ready_work(lease_owner="cli", now=now - timedelta(minutes=10))
        live = queue.claim_next_ready_work(lease_owner="daemon", now=now + timedelta(seconds=1))

        assert queue.recover_orphaned_attempts(lease_owner="daemon", orphaned_before=now, now=now) == 0
        assert {claimed.attempt.status for claimed in (leased, foreign, live)} == {"running"}


def test_late_orphaned_completion_closes_the_replacement_attempt(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        work = _create(session, "Late finish race", worker_kind="provider.default", max_attempts=1)
        queue = WorkQueue(session)
        original = queue.claim_next_ready_work(lease_owner="daemon", now=now - timedelta(minutes=10))
        queue.recover_orphaned_attempts(lease_owner="daemon", orphaned_before=now, now=now)
        replacement = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert replacement is not None
        provider_run = ProviderRun(
            attempt_id=replacement.attempt.id, provider="codex", status="running", started_at=now
        )
        session.add(provider_run)
        session.flush()

        queue.complete_attempt(original.attempt.id, summary="Original finished late.")

        assert session.get(WorkItem, work.id).status == "succeeded"
        assert session.get(WorkAttempt, original.attempt.id).status == "succeeded"
        superseded = session.get(WorkAttempt, replacement.attempt.id)
        assert superseded.status == "orphaned"
        assert superseded.error_type == "SupersededAttempt"
        run = session.get(ProviderRun, provider_run.id)
        assert run.status == "orphaned"
        assert run.usage["superseded_by_attempt_id"] == original.attempt.id
        assert len(_events(session, work.id, "work.sibling_attempt_superseded")) == 1


def test_cancel_pause_resume_and_retry_dead_letter(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Control me", worker_kind="unknown")
        queue = WorkQueue(session)

        queue.pause_work(work.id)
        assert session.get(WorkItem, work.id).status == "paused"
        assert queue.claim_next_ready_work(lease_owner="daemon") is None
        queue.resume_work(work.id)
        assert session.get(WorkItem, work.id).status == "ready"

        WorkRunner(session).run_next()
        assert session.get(WorkItem, work.id).status == "dead_letter"

        queue.retry_dead_letter(work.id)
        assert session.get(WorkItem, work.id).status == "ready"

        queue.request_cancel(work.id)
        assert session.get(WorkItem, work.id).status == "canceled"


def test_retry_dead_letter_resolves_failed_work_and_grants_an_attempt(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Retry me", max_attempts=1)
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(lease_owner="daemon")
        queue.fail_attempt(claimed.attempt.id, error_type="RuntimeError", error_message="boom")

        queue.retry_dead_letter(work.id)

        refreshed = session.get(WorkItem, work.id)
        assert (refreshed.status, refreshed.max_attempts, refreshed.not_before) == ("ready", 2, None)
        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert failed.status == "retrying"
        assert failed.resolved_at is not None
        assert len(_events(session, work.id, "work.retry_requested")) == 1
        retried = queue.claim_next_ready_work(lease_owner="daemon")
        assert retried is not None
        assert retried.attempt.attempt_number == 2


def test_controls_leave_terminal_work_alone(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Finished")
        WorkRunner(session).run_next()
        queue = WorkQueue(session)

        for control in (queue.pause_work, queue.resume_work, queue.request_cancel, queue.retry_dead_letter):
            assert control(work.id).status == "succeeded"
        with pytest.raises(KeyError):
            queue.request_cancel("no-such-work-item")


def test_cancel_of_running_work_waits_for_the_attempt(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Running")
        queue = WorkQueue(session)
        queue.claim_next_ready_work(lease_owner="daemon")

        assert queue.request_cancel(work.id).status == "cancel_requested"
        assert len(_events(session, work.id, "work.cancel_requested")) == 1


def test_running_attempt_failure_honors_an_external_cancel(fresh_db: Path) -> None:
    with session_scope() as runner_session:
        work = _create(runner_session, "Cancel race", worker_kind="provider.default")
        claimed = WorkQueue(runner_session).claim_next_ready_work(lease_owner="daemon")
        runner_session.commit()

        with session_scope() as control_session:
            WorkQueue(control_session).request_cancel(work.id)

        WorkQueue(runner_session).fail_attempt(
            claimed.attempt.id, error_type="ProviderKilled", error_message="Provider process was stopped."
        )

        assert runner_session.get(WorkItem, work.id).status == "canceled"
        assert runner_session.get(WorkAttempt, claimed.attempt.id).status == "canceled"
        assert runner_session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id)) is None


def test_running_attempt_completion_honors_an_external_cancel(fresh_db: Path) -> None:
    with session_scope() as runner_session:
        work = _create(runner_session, "Cancel complete race", worker_kind="provider.default")
        claimed = WorkQueue(runner_session).claim_next_ready_work(lease_owner="daemon")
        runner_session.commit()

        with session_scope() as control_session:
            WorkQueue(control_session).request_cancel(work.id)

        WorkQueue(runner_session).complete_attempt(claimed.attempt.id, summary="Provider finished late.")

        assert runner_session.get(WorkItem, work.id).status == "canceled"
        assert runner_session.get(WorkAttempt, claimed.attempt.id).status == "canceled"
