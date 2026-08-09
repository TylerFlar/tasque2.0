from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from sqlalchemy import select

from tasque2 import result_inbox
from tasque2.db import session_scope
from tasque2.models import (
    AgentResult,
    FailedWork,
    ProviderRun,
    WorkAttempt,
    WorkEvent,
    WorkItem,
    utc_now,
)
from tasque2.queue import (
    TRANSIENT_RETRY_DELAY_SECONDS,
    TRANSIENT_RETRY_FLOOR,
    WorkQueue,
    note_provider_limit_stop,
    provider_limit_gate_until,
)
from tasque2.repo import WorkRepository
from tasque2.runtime import FunctionWorkerRegistry, WorkerResult, WorkRunner


def test_claim_next_ready_work_creates_attempt_and_lease(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        low = repo.create_work_item(
            title="Low",
            task_instruction="Low priority.",
            worker_kind="manual",
            priority=0,
        )
        high = repo.create_work_item(
            title="High",
            task_instruction="High priority.",
            worker_kind="manual",
            priority=10,
        )

        claimed = WorkQueue(session).claim_next_ready_work(
            lease_owner="test-worker",
            lease_seconds=30,
        )

        assert claimed is not None
        assert claimed.work_item.id == high.id
        assert claimed.work_item.id != low.id
        assert claimed.work_item.status == "running"
        assert claimed.attempt.attempt_number == 1
        assert claimed.attempt.lease_owner == "test-worker"
        assert claimed.attempt.lease_expires_at is not None


def test_claim_next_ready_work_has_no_default_lease_expiry(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="No default lease timeout",
            task_instruction="Run as long as needed.",
            worker_kind="manual",
        )

        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test-worker")

        assert claimed is not None
        assert claimed.work_item.id == work.id
        assert claimed.attempt.lease_expires_at is None


def test_function_runner_succeeds_and_records_output(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Echo",
            task_instruction="Echo this instruction.",
            worker_kind="function.echo",
            context={"kind": "test"},
        )

        outcome = WorkRunner(session, lease_owner="test-runner").run_next()

        assert outcome is not None
        assert outcome.status == "succeeded"
        assert outcome.work_item_id == work.id

        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
        assert attempt is not None
        assert attempt.status == "succeeded"
        assert attempt.produces["context"] == {"kind": "test"}

        event_types = [
            event.event_type
            for event in session.scalars(
                select(WorkEvent)
                .where(WorkEvent.work_item_id == work.id)
                .order_by(WorkEvent.id)
            )
        ]
        assert event_types == ["work.created", "work.claimed", "work.succeeded"]


def test_worker_failure_retries_until_dead_letter(fresh_db: Path) -> None:
    registry = FunctionWorkerRegistry()

    def failing_worker(_work_item: WorkItem) -> WorkerResult:
        raise RuntimeError("boom")

    registry.register("function.fail", failing_worker)

    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Retry me",
            task_instruction="Fail twice.",
            worker_kind="function.fail",
            max_attempts=2,
        )
        runner = WorkRunner(session, registry=registry, lease_owner="test-runner")

        first = runner.run_next()
        assert first is not None
        assert first.status == "ready"
        assert session.get(WorkItem, work.id).attempt_count == 1

        second = runner.run_next()
        assert second is not None
        assert second.status == "dead_letter"

        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert failed is not None
        assert failed.error_type == "RuntimeError"
        assert failed.error_message == "boom"


def test_transient_provider_error_retries_past_max_attempts(fresh_db: Path) -> None:
    # A single-attempt work item should still be retried when it fails for a
    # transient/infra reason (e.g. a dropped API socket), up to the transient
    # retry floor, rather than dead-lettering on the first blip.
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Transient blip",
            task_instruction="Provider socket dropped before submit.",
            worker_kind="provider.default",
            max_attempts=1,
        )
        queue = WorkQueue(session)

        # Advance the clock each round so the transient backoff (not_before) has
        # elapsed and the requeued item is claimable again.
        base = utc_now()
        statuses = []
        for i in range(TRANSIENT_RETRY_FLOOR):
            now = base + timedelta(minutes=i)
            claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
            assert claimed is not None
            queue.fail_attempt(
                claimed.attempt.id,
                error_type="TransientProviderError",
                error_message="API Error: The socket connection was closed unexpectedly.",
                now=now,
            )
            statuses.append(session.get(WorkItem, work.id).status)

        # First (FLOOR - 1) failures requeue; the last exhausts the floor.
        assert statuses[:-1] == ["ready"] * (TRANSIENT_RETRY_FLOOR - 1)
        assert statuses[-1] == "dead_letter"

        retry_events = session.scalars(
            select(WorkEvent).where(
                WorkEvent.work_item_id == work.id,
                WorkEvent.event_type == "work.retry_scheduled",
            )
        ).all()
        assert len(retry_events) == TRANSIENT_RETRY_FLOOR - 1
        assert all(event.payload.get("transient") is True for event in retry_events)
        assert all(
            event.payload.get("delay_seconds") >= TRANSIENT_RETRY_DELAY_SECONDS
            for event in retry_events
        )


def test_claim_expires_overdue_work_instead_of_running_it(fresh_db: Path) -> None:
    # A work item whose deadline_at has passed must not be claimed and run late
    # (e.g. a daily trading step resuming after the daemon was down). It is
    # dead-lettered, and the claim returns the next still-runnable item instead.
    now = utc_now()
    with session_scope() as session:
        repo = WorkRepository(session)
        overdue = repo.create_work_item(
            title="Stale",
            task_instruction="Should not run after its deadline.",
            worker_kind="provider.default",
            deadline_at=now - timedelta(hours=1),
            priority=10,  # higher priority: it is considered first
        )
        fresh = repo.create_work_item(
            title="Fresh",
            task_instruction="Still runnable.",
            worker_kind="manual",
            priority=0,
        )

        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="daemon", now=now)

        assert claimed is not None
        assert claimed.work_item.id == fresh.id

        stale = session.get(WorkItem, overdue.id)
        assert stale.status == "dead_letter"
        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == overdue.id))
        assert failed is not None
        assert failed.error_type == "DeadlineExceeded"
        event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.work_item_id == overdue.id,
                WorkEvent.event_type == "work.deadline_exceeded",
            )
        )
        assert event is not None


def test_expire_overdue_work_sweep_only_touches_passed_deadlines(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        repo = WorkRepository(session)
        overdue = repo.create_work_item(
            title="Past deadline",
            task_instruction="Deadline already passed.",
            worker_kind="manual",
            deadline_at=now - timedelta(minutes=1),
        )
        future = repo.create_work_item(
            title="Future deadline",
            task_instruction="Deadline still ahead.",
            worker_kind="manual",
            deadline_at=now + timedelta(hours=2),
        )
        no_deadline = repo.create_work_item(
            title="No deadline",
            task_instruction="Runs whenever.",
            worker_kind="manual",
        )

        expired = WorkQueue(session).expire_overdue_work(now=now)

        assert expired == 1
        assert session.get(WorkItem, overdue.id).status == "dead_letter"
        assert session.get(WorkItem, future.id).status == "ready"
        assert session.get(WorkItem, no_deadline.id).status == "ready"


def test_agent_reported_failure_dead_letters_on_first_attempt(fresh_db: Path) -> None:
    # A genuine agent-reported failure is NOT transient: a max_attempts=1 item
    # dead-letters immediately and is not retried by the transient floor.
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Genuine failure",
            task_instruction="Agent reported it could not complete the task.",
            worker_kind="provider.default",
            max_attempts=1,
        )
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(lease_owner="daemon")
        assert claimed is not None
        queue.fail_attempt(
            claimed.attempt.id,
            error_type="ProviderExecutionError",
            error_message="Task is impossible as specified.",
        )

        assert session.get(WorkItem, work.id).status == "dead_letter"
        assert session.get(WorkItem, work.id).attempt_count == 1


def test_expired_lease_requeues_retryable_work(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Recover lease",
            task_instruction="Lease should expire.",
            worker_kind="manual",
            max_attempts=2,
        )
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(
            lease_owner="lost-worker",
            lease_seconds=1,
            now=now - timedelta(minutes=5),
        )
        assert claimed is not None

        recovered = queue.recover_expired_leases(now=now)

        assert recovered == 1
        assert session.get(WorkItem, work.id).status == "ready"
        assert session.get(WorkAttempt, claimed.attempt.id).status == "expired"


def test_orphaned_attempt_requeues_without_worker_timeout(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Recover orphan",
            task_instruction="Daemon vanished mid-run.",
            worker_kind="provider.default",
            max_attempts=1,
        )
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(
            lease_owner="daemon",
            now=now - timedelta(minutes=10),
        )
        assert claimed is not None
        provider_run = ProviderRun(
            attempt_id=claimed.attempt.id,
            provider="codex",
            status="running",
            started_at=claimed.attempt.started_at,
        )
        session.add(provider_run)
        session.flush()

        recovered = queue.recover_orphaned_attempts(
            lease_owner="daemon",
            orphaned_before=now,
            now=now,
        )

        assert recovered == 1
        assert session.get(WorkItem, work.id).status == "ready"
        assert session.get(WorkItem, work.id).max_attempts == 2
        assert session.get(WorkAttempt, claimed.attempt.id).status == "orphaned"
        assert session.get(ProviderRun, provider_run.id).status == "orphaned"
        event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.work_item_id == work.id,
                WorkEvent.event_type == "work.orphaned_attempt_recovered",
            )
        )
        assert event is not None


def _deposit_on(
    session,
    *,
    token: str,
    work_item_id: str,
    summary: str,
    report: str,
    produces: dict | None = None,
) -> None:
    """Write a worker result deposit on the caller's session."""
    session.add(
        AgentResult(
            result_token=token,
            agent_kind="worker",
            payload_json=json.dumps(
                {
                    "status": "succeeded",
                    "report": report,
                    "summary": summary,
                    "produces": produces or {},
                    "error": None,
                    "work_item_id": work_item_id,
                }
            ),
        )
    )
    session.flush()


def test_deposited_result_is_adopted_instead_of_rerunning_the_work(fresh_db: Path) -> None:
    # A provider subprocess outlives the daemon that spawned it, so it can still
    # submit its result after the parent dies -- the in-memory result_token is
    # gone, but the deposit names its work item. Re-running instead of adopting
    # it is what resubmitted a real job application on 2026-08-03.
    now = utc_now()
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Apply: Example Corp",
            task_instruction="Submit the application.",
            worker_kind="provider.default",
            max_attempts=2,
        )
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(
            lease_owner="daemon",
            now=now - timedelta(minutes=10),
        )
        assert claimed is not None
        provider_run = ProviderRun(
            attempt_id=claimed.attempt.id,
            provider="claude",
            status="running",
            started_at=claimed.attempt.started_at,
        )
        session.add(provider_run)
        session.flush()
        claimed.attempt.provider_run_id = provider_run.id
        session.flush()

        # Deposited by the surviving subprocess (a different process in
        # production, so written here on the test's own session).
        _deposit_on(
            session,
            token="tok-late",
            work_item_id=work.id,
            summary="Applied to Example Corp",
            report="Applied to Example Corp. Confirmation received.",
            produces={"applied": True},
        )

        adopted = WorkRunner(session, lease_owner="daemon").recover_deposited_results(
            orphaned_before=now,
        )
        assert adopted == 1

        # Completed, not requeued -- the application is not submitted twice.
        assert session.get(WorkItem, work.id).status == "succeeded"
        assert session.get(WorkAttempt, claimed.attempt.id).status == "succeeded"
        assert session.get(WorkAttempt, claimed.attempt.id).summary == "Applied to Example Corp"

        # And the payload is consumed, so a later tick cannot adopt it again.
        assert result_inbox.consume_for_work_item(session, work.id) is None

        # Orphan recovery now has nothing left to requeue.
        assert (
            queue.recover_orphaned_attempts(
                lease_owner="daemon",
                orphaned_before=now,
                now=now,
            )
            == 0
        )


def test_deposited_result_recovery_leaves_in_flight_attempts_alone(fresh_db: Path) -> None:
    # An attempt heartbeating after `orphaned_before` belongs to the live
    # daemon; its result must be consumed by the running poll loop, not stolen
    # by recovery.
    now = utc_now()
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Still running",
            task_instruction="In flight.",
            worker_kind="provider.default",
        )
        queue = WorkQueue(session)
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert claimed is not None
        session.flush()

        _deposit_on(session, token="tok-live", work_item_id=work.id, summary="s", report="r")

        adopted = WorkRunner(session, lease_owner="daemon").recover_deposited_results(
            orphaned_before=now - timedelta(minutes=5),
        )
        assert adopted == 0
        assert session.get(WorkItem, work.id).status == "running"
        # The live runtime can still claim its own result.
        assert result_inbox.consume_for_work_item(session, work.id) is not None


def test_late_orphaned_completion_closes_replacement_attempt(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Late finish race",
            task_instruction="Original provider finishes after recovery.",
            worker_kind="provider.default",
            max_attempts=1,
        )
        queue = WorkQueue(session)
        original = queue.claim_next_ready_work(
            lease_owner="daemon",
            now=now - timedelta(minutes=10),
        )
        assert original is not None

        queue.recover_orphaned_attempts(
            lease_owner="daemon",
            orphaned_before=now,
            now=now,
        )
        replacement = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert replacement is not None
        provider_run = ProviderRun(
            attempt_id=replacement.attempt.id,
            provider="codex",
            status="running",
            started_at=now,
        )
        session.add(provider_run)
        session.flush()

        queue.complete_attempt(original.attempt.id, summary="Original finished late.")

        assert session.get(WorkItem, work.id).status == "succeeded"
        assert session.get(WorkAttempt, original.attempt.id).status == "succeeded"
        assert session.get(WorkAttempt, replacement.attempt.id).status == "orphaned"
        assert session.get(ProviderRun, provider_run.id).status == "orphaned"
        event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.work_item_id == work.id,
                WorkEvent.event_type == "work.sibling_attempt_superseded",
            )
        )
        assert event is not None


def test_cancel_pause_resume_and_retry_dead_letter(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Control me",
            task_instruction="Exercise controls.",
            worker_kind="unknown",
        )
        queue = WorkQueue(session)

        queue.pause_work(work.id)
        assert session.get(WorkItem, work.id).status == "paused"
        queue.resume_work(work.id)
        assert session.get(WorkItem, work.id).status == "ready"

        WorkRunner(session).run_next()
        assert session.get(WorkItem, work.id).status == "dead_letter"

        queue.retry_dead_letter(work.id)
        assert session.get(WorkItem, work.id).status == "ready"

        queue.request_cancel(work.id)
        assert session.get(WorkItem, work.id).status == "canceled"


def test_running_attempt_failure_honors_external_cancel(fresh_db: Path) -> None:
    with session_scope() as runner_session:
        work = WorkRepository(runner_session).create_work_item(
            title="Cancel race",
            task_instruction="Provider is still running.",
            worker_kind="provider.default",
        )
        claimed = WorkQueue(runner_session).claim_next_ready_work(lease_owner="daemon")
        assert claimed is not None
        runner_session.commit()

        with session_scope() as control_session:
            WorkQueue(control_session).request_cancel(work.id)

        WorkQueue(runner_session).fail_attempt(
            claimed.attempt.id,
            error_type="ProviderKilled",
            error_message="Provider process was stopped.",
        )

        assert runner_session.get(WorkItem, work.id).status == "canceled"
        assert runner_session.get(WorkAttempt, claimed.attempt.id).status == "canceled"
        failed = runner_session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert failed is None


def test_running_attempt_completion_honors_external_cancel(fresh_db: Path) -> None:
    with session_scope() as runner_session:
        work = WorkRepository(runner_session).create_work_item(
            title="Cancel complete race",
            task_instruction="Provider finishes after cancel.",
            worker_kind="provider.default",
        )
        claimed = WorkQueue(runner_session).claim_next_ready_work(lease_owner="daemon")
        assert claimed is not None
        runner_session.commit()

        with session_scope() as control_session:
            WorkQueue(control_session).request_cancel(work.id)

        WorkQueue(runner_session).complete_attempt(
            claimed.attempt.id,
            summary="Provider finished late.",
        )

        assert runner_session.get(WorkItem, work.id).status == "canceled"
        assert runner_session.get(WorkAttempt, claimed.attempt.id).status == "canceled"


def test_limit_retry_delay_parses_reset_time() -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from tasque2.queue import (
        LIMIT_RETRY_BUFFER_SECONDS,
        LIMIT_RETRY_FALLBACK_SECONDS,
        limit_retry_delay_seconds,
    )

    tz = ZoneInfo("America/Los_Angeles")
    # The exact banner that dead-lettered a career-apply child on 2026-07-08.
    delay = limit_retry_delay_seconds(
        "You've hit your session limit · resets 11:40am (America/Los_Angeles)",
        now=datetime(2026, 7, 8, 10, 7, tzinfo=tz),
    )
    assert delay == 93 * 60 + LIMIT_RETRY_BUFFER_SECONDS  # 10:07 -> 11:40

    # A reset time already past today rolls over to tomorrow.
    delay = limit_retry_delay_seconds(
        "session limit reached - resets 3am",
        now=datetime(2026, 7, 8, 23, 30, tzinfo=tz),
        default_timezone="America/Los_Angeles",
    )
    assert delay == int(3.5 * 3600) + LIMIT_RETRY_BUFFER_SECONDS

    # Limit-shaped message without a parseable clock time backs off the
    # fallback instead of the 30-second transient cadence.
    assert (
        limit_retry_delay_seconds("You've hit your weekly limit - resets Jul 15, 2026")
        == LIMIT_RETRY_FALLBACK_SECONDS
    )

    # Ordinary transient messages keep the normal cadence.
    assert limit_retry_delay_seconds("API Error: socket closed unexpectedly") is None
    assert limit_retry_delay_seconds(None) is None


def test_session_limit_transient_failure_waits_for_reset(fresh_db: Path) -> None:
    # A provider stopped by a subscription session limit must not burn the
    # transient retry floor on the 30-second cadence while the limit is still
    # in force: the retry is scheduled for the reset time in the message.
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Limit-stopped apply",
            task_instruction="Provider hit the session limit mid-run.",
            worker_kind="provider.default",
            max_attempts=1,
        )
        queue = WorkQueue(session)
        now = utc_now()
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert claimed is not None
        queue.fail_attempt(
            claimed.attempt.id,
            error_type="TransientProviderError",
            error_message="You've hit your session limit · resets 11:40am (America/Los_Angeles)",
            now=now,
        )
        refreshed = session.get(WorkItem, work.id)
        assert refreshed.status == "ready"
        delay = (refreshed.not_before - now).total_seconds()
        # Reset-aware: at least the buffer, never the 30s cadence, and capped
        # regardless of what wall-clock time the test runs at.
        assert delay >= 300
        assert delay > TRANSIENT_RETRY_DELAY_SECONDS
        assert delay <= 12 * 3600 + 60


def test_session_limit_gates_other_provider_claims(fresh_db: Path) -> None:
    # A session limit is account-wide. Once one run reports it, claiming the
    # other ready provider items would spend a real attempt each on a
    # guaranteed instant failure (8 of 10 career applies died that way on
    # 2026-08-03), so provider work is held until the stated reset.
    with session_scope() as session:
        repo = WorkRepository(session)
        first = repo.create_work_item(
            title="Apply 1",
            task_instruction="Apply.",
            worker_kind="provider.default",
            max_attempts=1,
        )
        repo.create_work_item(
            title="Apply 2",
            task_instruction="Apply.",
            worker_kind="provider.default",
            max_attempts=1,
        )
        function_work = repo.create_work_item(
            title="Local bookkeeping",
            task_instruction="No provider needed.",
            worker_kind="manual",
        )
        session.flush()

        queue = WorkQueue(session)
        now = utc_now()
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert claimed is not None
        assert claimed.work_item.id == first.id
        queue.fail_attempt(
            claimed.attempt.id,
            error_type="TransientProviderError",
            error_message="You've hit your session limit · resets 11:40am (America/Los_Angeles)",
            now=now,
        )

        assert provider_limit_gate_until() is not None

        # The sibling provider item is ready and due, but must not be claimed.
        # Non-provider work is unaffected -- the limit is a provider quota, so
        # the queue skips past the gated apply and claims the local item.
        nxt = queue.claim_next_ready_work(lease_owner="daemon", now=now)
        assert nxt is not None
        assert nxt.work_item.id == function_work.id

        # With only gated provider work left, there is nothing to claim at all.
        assert queue.claim_next_ready_work(lease_owner="daemon", now=now) is None


def test_limit_gate_releases_after_the_reset_passes(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Apply",
            task_instruction="Apply.",
            worker_kind="provider.default",
        )
        session.flush()
        queue = WorkQueue(session)
        now = utc_now()
        note_provider_limit_stop(now + timedelta(minutes=30))

        assert queue.claim_next_ready_work(lease_owner="daemon", now=now) is None

        after_reset = now + timedelta(minutes=31)
        claimed = queue.claim_next_ready_work(lease_owner="daemon", now=after_reset)
        assert claimed is not None
        assert claimed.work_item.id == work.id
