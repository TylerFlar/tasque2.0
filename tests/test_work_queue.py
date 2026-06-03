from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.models import FailedWork, ProviderRun, WorkAttempt, WorkEvent, WorkItem, utc_now
from tasque2.queue import (
    TRANSIENT_RETRY_DELAY_SECONDS,
    TRANSIENT_RETRY_FLOOR,
    WorkQueue,
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
