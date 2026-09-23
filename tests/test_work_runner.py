from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from opentelemetry.trace import SpanKind, StatusCode
from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.extensions import registry as extension_registry
from tasque2.models import Artifact, FailedWork, ProviderRun, WorkAttempt, WorkEvent, WorkItem, utc_now
from tasque2.telemetry import span
from tasque2.work.queue import WorkQueue
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import (
    FunctionWorkerRegistry,
    WorkerResult,
    WorkRunner,
    default_function_registry,
    normalize_worker_result,
)
from tasque2.worker import results


def _create(session, title: str = "Work", **fields) -> WorkItem:
    fields.setdefault("task_instruction", f"Do {title}.")
    fields.setdefault("worker_kind", "function.echo")
    return WorkRepository(session).create_work_item(title=title, **fields)


def _fail(_work_item: WorkItem) -> WorkerResult:
    raise RuntimeError("boom")


def _registry_with_failure() -> FunctionWorkerRegistry:
    registry = default_function_registry()
    registry.register("function.fail", _fail)
    return registry


def _orphaned_provider_attempt(*, claimed_at) -> tuple[str, str, str]:
    """A provider attempt a previous daemon left running, with its provider run recorded."""
    with session_scope() as session:
        work = _create(session, "Apply: Example Corp", worker_kind="provider.default", max_attempts=2)
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="daemon", now=claimed_at)
        provider_run = ProviderRun(
            attempt_id=claimed.attempt.id, provider="claude", status="running", started_at=claimed_at
        )
        session.add(provider_run)
        session.flush()
        claimed.attempt.provider_run_id = provider_run.id
        return work.id, claimed.attempt.id, provider_run.id


def _deposit(work_item_id: str, token: str, **payload) -> None:
    payload.setdefault("status", "succeeded")
    payload.setdefault("summary", "Done")
    payload.setdefault("report", "")
    results.deposit(result_token=token, payload={**payload, "work_item_id": work_item_id})


def test_function_runner_succeeds_and_records_output(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Echo", task_instruction="Echo this instruction.", context={"kind": "test"})

        outcome = WorkRunner(session, lease_owner="test-runner").run_next()

        assert outcome is not None
        assert (outcome.work_item_id, outcome.status, outcome.summary) == (
            work.id,
            "succeeded",
            "Echo this instruction.",
        )
        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
        assert attempt.status == "succeeded"
        assert attempt.lease_owner == "test-runner"
        assert attempt.produces["context"] == {"kind": "test"}
        event_types = [
            event.event_type
            for event in session.scalars(
                select(WorkEvent).where(WorkEvent.work_item_id == work.id).order_by(WorkEvent.id)
            )
        ]
        assert event_types == ["work.created", "work.claimed", "work.succeeded"]


def test_run_next_returns_none_when_nothing_is_ready(fresh_db: Path) -> None:
    with session_scope() as session:
        assert WorkRunner(session).run_next() is None


def test_claim_then_execute_runs_the_claimed_item(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Split", worker_kind="function.noop")
        runner = WorkRunner(session, lease_owner="pool", lease_seconds=120)

        claimed = runner.claim()
        assert claimed is not None
        assert claimed.attempt.lease_expires_at is not None
        assert runner.claim() is None
        outcome = runner.execute(claimed)

        assert outcome.attempt_id == claimed.attempt.id
        assert outcome.status == "succeeded"
        assert claimed.attempt.produces == {"work_item_id": work.id}


def test_worker_failure_retries_until_dead_letter(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Retry me", worker_kind="function.fail", max_attempts=2)
        runner = WorkRunner(session, registry=_registry_with_failure(), lease_owner="test-runner")

        first = runner.run_next()
        assert (first.status, first.summary) == ("ready", "boom")
        assert session.get(WorkItem, work.id).attempt_count == 1

        second = runner.run_next()
        assert second.status == "dead_letter"
        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert (failed.error_type, failed.error_message) == ("RuntimeError", "boom")


def test_unknown_worker_kind_dead_letters(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _create(session, "Nobody runs this", worker_kind="function.missing")

        outcome = WorkRunner(session).run_next()

        message = "No function worker registered for 'function.missing'."
        assert (outcome.status, outcome.summary) == ("dead_letter", message)
        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert (failed.error_type, failed.error_message) == ("WorkerNotFoundError", message)


def test_default_function_workers(fresh_db: Path) -> None:
    registry = default_function_registry()
    with session_scope() as session:
        manual = _create(session, "Manual", worker_kind="manual")
        noop = _create(session, "Noop", worker_kind="function.noop")
        echo = _create(session, "Echo", task_instruction="Say it back.", context={"k": 1})

        assert registry.run(manual) == WorkerResult(
            summary="Manual work item acknowledged.", produces={"title": "Manual", "worker_kind": "manual"}
        )
        assert registry.run(noop).produces == {"work_item_id": noop.id}
        assert registry.run(echo) == WorkerResult(
            summary="Say it back.",
            produces={"title": "Echo", "task_instruction": "Say it back.", "context": {"k": 1}},
        )


def test_normalize_worker_result_accepts_every_return_shape() -> None:
    assert normalize_worker_result("Plain summary") == WorkerResult(summary="Plain summary")
    assert normalize_worker_result(None) == WorkerResult(summary="Function worker completed.")
    assert normalize_worker_result({"summary": "Counted", "count": 3}) == WorkerResult(
        summary="Counted", produces={"count": 3}
    )
    assert normalize_worker_result({"summary": "Nested", "produces": {"ok": True}, "extra": 1}) == WorkerResult(
        summary="Nested", produces={"ok": True}
    )
    assert normalize_worker_result({"produces": [1, 2], "report_artifact_id": 42}) == WorkerResult(
        summary="Function worker completed.", produces={"value": [1, 2]}, report_artifact_id="42"
    )
    kept = WorkerResult(summary="As is", produces={"a": 1})
    assert normalize_worker_result(kept) is kept


def test_attempt_ingestors_run_for_completed_attempts_and_never_fail_the_run(fresh_db: Path) -> None:
    seen: list[tuple[str, str]] = []

    def broken(_session, _work_item, _attempt) -> None:
        raise RuntimeError("ingestor bug")

    def record(_session, work_item, attempt) -> None:
        seen.append((work_item.id, attempt.status))

    extension_registry().add_attempt_ingestor("broken", broken)
    extension_registry().add_attempt_ingestor("record", record)
    with session_scope() as session:
        work = _create(session, "Ingested", priority=1)
        _create(session, "Fails", worker_kind="function.fail")
        runner = WorkRunner(session, registry=_registry_with_failure())

        assert runner.run_next().status == "succeeded"
        assert runner.run_next().status == "dead_letter"
        assert seen == [(work.id, "succeeded")]


def test_deposited_result_is_adopted_instead_of_rerunning_the_work(fresh_db: Path) -> None:
    now = utc_now()
    work_id, attempt_id, provider_run_id = _orphaned_provider_attempt(claimed_at=now - timedelta(minutes=10))
    _deposit(
        work_id,
        "tok-late",
        summary="Applied to Example Corp",
        report="Applied to Example Corp. Confirmation received.",
        produces={"applied": True},
    )

    with session_scope() as session:
        adopted = WorkRunner(session, lease_owner="daemon").recover_deposited_results(orphaned_before=now)

        assert adopted == 1
        assert session.get(WorkItem, work_id).status == "succeeded"
        attempt = session.get(WorkAttempt, attempt_id)
        assert (attempt.status, attempt.summary, attempt.produces) == (
            "succeeded",
            "Applied to Example Corp",
            {"applied": True},
        )
        report = session.get(Artifact, attempt.report_artifact_id)
        assert Path(report.local_path).read_text(encoding="utf-8").startswith("Applied to Example Corp.")
        provider_run = session.get(ProviderRun, provider_run_id)
        assert provider_run.status == "succeeded"
        assert provider_run.usage["recovered_deposited_result"] is True
        assert results.consume_for_work_item(session, work_id) is None
        assert WorkQueue(session).recover_orphaned_attempts(lease_owner="daemon", orphaned_before=now, now=now) == 0


def test_deposited_failure_is_recorded_as_a_failed_attempt(fresh_db: Path) -> None:
    now = utc_now()
    work_id, attempt_id, provider_run_id = _orphaned_provider_attempt(claimed_at=now - timedelta(minutes=10))
    _deposit(work_id, "tok-failed", status="failed", summary="Could not apply", error="Form rejected")

    with session_scope() as session:
        work = session.get(WorkItem, work_id)
        work.max_attempts = 1

        assert WorkRunner(session, lease_owner="daemon").recover_deposited_results(orphaned_before=now) == 1

        assert work.status == "dead_letter"
        attempt = session.get(WorkAttempt, attempt_id)
        assert (attempt.status, attempt.error_type, attempt.error_message) == (
            "failed",
            "ProviderExecutionError",
            "Form rejected",
        )
        provider_run = session.get(ProviderRun, provider_run_id)
        assert (provider_run.status, provider_run.usage) == ("succeeded", {"recovered_deposited_result": True})
        assert provider_run.ended_at is not None


def test_deposited_result_recovery_leaves_in_flight_attempts_alone(fresh_db: Path) -> None:
    now = utc_now()
    work_id, attempt_id, _ = _orphaned_provider_attempt(claimed_at=now)
    _deposit(work_id, "tok-live")

    with session_scope() as session:
        adopted = WorkRunner(session, lease_owner="daemon").recover_deposited_results(
            orphaned_before=now - timedelta(minutes=5)
        )

        assert adopted == 0
        assert session.get(WorkAttempt, attempt_id).status == "running"
        assert results.consume_for_work_item(session, work_id) is not None


def test_deposited_result_recovery_only_touches_its_own_lease_owner(fresh_db: Path) -> None:
    now = utc_now()
    work_id, attempt_id, _ = _orphaned_provider_attempt(claimed_at=now - timedelta(minutes=10))
    _deposit(work_id, "tok-other")

    with session_scope() as session:
        assert WorkRunner(session, lease_owner="cli").recover_deposited_results(orphaned_before=now) == 0
        assert session.get(WorkAttempt, attempt_id).status == "running"


def test_run_span_is_a_consumer_span_parented_to_the_stored_traceparent(fresh_db: Path, spans) -> None:
    with span("enqueue") as producer, session_scope() as session:
        work_id = _create(session, "Traced", lane="traced-lane").id

    with span("unrelated"), session_scope() as session:
        WorkRunner(session).run_next()

    [run_span] = [item for item in spans.get_finished_spans() if item.name == "tasque.work.run"]
    parent = producer.get_span_context()
    assert run_span.kind is SpanKind.CONSUMER
    assert run_span.parent.span_id == parent.span_id
    assert run_span.context.trace_id == parent.trace_id
    assert run_span.attributes["tasque.work.lane"] == "traced-lane"
    assert run_span.attributes["tasque.work.id"] == work_id
    assert run_span.attributes["tasque.worker.kind"] == "function.echo"
    assert run_span.attributes["tasque.work.attempt"] == 1
    assert run_span.attributes["tasque.work.status"] == "succeeded"
    assert "work.succeeded" in [event.name for event in run_span.events]


def test_run_span_without_a_stored_traceparent_starts_a_trace(fresh_db: Path, spans) -> None:
    with session_scope() as session:
        _create(session, "Untraced")
        WorkRunner(session).run_next()

    [run_span] = [item for item in spans.get_finished_spans() if item.name == "tasque.work.run"]
    assert run_span.parent is None
    assert "tasque.work.lane" not in run_span.attributes


def test_failed_run_span_records_the_error(fresh_db: Path, spans) -> None:
    with session_scope() as session:
        _create(session, "Fails", worker_kind="function.fail")
        WorkRunner(session, registry=_registry_with_failure()).run_next()

    [run_span] = [item for item in spans.get_finished_spans() if item.name == "tasque.work.run"]
    assert run_span.status.status_code is StatusCode.ERROR
    assert run_span.attributes["error.type"] == "RuntimeError"
    assert run_span.attributes["tasque.work.status"] == "dead_letter"
    assert "exception" in [event.name for event in run_span.events]


def test_finished_runs_are_counted_by_outcome_and_lane(fresh_db: Path, metric_points) -> None:
    lane = "runner-test-metrics-lane"
    with session_scope() as session:
        for index in range(2):
            _create(session, f"Counted {index}", lane=lane)
        runner = WorkRunner(session)
        runner.run_next()
        runner.run_next()

    points = [point for point in metric_points("tasque.work.runs") if point.attributes.get("tasque.work.lane") == lane]
    assert {point.attributes["tasque.work.outcome"] for point in points} == {"succeeded"}
    assert {point.attributes["tasque.worker.kind"] for point in points} == {"function.echo"}
    assert sum(point.value for point in points) == 2
