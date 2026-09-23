from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from opentelemetry.trace import SpanKind, StatusCode
from sqlalchemy import func, select
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.db import session_scope
from tasque2.models import Schedule, ScheduleOccurrence, WorkflowRun, WorkItem
from tasque2.schedules import ScheduleService
from tasque2.workflows import WorkflowService

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _occurrence_times(session) -> list[datetime]:
    return list(
        session.scalars(select(ScheduleOccurrence.scheduled_for).order_by(ScheduleOccurrence.scheduled_for)).all()
    )


def _span_ids(traceparent: str) -> tuple[str, str]:
    _, trace_id, span_id, _ = traceparent.split("-")
    return trace_id, span_id


def _echo_schedule(service: ScheduleService, **fields) -> Schedule:
    defaults = {
        "name": "Echo",
        "schedule_type": "interval",
        "expression": "minutes=1",
        "worker_kind": "function.echo",
        "payload": {"task_instruction": "Tick."},
        "timezone_name": "UTC",
    }
    return service.create_schedule(**{**defaults, **fields})


def test_date_schedule_enqueues_work_once(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        _echo_schedule(
            service,
            name="One shot",
            schedule_type="date",
            expression=(NOW - timedelta(minutes=1)).isoformat(),
            payload={"title": "One shot work", "task_instruction": "Run once."},
        )

        assert service.poll_due_schedules(now=NOW) == 1
        assert service.poll_due_schedules(now=NOW + timedelta(minutes=1)) == 0

        assert session.scalar(select(func.count()).select_from(ScheduleOccurrence)) == 1
        work = session.scalar(select(WorkItem).where(WorkItem.title == "One shot work"))
        assert work.source_kind == "schedule"
        assert work.task_instruction == "Run once."
        assert work.schedule_occurrence_id is not None


def test_date_schedule_waits_for_its_time(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        _echo_schedule(
            service, schedule_type="date", expression="2026-01-01T04:30:00", timezone_name="America/Los_Angeles"
        )

        assert service.poll_due_schedules(now=datetime(2026, 1, 1, 12, 29, tzinfo=UTC)) == 0
        assert service.poll_due_schedules(now=datetime(2026, 1, 1, 12, 30, tzinfo=UTC)) == 1
        assert _occurrence_times(session) == [datetime(2026, 1, 1, 12, 30, tzinfo=UTC)]


def test_schedule_loads_its_instruction_from_a_template_file(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "scheduled.template.md"
    template.write_text("# Scheduled Template\n\nRun from Markdown.", encoding="utf-8")

    with session_scope() as session:
        service = ScheduleService(session)
        _echo_schedule(
            service,
            name="Templated schedule",
            schedule_type="date",
            expression=(NOW - timedelta(minutes=1)).isoformat(),
            payload={
                "title": "Templated work",
                "task_template_path": "scheduled.template.md",
                "template_base_dir": str(tmp_path),
            },
        )

        assert service.poll_due_schedules(now=NOW) == 1
        work = session.scalar(select(WorkItem).where(WorkItem.title == "Templated work"))
        assert work.task_instruction == "# Scheduled Template\n\nRun from Markdown."


def test_schedule_template_is_read_at_each_fire(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "daily.md"
    template.write_text("First wording.", encoding="utf-8")

    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, payload={"task_template_path": str(template)})

        first = service.fire_schedule_now(schedule.id, now=NOW)
        template.write_text("Second wording.", encoding="utf-8")
        second = service.fire_schedule_now(schedule.id, now=NOW + timedelta(minutes=1))

        assert session.get(WorkItem, first.work_item_id).task_instruction == "First wording."
        assert session.get(WorkItem, second.work_item_id).task_instruction == "Second wording."


def test_interval_schedule_coalesces_missed_runs(fresh_db: Path) -> None:
    now = NOW + timedelta(minutes=5)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, name="Every minute", catchup_policy="coalesce")
        schedule.created_at = now - timedelta(minutes=5)
        schedule.last_evaluated_at = now - timedelta(minutes=5)

        assert service.poll_due_schedules(now=now) == 1
        assert _occurrence_times(session) == [now]


@pytest.mark.parametrize("poll_seconds", [3, 5, 7])
def test_interval_schedule_fires_on_its_created_at_grid_when_polled_often(fresh_db: Path, poll_seconds: int) -> None:
    created = NOW + timedelta(seconds=7)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, name="Every minute")
        schedule.created_at = created
        session.flush()

        fired = 0
        now = created
        while now < created + timedelta(minutes=5, seconds=30):
            now += timedelta(seconds=poll_seconds)
            fired += service.poll_due_schedules(now=now)

        assert fired == 5
        assert _occurrence_times(session) == [created + timedelta(minutes=minute) for minute in range(1, 6)]


def test_interval_schedule_keeps_its_grid_across_downtime(fresh_db: Path) -> None:
    created = NOW + timedelta(seconds=20)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, expression="minutes=10")
        schedule.created_at = created
        session.flush()

        assert service.poll_due_schedules(now=created + timedelta(minutes=1)) == 0
        assert service.poll_due_schedules(now=created + timedelta(minutes=34)) == 1
        assert service.poll_due_schedules(now=created + timedelta(minutes=39)) == 0
        assert service.poll_due_schedules(now=created + timedelta(minutes=40)) == 1

        assert _occurrence_times(session) == [created + timedelta(minutes=30), created + timedelta(minutes=40)]


def test_cron_schedule_enqueues_all_with_backfill_limit(fresh_db: Path) -> None:
    now = NOW + timedelta(minutes=5)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(
            service,
            name="Cron minute",
            schedule_type="cron",
            expression="* * * * *",
            catchup_policy="all",
            max_backfill=2,
        )
        schedule.created_at = now - timedelta(minutes=5)
        schedule.last_evaluated_at = now - timedelta(minutes=5)

        assert service.poll_due_schedules(now=now) == 2
        assert _occurrence_times(session) == [now - timedelta(minutes=4), now - timedelta(minutes=3)]


def test_cron_schedule_fires_at_local_time_in_its_timezone(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(
            service, schedule_type="cron", expression="0 9 * * *", timezone_name="America/Los_Angeles"
        )
        schedule.created_at = datetime(2026, 1, 1, 16, 0, tzinfo=UTC)
        session.flush()

        assert service.poll_due_schedules(now=datetime(2026, 1, 1, 16, 59, tzinfo=UTC)) == 0
        assert service.poll_due_schedules(now=datetime(2026, 1, 1, 17, 1, tzinfo=UTC)) == 1
        assert _occurrence_times(session) == [datetime(2026, 1, 1, 17, 0, tzinfo=UTC)]


def test_misfire_grace_skips_old_occurrences(fresh_db: Path) -> None:
    now = NOW + timedelta(minutes=5)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, name="Grace", catchup_policy="all", misfire_grace_seconds=90)
        schedule.created_at = now - timedelta(minutes=5)
        schedule.last_evaluated_at = now - timedelta(minutes=5)

        assert service.poll_due_schedules(now=now) == 2
        assert _occurrence_times(session) == [now - timedelta(minutes=1), now]


def test_schedule_can_start_a_workflow_run_directly(fresh_db: Path) -> None:
    with session_scope() as session:
        workflow_definition = WorkflowService(session).create_definition(
            name="scheduled-workflow",
            version="1",
            definition={
                "nodes": [
                    {
                        "key": "step",
                        "kind": "work",
                        "task_instruction": "Scheduled workflow step.",
                        "worker_kind": "function.echo",
                    }
                ]
            },
        )
        service = ScheduleService(session)
        schedule = _echo_schedule(
            service,
            name="Start workflow",
            schedule_type="date",
            expression=(NOW - timedelta(minutes=1)).isoformat(),
            worker_kind="workflow",
            payload={
                "workflow_definition_id": workflow_definition.id,
                "run_name": "Scheduled workflow run",
                "input": {"source": "scheduler-test"},
                "discord_thread_id": "thread-1",
            },
        )

        assert service.poll_due_schedules(now=NOW) == 1
        assert service.poll_due_schedules(now=NOW + timedelta(minutes=1)) == 0

        occurrence = session.scalar(select(ScheduleOccurrence).where(ScheduleOccurrence.schedule_id == schedule.id))
        assert occurrence.work_item_id is None
        assert occurrence.status == "enqueued"

        run = session.get(WorkflowRun, occurrence.workflow_run_id)
        assert run.name == "Scheduled workflow run"
        assert run.discord_thread_id == "thread-1"
        assert run.input["source"] == "scheduler-test"
        assert run.input["schedule_id"] == schedule.id
        assert run.input["schedule_occurrence_id"] == occurrence.id
        assert run.input["scheduled_for"] == (NOW - timedelta(minutes=1)).isoformat()


def test_workflow_schedule_finds_its_definition_by_name_and_version(fresh_db: Path) -> None:
    with session_scope() as session:
        workflows = WorkflowService(session)
        workflows.create_definition(name="digest", version="1", definition={"nodes": [{"key": "a"}]})
        second = workflows.create_definition(name="digest", version="2", definition={"nodes": [{"key": "b"}]})
        service = ScheduleService(session)
        named = _echo_schedule(
            service, worker_kind="workflow", payload={"workflow_name": "digest", "workflow_version": "2"}
        )
        unknown = _echo_schedule(service, name="Unknown", worker_kind="workflow", payload={"workflow_name": "absent"})
        nameless = _echo_schedule(service, name="Nameless", worker_kind="workflow", payload={})

        occurrence = service.fire_schedule_now(named.id, now=NOW)
        assert session.get(WorkflowRun, occurrence.workflow_run_id).workflow_definition_id == second.id
        with pytest.raises(ValueError, match="Unknown workflow definition: absent@1"):
            service.fire_schedule_now(unknown.id, now=NOW)
        with pytest.raises(ValueError, match="requires workflow_definition_id or workflow_name"):
            service.fire_schedule_now(nameless.id, now=NOW)


def test_disabled_schedule_does_not_enqueue_until_reenabled(fresh_db: Path) -> None:
    now = NOW + timedelta(minutes=5)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, name="Controlled", catchup_policy="coalesce")
        schedule.created_at = now - timedelta(minutes=5)
        schedule.last_evaluated_at = now - timedelta(minutes=5)

        service.disable_schedule(schedule.id)
        assert service.poll_due_schedules(now=now) == 0
        assert session.scalar(select(func.count()).select_from(ScheduleOccurrence)) == 0

        service.enable_schedule(schedule.id, now=now)
        assert service.poll_due_schedules(now=now + timedelta(minutes=1)) == 1
        assert _occurrence_times(session) == [now + timedelta(minutes=1)]


def test_enable_without_resuming_from_now_catches_up_once(fresh_db: Path) -> None:
    now = NOW + timedelta(minutes=5)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, enabled=False)
        schedule.created_at = NOW
        session.flush()

        service.enable_schedule(schedule.id, now=now, resume_from_now=False)

        assert service.poll_due_schedules(now=now + timedelta(seconds=30)) == 1
        assert _occurrence_times(session) == [now]


def test_fire_schedule_now_refuses_a_second_fire_at_the_same_time(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service)

        occurrence = service.fire_schedule_now(schedule.id, now=NOW)
        assert occurrence.status == "enqueued"
        with pytest.raises(ValueError, match="already exists"):
            service.fire_schedule_now(schedule.id, now=NOW)
        with pytest.raises(KeyError):
            service.fire_schedule_now("missing", now=NOW)


def test_changing_a_schedules_timing_waits_for_the_next_slot(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service)
        service.poll_due_schedules(now=NOW)
        assert schedule.last_evaluated_at == NOW

        service.update_schedule(schedule.id, name="Renamed", payload={"task_instruction": "New."})
        assert schedule.last_evaluated_at == NOW

        before = datetime.now(UTC)
        service.update_schedule(schedule.id, expression="minutes=5", catchup_policy="skip")
        assert schedule.last_evaluated_at >= before
        assert service.poll_due_schedules(now=schedule.last_evaluated_at + timedelta(seconds=30)) == 0
        assert (schedule.name, schedule.expression, schedule.catchup_policy) == ("Renamed", "minutes=5", "skip")
        with pytest.raises(ValueError):
            service.update_schedule(schedule.id, schedule_type="hourly")


def test_create_schedule_validates_type_policy_and_timezone(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        with pytest.raises(ValueError, match="schedule_type"):
            _echo_schedule(service, schedule_type="hourly")
        with pytest.raises(ValueError, match="catchup_policy"):
            _echo_schedule(service, catchup_policy="sometimes")
        with pytest.raises(KeyError):
            _echo_schedule(service, timezone_name="Mars/Base")
        assert _echo_schedule(service, timezone_name=None).timezone == "America/Los_Angeles"


@pytest.mark.parametrize(
    ("schedule_type", "expression"),
    [
        ("interval", "minutes=0"),
        ("interval", "every 5 minutes"),
        ("interval", "weeks=1"),
        ("cron", "0 9 * * FRII"),
        ("cron", "61 * * * *"),
        ("date", "next tuesday"),
    ],
)
def test_create_schedule_rejects_expressions_it_could_not_evaluate(
    fresh_db: Path, schedule_type: str, expression: str
) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        with pytest.raises(ValueError):
            _echo_schedule(service, schedule_type=schedule_type, expression=expression)
        assert session.scalar(select(func.count()).select_from(Schedule)) == 0


def test_update_schedule_rejects_a_bad_expression_without_changing_anything(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, schedule_type="cron", expression="0 9 * * *")

        with pytest.raises(ValueError, match="Invalid cron expression"):
            service.update_schedule(schedule.id, name="Renamed", expression="0 9 * * FRII")
        with pytest.raises(ValueError, match="Interval"):
            service.update_schedule(schedule.id, schedule_type="interval")

        assert (schedule.name, schedule.schedule_type, schedule.expression) == ("Echo", "cron", "0 9 * * *")
        service.update_schedule(schedule.id, schedule_type="interval", expression="hours=6")
        assert (schedule.schedule_type, schedule.expression) == ("interval", "hours=6")


def test_a_schedule_that_cannot_fire_stays_due_without_holding_up_the_others(
    fresh_db: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger="tasque2.schedules")
    template = tmp_path / "later.md"
    with session_scope() as session:
        service = ScheduleService(session)
        broken = _echo_schedule(service, name="Broken", payload={"task_template_path": str(template)})
        healthy = _echo_schedule(service, name="Healthy")
        broken.created_at = healthy.created_at = NOW
        session.flush()

        assert service.poll_due_schedules(now=NOW + timedelta(minutes=1)) == 1
        assert [work.title for work in session.scalars(select(WorkItem)).all()] == ["Healthy"]
        assert broken.last_evaluated_at is None
        assert any("Broken" in record.getMessage() for record in caplog.records)

        template.write_text("Written late.", encoding="utf-8")
        assert service.poll_due_schedules(now=NOW + timedelta(minutes=1, seconds=30)) == 1

        work = session.scalar(select(WorkItem).where(WorkItem.title == "Broken"))
        assert work.task_instruction == "Written late."
        assert session.get(ScheduleOccurrence, work.schedule_occurrence_id).scheduled_for == NOW + timedelta(minutes=1)


def test_a_failed_launch_writes_no_occurrence(fresh_db: Path, spans) -> None:
    with session_scope() as session:
        definition = WorkflowService(session).create_definition(
            name="paused-flow", version="1", definition={"nodes": [{"key": "a"}]}, enabled=False
        )
        service = ScheduleService(session)
        schedule = _echo_schedule(service, worker_kind="workflow", payload={"workflow_definition_id": definition.id})

        with pytest.raises(ValueError, match="paused-flow@1 is disabled"):
            service.fire_schedule_now(schedule.id, now=NOW)
        assert session.scalar(select(func.count()).select_from(ScheduleOccurrence)) == 0
        assert session.scalar(select(func.count()).select_from(WorkflowRun)) == 0

        definition.enabled = True
        assert service.fire_schedule_now(schedule.id, now=NOW).workflow_run_id is not None

    failed = next(span for span in spans.get_finished_spans() if span.name == "tasque.schedule.fire")
    assert failed.status.status_code is StatusCode.ERROR
    assert failed.attributes["error.type"] == "ValueError"


def test_schedule_payload_can_keep_runs_out_of_discord(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        for name, visible in (("Quiet", False), ("Quiet text", "off"), ("Loud", None)):
            payload = {"title": f"{name} work", "task_instruction": "Go."}
            if visible is not None:
                payload["visible"] = visible
            _echo_schedule(
                service,
                name=name,
                schedule_type="date",
                expression=(NOW - timedelta(minutes=1)).isoformat(),
                payload=payload,
            )

        assert service.poll_due_schedules(now=NOW) == 3

        visibility = {work.title: work.visible for work in session.scalars(select(WorkItem)).all()}
        assert visibility == {"Quiet work": False, "Quiet text work": False, "Loud work": True}


def test_schedule_payload_carries_work_settings(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(
            service,
            worker_kind="provider.fake",
            runtime_contract={"model_profile": "high"},
            payload={
                "task_instruction": "Go.",
                "context": {"memory_namespace": "finance"},
                "priority": 5,
                "max_attempts": 3,
                "retry_policy": {"delay_seconds": 30},
                "discord_thread_id": "thread-2",
            },
        )

        work = session.get(WorkItem, service.fire_schedule_now(schedule.id, now=NOW).work_item_id)

        assert work.title == "Echo"
        assert work.schedule_id == schedule.id
        assert work.runtime_contract == {"model_profile": "high"}
        assert work.context == {"memory_namespace": "finance"}
        assert (work.priority, work.max_attempts, work.retry_policy) == (5, 3, {"delay_seconds": 30})
        assert work.discord_thread_id == "thread-2"


def test_schedule_payload_lane_sets_the_work_lane(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        expression = (NOW - timedelta(minutes=1)).isoformat()
        _echo_schedule(
            service,
            name="Kitchen prep",
            schedule_type="date",
            expression=expression,
            payload={"task_instruction": "Prep.", "lane": "kitchen"},
        )
        _echo_schedule(service, name="Morning desk", schedule_type="date", expression=expression)

        assert service.poll_due_schedules(now=NOW) == 2

        lanes = {work.title: work.lane for work in session.scalars(select(WorkItem)).all()}
        assert lanes == {"Kitchen prep": "kitchen", "Morning desk": "Morning desk"}


def test_schedule_fire_records_a_producer_span_that_parents_the_work(fresh_db: Path, spans, metric_points) -> None:
    scheduled_for = NOW - timedelta(minutes=1)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(
            service, name="Traced fire", schedule_type="date", expression=scheduled_for.isoformat()
        )
        assert service.poll_due_schedules(now=NOW) == 1
        work = session.scalar(select(WorkItem).where(WorkItem.schedule_id == schedule.id))
        schedule_id, traceparent = schedule.id, work.traceparent

    fire = next(span for span in spans.get_finished_spans() if span.name == "tasque.schedule.fire")
    assert fire.kind is SpanKind.PRODUCER
    assert dict(fire.attributes) == {
        "tasque.schedule.id": schedule_id,
        "tasque.schedule.name": "Traced fire",
        "tasque.schedule.scheduled_for": scheduled_for.isoformat(),
        "tasque.schedule.target": "function.echo",
    }
    assert _span_ids(traceparent) == (format(fire.context.trace_id, "032x"), format(fire.context.span_id, "016x"))
    points = [
        point
        for point in metric_points("tasque.schedule.occurrences")
        if point.attributes.get("tasque.schedule.name") == "Traced fire"
    ]
    assert [(point.attributes["tasque.schedule.target"], point.value) for point in points] == [("work", 1)]


def test_workflow_schedule_fire_span_parents_the_workflow_start(fresh_db: Path, spans) -> None:
    with session_scope() as session:
        definition = WorkflowService(session).create_definition(
            name="traced-flow", version="1", definition={"nodes": [{"key": "step"}]}
        )
        service = ScheduleService(session)
        schedule = _echo_schedule(service, worker_kind="workflow", payload={"workflow_definition_id": definition.id})
        run_id = service.fire_schedule_now(schedule.id, now=NOW).workflow_run_id
        traceparent = session.get(WorkflowRun, run_id).traceparent

    finished = {span.name: span for span in spans.get_finished_spans()}
    fire, start = finished["tasque.schedule.fire"], finished["tasque.workflow.start"]
    assert fire.attributes["tasque.schedule.target"] == "workflow"
    assert start.parent.span_id == fire.context.span_id
    assert _span_ids(traceparent) == (format(start.context.trace_id, "032x"), format(start.context.span_id, "016x"))


def test_next_fire_time_for_a_cron_schedule_is_local_to_its_timezone(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(
            service, schedule_type="cron", expression="0 9 * * *", timezone_name="America/Los_Angeles"
        )

        winter = service.next_fire_time(schedule, now=datetime(2026, 1, 1, 12, 0, tzinfo=UTC))
        at_fire = service.next_fire_time(schedule, now=datetime(2026, 1, 1, 17, 0, tzinfo=UTC))
        summer = service.next_fire_time(schedule, now=datetime(2026, 7, 1, 12, 0, tzinfo=UTC))

        assert winter == datetime(2026, 1, 1, 17, 0, tzinfo=UTC)
        assert winter.utcoffset() == timedelta(hours=-8)
        assert at_fire == datetime(2026, 1, 2, 17, 0, tzinfo=UTC)
        assert summer == datetime(2026, 7, 1, 16, 0, tzinfo=UTC)
        assert summer.utcoffset() == timedelta(hours=-7)


def test_next_fire_time_for_an_interval_schedule_follows_the_created_at_grid(fresh_db: Path) -> None:
    created = NOW + timedelta(seconds=7)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(service, expression="minutes=15", timezone_name="America/Los_Angeles")
        schedule.created_at = created
        session.flush()

        before_creation = service.next_fire_time(schedule, now=created - timedelta(hours=1))
        between = service.next_fire_time(schedule, now=created + timedelta(minutes=20))
        on_the_grid = service.next_fire_time(schedule, now=created + timedelta(minutes=30))

        assert before_creation == created + timedelta(minutes=15)
        assert between == created + timedelta(minutes=30)
        assert on_the_grid == created + timedelta(minutes=45)
        assert between.tzinfo == ZoneInfo("America/Los_Angeles")

        service.poll_due_schedules(now=created + timedelta(minutes=31))
        assert _occurrence_times(session) == [between]


def test_next_fire_time_for_a_date_schedule_is_none_once_it_passed(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = _echo_schedule(
            service, schedule_type="date", expression="2026-03-01T09:30:00", timezone_name="America/Los_Angeles"
        )

        upcoming = service.next_fire_time(schedule, now=datetime(2026, 2, 1, tzinfo=UTC))

        assert upcoming == datetime(2026, 3, 1, 17, 30, tzinfo=UTC)
        assert upcoming.utcoffset() == timedelta(hours=-8)
        assert service.next_fire_time(schedule, now=datetime(2026, 3, 2, tzinfo=UTC)) is None


def _invoke(*args: str):
    result = CliRunner().invoke(app, list(args))
    assert result.exit_code == 0, result.output
    return result


def test_schedule_cli_creates_shows_and_toggles_a_schedule() -> None:
    schedule_id = _invoke(
        "schedule-create",
        "CLI controlled",
        "--type",
        "interval",
        "--expr",
        "minutes=1",
        "--task",
        "Tick.",
        "--lane",
        "cli-lane",
        "--profile",
        "low",
        "--timezone",
        "UTC",
    ).stdout.strip()

    shown = json.loads(_invoke("schedule-show", schedule_id, "--json").output)
    assert shown["payload"] == {"title": "CLI controlled", "task_instruction": "Tick.", "lane": "cli-lane"}
    assert shown["runtime_contract"] == {"model_profile": "low"}

    assert "CLI controlled: disabled" in _invoke("schedule-disable", schedule_id).output
    with session_scope() as session:
        assert session.get(Schedule, schedule_id).enabled is False
    assert "CLI controlled: enabled" in _invoke("schedule-enable", "CLI controlled").output
    with session_scope() as session:
        assert session.get(Schedule, schedule_id).enabled is True


def test_schedule_cli_edits_fires_and_deletes_a_schedule() -> None:
    schedule_id = _invoke(
        "schedule-create", "CLI fire", "--type", "interval", "--expr", "minutes=10", "--task", "Original."
    ).stdout.strip()

    _invoke("schedule-edit", schedule_id, "--name", "CLI edited", "--expr", "minutes=5", "--task", "Edited.")
    fired = _invoke("schedule-fire-now", schedule_id[:8])
    refused = CliRunner().invoke(app, ["schedule-delete", schedule_id])
    _invoke("schedule-delete", schedule_id, "--yes")

    assert "work item:" in fired.output
    assert refused.exit_code == 1
    with session_scope() as session:
        assert session.get(Schedule, schedule_id) is None
        work = session.scalar(select(WorkItem).where(WorkItem.schedule_id == schedule_id))
        assert work.task_instruction == "Edited."
        assert work.lane == "CLI edited"
