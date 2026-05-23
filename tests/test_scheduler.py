from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.db import session_scope
from tasque2.models import Schedule, ScheduleOccurrence, WorkflowRun, WorkItem
from tasque2.scheduler import ScheduleService
from tasque2.workflows import WorkflowService


def test_date_schedule_enqueues_work_once(fresh_db: Path) -> None:
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    with session_scope() as session:
        service = ScheduleService(session)
        service.create_schedule(
            name="One shot",
            schedule_type="date",
            expression=(now - timedelta(minutes=1)).isoformat(),
            worker_kind="function.echo",
            payload={"title": "One shot work", "task_instruction": "Run once."},
            timezone_name="UTC",
        )

        assert service.poll_due_schedules(now=now) == 1
        assert service.poll_due_schedules(now=now + timedelta(minutes=1)) == 0

        assert session.scalar(select(func.count()).select_from(ScheduleOccurrence)) == 1
        work = session.scalar(select(WorkItem).where(WorkItem.title == "One shot work"))
        assert work is not None
        assert work.source_kind == "schedule"
        assert work.schedule_occurrence_id is not None


def test_schedule_can_load_task_instruction_from_template_file(
    fresh_db: Path,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    template = tmp_path / "scheduled.template.md"
    template.write_text("# Scheduled Template\n\nRun from Markdown.", encoding="utf-8")

    with session_scope() as session:
        service = ScheduleService(session)
        service.create_schedule(
            name="Templated schedule",
            schedule_type="date",
            expression=(now - timedelta(minutes=1)).isoformat(),
            worker_kind="function.echo",
            payload={
                "title": "Templated work",
                "task_template_path": "scheduled.template.md",
                "template_base_dir": str(tmp_path),
            },
            timezone_name="UTC",
        )

        assert service.poll_due_schedules(now=now) == 1
        work = session.scalar(select(WorkItem).where(WorkItem.title == "Templated work"))
        assert work is not None
        assert work.task_instruction == "# Scheduled Template\n\nRun from Markdown."


def test_interval_schedule_coalesces_missed_runs(fresh_db: Path) -> None:
    now = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = service.create_schedule(
            name="Every minute",
            schedule_type="interval",
            expression="minutes=1",
            worker_kind="function.echo",
            payload={"task_instruction": "Tick."},
            timezone_name="UTC",
            catchup_policy="coalesce",
        )
        schedule.created_at = now - timedelta(minutes=5)
        schedule.last_evaluated_at = now - timedelta(minutes=5)

        assert service.poll_due_schedules(now=now) == 1

        occurrence = session.scalar(select(ScheduleOccurrence))
        assert occurrence is not None
        assert occurrence.scheduled_for == now


def test_cron_schedule_enqueues_all_with_backfill_limit(fresh_db: Path) -> None:
    now = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = service.create_schedule(
            name="Cron minute",
            schedule_type="cron",
            expression="* * * * *",
            worker_kind="function.echo",
            payload={"task_instruction": "Cron tick."},
            timezone_name="UTC",
            catchup_policy="all",
            max_backfill=2,
        )
        schedule.created_at = now - timedelta(minutes=5)
        schedule.last_evaluated_at = now - timedelta(minutes=5)

        assert service.poll_due_schedules(now=now) == 2

        occurrences = session.scalars(
            select(ScheduleOccurrence).order_by(ScheduleOccurrence.scheduled_for)
        ).all()
        assert len(occurrences) == 2
        assert [occ.scheduled_for for occ in occurrences] == [
            now - timedelta(minutes=4),
            now - timedelta(minutes=3),
        ]


def test_misfire_grace_skips_old_occurrences(fresh_db: Path) -> None:
    now = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = service.create_schedule(
            name="Grace",
            schedule_type="interval",
            expression="minutes=1",
            worker_kind="function.echo",
            payload={"task_instruction": "Tick."},
            timezone_name="UTC",
            catchup_policy="all",
            misfire_grace_seconds=90,
        )
        schedule.created_at = now - timedelta(minutes=5)
        schedule.last_evaluated_at = now - timedelta(minutes=5)

        assert service.poll_due_schedules(now=now) == 2

        occurrences = session.scalars(
            select(ScheduleOccurrence).order_by(ScheduleOccurrence.scheduled_for)
        ).all()
        assert [occ.scheduled_for for occ in occurrences] == [
            now - timedelta(minutes=1),
            now,
        ]


def test_schedule_can_start_workflow_run_directly(fresh_db: Path) -> None:
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
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
        schedule = service.create_schedule(
            name="Start workflow",
            schedule_type="date",
            expression=(now - timedelta(minutes=1)).isoformat(),
            worker_kind="workflow",
            payload={
                "workflow_definition_id": workflow_definition.id,
                "run_name": "Scheduled workflow run",
                "input": {"source": "scheduler-test"},
            },
            timezone_name="UTC",
        )

        assert service.poll_due_schedules(now=now) == 1
        assert service.poll_due_schedules(now=now + timedelta(minutes=1)) == 0

        occurrence = session.scalar(
            select(ScheduleOccurrence).where(ScheduleOccurrence.schedule_id == schedule.id)
        )
        assert occurrence is not None
        assert occurrence.work_item_id is None
        assert occurrence.workflow_run_id is not None

        run = session.get(WorkflowRun, occurrence.workflow_run_id)
        assert run is not None
        assert run.name == "Scheduled workflow run"
        assert run.input["source"] == "scheduler-test"
        assert run.input["schedule_occurrence_id"] == occurrence.id


def test_disabled_schedule_does_not_enqueue_until_reenabled(fresh_db: Path) -> None:
    now = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = service.create_schedule(
            name="Controlled",
            schedule_type="interval",
            expression="minutes=1",
            worker_kind="function.echo",
            payload={"task_instruction": "Tick."},
            timezone_name="UTC",
            catchup_policy="coalesce",
        )
        schedule.created_at = now - timedelta(minutes=5)
        schedule.last_evaluated_at = now - timedelta(minutes=5)

        service.disable_schedule(schedule.id)
        assert service.poll_due_schedules(now=now) == 0
        assert session.scalar(select(func.count()).select_from(ScheduleOccurrence)) == 0

        service.enable_schedule(schedule.id, now=now)
        assert service.poll_due_schedules(now=now + timedelta(minutes=1)) == 1

        occurrence = session.scalar(select(ScheduleOccurrence))
        assert occurrence is not None
        assert occurrence.scheduled_for == now + timedelta(minutes=1)


def test_schedule_enable_disable_cli_commands(fresh_db: Path) -> None:
    with session_scope() as session:
        schedule = ScheduleService(session).create_schedule(
            name="CLI controlled",
            schedule_type="interval",
            expression="minutes=1",
            worker_kind="function.echo",
            payload={"task_instruction": "Tick."},
            timezone_name="UTC",
        )
        schedule_id = schedule.id

    runner = CliRunner()
    disabled = runner.invoke(app, ["schedule-disable", schedule_id])
    enabled = runner.invoke(app, ["schedule-enable", schedule_id])

    assert disabled.exit_code == 0
    assert "enabled=False" in disabled.output
    assert enabled.exit_code == 0
    assert "enabled=True" in enabled.output
    with session_scope() as session:
        assert session.get(Schedule, schedule_id).enabled is True


def test_schedule_fire_edit_and_delete_cli_commands(fresh_db: Path) -> None:
    with session_scope() as session:
        schedule = ScheduleService(session).create_schedule(
            name="CLI fire",
            schedule_type="interval",
            expression="minutes=10",
            worker_kind="function.echo",
            payload={"title": "CLI fire", "task_instruction": "Original."},
            timezone_name="UTC",
        )
        schedule_id = schedule.id

    runner = CliRunner()
    edited = runner.invoke(
        app,
        [
            "schedule-edit",
            schedule_id,
            "--name",
            "CLI edited",
            "--expr",
            "minutes=5",
            "--task",
            "Edited.",
        ],
    )
    fired = runner.invoke(app, ["schedule-fire-now", schedule_id])
    deleted = runner.invoke(app, ["schedule-delete", schedule_id])

    assert edited.exit_code == 0
    assert fired.exit_code == 0
    assert "work_item:" in fired.output
    assert deleted.exit_code == 0

    with session_scope() as session:
        assert session.get(Schedule, schedule_id) is None
        work = session.scalar(select(WorkItem).where(WorkItem.title == "CLI edited"))
        assert work is not None
        assert work.task_instruction == "Edited."
