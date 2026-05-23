from __future__ import annotations

from pathlib import Path

from sqlalchemy import func, inspect, select

from tasque2.db import create_schema, get_engine, reset_engine, session_scope
from tasque2.migrations import schema_status, upgrade_database
from tasque2.models import Artifact, WorkEvent, WorkItem
from tasque2.repo import WorkRepository


def test_create_work_item_persists_and_emits_event(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Write status report",
            task_instruction="Summarize the current project state.",
            worker_kind="manual",
            context={"project": "tasque2"},
            idempotency_key="status-report-1",
        )
        work_id = work.id

    reset_engine()
    create_schema()

    with session_scope() as session:
        work = session.get(WorkItem, work_id)
        assert work is not None
        assert work.title == "Write status report"
        assert work.context == {"project": "tasque2"}
        assert work.status == "ready"

        events = session.scalars(
            select(WorkEvent).where(WorkEvent.work_item_id == work_id)
        ).all()
        assert [event.event_type for event in events] == ["work.created"]
        assert events[0].payload["worker_kind"] == "manual"


def test_idempotency_key_reuses_existing_work_item(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        first = repo.create_work_item(
            title="First",
            task_instruction="Do the first thing.",
            worker_kind="manual",
            idempotency_key="same-key",
        )
        second = repo.create_work_item(
            title="Second",
            task_instruction="Do the second thing.",
            worker_kind="manual",
            idempotency_key="same-key",
        )

        assert second.id == first.id
        assert session.scalar(select(func.count()).select_from(WorkItem)) == 1
        assert session.scalar(select(func.count()).select_from(WorkEvent)) == 1


def test_event_timeline_is_ordered(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Timeline",
            task_instruction="Exercise event ordering.",
            worker_kind="manual",
        )
        repo.emit_event(
            event_type="work.note",
            entity_kind="work_item",
            entity_id=work.id,
            work_item_id=work.id,
            summary="First note",
        )
        repo.emit_event(
            event_type="work.ready",
            entity_kind="work_item",
            entity_id=work.id,
            work_item_id=work.id,
            summary="Ready",
        )

        event_types = [event.event_type for event in repo.list_events_for_work(work.id)]

    assert event_types == ["work.created", "work.note", "work.ready"]


def test_record_artifact_associates_with_work_item(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Artifact source",
            task_instruction="Create a report artifact.",
            worker_kind="manual",
        )
        artifact = repo.record_artifact(
            kind="report",
            title="Run report",
            local_path="artifacts/run-report.md",
            work_item_id=work.id,
            tags=["report", "phase1"],
        )
        artifact_id = artifact.id

    with session_scope() as session:
        artifact = session.get(Artifact, artifact_id)
        assert artifact is not None
        assert artifact.work_item_id == work.id
        assert artifact.tags == ["report", "phase1"]

        events = session.scalars(
            select(WorkEvent).where(WorkEvent.work_item_id == work.id)
        ).all()
        assert [event.event_type for event in events] == [
            "work.created",
            "artifact.recorded",
        ]


def test_sqlite_pragmas_enable_foreign_keys_and_wal(fresh_db: Path) -> None:
    with get_engine().connect() as connection:
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        assert connection.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"


def test_alembic_upgrade_creates_core_schema(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "migrated.sqlite3"
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()

    status = upgrade_database()
    reset_engine()

    engine = get_engine()
    table_names = set(inspect(engine).get_table_names())
    schedule_occurrence_columns = {
        column["name"] for column in inspect(engine).get_columns("schedule_occurrences")
    }
    assert status.is_current
    assert schema_status().is_current
    assert {
        "alembic_version",
        "artifacts",
        "discord_messages",
        "discord_threads",
        "failed_work",
        "memories",
        "provider_runs",
        "schedule_occurrences",
        "schedules",
        "work_attempts",
        "work_dependencies",
        "work_events",
        "work_items",
        "workflow_definitions",
        "workflow_edges",
        "workflow_nodes",
        "workflow_runs",
        "memory_fts",
    }.issubset(table_names)
    assert {"work_item_id", "workflow_run_id"}.issubset(schedule_occurrence_columns)
    assert "project_id" not in schedule_occurrence_columns
