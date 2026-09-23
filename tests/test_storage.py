from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import func, select

from tasque2.artifacts import ArtifactStore
from tasque2.db import create_schema, get_engine, reset_engine, session_scope
from tasque2.events import record_event
from tasque2.models import Artifact, WorkEvent, WorkItem
from tasque2.telemetry import span
from tasque2.work.repository import WorkRepository


def test_create_work_item_persists_and_emits_event(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
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

        events = session.scalars(select(WorkEvent).where(WorkEvent.work_item_id == work_id)).all()
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
        for event_type, summary in (("work.note", "First note"), ("work.ready", "Ready")):
            record_event(
                session,
                event_type=event_type,
                entity_kind="work_item",
                entity_id=work.id,
                work_item_id=work.id,
                source="test",
                summary=summary,
            )

        event_types = [event.event_type for event in repo.list_events_for_work(work.id)]

    assert event_types == ["work.created", "work.note", "work.ready"]


def test_record_event_adds_an_event_to_the_active_span(fresh_db: Path, spans: InMemorySpanExporter) -> None:
    with session_scope() as session, span("test.operation"):
        event = record_event(
            session,
            event_type="memory.created",
            entity_kind="memory",
            entity_id="memory-1",
            source="test",
            summary="Created memory in global",
            payload={"kind": "note"},
        )

    assert event.id is not None
    assert event.payload == {"kind": "note"}
    (finished,) = [item for item in spans.get_finished_spans() if item.name == "test.operation"]
    (span_event,) = finished.events
    assert span_event.name == "memory.created"
    assert dict(span_event.attributes) == {
        "tasque.entity.kind": "memory",
        "tasque.entity.id": "memory-1",
        "tasque.event.summary": "Created memory in global",
    }


def test_artifact_store_records_file_for_work_item(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Artifact source",
            task_instruction="Create a report artifact.",
            worker_kind="manual",
        )
        artifact = ArtifactStore().write_text(
            session,
            kind="report",
            title="Run report",
            content="# Report\n",
            suffix=".md",
            work_item_id=work.id,
            tags=["report", "phase1"],
        )
        artifact_id, work_id = artifact.id, work.id

    with session_scope() as session:
        artifact = session.get(Artifact, artifact_id)
        assert artifact is not None
        assert artifact.work_item_id == work_id
        assert artifact.tags == ["report", "phase1"]
        assert artifact.content_type == "text/markdown; charset=utf-8"
        path = Path(artifact.local_path)
        assert path.read_text(encoding="utf-8") == "# Report\n"
        assert path.suffix == ".md"
        assert artifact.size_bytes == len(b"# Report\n")
        assert artifact.sha256 == sha256(b"# Report\n").hexdigest()


def test_datetimes_are_stored_as_utc_and_read_back_aware(fresh_db: Path) -> None:
    pacific = timezone(timedelta(hours=-7))
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Timed",
            task_instruction="Wait.",
            worker_kind="manual",
            not_before=datetime(2026, 9, 22, 8, 30, tzinfo=pacific),
            deadline_at=datetime(2026, 9, 22, 20, 0),
        )
        work_id = work.id

    with session_scope() as session:
        work = session.get(WorkItem, work_id)
        assert work.not_before == datetime(2026, 9, 22, 15, 30, tzinfo=UTC)
        assert work.not_before.tzinfo == UTC
        assert work.deadline_at == datetime(2026, 9, 22, 20, 0, tzinfo=UTC)
        assert work.created_at.tzinfo == UTC

    with get_engine().connect() as connection:
        stored = connection.exec_driver_sql("SELECT not_before FROM work_items WHERE id = ?", (work_id,)).scalar()
    assert str(stored).startswith("2026-09-22 15:30:00")


def test_sqlite_pragmas_enable_foreign_keys_and_wal(fresh_db: Path) -> None:
    with get_engine().connect() as connection:
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        assert connection.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
        assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar() == 5000


def test_deleting_a_work_item_cascades_to_its_events_and_artifacts(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(title="Doomed", task_instruction="Go.", worker_kind="manual")
        ArtifactStore().write_text(session, kind="report", title="Report", content="x", work_item_id=work.id)
        work_id = work.id

    with get_engine().begin() as connection:
        connection.exec_driver_sql("DELETE FROM work_items WHERE id = ?", (work_id,))

    with session_scope() as session:
        assert session.scalar(select(func.count()).select_from(WorkEvent).where(WorkEvent.work_item_id == work_id)) == 0
        assert session.scalar(select(func.count()).select_from(Artifact).where(Artifact.work_item_id == work_id)) == 0
