from __future__ import annotations

from pathlib import Path

from sqlalchemy import select

from tasque2.artifacts import ArtifactStore
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.memory_ingest import MemoryIngestService
from tasque2.models import Memory


def test_memory_create_and_search_by_text_namespace_and_tag(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.create_memory(
            namespace="project:alpha",
            kind="preference",
            content="Prefer concise status updates for this project.",
            tags=["preference", "status"],
        )
        service.create_memory(
            namespace="project:beta",
            kind="note",
            content="A separate beta note.",
            tags=["note"],
        )

        results = service.search(
            query="concise",
            namespace="project:alpha",
            tags=["status"],
        )

        assert len(results) == 1
        assert results[0].kind == "preference"


def test_memory_create_mirrors_to_markdown_vault(fresh_db: Path) -> None:
    with session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace="project:alpha",
            kind="note",
            content="Mirror this useful note.",
            tags=["vault"],
            canonical_key="current-note",
        )

        vault_files = list((fresh_db.parent / "data" / "memory-vault").rglob("*.md"))
        assert vault_files
        assert any(memory.id in path.read_text(encoding="utf-8") for path in vault_files)
        assert any("Mirror this useful note." in path.read_text(encoding="utf-8") for path in vault_files)


def test_memory_ingest_text_creates_searchable_summary_and_chunks(fresh_db: Path) -> None:
    with session_scope() as session:
        result = MemoryIngestService(session).ingest_text(
            namespace="creative",
            title="Practice notes",
            content="Gesture drawing needs cleaner shoulder rhythm.\n\nTry canine muzzle construction.",
            source_kind="test_source",
            source_id="practice-1",
            tags=["art"],
        )

        assert len(result.memory_ids) == 2
        found = MemoryService(session).search(query="canine muzzle", namespace="creative")
        assert any(memory.kind == "source_chunk" for memory in found)


def test_memory_ingest_artifact_skips_binary_and_ingests_text(fresh_db: Path) -> None:
    with session_scope() as session:
        text_artifact = ArtifactStore().write_text(
            session,
            kind="worker_report",
            title="Report",
            content="The provider produced an actionable report.",
            tags=["report"],
        )
        binary_artifact = ArtifactStore().write_bytes(
            session,
            kind="worker_file",
            title="image.bin",
            content=b"\x00\x01\x02",
            content_type="application/octet-stream",
        )

        ingested = MemoryIngestService(session).ingest_artifact(text_artifact.id, namespace="global")
        skipped = MemoryIngestService(session).ingest_artifact(binary_artifact.id, namespace="global")

        assert ingested is not None
        assert skipped is None
        assert MemoryService(session).search(query="actionable report")


def test_memory_archive_removes_from_search(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(
            namespace="global",
            kind="note",
            content="Archive me later.",
            tags=["cleanup"],
        )

        assert service.search(query="archive")
        service.archive_memory(memory.id)

        assert service.search(query="archive") == []
        assert session.get(Memory, memory.id).archived_at is not None


def test_memory_supersede_keeps_replacement_searchable(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        old = service.create_memory(
            namespace="global",
            kind="preference",
            content="Use verbose reports.",
            tags=["style"],
            canonical_key="report-style",
        )

        new = service.supersede_memory(
            old.id,
            content="Use concise reports.",
        )

        assert session.get(Memory, old.id).superseded_by == new.id
        assert service.search(query="verbose") == []
        results = service.search(query="concise", namespace="global")
        assert [memory.id for memory in results] == [new.id]


def test_memory_rows_persist_without_query_search(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.create_memory(namespace="global", kind="note", content="Pinned", pinned=True)
        service.create_memory(namespace="global", kind="note", content="Normal")

        results = service.search(namespace="global")

        assert [memory.content for memory in results][:2] == ["Pinned", "Normal"]
        assert session.scalar(select(Memory).where(Memory.content == "Pinned")) is not None


def test_memory_canonical_upsert_archives_previous_value(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        old = service.upsert_canonical(
            namespace="health",
            canonical_key="current_workout_state",
            kind="summary",
            content="old workout state",
            tags=["workout"],
        )
        new = service.upsert_canonical(
            namespace="health",
            canonical_key="current_workout_state",
            kind="summary",
            content="new workout state",
            tags=["workout"],
        )

        assert service.get_canonical(
            namespace="health",
            canonical_key="current_workout_state",
        ).id == new.id
        assert session.get(Memory, old.id).archived_at is not None
        assert session.get(Memory, old.id).superseded_by == new.id
