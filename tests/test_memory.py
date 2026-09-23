from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.artifacts import ArtifactStore
from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.memory import MemoryBudgetExceeded, MemoryService, canonical_budget, expire_ttl_memories, safe_fts_query
from tasque2.memory.ingest import MemoryIngestService
from tasque2.models import Memory, WorkEvent, utc_now
from tasque2.work.repository import WorkRepository
from tasque2.worker.context import memory_data


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

        results = service.search(query="concise", namespace="project:alpha", tags=["status"])

        assert len(results) == 1
        assert results[0].kind == "preference"


def test_memory_search_escapes_date_like_free_text_for_fts(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(
            namespace="health",
            kind="workout_completion",
            content="Latest confirmed 2026-05-31 pull completion. 2026-06-01 completed as prescribed; next focus legs.",
            tags=["workout", "completion"],
        )

        results = service.search(
            query="latest confirmed 2026-05-31 pull completion 2026-06-01 completed as prescribed next focus legs",
            namespace="health",
            limit=48,
        )

        assert [item.id for item in results] == [memory.id]


def test_safe_fts_query_quotes_tokens_and_drops_operators() -> None:
    assert safe_fts_query('NOT "x" 2026-05-31 and a:b OR near') == '"2026" OR "05" OR "31"'
    assert safe_fts_query("finance: budget budget") == '"finance" OR "budget"'
    assert safe_fts_query("- : a") is None
    assert safe_fts_query(None) is None


def test_search_with_only_operator_text_returns_nothing(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.create_memory(namespace="global", kind="note", content="and or not")
        assert service.search(query="AND OR NOT") == []


def test_memory_create_mirrors_to_markdown_vault(fresh_db: Path) -> None:
    with session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace="project:alpha",
            kind="note",
            content="Mirror this useful note.",
            tags=["vault"],
            canonical_key="current-note",
        )

        path = get_settings().resolved_memory_vault_dir / "project-alpha" / "note" / "current-note.md"
        text = path.read_text(encoding="utf-8")
        assert memory.id in text
        assert text.rstrip().endswith("Mirror this useful note.")


def test_memory_writes_record_events(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="global", kind="note", content="Event trail.")
        service.update_memory(memory.id, tags=["edited"])
        service.archive_memory(memory.id)

        events = session.scalars(
            select(WorkEvent.event_type).where(WorkEvent.entity_id == memory.id).order_by(WorkEvent.id)
        ).all()
        assert events == ["memory.created", "memory.updated", "memory.archived"]


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
        assert result.skipped is False
        found = MemoryService(session).search(query="canine muzzle", namespace="creative")
        assert any(memory.kind == "source_chunk" for memory in found)
        assert all("ingested" in memory.tags for memory in found)


def test_memory_ingest_text_skips_a_source_already_ingested_unless_forced(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryIngestService(session)
        fields = {
            "namespace": "research",
            "title": "Paper",
            "source_kind": "test_source",
            "source_id": "paper-1",
        }
        first = service.ingest_text(content="Attention preserves provenance.", **fields)
        again = service.ingest_text(content="A different body.", **fields)
        forced = service.ingest_text(content="Retrieval needs provenance.", force=True, **fields)

        assert again.skipped is True
        assert again.reason == "already_ingested"
        assert again.memory_ids == first.memory_ids
        assert forced.skipped is False
        assert forced.source_memory_id != first.source_memory_id
        assert session.get(Memory, first.source_memory_id).superseded_by == forced.source_memory_id


def test_memory_ingest_text_chunks_long_content(fresh_db: Path) -> None:
    paragraphs = [f"Paragraph {index} " + ("detail " * 120) for index in range(12)]
    with session_scope() as session:
        result = MemoryIngestService(session).ingest_text(
            namespace="research",
            title="Long source",
            content="\n\n".join(paragraphs),
            source_kind="test_source",
            source_id="long-1",
            chunk_chars=2000,
        )

        chunks = [session.get(Memory, memory_id) for memory_id in result.chunk_memory_ids]
        assert len(chunks) == 6
        assert chunks[0].content.startswith("# Long source chunk 1/6")
        assert "Paragraph 11" in chunks[-1].content


def test_forced_reingest_of_a_shorter_source_archives_its_leftover_chunks(fresh_db: Path) -> None:
    long_body = "\n\n".join(f"Section {index} " + ("filler " * 100) for index in range(3))
    fields = {"namespace": "research", "title": "Draft", "source_kind": "test_source", "source_id": "draft-1"}
    with session_scope() as session:
        service = MemoryIngestService(session)
        first = service.ingest_text(content=long_body, chunk_chars=1000, **fields)
        second = service.ingest_text(content="A short final draft.", chunk_chars=1000, force=True, **fields)

        assert len(first.chunk_memory_ids) == 3
        assert len(second.chunk_memory_ids) == 1
        active = service.ingest_text(content="ignored", **fields)
        assert active.skipped is True
        assert active.chunk_memory_ids == second.chunk_memory_ids
        assert all(session.get(Memory, memory_id).archived_at is not None for memory_id in first.chunk_memory_ids)
        assert MemoryService(session).search(query="filler", namespace="research") == []


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


def test_memory_ingest_artifact_uses_the_owning_work_namespace(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Research",
            task_instruction="Read.",
            worker_kind="manual",
            context={"memory_namespace": "research"},
        )
        artifact = ArtifactStore().write_text(
            session, kind="worker_report", title="Findings", content="Sparse attention notes.", work_item_id=work.id
        )

        result = MemoryIngestService(session).ingest_artifact(artifact.id)

        summary = session.get(Memory, result.source_memory_id)
        assert summary.namespace == "research"
        assert summary.work_item_id == work.id
        assert "artifact" in summary.tags


def test_search_scopes_to_namespace_under_common_word_load(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        for index in range(40):
            service.create_memory(namespace="bulk", kind="note", content=f"social club meetup number {index}")
        target = service.create_memory(namespace="scout", kind="fact", content="social club he would enjoy")

        results = service.search(query="social club", namespace="scout", limit=5)
        assert [memory.id for memory in results] == [target.id]


def test_memory_archive_removes_from_search(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="global", kind="note", content="Archive me later.", tags=["cleanup"])

        assert service.search(query="archive")
        service.archive_memory(memory.id)

        assert service.search(query="archive") == []
        assert session.get(Memory, memory.id).archived_at is not None


def test_search_without_query_lists_pinned_first_then_newest(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.create_memory(namespace="global", kind="note", content="Pinned", pinned=True)
        older = service.create_memory(namespace="global", kind="note", content="Older")
        service.create_memory(namespace="global", kind="note", content="Newest")
        older.created_at = utc_now() - timedelta(minutes=5)
        session.flush()

        results = service.search(namespace="global")

        assert [memory.content for memory in results] == ["Pinned", "Newest", "Older"]


def test_list_active_by_kind_returns_the_whole_register(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.upsert_canonical(namespace="local", canonical_key="interest:b", kind="interest", content="B")
        service.upsert_canonical(namespace="local", canonical_key="interest:a", kind="interest", content="A")
        service.create_memory(namespace="local", kind="note", content="unrelated")
        archived = service.create_memory(namespace="local", kind="interest", content="gone", canonical_key="interest:c")
        service.archive_memory(archived.id)

        register = service.list_active_by_kind(namespace="local", kind="interest")
        assert [memory.canonical_key for memory in register] == ["interest:a", "interest:b"]


def test_canonical_upsert_archives_the_previous_value(fresh_db: Path) -> None:
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

        assert service.get_canonical(namespace="health", canonical_key="current_workout_state").id == new.id
        assert session.get(Memory, old.id).archived_at is not None
        assert session.get(Memory, old.id).superseded_by == new.id


def test_canonical_upsert_keeps_only_the_replacement_searchable(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.upsert_canonical(
            namespace="global", canonical_key="report-style", kind="preference", content="Use verbose reports."
        )
        new = service.upsert_canonical(
            namespace="global", canonical_key="report-style", kind="preference", content="Use concise reports."
        )

        assert service.search(query="verbose") == []
        assert [memory.id for memory in service.search(query="concise", namespace="global")] == [new.id]


def test_update_memory_edits_in_place_and_reindexes(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="local", kind="fact", content="He is interested in archery.")
        service.update_memory(memory.id, content="He took up fencing instead.", tags=["hobby"], importance=4)

        rows = session.scalars(select(Memory).where(Memory.namespace == "local")).all()
        assert [row.id for row in rows] == [memory.id]
        assert rows[0].archived_at is None
        assert rows[0].content == "He took up fencing instead."
        assert rows[0].tags == ["hobby"]
        assert rows[0].importance == 4
        assert service.search(query="fencing", namespace="local")
        assert service.search(query="archery", namespace="local") == []


def test_delete_memory_removes_the_row_and_its_search_entry(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="local", kind="fact", content="stale fact to forget")

        service.delete_memory(memory.id)

        assert session.get(Memory, memory.id) is None
        assert service.search(query="stale", namespace="local") == []
        with pytest.raises(KeyError):
            service.delete_memory(memory.id)


def test_prune_superseded_deletes_old_archived_rows(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        old = service.upsert_canonical(namespace="local", canonical_key="state", kind="summary", content="old state")
        middle = service.upsert_canonical(namespace="local", canonical_key="state", kind="summary", content="mid state")
        service.upsert_canonical(namespace="local", canonical_key="state", kind="summary", content="new state")
        session.get(Memory, old.id).archived_at = utc_now() - timedelta(days=60)
        session.flush()

        assert service.prune_superseded(older_than_days=30) == 1
        assert session.get(Memory, old.id) is None
        assert session.get(Memory, middle.id) is not None
        assert service.get_canonical(namespace="local", canonical_key="state").content == "new state"


def test_expire_ttl_memories_archives_only_elapsed_plain_rows(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        elapsed = service.create_memory(namespace="global", kind="note", content="short lived", ttl_days=1)
        fresh = service.create_memory(namespace="global", kind="note", content="still fresh", ttl_days=30)
        pinned = service.create_memory(namespace="global", kind="note", content="pinned", ttl_days=1, pinned=True)
        canonical = service.upsert_canonical(
            namespace="global", canonical_key="doc", kind="summary", content="doc", ttl_days=1
        )

        assert expire_ttl_memories(session, now=utc_now() + timedelta(days=2)) == 1
        assert session.get(Memory, elapsed.id).archived_at is not None
        for memory in (fresh, pinned, canonical):
            assert session.get(Memory, memory.id).archived_at is None


def test_canonical_budget_reads_the_marker() -> None:
    assert canonical_budget("body\n<!-- tasque:max_chars=1500 -->") == 1500
    assert canonical_budget("body\n<!--tasque:max_chars = 900-->") == 900
    assert canonical_budget("no marker") is None
    assert canonical_budget(None) is None


def test_canonical_budget_refuses_oversized_write(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.upsert_canonical(
            namespace="health",
            canonical_key="health_state",
            kind="summary",
            content="last_run: 2026-07-24\n\n<!-- tasque:max_chars=200 -->\n",
        )

        with pytest.raises(MemoryBudgetExceeded) as excinfo:
            service.upsert_canonical(
                namespace="health",
                canonical_key="health_state",
                kind="summary",
                content="x" * 500 + "\n<!-- tasque:max_chars=200 -->\n",
            )

        assert excinfo.value.max_chars == 200
        assert excinfo.value.canonical_key == "health_state"
        assert "Compact it" in str(excinfo.value)
        current = service.get_canonical(namespace="health", canonical_key="health_state")
        assert current.content.startswith("last_run: 2026-07-24")


def test_canonical_budget_is_carried_forward_when_a_rewrite_drops_the_marker(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.upsert_canonical(
            namespace="career",
            canonical_key="career_state",
            kind="summary",
            content="pointer\n\n<!-- tasque:max_chars=300 -->\n",
        )

        rewritten = service.upsert_canonical(
            namespace="career",
            canonical_key="career_state",
            kind="summary",
            content="a fresh pointer with no marker",
        )
        assert canonical_budget(rewritten.content) == 300

        with pytest.raises(MemoryBudgetExceeded):
            service.upsert_canonical(
                namespace="career", canonical_key="career_state", kind="summary", content="y" * 400
            )


def test_canonical_writes_without_a_marker_are_unbounded(fresh_db: Path) -> None:
    with session_scope() as session:
        memory = MemoryService(session).upsert_canonical(
            namespace="cooking", canonical_key="cooking_library", kind="summary", content="z" * 40_000
        )
        assert len(memory.content) == 40_000
        assert canonical_budget(memory.content) is None


def test_allow_over_budget_attaches_a_marker_to_an_oversized_doc(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.upsert_canonical(namespace="career", canonical_key="career_state", kind="summary", content="w" * 5_000)

        marked = service.upsert_canonical(
            namespace="career",
            canonical_key="career_state",
            kind="summary",
            content="w" * 5_000 + "\n<!-- tasque:max_chars=800 -->\n",
            allow_over_budget=True,
        )
        assert canonical_budget(marked.content) == 800

        delivered = memory_data(marked)
        assert delivered["over_budget"] is True
        assert delivered["max_chars"] == 800
        assert "Rewrite it inside the budget" in delivered["compact_this_run"]
        with pytest.raises(MemoryBudgetExceeded):
            service.upsert_canonical(
                namespace="career", canonical_key="career_state", kind="summary", content="w" * 4_000
            )

        compacted = service.upsert_canonical(
            namespace="career",
            canonical_key="career_state",
            kind="summary",
            content="pointer only\n<!-- tasque:max_chars=800 -->\n",
        )
        assert canonical_budget(compacted.content) == 800
        assert "over_budget" not in memory_data(compacted)


def test_update_memory_refuses_content_over_the_canonical_budget(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.upsert_canonical(
            namespace="health",
            canonical_key="health_state",
            kind="summary",
            content="compact\n<!-- tasque:max_chars=200 -->\n",
        )

        with pytest.raises(MemoryBudgetExceeded) as excinfo:
            service.update_memory(memory.id, content="x" * 500 + "\n<!-- tasque:max_chars=200 -->\n")

        assert excinfo.value.size > 500
        assert session.get(Memory, memory.id).content.startswith("compact")


def test_update_memory_carries_the_budget_marker_forward(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.upsert_canonical(
            namespace="health",
            canonical_key="health_state",
            kind="summary",
            content="compact\n<!-- tasque:max_chars=200 -->\n",
        )

        updated = service.update_memory(memory.id, content="rewritten without the marker")
        assert updated.content.startswith("rewritten without the marker")
        assert canonical_budget(updated.content) == 200

        with pytest.raises(MemoryBudgetExceeded):
            service.update_memory(memory.id, content="y" * 300)


def test_update_memory_leaves_non_canonical_rows_unbounded(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="global", kind="note", content="note\n<!-- tasque:max_chars=100 -->")

        updated = service.update_memory(memory.id, content="n" * 500 + "\n<!-- tasque:max_chars=100 -->")

        assert len(updated.content) > 500


def test_memory_events_link_only_existing_work_items(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(title="Writer", task_instruction="Write.", worker_kind="manual")
        service = MemoryService(session)
        linked = service.create_memory(namespace="global", kind="note", content="linked", work_item_id=work.id)
        dangling = service.create_memory(namespace="global", kind="note", content="dangling", work_item_id="gone")

        linked_event = session.scalar(select(WorkEvent).where(WorkEvent.entity_id == linked.id))
        dangling_event = session.scalar(select(WorkEvent).where(WorkEvent.entity_id == dangling.id))
        assert linked_event.work_item_id == work.id
        assert dangling_event.work_item_id is None
        assert dangling.work_item_id == "gone"
