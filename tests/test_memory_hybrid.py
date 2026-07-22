from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from sqlalchemy import func, select

from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.models import Memory, MemoryEmbedding, utc_now
from tasque2.repo import WorkRepository
from tasque2.worker_context import WorkerContextBuilder


def test_create_memory_writes_an_embedding_row(fresh_db: Path) -> None:
    with session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace="local",
            kind="fact",
            content="He loves beginner cooking classes.",
        )
        embedding = session.get(MemoryEmbedding, memory.id)
        assert embedding is not None
        assert embedding.namespace == "local"
        assert embedding.dim > 0


def test_search_hybrid_ranks_relevant_item_first(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.create_memory(namespace="local", kind="fact", content="archery bow shooting range")
        service.create_memory(namespace="local", kind="fact", content="oil painting figure drawing")
        cooking = service.create_memory(
            namespace="local", kind="fact", content="beginner cooking class pasta workshop"
        )

        scored = service.search_hybrid(query="cooking class", namespace="local", limit=3)
        assert scored
        assert scored[0].memory.id == cooking.id


def test_search_hybrid_importance_and_recency_lift_ranking(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        # Identical content -> same lexical+semantic relevance; ranking must come
        # from importance + recency. The salient, newer item should win.
        low = service.create_memory(namespace="local", kind="fact", content="weekly social run club meetup")
        high = service.create_memory(
            namespace="local", kind="fact", content="weekly social run club meetup"
        )
        service.update_memory(low.id, importance=1)
        service.update_memory(high.id, importance=5)

        scored = service.search_hybrid(query="social run club", namespace="local", limit=2)
        assert scored[0].memory.id == high.id


def test_search_hybrid_degrades_to_keyword_without_embeddings(
    fresh_db: Path, monkeypatch
) -> None:
    monkeypatch.setenv("TASQUE2_MEMORY_HYBRID_RETRIEVAL", "false")
    from tasque2.config import reset_settings

    reset_settings()
    with session_scope() as session:
        service = MemoryService(session)
        service.create_memory(namespace="local", kind="fact", content="beginner cooking class pasta")
        scored = service.search_hybrid(query="cooking", namespace="local", limit=5)
        assert scored  # still returns results via FTS, no embeddings required
    reset_settings()


def test_embed_missing_backfills_across_batches(fresh_db: Path, monkeypatch) -> None:
    from tasque2.config import reset_settings

    # Create rows with embeddings disabled, so they need backfilling.
    monkeypatch.setenv("TASQUE2_MEMORY_HYBRID_RETRIEVAL", "false")
    reset_settings()
    with session_scope() as session:
        service = MemoryService(session)
        for index in range(5):
            service.create_memory(namespace="local", kind="fact", content=f"distinct fact number {index}")
        assert session.scalar(select(func.count()).select_from(MemoryEmbedding)) == 0

    monkeypatch.setenv("TASQUE2_MEMORY_HYBRID_RETRIEVAL", "true")
    reset_settings()
    with session_scope() as session:
        service = MemoryService(session)
        # Small batch first must make progress, not re-scan the same window.
        first = service.embed_missing(namespace="local", limit=2)
        assert first == 2
        rest = service.embed_missing(namespace="local", limit=50)
        assert first + rest == 5
        assert session.scalar(select(func.count()).select_from(MemoryEmbedding)) == 5
    reset_settings()


def test_update_memory_is_in_place_no_new_row(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(
            namespace="local", kind="fact", content="He is interested in archery."
        )
        service.update_memory(memory.id, content="He is no longer interested in archery.")

        rows = session.scalars(select(Memory).where(Memory.namespace == "local")).all()
        assert len(rows) == 1
        assert rows[0].id == memory.id
        assert rows[0].archived_at is None
        assert "no longer" in rows[0].content
        # The new content is searchable; the old wording is not.
        assert service.search(query="longer", namespace="local")


def test_delete_memory_hard_removes_row_and_embedding(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="local", kind="fact", content="stale fact to forget")
        memory_id = memory.id
        assert session.get(MemoryEmbedding, memory_id) is not None

        service.delete_memory(memory_id)
        assert session.get(Memory, memory_id) is None
        assert session.get(MemoryEmbedding, memory_id) is None
        assert service.search(query="stale", namespace="local") == []


def test_prune_superseded_hard_deletes_old_archived_rows(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        old = service.upsert_canonical(
            namespace="local", canonical_key="state", kind="summary", content="old state"
        )
        service.upsert_canonical(
            namespace="local", canonical_key="state", kind="summary", content="new state"
        )
        archived = session.get(Memory, old.id)
        archived.archived_at = utc_now() - timedelta(days=60)
        session.flush()

        removed = service.prune_superseded(older_than_days=30)
        assert removed == 1
        assert session.get(Memory, old.id) is None
        # The active canonical survives.
        assert service.get_canonical(namespace="local", canonical_key="state").content == "new state"


def test_worker_context_force_loads_memory_kinds_register(fresh_db: Path) -> None:
    with session_scope() as session:
        svc = MemoryService(session)
        svc.upsert_canonical(
            namespace="local", canonical_key="interest:cooking", kind="interest",
            content="topic: Cooking classes\ntier: core\nwant: beginner, hands-on",
        )
        svc.upsert_canonical(
            namespace="local", canonical_key="interest:art", kind="interest",
            content="topic: Art classes\ntier: core\nwant: foundational drawing and oil",
        )
        svc.create_memory(namespace="local", kind="working", content="an unrelated working note")
        work = WorkRepository(session).create_work_item(
            title="Watch run",
            task_instruction="Run the watch.",
            worker_kind="provider.default",
            context={"memory_namespace": "local", "memory_kinds": ["interest"]},
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    interests = [m for m in packet["memories"] if m["kind"] == "interest"]
    assert len(interests) == 2  # the whole register, regardless of the run query
    blob = " ".join(m["content"] for m in interests)
    assert "Cooking classes" in blob and "Art classes" in blob


def test_worker_context_delivers_relevant_middle_of_large_ledger(fresh_db: Path) -> None:
    head = "# Interests\n" + ("alpha beta gamma delta epsilon zeta " * 220)
    target = (
        "## Cooking lane\nCOOKINGTARGETMARKER — he loves beginner cooking classes "
        "and pasta making workshops, group-expandable."
    )
    tail = "## Logistics\nTAILMARKER " + ("omega sigma tau upsilon phi chi " * 220)
    big_doc = "\n\n".join([head, target, tail])
    assert len(big_doc) > 12000  # must exceed the pinned budget so excerpting kicks in

    with session_scope() as session:
        MemoryService(session).upsert_canonical(
            namespace="local",
            canonical_key="local_interests",
            kind="summary",
            content=big_doc,
            pinned=True,
        )
        work = WorkRepository(session).create_work_item(
            title="Find beginner cooking classes",
            task_instruction="Surface beginner cooking workshops he could make a hangout of.",
            worker_kind="provider.default",
            context={
                "memory_namespace": "local",
                "memory_canonical_keys": ["local_interests"],
            },
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    delivered = next(m for m in packet["memories"] if m["namespace"] == "local")
    # The relevant middle survives (old head+tail truncation would have dropped it)...
    assert "COOKINGTARGETMARKER" in delivered["content"]
    # ...and the irrelevant tail filler is excerpted away.
    assert "TAILMARKER" not in delivered["content"]
    assert delivered["content_compacted"] is True
