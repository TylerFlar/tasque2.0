from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import func, select

from tasque2.config import reset_settings
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.models import Memory, MemoryEmbedding, utc_now
from tasque2.work.repository import WorkRepository
from tasque2.worker.context import WorkerContextBuilder


def _embedding_count(session) -> int:
    return session.scalar(select(func.count()).select_from(MemoryEmbedding))


def test_create_memory_writes_an_embedding_row(fresh_db: Path) -> None:
    with session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace="local", kind="fact", content="He loves beginner cooking classes."
        )
        embedding = session.get(MemoryEmbedding, memory.id)
        assert embedding is not None
        assert embedding.namespace == "local"
        assert embedding.model == "hash-256"
        assert embedding.dim == 256


def test_update_memory_refreshes_the_embedding(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="local", kind="fact", content="archery bow shooting range")
        before = session.get(MemoryEmbedding, memory.id).vector

        service.update_memory(memory.id, content="oil painting figure drawing")

        assert session.get(MemoryEmbedding, memory.id).vector != before
        assert _embedding_count(session) == 1


def test_recall_ranks_the_relevant_memory_first(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.create_memory(namespace="local", kind="fact", content="archery bow shooting range")
        service.create_memory(namespace="local", kind="fact", content="oil painting figure drawing")
        cooking = service.create_memory(namespace="local", kind="fact", content="beginner cooking class pasta workshop")

        scored = service.recall(query="cooking class", namespace="local", limit=3)

        assert scored[0].memory.id == cooking.id
        assert scored[0].score > scored[-1].score


def test_recall_ranks_vector_matches_when_keyword_search_finds_nothing(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        gym = service.create_memory(namespace="local", kind="fact", content="the gym is not near the office")
        archery = service.create_memory(namespace="local", kind="fact", content="archery bow shooting range")
        gym.created_at = archery.created_at = utc_now() - timedelta(days=1)
        session.flush()

        assert service.search(query="not near", namespace="local") == []
        scored = service.recall(query="not near", namespace="local", limit=2)
        assert scored[0].memory.id == gym.id


def test_recall_lifts_the_more_important_of_equal_matches(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        low = service.create_memory(
            namespace="local", kind="fact", content="weekly social run club meetup", importance=1
        )
        high = service.create_memory(
            namespace="local", kind="fact", content="weekly social run club meetup", importance=5
        )
        same_time = utc_now() - timedelta(days=1)
        low.created_at = high.created_at = same_time
        session.flush()

        scored = service.recall(query="social run club", namespace="local", limit=2)
        assert [item.memory.id for item in scored] == [high.id, low.id]


def test_recall_lifts_the_newer_of_equal_matches(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        newer = service.create_memory(namespace="local", kind="fact", content="weekly social run club meetup")
        older = service.create_memory(namespace="local", kind="fact", content="weekly social run club meetup")
        older.created_at = utc_now() - timedelta(days=30)
        session.flush()

        scored = service.recall(query="social run club", namespace="local", limit=2)
        assert [item.memory.id for item in scored] == [newer.id, older.id]


def test_recall_respects_namespace_tags_and_archive(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        wanted = service.create_memory(namespace="local", kind="fact", content="pasta class", tags=["cooking"])
        service.create_memory(namespace="local", kind="fact", content="pasta class untagged")
        service.create_memory(namespace="other", kind="fact", content="pasta class", tags=["cooking"])
        archived = service.create_memory(namespace="local", kind="fact", content="pasta class", tags=["cooking"])
        service.archive_memory(archived.id)

        scored = service.recall(query="pasta class", namespace="local", tags=["cooking"], limit=10)

        assert [item.memory.id for item in scored] == [wanted.id]


def test_recall_without_a_query_prefers_the_newest(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        first = service.create_memory(namespace="local", kind="fact", content="first fact")
        latest = service.create_memory(namespace="local", kind="fact", content="second fact")
        first.created_at = utc_now() - timedelta(days=1)
        session.flush()

        scored = service.recall(query=None, namespace="local", limit=1)

        assert [item.memory.id for item in scored] == [latest.id]


def test_recall_falls_back_to_keyword_ranking_without_embeddings(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_MEMORY_HYBRID_RETRIEVAL", "false")
    reset_settings()
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="local", kind="fact", content="beginner cooking class pasta")

        scored = service.recall(query="cooking", namespace="local", limit=5)

        assert [item.memory.id for item in scored] == [memory.id]
        assert _embedding_count(session) == 0


def test_recall_survives_an_embedder_that_fails(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class BrokenEmbedder:
        name = "broken"
        dim = 8

        def embed(self, texts: list[str]) -> list[list[float]]:
            raise ConnectionError("embedding service unreachable")

    monkeypatch.setattr("tasque2.memory.service.get_embedder", lambda: BrokenEmbedder())
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="local", kind="fact", content="beginner cooking class pasta")

        assert _embedding_count(session) == 0
        assert [item.memory.id for item in service.recall(query="cooking", namespace="local")] == [memory.id]


def test_embed_missing_backfills_across_batches(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_MEMORY_HYBRID_RETRIEVAL", "false")
    reset_settings()
    with session_scope() as session:
        service = MemoryService(session)
        for index in range(5):
            service.create_memory(namespace="local", kind="fact", content=f"distinct fact number {index}")
        assert service.embed_missing() == 0
        assert _embedding_count(session) == 0

    monkeypatch.setenv("TASQUE2_MEMORY_HYBRID_RETRIEVAL", "true")
    reset_settings()
    with session_scope() as session:
        service = MemoryService(session)
        first = service.embed_missing(namespace="local", limit=2)
        rest = service.embed_missing(namespace="local", limit=50)

        assert first == 2
        assert first + rest == 5
        assert _embedding_count(session) == 5
        assert service.embed_missing(namespace="local") == 0


def test_embed_missing_reembeds_rows_from_another_model(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with session_scope() as session:
        MemoryService(session).create_memory(namespace="local", kind="fact", content="hashed at 256 dimensions")

    monkeypatch.setenv("TASQUE2_EMBEDDING_DIM", "64")
    reset_settings()
    with session_scope() as session:
        assert MemoryService(session).embed_missing() == 1
        embedding = session.scalars(select(MemoryEmbedding)).one()
        assert (embedding.model, embedding.dim) == ("hash-64", 64)


def test_delete_memory_removes_its_embedding(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        memory = service.create_memory(namespace="local", kind="fact", content="stale fact to forget")
        assert session.get(MemoryEmbedding, memory.id) is not None

        service.delete_memory(memory.id)

        assert session.get(Memory, memory.id) is None
        assert session.get(MemoryEmbedding, memory.id) is None
        assert service.recall(query="stale fact", namespace="local") == []


def test_prune_superseded_removes_embeddings_of_pruned_rows(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        old = service.upsert_canonical(namespace="local", canonical_key="state", kind="summary", content="old state")
        service.upsert_canonical(namespace="local", canonical_key="state", kind="summary", content="new state")
        session.get(Memory, old.id).archived_at = utc_now() - timedelta(days=60)
        session.flush()

        assert service.prune_superseded(older_than_days=30) == 1
        assert session.get(MemoryEmbedding, old.id) is None
        assert _embedding_count(session) == 1


def test_worker_context_force_loads_memory_kinds_register(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        service.upsert_canonical(
            namespace="local",
            canonical_key="interest:cooking",
            kind="interest",
            content="topic: Cooking classes\ntier: core\nwant: beginner, hands-on",
        )
        service.upsert_canonical(
            namespace="local",
            canonical_key="interest:art",
            kind="interest",
            content="topic: Art classes\ntier: core\nwant: foundational drawing and oil",
        )
        service.create_memory(namespace="local", kind="working", content="an unrelated working note")
        work = WorkRepository(session).create_work_item(
            title="Watch run",
            task_instruction="Run the watch.",
            worker_kind="provider.default",
            context={"memory_namespace": "local", "memory_kinds": ["interest"]},
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    interests = [memory for memory in packet["memories"] if memory["kind"] == "interest"]
    assert len(interests) == 2
    blob = " ".join(memory["content"] for memory in interests)
    assert "Cooking classes" in blob and "Art classes" in blob


def test_worker_context_delivers_relevant_middle_of_large_ledger(fresh_db: Path) -> None:
    head = "# Interests\n" + ("alpha beta gamma delta epsilon zeta " * 220)
    target = (
        "## Cooking lane\nCOOKINGTARGETMARKER - he loves beginner cooking classes "
        "and pasta making workshops, group-expandable."
    )
    tail = "## Logistics\nTAILMARKER " + ("omega sigma tau upsilon phi chi " * 220)
    big_doc = "\n\n".join([head, target, tail])
    assert len(big_doc) > 12000

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
            context={"memory_namespace": "local", "memory_canonical_keys": ["local_interests"]},
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    delivered = next(memory for memory in packet["memories"] if memory["namespace"] == "local")
    assert "COOKINGTARGETMARKER" in delivered["content"]
    assert "TAILMARKER" not in delivered["content"]
    assert delivered["content_compacted"] is True
    assert delivered["content_chars"] == len(big_doc)
