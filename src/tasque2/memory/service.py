from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.events import record_event
from tasque2.memory.embeddings import get_embedder, pack_vector, top_k_by_vector
from tasque2.memory.vault import mirror_memory
from tasque2.models import Memory, MemoryEmbedding, WorkItem, utc_now
from tasque2.telemetry import instruments

CANONICAL_BUDGET_RE = re.compile(r"<!--\s*tasque:max_chars\s*=\s*(\d{2,7})\s*-->")
_FTS_TOKEN_RE = re.compile(r"[\w]+", re.UNICODE)
_FTS_OPERATOR_WORDS = {"and", "or", "not", "near"}
_FTS_MAX_TOKENS = 24
_RRF_K = 60.0

FTS_DDL = """
CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts
USING fts5(memory_id UNINDEXED, namespace, kind, content, tags)
"""


@dataclass(frozen=True)
class ScoredMemory:
    """A memory with its fused retrieval score (higher is more relevant)."""

    memory: Memory
    score: float


class MemoryBudgetExceeded(ValueError):
    """A canonical write is larger than the budget its document declares."""

    def __init__(self, *, canonical_key: str, size: int, max_chars: int) -> None:
        self.canonical_key = canonical_key
        self.size = size
        self.max_chars = max_chars
        super().__init__(
            f"{canonical_key} is {size} characters against its declared budget of {max_chars}. "
            "Compact it: drop what is stale, move detail into the ledger that owns it, keep the "
            "marker line, then write again."
        )


def canonical_budget(content: str | None) -> int | None:
    """The ``<!-- tasque:max_chars=N -->`` budget a document declares, if any."""
    if not content:
        return None
    match = CANONICAL_BUDGET_RE.search(content)
    return int(match.group(1)) if match else None


class MemoryService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def ensure_fts(self) -> None:
        self.session.execute(text(FTS_DDL))

    def create_memory(
        self,
        *,
        namespace: str,
        kind: str,
        content: str,
        tags: list[str] | None = None,
        source_kind: str | None = None,
        source_id: str | None = None,
        work_item_id: str | None = None,
        canonical_key: str | None = None,
        pinned: bool = False,
        ttl_days: int | None = None,
        importance: int | None = None,
    ) -> Memory:
        self.ensure_fts()
        memory = Memory(
            namespace=namespace,
            kind=kind,
            content=content,
            tags=tags or [],
            source_kind=source_kind,
            source_id=source_id,
            work_item_id=work_item_id,
            canonical_key=canonical_key,
            pinned=pinned,
            ttl_days=ttl_days,
            importance=importance,
        )
        self.session.add(memory)
        self.session.flush()
        self._index(memory)
        mirror_memory(memory)
        self._embed(memory)
        self._record("memory.created", memory, summary=f"Created memory in {namespace}")
        return memory

    def search(
        self,
        *,
        query: str | None = None,
        namespace: str | None = None,
        tags: list[str] | None = None,
        limit: int | None = 10,
    ) -> Sequence[Memory]:
        """Keyword search ranked by bm25, or newest-first when there is no query."""
        self.ensure_fts()
        expanded_limit = None if limit is None else max(0, limit) * 4
        if query:
            fts_query = safe_fts_query(query)
            if fts_query is None:
                return []
            sql = (
                "SELECT memory_fts.memory_id FROM memory_fts "
                "JOIN memories m ON m.id = memory_fts.memory_id "
                "WHERE memory_fts MATCH :query AND m.archived_at IS NULL"
            )
            params: dict[str, Any] = {"query": fts_query}
            if namespace is not None:
                sql += " AND m.namespace = :namespace"
                params["namespace"] = namespace
            sql += " ORDER BY rank"
            if expanded_limit is not None:
                sql += " LIMIT :limit"
                params["limit"] = expanded_limit
            ids = [row[0] for row in self.session.execute(text(sql), params).all()]
            if not ids:
                return []
            rows = self.session.scalars(select(Memory).where(Memory.id.in_(ids), Memory.archived_at.is_(None))).all()
            by_id = {memory.id: memory for memory in rows}
            ordered = [by_id[memory_id] for memory_id in ids if memory_id in by_id]
        else:
            statement = (
                select(Memory)
                .where(Memory.archived_at.is_(None))
                .order_by(Memory.pinned.desc(), Memory.created_at.desc())
            )
            if namespace is not None:
                statement = statement.where(Memory.namespace == namespace)
            if expanded_limit is not None:
                statement = statement.limit(expanded_limit)
            ordered = list(self.session.scalars(statement).all())

        if tags:
            wanted = set(tags)
            ordered = [memory for memory in ordered if wanted.issubset(set(memory.tags or []))]
        if limit is None:
            return ordered
        return ordered[: max(0, limit)]

    def recall(
        self,
        *,
        query: str | None,
        namespace: str | None = None,
        tags: list[str] | None = None,
        limit: int = 10,
        recency_weight: float = 0.15,
        importance_weight: float = 0.15,
        candidate_pool: int = 60,
    ) -> list[ScoredMemory]:
        """Rank memories by fused keyword and vector relevance, nudged by recency and importance.

        Keyword and vector ranks combine by reciprocal-rank fusion; the fused relevance is
        scaled by its maximum so near-ties stay near-ties, then light recency and importance
        weights break them. Without embeddings this degrades to keyword ranking.
        """
        pool = max(limit, candidate_pool)
        lexical = list(self.search(query=query, namespace=namespace, tags=tags, limit=pool))
        semantic = self._semantic_candidates(query, namespace=namespace, tags=tags, pool=pool)

        by_id: dict[str, Memory] = {memory.id: memory for memory in lexical}
        missing = [memory_id for memory_id, _ in semantic if memory_id not in by_id]
        if missing:
            for memory in self.session.scalars(
                select(Memory).where(Memory.id.in_(missing), Memory.archived_at.is_(None))
            ).all():
                by_id[memory.id] = memory
        if not by_id:
            return []

        lexical_rank = {memory.id: index + 1 for index, memory in enumerate(lexical)}
        semantic_rank = {memory_id: index + 1 for index, (memory_id, _) in enumerate(semantic)}
        relevance = {
            memory_id: (1.0 / (_RRF_K + lexical_rank[memory_id]) if memory_id in lexical_rank else 0.0)
            + (1.0 / (_RRF_K + semantic_rank[memory_id]) if memory_id in semantic_rank else 0.0)
            for memory_id in by_id
        }
        rel_norm = _scale_by_max(relevance)
        recency = _min_max({memory_id: _epoch(memory.created_at) for memory_id, memory in by_id.items()})

        scored = []
        for memory_id, memory in by_id.items():
            importance = 0.5 if memory.importance is None else max(0.0, min(1.0, (memory.importance - 1) / 4))
            score = (
                rel_norm.get(memory_id, 0.0)
                + recency_weight * recency.get(memory_id, 0.0)
                + importance_weight * importance
            )
            scored.append(ScoredMemory(memory=memory, score=score))
        scored.sort(key=lambda item: item.score, reverse=True)
        return scored[: max(0, limit)]

    def get_canonical(self, *, namespace: str, canonical_key: str) -> Memory | None:
        return self.session.scalar(
            select(Memory)
            .where(
                Memory.namespace == namespace,
                Memory.canonical_key == canonical_key,
                Memory.archived_at.is_(None),
            )
            .order_by(Memory.created_at.desc(), Memory.id.desc())
        )

    def list_active_by_kind(self, *, namespace: str, kind: str, limit: int | None = None) -> list[Memory]:
        """Every active memory of one kind in a namespace, such as a structured register."""
        statement = (
            select(Memory)
            .where(Memory.namespace == namespace, Memory.kind == kind, Memory.archived_at.is_(None))
            .order_by(Memory.pinned.desc(), Memory.canonical_key.asc(), Memory.created_at.asc())
        )
        if limit is not None:
            statement = statement.limit(max(0, limit))
        return list(self.session.scalars(statement).all())

    def upsert_canonical(
        self,
        *,
        namespace: str,
        canonical_key: str,
        kind: str,
        content: str,
        tags: list[str] | None = None,
        source_kind: str | None = None,
        source_id: str | None = None,
        work_item_id: str | None = None,
        pinned: bool = False,
        ttl_days: int | None = None,
        allow_over_budget: bool = False,
    ) -> Memory:
        """Replace the active canonical document for a key; the previous row is archived."""
        old = self.get_canonical(namespace=namespace, canonical_key=canonical_key)
        content = self._enforce_budget(
            canonical_key=canonical_key,
            content=content,
            previous=old.content if old is not None else None,
            allow_over_budget=allow_over_budget,
        )
        new = self.create_memory(
            namespace=namespace,
            kind=kind,
            content=content,
            tags=tags or [],
            source_kind=source_kind,
            source_id=source_id,
            work_item_id=work_item_id,
            canonical_key=canonical_key,
            pinned=pinned,
            ttl_days=ttl_days,
        )
        if old is not None:
            old.superseded_by = new.id
            old.archived_at = utc_now()
            self._delete_index(old.id)
            self._record(
                "memory.superseded",
                old,
                summary="Superseded canonical memory",
                payload={"superseded_by": new.id, "canonical_key": canonical_key},
            )
        return new

    def archive_memory(self, memory_id: str) -> Memory:
        memory = self._get(memory_id)
        if memory.archived_at is None:
            memory.archived_at = utc_now()
            self._delete_index(memory.id)
            self._record("memory.archived", memory, summary="Archived memory")
        return memory

    def update_memory(
        self,
        memory_id: str,
        *,
        content: str | None = None,
        tags: list[str] | None = None,
        importance: int | None = None,
    ) -> Memory:
        """Edit one memory in place: no new row and no archived copy."""
        memory = self._get(memory_id)
        if content is not None:
            if memory.canonical_key:
                content = self._enforce_budget(
                    canonical_key=memory.canonical_key,
                    content=content,
                    previous=memory.content,
                )
            memory.content = content
        if tags is not None:
            memory.tags = list(tags)
        if importance is not None:
            memory.importance = int(importance)
        memory.updated_at = utc_now()
        self.session.flush()
        self._delete_index(memory.id)
        self._index(memory)
        mirror_memory(memory)
        self._embed(memory)
        self._record("memory.updated", memory, summary="Updated memory")
        return memory

    def delete_memory(self, memory_id: str) -> None:
        """Remove one memory and its search and embedding rows."""
        memory = self._get(memory_id)
        self._delete_index(memory.id)
        self._delete_embedding(memory.id)
        self._record("memory.deleted", memory, summary="Deleted memory")
        self.session.delete(memory)
        self.session.flush()

    def prune_superseded(self, *, older_than_days: int = 30, limit: int = 500) -> int:
        """Delete archived rows older than the window; returns how many were removed."""
        cutoff = utc_now() - timedelta(days=max(0, older_than_days))
        rows = self.session.scalars(
            select(Memory)
            .where(Memory.archived_at.is_not(None), Memory.archived_at < cutoff)
            .order_by(Memory.archived_at.asc())
            .limit(max(0, limit))
        ).all()
        for memory in rows:
            self._delete_index(memory.id)
            self._delete_embedding(memory.id)
            self.session.delete(memory)
        self.session.flush()
        return len(rows)

    def embed_missing(self, *, namespace: str | None = None, limit: int = 500) -> int:
        """Embed active memories that have no vector for the current embedder yet."""
        if not get_settings().memory_hybrid_retrieval:
            return 0
        embedder = get_embedder()
        if embedder is None:
            return 0
        current = select(MemoryEmbedding.memory_id).where(MemoryEmbedding.model == embedder.name)
        statement = select(Memory).where(Memory.archived_at.is_(None), Memory.id.not_in(current))
        if namespace is not None:
            statement = statement.where(Memory.namespace == namespace)
        done = 0
        for memory in self.session.scalars(statement.limit(max(0, limit))).all():
            self._embed(memory, embedder=embedder)
            done += 1
        return done

    def _enforce_budget(
        self,
        *,
        canonical_key: str,
        content: str,
        previous: str | None,
        allow_over_budget: bool = False,
    ) -> str:
        """Hold a canonical document to the budget it, or its predecessor, declares.

        A rewrite that drops the marker inherits the previous budget, so a worker cannot
        escape a cap by omitting the line; raising a budget means writing a new marker.
        """
        declared = canonical_budget(content)
        inherited = canonical_budget(previous)
        if declared is None and inherited is not None:
            content = f"{content.rstrip()}\n\n<!-- tasque:max_chars={inherited} -->\n"
            declared = inherited
        if declared is None or allow_over_budget:
            return content
        if len(content) > declared:
            raise MemoryBudgetExceeded(canonical_key=canonical_key, size=len(content), max_chars=declared)
        return content

    def _semantic_candidates(
        self,
        query: str | None,
        *,
        namespace: str | None,
        tags: list[str] | None,
        pool: int,
    ) -> list[tuple[str, float]]:
        query_text = (query or "").strip()
        embedder = get_embedder()
        if not query_text or embedder is None or not get_settings().memory_hybrid_retrieval:
            return []
        try:
            query_vector = embedder.embed([query_text])[0]
        except Exception:  # noqa: BLE001 - embedding failure falls back to keyword ranking
            return []
        statement = (
            select(MemoryEmbedding.memory_id, MemoryEmbedding.vector)
            .join(Memory, Memory.id == MemoryEmbedding.memory_id)
            .where(Memory.archived_at.is_(None), MemoryEmbedding.model == embedder.name)
        )
        if namespace is not None:
            statement = statement.where(MemoryEmbedding.namespace == namespace)
        if tags:
            statement = statement.add_columns(Memory.tags)
            wanted = set(tags)
            rows = [
                (memory_id, blob)
                for memory_id, blob, memory_tags in self.session.execute(statement).all()
                if wanted.issubset(set(memory_tags or []))
            ]
        else:
            rows = [(memory_id, blob) for memory_id, blob in self.session.execute(statement).all()]
        return top_k_by_vector(query_vector, rows, k=pool)

    def _index(self, memory: Memory) -> None:
        self.session.execute(
            text(
                "INSERT INTO memory_fts(memory_id, namespace, kind, content, tags) "
                "VALUES (:memory_id, :namespace, :kind, :content, :tags)"
            ),
            {
                "memory_id": memory.id,
                "namespace": memory.namespace,
                "kind": memory.kind,
                "content": memory.content,
                "tags": " ".join(memory.tags or []),
            },
        )

    def _delete_index(self, memory_id: str) -> None:
        self.ensure_fts()
        self.session.execute(text("DELETE FROM memory_fts WHERE memory_id = :memory_id"), {"memory_id": memory_id})

    def _embed(self, memory: Memory, *, embedder=None) -> None:
        """Compute and store this memory's vector; a failure leaves it keyword-only."""
        if not get_settings().memory_hybrid_retrieval:
            return
        embedder = embedder or get_embedder()
        if embedder is None:
            return
        try:
            vector = embedder.embed([memory.content[:8000]])[0]
        except Exception:  # noqa: BLE001 - never fail a memory write on embedding
            return
        blob = pack_vector(vector)
        existing = self.session.get(MemoryEmbedding, memory.id)
        if existing is None:
            self.session.add(
                MemoryEmbedding(
                    memory_id=memory.id,
                    namespace=memory.namespace,
                    model=embedder.name,
                    dim=embedder.dim,
                    vector=blob,
                )
            )
        else:
            existing.namespace = memory.namespace
            existing.model = embedder.name
            existing.dim = embedder.dim
            existing.vector = blob
        self.session.flush()

    def _delete_embedding(self, memory_id: str) -> None:
        embedding = self.session.get(MemoryEmbedding, memory_id)
        if embedding is not None:
            self.session.delete(embedding)

    def _get(self, memory_id: str) -> Memory:
        memory = self.session.get(Memory, memory_id)
        if memory is None:
            raise KeyError(f"Unknown memory: {memory_id}")
        return memory

    def _record(
        self,
        event_type: str,
        memory: Memory,
        *,
        summary: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        work_item_id = memory.work_item_id
        if work_item_id is not None and self.session.get(WorkItem, work_item_id) is None:
            work_item_id = None
        record_event(
            self.session,
            event_type=event_type,
            entity_kind="memory",
            entity_id=memory.id,
            work_item_id=work_item_id,
            source="memory",
            summary=summary,
            payload=payload or {"kind": memory.kind, "tags": memory.tags or []},
        )
        instruments().memory_operations.add(
            1,
            {
                "tasque.memory.operation": event_type.removeprefix("memory."),
                "tasque.memory.namespace": memory.namespace,
            },
        )


def safe_fts_query(value: str | None) -> str | None:
    """Turn free text into an FTS5-safe OR query of quoted tokens.

    FTS5 treats characters such as ``-`` and ``:`` as operators, so raw text like an ISO
    date would raise instead of matching.
    """
    if not value:
        return None
    tokens: list[str] = []
    for raw in _FTS_TOKEN_RE.findall(str(value).lower()):
        token = raw.strip("_")
        if len(token) < 2 or token in _FTS_OPERATOR_WORDS or token in tokens:
            continue
        tokens.append(token)
        if len(tokens) >= _FTS_MAX_TOKENS:
            break
    if not tokens:
        return None
    return " OR ".join(f'"{token}"' for token in tokens)


def expire_ttl_memories(session: Session, *, now: datetime | None = None, limit: int = 500) -> int:
    """Archive active rows whose ``ttl_days`` has elapsed.

    Canonical and pinned rows never expire here: a TTL on a document workers pin is a
    mistake to surface, not to act on.
    """
    now = now or utc_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    candidates = session.scalars(
        select(Memory)
        .where(
            Memory.archived_at.is_(None),
            Memory.ttl_days.is_not(None),
            Memory.pinned.is_(False),
            Memory.canonical_key.is_(None),
        )
        .order_by(Memory.created_at.asc())
    ).all()
    service = MemoryService(session)
    expired = 0
    for memory in candidates:
        created = memory.created_at
        if created is None or memory.ttl_days is None:
            continue
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        if created + timedelta(days=int(memory.ttl_days)) > now:
            continue
        service.archive_memory(memory.id)
        expired += 1
        if expired >= limit:
            break
    if expired:
        session.flush()
    return expired


def _epoch(value: datetime | None) -> float:
    if value is None:
        return 0.0
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


def _min_max(values: dict[str, float]) -> dict[str, float]:
    if not values:
        return {}
    low = min(values.values())
    span = max(values.values()) - low
    if span <= 1e-12:
        return {key: 0.0 for key in values}
    return {key: (value - low) / span for key, value in values.items()}


def _scale_by_max(values: dict[str, float]) -> dict[str, float]:
    if not values:
        return {}
    high = max(values.values())
    if high <= 1e-12:
        return {key: 0.0 for key in values}
    return {key: value / high for key, value in values.items()}
