from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.embeddings import get_embedder, pack_vector, top_k_by_vector, unpack_vector
from tasque2.memory_vault import mirror_memory
from tasque2.models import Memory, MemoryEmbedding, WorkEvent, utc_now


@dataclass(frozen=True)
class ScoredMemory:
    """A memory paired with its fused retrieval score (higher = more relevant)."""

    memory: Memory
    score: float

_FTS_TOKEN_RE = re.compile(r"[\w]+", re.UNICODE)
_FTS_OPERATOR_WORDS = {"and", "or", "not", "near"}


class MemoryService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def ensure_fts(self) -> None:
        self.session.execute(
            text(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts
                USING fts5(memory_id UNINDEXED, namespace, kind, content, tags)
                """
            )
        )

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
        )
        self.session.add(memory)
        self.session.flush()
        self._index_memory(memory)
        mirror_memory(memory)
        self._embed_memory(memory)
        self._emit_event(
            event_type="memory.created",
            entity_id=memory.id,
            work_item_id=work_item_id,
            summary=f"Created memory in {namespace}",
            payload={"kind": kind, "tags": tags or []},
        )
        return memory

    def search(
        self,
        *,
        query: str | None = None,
        namespace: str | None = None,
        tags: list[str] | None = None,
        limit: int | None = 10,
    ) -> Sequence[Memory]:
        self.ensure_fts()
        expanded_limit = None if limit is None else max(0, limit) * 4
        if query:
            fts_query = _safe_fts_query(query)
            if fts_query is None:
                return []
            # Filter namespace + archived INSIDE the query and order by bm25 rank, so
            # the LIMIT keeps the most relevant in-namespace rows instead of filling up
            # with other namespaces' common-word matches (which starved scoped search at
            # scale) in arbitrary insertion order.
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
            rows = self.session.execute(
                text(sql),
                params,
            ).all()
            ids = [row[0] for row in rows]
            if not ids:
                return []
            memories = self.session.scalars(
                select(Memory).where(Memory.id.in_(ids), Memory.archived_at.is_(None))
            ).all()
            by_id = {memory.id: memory for memory in memories}
            ordered = [by_id[memory_id] for memory_id in ids if memory_id in by_id]
        else:
            statement = (
                select(Memory)
                .where(Memory.archived_at.is_(None))
                .order_by(Memory.pinned.desc(), Memory.created_at.desc())
            )
            if expanded_limit is not None:
                statement = statement.limit(expanded_limit)
            ordered = list(self.session.scalars(statement).all())

        if namespace is not None:
            ordered = [memory for memory in ordered if memory.namespace == namespace]
        if tags:
            wanted = set(tags)
            ordered = [memory for memory in ordered if wanted.issubset(set(memory.tags or []))]
        if limit is None:
            return ordered
        return ordered[: max(0, limit)]

    def search_hybrid(
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
        """Rank memories by fused lexical (FTS) + semantic (vector) relevance.

        Channels are combined with reciprocal-rank fusion, then nudged by
        min-max-normalized recency and importance (the Generative Agents
        weighted-sum pattern). Degrades to pure keyword search when no embeddings
        exist, so it is safe to call before any embeddings have been built.
        """
        self.ensure_fts()
        pool = max(limit, candidate_pool)
        lexical = list(self.search(query=query, namespace=namespace, tags=tags, limit=pool))

        semantic: list[tuple[str, float]] = []
        query_text = (query or "").strip()
        embedder = get_embedder()
        if query_text and embedder is not None and get_settings().memory_hybrid_retrieval:
            try:
                query_vector = embedder.embed([query_text])[0]
                semantic = self._vector_candidates(
                    query_vector,
                    namespace=namespace,
                    tags=tags,
                    model=embedder.name,
                    pool=pool,
                )
            except Exception:
                semantic = []

        by_id: dict[str, Memory] = {memory.id: memory for memory in lexical}
        missing_ids = [memory_id for memory_id, _ in semantic if memory_id not in by_id]
        if missing_ids:
            for memory in self.session.scalars(
                select(Memory).where(Memory.id.in_(missing_ids), Memory.archived_at.is_(None))
            ).all():
                by_id[memory.id] = memory
        if not by_id:
            return []

        lexical_rank = {memory.id: index + 1 for index, memory in enumerate(lexical)}
        semantic_rank = {memory_id: index + 1 for index, (memory_id, _) in enumerate(semantic)}

        rrf_k = 60.0
        relevance: dict[str, float] = {}
        for memory_id in by_id:
            score = 0.0
            if memory_id in lexical_rank:
                score += 1.0 / (rrf_k + lexical_rank[memory_id])
            if memory_id in semantic_rank:
                score += 1.0 / (rrf_k + semantic_rank[memory_id])
            relevance[memory_id] = score

        # Proportional (not min-max) so genuinely co-relevant items keep nearly
        # equal relevance and the secondary signals decide between them, while a
        # real rank gap still dominates. recency is min-max (a relative axis).
        rel_norm = _proportional(relevance)
        recency = _min_max({memory_id: _epoch(memory.created_at) for memory_id, memory in by_id.items()})

        scored: list[ScoredMemory] = []
        for memory_id, memory in by_id.items():
            importance = (
                0.5 if memory.importance is None else max(0.0, min(1.0, (memory.importance - 1) / 4))
            )
            final = (
                rel_norm.get(memory_id, 0.0)
                + recency_weight * recency.get(memory_id, 0.0)
                + importance_weight * importance
            )
            scored.append(ScoredMemory(memory=memory, score=final))
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

    def list_active_by_kind(
        self, *, namespace: str, kind: str, limit: int | None = None
    ) -> list[Memory]:
        """All active memories of one kind in a namespace (e.g. a structured register).

        Used to force-load a complete structured set (like every ``interest`` record)
        into a worker's context, so the routine always checks the full register.
        """
        statement = (
            select(Memory)
            .where(
                Memory.namespace == namespace,
                Memory.kind == kind,
                Memory.archived_at.is_(None),
            )
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
    ) -> Memory:
        old = self.get_canonical(namespace=namespace, canonical_key=canonical_key)
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
            self._emit_event(
                event_type="memory.superseded",
                entity_id=old.id,
                work_item_id=old.work_item_id,
                summary="Superseded canonical memory",
                payload={"superseded_by": new.id, "canonical_key": canonical_key},
            )
        return new

    def archive_memory(self, memory_id: str) -> Memory:
        memory = self._get_memory(memory_id)
        if memory.archived_at is None:
            memory.archived_at = utc_now()
            self._delete_index(memory.id)
            self._emit_event(
                event_type="memory.archived",
                entity_id=memory.id,
                work_item_id=memory.work_item_id,
                summary="Archived memory",
            )
        return memory

    def supersede_memory(self, memory_id: str, *, content: str, tags: list[str] | None = None) -> Memory:
        old = self._get_memory(memory_id)
        new = self.create_memory(
            namespace=old.namespace,
            kind=old.kind,
            content=content,
            tags=tags if tags is not None else list(old.tags or []),
            source_kind=old.source_kind,
            source_id=old.source_id,
            work_item_id=old.work_item_id,
            canonical_key=old.canonical_key,
            pinned=old.pinned,
            ttl_days=old.ttl_days,
        )
        old.superseded_by = new.id
        old.archived_at = utc_now()
        self._delete_index(old.id)
        self._emit_event(
            event_type="memory.superseded",
            entity_id=old.id,
            work_item_id=old.work_item_id,
            summary="Superseded memory",
            payload={"superseded_by": new.id},
        )
        return new

    def update_memory(
        self,
        memory_id: str,
        *,
        content: str | None = None,
        tags: list[str] | None = None,
        importance: int | None = None,
    ) -> Memory:
        """Edit one memory item IN PLACE (no new row, no archived copy).

        This is the fact-level UPDATE primitive: it lets a worker change a single
        discrete memory without re-emitting a whole document or piling up
        superseded rows. Reindexes FTS and re-embeds the new content.
        """
        memory = self._get_memory(memory_id)
        if content is not None:
            memory.content = content
        if tags is not None:
            memory.tags = list(tags)
        if importance is not None:
            memory.importance = int(importance)
        memory.updated_at = utc_now()
        self.session.flush()
        self._delete_index(memory.id)
        self._index_memory(memory)
        mirror_memory(memory)
        self._embed_memory(memory)
        self._emit_event(
            event_type="memory.updated",
            entity_id=memory.id,
            work_item_id=memory.work_item_id,
            summary="Updated memory",
        )
        return memory

    def delete_memory(self, memory_id: str) -> None:
        """Hard-delete one memory item and its index/embedding rows.

        The fact-level DELETE primitive (true unlearning), as opposed to
        archiving a dead copy. Use for stale/contradicted facts.
        """
        memory = self._get_memory(memory_id)
        self._delete_index(memory.id)
        self._delete_embedding(memory.id)
        self._emit_event(
            event_type="memory.deleted",
            entity_id=memory.id,
            work_item_id=memory.work_item_id,
            summary="Deleted memory",
        )
        self.session.delete(memory)
        self.session.flush()

    def prune_superseded(self, *, older_than_days: int = 30, limit: int = 500) -> int:
        """Hard-delete archived/superseded rows older than a window.

        Reclaims the dead-row pileup left by whole-document supersession. Returns
        the number of rows removed.
        """
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
        """Backfill embeddings for active memories missing one (or on an old model).

        Returns how many rows were embedded.
        """
        if not get_settings().memory_hybrid_retrieval:
            return 0
        embedder = get_embedder()
        if embedder is None:
            return 0
        # Exclude rows already embedded with the CURRENT model in SQL, so repeated
        # batched calls keep making progress instead of re-scanning the same window.
        current = select(MemoryEmbedding.memory_id).where(MemoryEmbedding.model == embedder.name)
        statement = select(Memory).where(
            Memory.archived_at.is_(None), Memory.id.not_in(current)
        )
        if namespace is not None:
            statement = statement.where(Memory.namespace == namespace)
        done = 0
        for memory in self.session.scalars(statement.limit(max(0, limit))).all():
            self._embed_memory(memory, embedder=embedder)
            done += 1
        return done

    def _index_memory(self, memory: Memory) -> None:
        self.session.execute(
            text(
                """
                INSERT INTO memory_fts(memory_id, namespace, kind, content, tags)
                VALUES (:memory_id, :namespace, :kind, :content, :tags)
                """
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
        self.session.execute(
            text("DELETE FROM memory_fts WHERE memory_id = :memory_id"),
            {"memory_id": memory_id},
        )

    def _embed_memory(self, memory: Memory, *, embedder=None) -> None:
        """Best-effort: compute and upsert this memory's vector. Never raises.

        A provider/network failure leaves the memory un-embedded (retrieval falls
        back to keyword search for it) rather than failing the write.
        """
        if not get_settings().memory_hybrid_retrieval:
            return
        embedder = embedder or get_embedder()
        if embedder is None:
            return
        try:
            vector = embedder.embed([memory.content[:8000]])[0]
        except Exception:
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

    def _vector_candidates(
        self,
        query_vector: list[float],
        *,
        namespace: str | None,
        tags: list[str] | None,
        model: str,
        pool: int,
    ) -> list[tuple[str, float]]:
        statement = (
            select(MemoryEmbedding.memory_id, MemoryEmbedding.vector)
            .join(Memory, Memory.id == MemoryEmbedding.memory_id)
            .where(Memory.archived_at.is_(None), MemoryEmbedding.model == model)
        )
        if namespace is not None:
            statement = statement.where(MemoryEmbedding.namespace == namespace)
        rows = self.session.execute(statement).all()
        candidates = [(memory_id, unpack_vector(blob)) for memory_id, blob in rows]
        if tags:
            wanted = set(tags)
            allowed = {
                memory.id
                for memory in self.session.scalars(
                    select(Memory).where(Memory.id.in_([cid for cid, _ in candidates]))
                ).all()
                if wanted.issubset(set(memory.tags or []))
            }
            candidates = [item for item in candidates if item[0] in allowed]
        return top_k_by_vector(query_vector, candidates, k=pool)

    def _get_memory(self, memory_id: str) -> Memory:
        memory = self.session.get(Memory, memory_id)
        if memory is None:
            raise KeyError(f"Unknown memory: {memory_id}")
        return memory

    def _emit_event(
        self,
        *,
        event_type: str,
        entity_id: str,
        work_item_id: str | None = None,
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WorkEvent:
        event = WorkEvent(
            event_type=event_type,
            entity_kind="memory",
            entity_id=entity_id,
            work_item_id=work_item_id,
            source="memory",
            summary=summary,
            payload=payload or {},
        )
        self.session.add(event)
        self.session.flush()
        return event


def _safe_fts_query(value: str | None) -> str | None:
    """Convert user/provider free text into a SQLite FTS5-safe OR query.

    SQLite FTS query syntax treats characters such as '-' and ':' as operators
    or column syntax. Workout/status queries often contain ISO dates like
    2026-05-31, so raw MATCH input can raise OperationalError instead of simply
    returning memories. Keep this function intentionally conservative: search
    by quoted tokens and ignore incoming FTS syntax.
    """

    if not value:
        return None
    tokens: list[str] = []
    for raw in _FTS_TOKEN_RE.findall(str(value).lower()):
        token = raw.strip("_")
        if len(token) < 2 or token in _FTS_OPERATOR_WORDS or token in tokens:
            continue
        tokens.append(token)
        if len(tokens) >= 24:
            break
    if not tokens:
        return None
    return " OR ".join(f'"{token}"' for token in tokens)


def _epoch(value: datetime | None) -> float:
    if value is None:
        return 0.0
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


def _min_max(values: dict[str, float]) -> dict[str, float]:
    """Scale a dict of scores into [0, 1]; all-equal inputs map to 0.0."""
    if not values:
        return {}
    low = min(values.values())
    high = max(values.values())
    span = high - low
    if span <= 1e-12:
        return {key: 0.0 for key in values}
    return {key: (value - low) / span for key, value in values.items()}


def _proportional(values: dict[str, float]) -> dict[str, float]:
    """Scale scores by the max so the top item is 1.0 and near-ties stay near 1.0.

    Unlike min-max this does not stretch a tiny rank gap to the full [0, 1] range,
    so light recency/importance weights can break near-ties without a real
    relevance gap being overwhelmed.
    """
    if not values:
        return {}
    high = max(values.values())
    if high <= 1e-12:
        return {key: 0.0 for key in values}
    return {key: value / high for key, value in values.items()}
