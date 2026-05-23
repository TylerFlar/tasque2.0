from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from tasque2.memory_vault import mirror_memory
from tasque2.models import Memory, WorkEvent, utc_now


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
            sql = """
                    SELECT memory_id
                    FROM memory_fts
                    WHERE memory_fts MATCH :query
                    """
            params: dict[str, Any] = {"query": query}
            if expanded_limit is not None:
                sql += "\n                    LIMIT :limit"
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
