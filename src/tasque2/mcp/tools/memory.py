from __future__ import annotations

import hashlib
from typing import Any

from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.mcp.tools._shared import (
    calling_work_item,
    clamp,
    memory_ack,
    memory_data,
    optional_int,
    optional_string,
    required,
    run_json,
    string_list,
)
from tasque2.memory import MemoryService
from tasque2.memory.ingest import MemoryIngestService
from tasque2.models import Memory


def memory_recall(
    query: str,
    namespace: str | None = None,
    tags: list[str] | None = None,
    limit: int = 8,
    intent: str = "",
) -> str:
    """Find the memories most relevant to a query, ranked by keyword and semantic match,
    recency and importance. Scope to a domain with ``namespace`` (for example ``finance``)."""
    return run_json(lambda: _recall(query, namespace, tags, limit), intent=intent)


def memory_list(
    namespace: str | None = None,
    kind: str | None = None,
    tags: list[str] | None = None,
    source_kind: str | None = None,
    include_archived: bool = False,
    limit: int = 20,
    intent: str = "",
) -> str:
    """List recent memories, filtered by namespace, kind, tags (all must match), or source kind
    (``discord_reply`` lists the user's own messages in a lane's threads, newest first)."""
    return run_json(lambda: _list(namespace, kind, tags, source_kind, include_archived, limit), intent=intent)


def memory_get(memory_id: str, intent: str = "") -> str:
    """Fetch one memory in full by id."""
    return run_json(lambda: _get(memory_id), intent=intent)


def memory_get_canonical(namespace: str, canonical_key: str, intent: str = "") -> str:
    """Fetch the active canonical document for a namespace and key, in full."""
    return run_json(lambda: _get_canonical(namespace, canonical_key), intent=intent)


def memory_create(
    namespace: str,
    kind: str,
    content: str,
    tags: list[str] | None = None,
    canonical_key: str | None = None,
    pinned: bool = False,
    ttl_days: int | None = None,
    importance: int | None = None,
) -> str:
    """Record one new memory: a discrete fact, observation, or log entry.

    ``importance`` (1-5) raises a fact in future recall; ``ttl_days`` archives it after
    that many days.
    """
    return run_json(
        lambda: _create(
            namespace=namespace,
            kind=kind,
            content=content,
            tags=tags,
            canonical_key=canonical_key,
            pinned=pinned,
            ttl_days=ttl_days,
            importance=importance,
        )
    )


def memory_upsert_canonical(
    namespace: str,
    canonical_key: str,
    kind: str,
    content: str,
    tags: list[str] | None = None,
    pinned: bool = False,
) -> str:
    """Replace the whole canonical document for a namespace and key; the previous version is
    archived. A ``<!-- tasque:max_chars=N -->`` marker caps the document at N characters."""
    return run_json(
        lambda: _upsert(
            namespace=namespace, canonical_key=canonical_key, kind=kind, content=content, tags=tags, pinned=pinned
        )
    )


def memory_update(
    memory_id: str,
    content: str | None = None,
    tags: list[str] | None = None,
    importance: int | None = None,
) -> str:
    """Change one memory in place (content, tags, or 1-5 importance) without keeping a copy."""
    return run_json(lambda: _update(memory_id, content, tags, importance))


def memory_archive(memory_id: str) -> str:
    """Archive one memory: it leaves search and packets but stays on record."""
    return run_json(lambda: _archive(memory_id))


def memory_delete(memory_id: str) -> str:
    """Delete one memory and its search entries permanently (for stale or wrong facts)."""
    return run_json(lambda: _delete(memory_id))


def memory_ingest_text(
    namespace: str,
    title: str,
    content: str,
    source_kind: str = "mcp_text",
    source_id: str | None = None,
    tags: list[str] | None = None,
    force: bool = False,
) -> str:
    """Make a text source searchable: one summary memory plus content chunks."""
    return run_json(lambda: _ingest_text(namespace, title, content, source_kind, source_id, tags, force))


def memory_ingest_artifact(
    artifact_id: str,
    namespace: str | None = None,
    tags: list[str] | None = None,
    force: bool = False,
) -> str:
    """Make a text artifact searchable: one summary memory plus content chunks."""
    return run_json(lambda: _ingest_artifact(artifact_id, namespace, tags, force))


def _recall(query: str, namespace: str | None, tags: list[str] | None, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        scored = MemoryService(session).recall(
            query=required(query, "query"),
            namespace=optional_string(namespace),
            tags=string_list(tags),
            limit=clamp(limit),
        )
        items = []
        for entry in scored:
            data = memory_data(entry.memory)
            data["score"] = round(entry.score, 4)
            items.append(data)
        return {"ok": True, "items": items}


def _list(
    namespace: str | None,
    kind: str | None,
    tags: list[str] | None,
    source_kind: str | None,
    include_archived: bool,
    limit: int,
) -> dict[str, Any]:
    count = clamp(limit)
    with session_scope() as session:
        statement = select(Memory).order_by(Memory.pinned.desc(), Memory.created_at.desc()).limit(count * 4)
        if not include_archived:
            statement = statement.where(Memory.archived_at.is_(None))
        if namespace:
            statement = statement.where(Memory.namespace == namespace)
        if kind:
            statement = statement.where(Memory.kind == kind)
        if source_kind:
            statement = statement.where(Memory.source_kind == source_kind)
        rows = list(session.scalars(statement).all())
        wanted = set(string_list(tags))
        if wanted:
            rows = [memory for memory in rows if wanted.issubset(set(memory.tags or []))]
        return {"ok": True, "items": [memory_data(memory) for memory in rows[:count]]}


def _get(memory_id: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = session.get(Memory, required(memory_id, "memory_id"))
        if memory is None:
            raise KeyError(f"Unknown memory: {memory_id}")
        return {"ok": True, "memory": memory_data(memory, full=True)}


def _get_canonical(namespace: str, canonical_key: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).get_canonical(
            namespace=required(namespace, "namespace"), canonical_key=required(canonical_key, "canonical_key")
        )
        return {"ok": True, "memory": memory_data(memory, full=True)}


def _create(**fields: Any) -> dict[str, Any]:
    with session_scope() as session:
        caller = calling_work_item(session)
        memory = MemoryService(session).create_memory(
            namespace=required(fields["namespace"], "namespace"),
            kind=required(fields["kind"], "kind"),
            content=required(fields["content"], "content"),
            tags=string_list(fields["tags"]),
            source_kind="mcp",
            work_item_id=caller.id if caller is not None else None,
            canonical_key=optional_string(fields["canonical_key"]),
            pinned=bool(fields["pinned"]),
            ttl_days=optional_int(fields["ttl_days"]),
            importance=optional_int(fields["importance"]),
        )
        return {"ok": True, "memory": memory_ack(memory)}


def _upsert(**fields: Any) -> dict[str, Any]:
    with session_scope() as session:
        caller = calling_work_item(session)
        memory = MemoryService(session).upsert_canonical(
            namespace=required(fields["namespace"], "namespace"),
            canonical_key=required(fields["canonical_key"], "canonical_key"),
            kind=required(fields["kind"], "kind"),
            content=required(fields["content"], "content"),
            tags=string_list(fields["tags"]),
            source_kind="mcp",
            work_item_id=caller.id if caller is not None else None,
            pinned=bool(fields["pinned"]),
        )
        return {"ok": True, "memory": memory_ack(memory)}


def _update(memory_id: str, content: str | None, tags: list[str] | None, importance: int | None) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).update_memory(
            required(memory_id, "memory_id"),
            content=content if content is not None and content.strip() else None,
            tags=string_list(tags) if tags is not None else None,
            importance=optional_int(importance),
        )
        return {"ok": True, "memory": memory_ack(memory)}


def _archive(memory_id: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).archive_memory(required(memory_id, "memory_id"))
        return {"ok": True, "memory": memory_ack(memory)}


def _delete(memory_id: str) -> dict[str, Any]:
    with session_scope() as session:
        MemoryService(session).delete_memory(required(memory_id, "memory_id"))
        return {"ok": True, "deleted_memory_id": memory_id}


def _ingest_text(namespace, title, content, source_kind, source_id, tags, force) -> dict[str, Any]:
    source = optional_string(source_id) or "text:" + hashlib.sha256(str(content).encode("utf-8")).hexdigest()[:24]
    with session_scope() as session:
        caller = calling_work_item(session)
        result = MemoryIngestService(session).ingest_text(
            namespace=required(namespace, "namespace"),
            title=required(title, "title"),
            content=required(content, "content"),
            source_kind=required(source_kind, "source_kind"),
            source_id=source,
            tags=string_list(tags),
            work_item_id=caller.id if caller is not None else None,
            force=bool(force),
        )
        return {"ok": True, "skipped": result.skipped, "reason": result.reason, "memory_ids": result.memory_ids}


def _ingest_artifact(artifact_id, namespace, tags, force) -> dict[str, Any]:
    with session_scope() as session:
        result = MemoryIngestService(session).ingest_artifact(
            required(artifact_id, "artifact_id"),
            namespace=optional_string(namespace),
            tags=string_list(tags),
            force=bool(force),
        )
        if result is None:
            return {"ok": True, "ingested": False, "reason": "not_text_or_too_large"}
        return {"ok": True, "ingested": not result.skipped, "reason": result.reason, "memory_ids": result.memory_ids}
