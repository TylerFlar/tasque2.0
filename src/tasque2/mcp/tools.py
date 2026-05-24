from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select

from tasque2 import result_inbox
from tasque2.artifacts import ArtifactService, ArtifactStore
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.memory_ingest import MemoryIngestService
from tasque2.models import (
    Artifact,
    Memory,
    Schedule,
    WorkEvent,
    WorkflowDefinition,
    WorkflowRun,
    WorkItem,
)
from tasque2.queue import WorkQueue
from tasque2.repo import WorkRepository
from tasque2.scheduler import ScheduleService
from tasque2.status import get_system_status
from tasque2.templates import read_template_file
from tasque2.workflows import WorkflowService


def memory_search(
    intent: str,
    query: str | None = None,
    namespace: str | None = None,
    tags: list[str] | None = None,
    limit: int = 10,
) -> str:
    """Search memories by natural language-ish terms, namespace, or tags."""
    return _run_json(
        lambda: _memory_search(
            query=query,
            namespace=namespace,
            tags=tags,
            limit=limit,
        ),
        intent=intent,
    )


def memory_search_any(
    intent: str,
    keywords: list[str],
    namespace: str | None = None,
    tags: list[str] | None = None,
    limit: int = 10,
) -> str:
    """Search memories with several keyword probes and return de-duplicated matches."""
    return _run_json(
        lambda: _memory_search_any(
            keywords=keywords,
            namespace=namespace,
            tags=tags,
            limit=limit,
        ),
        intent=intent,
    )


def memory_list(
    intent: str,
    namespace: str | None = None,
    kind: str | None = None,
    tags: list[str] | None = None,
    include_archived: bool = False,
    limit: int = 20,
) -> str:
    """List recent memories, optionally scoped by namespace, kind, or tags."""
    return _run_json(
        lambda: _memory_list(
            namespace=namespace,
            kind=kind,
            tags=tags,
            include_archived=include_archived,
            limit=limit,
        ),
        intent=intent,
    )


def memory_get(intent: str, memory_id: str) -> str:
    """Fetch one memory by id."""
    return _run_json(lambda: _memory_get(memory_id), intent=intent)


def memory_get_canonical(intent: str, namespace: str, canonical_key: str) -> str:
    """Fetch the active canonical memory for a namespace/key pair."""
    return _run_json(
        lambda: _memory_get_canonical(namespace=namespace, canonical_key=canonical_key),
        intent=intent,
    )


def memory_create(
    namespace: str,
    kind: str,
    content: str,
    tags: list[str] | None = None,
    canonical_key: str | None = None,
    pinned: bool = False,
    ttl_days: int | None = None,
    source: str = "mcp",
    work_item_id: str | None = None,
) -> str:
    """Create a durable or working memory."""
    return _run_json(
        lambda: _memory_create(
            namespace=namespace,
            kind=kind,
            content=content,
            tags=tags,
            canonical_key=canonical_key,
            pinned=pinned,
            ttl_days=ttl_days,
            source=source,
            work_item_id=work_item_id,
        )
    )


def memory_upsert_canonical(
    namespace: str,
    canonical_key: str,
    kind: str,
    content: str,
    tags: list[str] | None = None,
    pinned: bool = False,
    ttl_days: int | None = None,
    source: str = "mcp",
    work_item_id: str | None = None,
) -> str:
    """Replace the active canonical memory for a namespace/key pair."""
    return _run_json(
        lambda: _memory_upsert_canonical(
            namespace=namespace,
            canonical_key=canonical_key,
            kind=kind,
            content=content,
            tags=tags,
            pinned=pinned,
            ttl_days=ttl_days,
            source=source,
            work_item_id=work_item_id,
        )
    )


def memory_supersede(memory_id: str, content: str, tags: list[str] | None = None) -> str:
    """Archive an old memory and create a replacement carrying its metadata."""
    return _run_json(lambda: _memory_supersede(memory_id=memory_id, content=content, tags=tags))


def memory_archive(memory_id: str) -> str:
    """Archive a memory without deleting it."""
    return _run_json(lambda: _memory_archive(memory_id=memory_id))


def memory_ingest_text(
    namespace: str,
    title: str,
    content: str,
    source_kind: str = "mcp_text",
    source_id: str | None = None,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    force: bool = False,
) -> str:
    """Ingest a text source into searchable summary/chunk memories."""
    return _run_json(
        lambda: _memory_ingest_text(
            namespace=namespace,
            title=title,
            content=content,
            source_kind=source_kind,
            source_id=source_id,
            tags=tags,
            work_item_id=work_item_id,
            force=force,
        )
    )


def memory_ingest_artifact(
    artifact_id: str,
    namespace: str | None = None,
    tags: list[str] | None = None,
    max_bytes: int = 2_000_000,
    force: bool = False,
) -> str:
    """Ingest a text artifact into searchable summary/chunk memories."""
    return _run_json(
        lambda: _memory_ingest_artifact(
            artifact_id=artifact_id,
            namespace=namespace,
            tags=tags,
            max_bytes=max_bytes,
            force=force,
        )
    )


def memory_ingest_pending(limit: int = 25) -> str:
    """Ingest pending text artifacts and inbound Discord messages."""
    return _run_json(lambda: _memory_ingest_pending(limit=limit))


def todo_write(
    scope: str,
    items: list[Any],
    namespace: str = "global",
    work_item_id: str | None = None,
) -> str:
    """Write a durable todo/checklist memory for coordination."""
    return _run_json(
        lambda: _todo_write(
            scope=scope,
            items=items,
            namespace=namespace,
            work_item_id=work_item_id,
        )
    )


def ask_user(
    question: str,
    context: str | None = None,
    namespace: str = "global",
    work_item_id: str | None = None,
) -> str:
    """Record a blocking or useful question for the user."""
    return _run_json(
        lambda: _ask_user(
            question=question,
            context=context,
            namespace=namespace,
            work_item_id=work_item_id,
        )
    )


def artifact_list(
    intent: str,
    query: str | None = None,
    kind: str | None = None,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    source_kind: str | None = None,
    include_archived: bool = False,
    limit: int = 20,
) -> str:
    """List/search artifact metadata and local paths."""
    return _run_json(
        lambda: _artifact_list(
            query=query,
            kind=kind,
            tags=tags,
            work_item_id=work_item_id,
            source_kind=source_kind,
            include_archived=include_archived,
            limit=limit,
        ),
        intent=intent,
    )


def artifact_get(intent: str, artifact_id: str, include_text: bool = False, max_chars: int = 20000) -> str:
    """Fetch artifact metadata and optionally a text preview from its local file."""
    return _run_json(
        lambda: _artifact_get(artifact_id=artifact_id, include_text=include_text, max_chars=max_chars),
        intent=intent,
    )


def artifact_read_text(
    intent: str,
    artifact_id: str | None = None,
    path: str | None = None,
    max_chars: int = 20000,
) -> str:
    """Read a text artifact or local path. Binary files return metadata plus an error."""
    return _run_json(
        lambda: _artifact_read_text(artifact_id=artifact_id, path=path, max_chars=max_chars),
        intent=intent,
    )


def artifact_capture_file(
    path: str,
    kind: str = "worker_file",
    title: str | None = None,
    tags: list[str] | None = None,
    discord_upload: bool = False,
    work_item_id: str | None = None,
    workflow_run_id: str | None = None,
    source: str = "mcp",
) -> str:
    """Copy a local file into Tasque artifact storage and return its artifact id/path."""
    return _run_json(
        lambda: _artifact_capture_file(
            path=path,
            kind=kind,
            title=title,
            tags=tags,
            discord_upload=discord_upload,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source=source,
        )
    )


def artifact_archive(artifact_id: str) -> str:
    """Archive artifact metadata without deleting the local file."""
    return _run_json(lambda: _artifact_archive(artifact_id=artifact_id))


def work_enqueue(
    title: str,
    task_instruction: str | None = None,
    worker_kind: str = "provider.default",
    runtime_contract: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    priority: int = 0,
    max_attempts: int = 1,
    idempotency_key: str | None = None,
    workflow_run_id: str | None = None,
    discord_thread_id: str | None = None,
    task_template_path: str | None = None,
    template_base_dir: str | None = None,
) -> str:
    """Queue a normal Tasque WorkItem from direct instructions or a Markdown template file."""
    return _run_json(
        lambda: _work_enqueue(
            title=title,
            task_instruction=task_instruction,
            worker_kind=worker_kind,
            runtime_contract=runtime_contract,
            context=context,
            priority=priority,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            workflow_run_id=workflow_run_id,
            discord_thread_id=discord_thread_id,
            task_template_path=task_template_path,
            template_base_dir=template_base_dir,
        )
    )


def work_list(
    intent: str,
    status: str | list[str] | None = None,
    worker_kind: str | None = None,
    limit: int = 20,
) -> str:
    """List recent work items."""
    return _run_json(
        lambda: _work_list(status=status, worker_kind=worker_kind, limit=limit),
        intent=intent,
    )


def work_get(intent: str, work_item_id: str) -> str:
    """Fetch one WorkItem with latest attempt metadata."""
    return _run_json(lambda: _work_get(work_item_id=work_item_id), intent=intent)


def work_events(intent: str, work_item_id: str, limit: int = 50) -> str:
    """List recent event history for a WorkItem."""
    return _run_json(lambda: _work_events(work_item_id=work_item_id, limit=limit), intent=intent)


def work_pause(work_item_id: str) -> str:
    """Pause a non-terminal WorkItem."""
    return _run_json(lambda: _work_transition(work_item_id=work_item_id, action="pause"))


def work_resume(work_item_id: str) -> str:
    """Resume a paused WorkItem."""
    return _run_json(lambda: _work_transition(work_item_id=work_item_id, action="resume"))


def work_cancel(work_item_id: str) -> str:
    """Cancel or request cancellation for a WorkItem."""
    return _run_json(lambda: _work_transition(work_item_id=work_item_id, action="cancel"))


def work_retry(work_item_id: str) -> str:
    """Return a dead-lettered WorkItem to the ready queue."""
    return _run_json(lambda: _work_transition(work_item_id=work_item_id, action="retry"))


def schedule_create_work(
    name: str,
    schedule_type: str,
    expression: str,
    task_instruction: str | None = None,
    worker_kind: str = "provider.default",
    timezone_name: str | None = None,
    runtime_contract: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    catchup_policy: str = "coalesce",
    max_backfill: int = 10,
    enabled: bool = True,
    task_template_path: str | None = None,
    template_base_dir: str | None = None,
) -> str:
    """Create a recurring or future schedule that enqueues a normal WorkItem."""
    return _run_json(
        lambda: _schedule_create_work(
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            task_instruction=task_instruction,
            worker_kind=worker_kind,
            timezone_name=timezone_name,
            runtime_contract=runtime_contract,
            context=context,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
            enabled=enabled,
            task_template_path=task_template_path,
            template_base_dir=template_base_dir,
        )
    )


def schedule_list(intent: str, enabled: bool | None = None, limit: int = 20) -> str:
    """List recent schedules."""
    return _run_json(lambda: _schedule_list(enabled=enabled, limit=limit), intent=intent)


def workflow_list(intent: str, enabled: bool | None = None, limit: int = 20) -> str:
    """List workflow definitions available to start."""
    return _run_json(lambda: _workflow_list(enabled=enabled, limit=limit), intent=intent)


def workflow_start(
    workflow_name: str | None = None,
    workflow_definition_id: str | None = None,
    version: str = "1",
    run_name: str | None = None,
    input: dict[str, Any] | None = None,
    discord_thread_id: str | None = None,
) -> str:
    """Start a WorkflowRun by workflow name or definition id."""
    return _run_json(
        lambda: _workflow_start(
            workflow_name=workflow_name,
            workflow_definition_id=workflow_definition_id,
            version=version,
            run_name=run_name,
            input=input,
            discord_thread_id=discord_thread_id,
        )
    )


def system_status(intent: str) -> str:
    """Summarize queue, scheduler, and workflow counts."""
    return _run_json(lambda: _system_status(), intent=intent)


def submit_worker_result(
    result_token: str,
    report: str,
    summary: str,
    produces: dict[str, Any] | None = None,
    status: str = "succeeded",
    error: str | None = None,
) -> str:
    """Submit the authoritative structured result for this worker run.

    Call exactly once near the end of the coordinator turn with the result_token
    from the prompt. Tasque reads this payload after the provider process exits;
    provider stdout is not used as a result fallback.
    """
    return _run_json(
        lambda: _submit_worker_result(
            result_token=result_token,
            report=report,
            summary=summary,
            produces=produces,
            status=status,
            error=error,
        )
    )


def submit_result(
    result_key: str,
    report: str,
    summary: str,
    produces: dict[str, Any] | None = None,
    status: str = "succeeded",
    error: str | None = None,
) -> str:
    """Alias for submit_worker_result using the more generic result_key name."""
    return _run_json(
        lambda: _submit_worker_result(
            result_token=result_key,
            report=report,
            summary=summary,
            produces=produces,
            status=status,
            error=error,
        )
    )


def _memory_search(
    *,
    query: str | None,
    namespace: str | None,
    tags: list[str] | None,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        memories = MemoryService(session).search(
            query=_fts_query(query),
            namespace=_optional_string(namespace),
            tags=_string_list(tags),
            limit=_limit(limit),
        )
        return {"ok": True, "items": [_memory_data(memory) for memory in memories]}


def _memory_search_any(
    *,
    keywords: list[str],
    namespace: str | None,
    tags: list[str] | None,
    limit: int,
) -> dict[str, Any]:
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    per_query_limit = max(_limit(limit), 1)
    for keyword in _string_list(keywords):
        result = _memory_search(
            query=keyword,
            namespace=namespace,
            tags=tags,
            limit=per_query_limit,
        )
        for item in result["items"]:
            if item["id"] in seen:
                continue
            seen.add(item["id"])
            items.append(item)
            if len(items) >= _limit(limit):
                return {"ok": True, "items": items}
    return {"ok": True, "items": items}


def _memory_list(
    *,
    namespace: str | None,
    kind: str | None,
    tags: list[str] | None,
    include_archived: bool,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(Memory).order_by(Memory.pinned.desc(), Memory.created_at.desc()).limit(
            _limit(limit) * 4
        )
        if not include_archived:
            statement = statement.where(Memory.archived_at.is_(None))
        if namespace:
            statement = statement.where(Memory.namespace == namespace)
        if kind:
            statement = statement.where(Memory.kind == kind)
        memories = list(session.scalars(statement).all())
        wanted_tags = set(_string_list(tags))
        if wanted_tags:
            memories = [memory for memory in memories if wanted_tags.issubset(set(memory.tags or []))]
        return {"ok": True, "items": [_memory_data(memory) for memory in memories[: _limit(limit)]]}


def _memory_get(memory_id: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = session.get(Memory, memory_id)
        if memory is None:
            raise KeyError(f"Unknown memory: {memory_id}")
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_get_canonical(*, namespace: str, canonical_key: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).get_canonical(namespace=namespace, canonical_key=canonical_key)
        return {"ok": True, "memory": _memory_data(memory, full=True) if memory is not None else None}


def _memory_create(
    *,
    namespace: str,
    kind: str,
    content: str,
    tags: list[str] | None,
    canonical_key: str | None,
    pinned: bool,
    ttl_days: int | None,
    source: str,
    work_item_id: str | None,
) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace=_required(namespace, "namespace"),
            kind=_required(kind, "kind"),
            content=_required(content, "content"),
            tags=_string_list(tags),
            source_kind=source,
            work_item_id=_optional_string(work_item_id),
            canonical_key=_optional_string(canonical_key),
            pinned=bool(pinned),
            ttl_days=_optional_int(ttl_days),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_upsert_canonical(
    *,
    namespace: str,
    canonical_key: str,
    kind: str,
    content: str,
    tags: list[str] | None,
    pinned: bool,
    ttl_days: int | None,
    source: str,
    work_item_id: str | None,
) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).upsert_canonical(
            namespace=_required(namespace, "namespace"),
            canonical_key=_required(canonical_key, "canonical_key"),
            kind=_required(kind, "kind"),
            content=_required(content, "content"),
            tags=_string_list(tags),
            source_kind=source,
            work_item_id=_optional_string(work_item_id),
            pinned=bool(pinned),
            ttl_days=_optional_int(ttl_days),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_supersede(*, memory_id: str, content: str, tags: list[str] | None) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).supersede_memory(
            _required(memory_id, "memory_id"),
            content=_required(content, "content"),
            tags=_string_list(tags) if tags is not None else None,
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_archive(*, memory_id: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).archive_memory(_required(memory_id, "memory_id"))
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_ingest_text(
    *,
    namespace: str,
    title: str,
    content: str,
    source_kind: str,
    source_id: str | None,
    tags: list[str] | None,
    work_item_id: str | None,
    force: bool,
) -> dict[str, Any]:
    clean_source_id = _optional_string(source_id) or _content_source_id(content)
    with session_scope() as session:
        result = MemoryIngestService(session).ingest_text(
            namespace=_required(namespace, "namespace"),
            title=_required(title, "title"),
            content=_required(content, "content"),
            source_kind=_required(source_kind, "source_kind"),
            source_id=clean_source_id,
            tags=_string_list(tags),
            work_item_id=_optional_string(work_item_id),
            force=bool(force),
        )
        memories = [
            session.get(Memory, memory_id)
            for memory_id in result.memory_ids
        ]
        return {
            "ok": True,
            "source_kind": result.source_kind,
            "source_id": result.source_id,
            "skipped": result.skipped,
            "reason": result.reason,
            "memory_ids": result.memory_ids,
            "memories": [_memory_data(memory) for memory in memories if memory is not None],
        }


def _memory_ingest_artifact(
    *,
    artifact_id: str,
    namespace: str | None,
    tags: list[str] | None,
    max_bytes: int,
    force: bool,
) -> dict[str, Any]:
    with session_scope() as session:
        result = MemoryIngestService(session).ingest_artifact(
            _required(artifact_id, "artifact_id"),
            namespace=_optional_string(namespace),
            tags=_string_list(tags),
            max_bytes=_limit_bytes(max_bytes),
            force=bool(force),
        )
        if result is None:
            return {"ok": True, "ingested": False, "reason": "not_text_or_too_large"}
        memories = [
            session.get(Memory, memory_id)
            for memory_id in result.memory_ids
        ]
        return {
            "ok": True,
            "ingested": not result.skipped,
            "source_kind": result.source_kind,
            "source_id": result.source_id,
            "skipped": result.skipped,
            "reason": result.reason,
            "memory_ids": result.memory_ids,
            "memories": [_memory_data(memory) for memory in memories if memory is not None],
        }


def _memory_ingest_pending(*, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        result = MemoryIngestService(session).auto_ingest_pending(limit=_limit(limit))
        return {
            "ok": True,
            "ingested_sources": result.ingested_sources,
            "skipped_sources": result.skipped_sources,
            "memory_ids": result.memory_ids,
        }


def _todo_write(
    *,
    scope: str,
    items: list[Any],
    namespace: str,
    work_item_id: str | None,
) -> dict[str, Any]:
    clean_scope = _required(scope, "scope")
    if not isinstance(items, list):
        raise ValueError("items must be a list.")
    lines = [f"# Todo: {clean_scope}", ""]
    for index, item in enumerate(items, start=1):
        if isinstance(item, dict):
            text = item.get("text") or item.get("task") or item.get("title") or json.dumps(item)
            status = item.get("status")
            prefix = f"{index}. [{status}] " if status else f"{index}. "
            lines.append(prefix + str(text).strip())
        else:
            lines.append(f"{index}. {str(item).strip()}")
    with session_scope() as session:
        memory = MemoryService(session).upsert_canonical(
            namespace=_required(namespace, "namespace"),
            canonical_key=f"todo:{clean_scope}",
            kind="todo",
            content="\n".join(lines).strip(),
            tags=["todo", "coordination"],
            source_kind="mcp_todo",
            work_item_id=_optional_string(work_item_id),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _ask_user(
    *,
    question: str,
    context: str | None,
    namespace: str,
    work_item_id: str | None,
) -> dict[str, Any]:
    body = _required(question, "question")
    if context and str(context).strip():
        body = f"{body}\n\nContext:\n{str(context).strip()}"
    with session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace=_required(namespace, "namespace"),
            kind="question",
            content=body,
            tags=["question", "needs_user"],
            source_kind="mcp_question",
            work_item_id=_optional_string(work_item_id),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _artifact_list(
    *,
    query: str | None,
    kind: str | None,
    tags: list[str] | None,
    work_item_id: str | None,
    source_kind: str | None,
    include_archived: bool,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        artifacts = ArtifactService(session).list_artifacts(
            query=_optional_string(query),
            kind=_optional_string(kind),
            tag=_string_list(tags),
            work_item_id=_optional_string(work_item_id),
            source_kind=_optional_string(source_kind),
            include_archived=include_archived,
            limit=_limit(limit),
        )
        return {"ok": True, "items": [_artifact_data(artifact) for artifact in artifacts]}


def _artifact_get(*, artifact_id: str, include_text: bool, max_chars: int) -> dict[str, Any]:
    with session_scope() as session:
        artifact = ArtifactService(session).get_artifact(_required(artifact_id, "artifact_id"))
        data = _artifact_data(artifact, full=True)
    if include_text:
        data["text"] = _read_text_file(data["local_path"], max_chars=_limit_chars(max_chars))
    return {"ok": True, "artifact": data}


def _artifact_read_text(
    *,
    artifact_id: str | None,
    path: str | None,
    max_chars: int,
) -> dict[str, Any]:
    if artifact_id:
        with session_scope() as session:
            artifact = ArtifactService(session).get_artifact(artifact_id)
            path = artifact.local_path
    if not path:
        raise ValueError("artifact_id or path is required.")
    return {
        "ok": True,
        "path": str(Path(path).expanduser().resolve()),
        "text": _read_text_file(path, max_chars=_limit_chars(max_chars)),
    }


def _artifact_capture_file(
    *,
    path: str,
    kind: str,
    title: str | None,
    tags: list[str] | None,
    discord_upload: bool,
    work_item_id: str | None,
    workflow_run_id: str | None,
    source: str,
) -> dict[str, Any]:
    clean_tags = _string_list(tags)
    if discord_upload and "discord_upload" not in clean_tags:
        clean_tags.append("discord_upload")
    with session_scope() as session:
        artifact = ArtifactStore().capture_file(
            session,
            path=_required(path, "path"),
            kind=_required(kind, "kind"),
            title=_optional_string(title),
            tags=clean_tags,
            work_item_id=_optional_string(work_item_id),
            workflow_run_id=_optional_string(workflow_run_id),
            source_kind=source,
            source_id=str(Path(path).expanduser()),
        )
        return {"ok": True, "artifact": _artifact_data(artifact, full=True)}


def _artifact_archive(*, artifact_id: str) -> dict[str, Any]:
    with session_scope() as session:
        artifact = ArtifactService(session).archive_artifact(_required(artifact_id, "artifact_id"))
        return {"ok": True, "artifact": _artifact_data(artifact, full=True)}


def _work_enqueue(
    *,
    title: str,
    task_instruction: str | None,
    worker_kind: str,
    runtime_contract: dict[str, Any] | None,
    context: dict[str, Any] | None,
    priority: int,
    max_attempts: int,
    idempotency_key: str | None,
    workflow_run_id: str | None,
    discord_thread_id: str | None,
    task_template_path: str | None,
    template_base_dir: str | None,
) -> dict[str, Any]:
    resolved_task_instruction = _resolve_task_instruction(
        task_instruction=task_instruction,
        task_template_path=task_template_path,
        template_base_dir=template_base_dir,
    )
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title=_required(title, "title"),
            task_instruction=resolved_task_instruction,
            worker_kind=_required(worker_kind, "worker_kind"),
            runtime_contract=dict(runtime_contract or {}),
            context=dict(context or {}),
            priority=int(priority),
            max_attempts=max(1, int(max_attempts)),
            idempotency_key=_optional_string(idempotency_key),
            source_kind="mcp",
            workflow_run_id=_optional_string(workflow_run_id),
            discord_thread_id=_optional_string(discord_thread_id),
        )
        return {"ok": True, "work_item": _work_data(work)}


def _work_list(
    *,
    status: str | list[str] | None,
    worker_kind: str | None,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(WorkItem).order_by(WorkItem.created_at.desc()).limit(_limit(limit) * 4)
        statuses = _string_list(status)
        if statuses:
            statement = statement.where(WorkItem.status.in_(statuses))
        if worker_kind:
            statement = statement.where(WorkItem.worker_kind == worker_kind)
        rows = list(session.scalars(statement).all())[: _limit(limit)]
        return {"ok": True, "items": [_work_data(work) for work in rows]}


def _work_get(*, work_item_id: str) -> dict[str, Any]:
    with session_scope() as session:
        work = session.get(WorkItem, _required(work_item_id, "work_item_id"))
        if work is None:
            raise KeyError(f"Unknown work item: {work_item_id}")
        return {"ok": True, "work_item": _work_data(work, full=True)}


def _work_events(*, work_item_id: str, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        rows = session.scalars(
            select(WorkEvent)
            .where(WorkEvent.work_item_id == _required(work_item_id, "work_item_id"))
            .order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc())
            .limit(_limit(limit))
        ).all()
        return {"ok": True, "items": [_event_data(event) for event in rows]}


def _work_transition(*, work_item_id: str, action: str) -> dict[str, Any]:
    with session_scope() as session:
        queue = WorkQueue(session)
        if action == "pause":
            work = queue.pause_work(work_item_id)
        elif action == "resume":
            work = queue.resume_work(work_item_id)
        elif action == "cancel":
            work = queue.request_cancel(work_item_id)
        elif action == "retry":
            work = queue.retry_dead_letter(work_item_id)
        else:
            raise ValueError(f"Unknown work transition action: {action}")
        return {"ok": True, "work_item": _work_data(work)}


def _schedule_create_work(
    *,
    name: str,
    schedule_type: str,
    expression: str,
    task_instruction: str | None,
    worker_kind: str,
    timezone_name: str | None,
    runtime_contract: dict[str, Any] | None,
    context: dict[str, Any] | None,
    catchup_policy: str,
    max_backfill: int,
    enabled: bool,
    task_template_path: str | None,
    template_base_dir: str | None,
) -> dict[str, Any]:
    if runtime_contract is not None and not isinstance(runtime_contract, dict):
        raise ValueError("runtime_contract must be an object or omitted.")
    if context is not None and not isinstance(context, dict):
        raise ValueError("context must be an object or omitted.")
    payload = _schedule_work_payload(
        name=name,
        task_instruction=task_instruction,
        task_template_path=task_template_path,
        template_base_dir=template_base_dir,
        context=context,
    )
    with session_scope() as session:
        schedule = ScheduleService(session).create_schedule(
            name=_required(name, "name"),
            schedule_type=_required(schedule_type, "schedule_type"),
            expression=_required(expression, "expression"),
            worker_kind=_required(worker_kind, "worker_kind"),
            payload=payload,
            timezone_name=_optional_string(timezone_name),
            runtime_contract=dict(runtime_contract or {}),
            catchup_policy=_required(catchup_policy, "catchup_policy"),
            max_backfill=max(1, int(max_backfill)),
            enabled=bool(enabled),
        )
        return {"ok": True, "schedule": _schedule_data(schedule, full=True)}


def _schedule_list(*, enabled: bool | None, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(Schedule).order_by(Schedule.created_at.desc()).limit(_limit(limit))
        if enabled is not None:
            statement = statement.where(Schedule.enabled.is_(bool(enabled)))
        schedules = session.scalars(statement).all()
        return {"ok": True, "items": [_schedule_data(schedule) for schedule in schedules]}


def _workflow_list(*, enabled: bool | None, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(WorkflowDefinition).order_by(WorkflowDefinition.created_at.desc()).limit(
            _limit(limit)
        )
        if enabled is not None:
            statement = statement.where(WorkflowDefinition.enabled.is_(bool(enabled)))
        definitions = session.scalars(statement).all()
        return {
            "ok": True,
            "items": [_workflow_definition_data(definition) for definition in definitions],
        }


def _workflow_start(
    *,
    workflow_name: str | None,
    workflow_definition_id: str | None,
    version: str,
    run_name: str | None,
    input: dict[str, Any] | None,
    discord_thread_id: str | None,
) -> dict[str, Any]:
    if input is not None and not isinstance(input, dict):
        raise ValueError("input must be an object or omitted.")
    with session_scope() as session:
        definition = _resolve_workflow_definition(
            session,
            workflow_name=workflow_name,
            workflow_definition_id=workflow_definition_id,
            version=version,
        )
        run = WorkflowService(session).start_run(
            workflow_definition_id=definition.id,
            name=_optional_string(run_name) or definition.name,
            input=dict(input or {}),
            discord_thread_id=_optional_string(discord_thread_id),
        )
        return {
            "ok": True,
            "workflow_definition": _workflow_definition_data(definition),
            "workflow_run": _workflow_run_data(run),
        }


def _resolve_workflow_definition(
    session,
    *,
    workflow_name: str | None,
    workflow_definition_id: str | None,
    version: str,
) -> WorkflowDefinition:
    if workflow_definition_id:
        definition = session.get(WorkflowDefinition, workflow_definition_id)
        if definition is None:
            raise KeyError(f"Unknown workflow definition: {workflow_definition_id}")
        return definition
    name = _required(workflow_name, "workflow_name")
    definition = session.scalar(
        select(WorkflowDefinition).where(
            WorkflowDefinition.name == name,
            WorkflowDefinition.version == str(version or "1"),
        )
    )
    if definition is None:
        raise KeyError(f"Unknown workflow definition: {name}@{version}")
    return definition


def _system_status() -> dict[str, Any]:
    with session_scope() as session:
        status = get_system_status(session)
        return {
            "ok": True,
            "status": {
                "work_items": status.work_items,
                "work_attempts": status.work_attempts,
                "failed_work_unresolved": status.failed_work_unresolved,
                "schedules_enabled": status.schedules_enabled,
                "workflow_runs": status.workflow_runs,
                "ready_work": status.ready_work,
                "running_work": status.running_work,
            },
        }


def _submit_worker_result(
    *,
    result_token: str,
    report: str,
    summary: str,
    produces: dict[str, Any] | None,
    status: str,
    error: str | None,
) -> dict[str, Any]:
    token = _required(result_token, "result_token")
    if produces is not None and not isinstance(produces, dict):
        raise ValueError("produces must be an object or omitted.")
    clean_status = str(status or "succeeded").strip().lower()
    clean_error = str(error).strip() if error is not None else None
    if clean_error == "":
        clean_error = None
    result_inbox.deposit(
        result_token=token,
        agent_kind="worker",
        payload={
            "status": clean_status,
            "report": _string_field(report, "report"),
            "summary": _string_field(summary, "summary"),
            "produces": produces or {},
            "error": clean_error,
        },
    )
    return {"ok": True, "result_token": token}


def _memory_data(memory: Memory | None, *, full: bool = False) -> dict[str, Any] | None:
    if memory is None:
        return None
    content = memory.content if full else _truncate(memory.content, 1200)
    return {
        "id": memory.id,
        "namespace": memory.namespace,
        "kind": memory.kind,
        "content": content,
        "tags": memory.tags or [],
        "canonical_key": memory.canonical_key,
        "work_item_id": memory.work_item_id,
        "source_kind": memory.source_kind,
        "source_id": memory.source_id,
        "pinned": memory.pinned,
        "ttl_days": memory.ttl_days,
        "superseded_by": memory.superseded_by,
        "archived_at": _iso(memory.archived_at),
        "created_at": _iso(memory.created_at),
        "updated_at": _iso(memory.updated_at),
    }


def _artifact_data(artifact: Artifact, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": artifact.id,
        "kind": artifact.kind,
        "title": artifact.title,
        "local_path": artifact.local_path,
        "content_type": artifact.content_type,
        "size_bytes": artifact.size_bytes,
        "sha256": artifact.sha256,
        "summary": artifact.summary if full else _truncate(artifact.summary or "", 500),
        "tags": artifact.tags or [],
        "workflow_run_id": artifact.workflow_run_id,
        "work_item_id": artifact.work_item_id,
        "attempt_id": artifact.attempt_id,
        "source_kind": artifact.source_kind,
        "source_id": artifact.source_id,
        "archived_at": _iso(artifact.archived_at),
        "created_at": _iso(artifact.created_at),
        "updated_at": _iso(artifact.updated_at),
    }
    if not data["summary"]:
        data.pop("summary")
    return data


def _work_data(work: WorkItem, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": work.id,
        "title": work.title,
        "status": work.status,
        "worker_kind": work.worker_kind,
        "priority": work.priority,
        "attempt_count": work.attempt_count,
        "max_attempts": work.max_attempts,
        "source_kind": work.source_kind,
        "source_id": work.source_id,
        "workflow_run_id": work.workflow_run_id,
        "workflow_node_id": work.workflow_node_id,
        "schedule_id": work.schedule_id,
        "discord_thread_id": work.discord_thread_id,
        "not_before": _iso(work.not_before),
        "deadline_at": _iso(work.deadline_at),
        "created_at": _iso(work.created_at),
        "updated_at": _iso(work.updated_at),
    }
    if full:
        data.update(
            {
                "task_instruction": work.task_instruction,
                "runtime_contract": work.runtime_contract or {},
                "context": work.context or {},
                "retry_policy": work.retry_policy or {},
            }
        )
    return data


def _event_data(event: WorkEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "entity_kind": event.entity_kind,
        "entity_id": event.entity_id,
        "work_item_id": event.work_item_id,
        "attempt_id": event.attempt_id,
        "workflow_run_id": event.workflow_run_id,
        "schedule_id": event.schedule_id,
        "source": event.source,
        "summary": event.summary,
        "payload": event.payload or {},
        "created_at": _iso(event.created_at),
    }


def _schedule_data(schedule: Schedule, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": schedule.id,
        "name": schedule.name,
        "enabled": schedule.enabled,
        "schedule_type": schedule.schedule_type,
        "expression": schedule.expression,
        "timezone": schedule.timezone,
        "worker_kind": schedule.worker_kind,
        "catchup_policy": schedule.catchup_policy,
        "max_backfill": schedule.max_backfill,
        "max_active_runs": schedule.max_active_runs,
        "last_evaluated_at": _iso(schedule.last_evaluated_at),
        "created_at": _iso(schedule.created_at),
        "updated_at": _iso(schedule.updated_at),
    }
    if full:
        data.update(
            {
                "payload": schedule.payload or {},
                "runtime_contract": schedule.runtime_contract or {},
                "misfire_grace_seconds": schedule.misfire_grace_seconds,
            }
        )
    return data


def _workflow_definition_data(definition: WorkflowDefinition) -> dict[str, Any]:
    nodes = (definition.definition or {}).get("nodes") or []
    return {
        "id": definition.id,
        "name": definition.name,
        "version": definition.version,
        "enabled": definition.enabled,
        "node_count": len(nodes) if isinstance(nodes, list) else 0,
        "created_at": _iso(definition.created_at),
        "updated_at": _iso(definition.updated_at),
    }


def _workflow_run_data(run: WorkflowRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "name": run.name,
        "status": run.status,
        "workflow_definition_id": run.workflow_definition_id,
        "discord_thread_id": run.discord_thread_id,
        "started_at": _iso(run.started_at),
        "ended_at": _iso(run.ended_at),
    }


def _read_text_file(path: str | Path, *, max_chars: int) -> str:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"File does not exist: {resolved}")
    content = resolved.read_text(encoding="utf-8", errors="replace")
    return content[:max_chars]


def _run_json(callback: Callable[[], dict[str, Any]], *, intent: str | None = None) -> str:
    try:
        result = callback()
        if intent:
            result.setdefault("_intent", intent)
        return _json(result)
    except Exception as exc:
        return _json({"ok": False, "error": str(exc), "error_type": type(exc).__name__})


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _fts_query(value: str | None) -> str | None:
    if not value:
        return None
    tokens = []
    for raw in str(value).split():
        token = "".join(character for character in raw.lower() if character.isalnum() or character == "_")
        if len(token) >= 2 and token not in tokens:
            tokens.append(token)
    return " OR ".join(tokens[:12]) or None


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        return []
    result: list[str] = []
    for item in values:
        text = str(item).strip()
        if text:
            result.append(text)
    return result


def _resolve_task_instruction(
    *,
    task_instruction: str | None,
    task_template_path: str | None,
    template_base_dir: str | None,
) -> str:
    instruction = _optional_string(task_instruction)
    template_path = _optional_string(task_template_path)
    if bool(instruction) == bool(template_path):
        raise ValueError("Provide exactly one of task_instruction or task_template_path.")
    if template_path:
        base_dir = _optional_string(template_base_dir)
        return read_template_file(
            template_path,
            base_dir=Path(base_dir) if base_dir else None,
        )
    return _required(instruction, "task_instruction")


def _schedule_work_payload(
    *,
    name: str,
    task_instruction: str | None,
    task_template_path: str | None,
    template_base_dir: str | None,
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    instruction = _optional_string(task_instruction)
    template_path = _optional_string(task_template_path)
    if bool(instruction) == bool(template_path):
        raise ValueError("Provide exactly one of task_instruction or task_template_path.")
    payload: dict[str, Any] = {
        "title": _required(name, "name"),
        "context": dict(context or {}),
    }
    if template_path:
        payload["task_template_path"] = template_path
        base_dir = _optional_string(template_base_dir)
        if base_dir:
            payload["template_base_dir"] = base_dir
    else:
        payload["task_instruction"] = _required(instruction, "task_instruction")
    return payload


def _required(value: Any, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{field_name} is required.")
    return text


def _string_field(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")
    return value


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def _limit(value: int | None, *, default: int = 20, max_value: int = 100) -> int:
    if value is None:
        return default
    return max(1, min(int(value), max_value))


def _limit_chars(value: int | None) -> int:
    return _limit(value, default=20000, max_value=200000)


def _limit_bytes(value: int | None) -> int:
    return _limit(value, default=2_000_000, max_value=20_000_000)


def _content_source_id(content: str) -> str:
    digest = hashlib.sha256(str(content).encode("utf-8")).hexdigest()[:24]
    return f"text:{digest}"


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 15)] + "\n[truncated]"
