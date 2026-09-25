"""Helpers shared by every MCP tool: JSON envelopes, argument coercion, row serializers."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from tasque2.models import Artifact, Memory, Schedule, WorkEvent, WorkflowDefinition, WorkflowRun, WorkItem
from tasque2.templates import read_template_file
from tasque2.text import truncate

REPLY_CONFIG_KEYS = ("reply_followup_work",)


def run_json(callback: Callable[[], dict[str, Any]], *, intent: str | None = None) -> str:
    """Run a tool body and return ``{ok: true, ...}`` or ``{ok: false, error, error_type}`` as JSON."""
    try:
        result = callback()
        if intent:
            result.setdefault("_intent", intent)
        return json_payload(result)
    except Exception as exc:  # noqa: BLE001 - tool errors go back to the model as data
        return json_payload({"ok": False, "error": str(exc), "error_type": type(exc).__name__})


def json_payload(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=_json_default)


def calling_work_item(session: Session) -> WorkItem | None:
    """The work item whose provider run started this MCP server, when known."""
    work_item_id = (os.environ.get("TASQUE2_WORK_ITEM_ID") or "").strip()
    return session.get(WorkItem, work_item_id) if work_item_id else None


def calling_thread(session: Session) -> str | None:
    """The Discord thread the calling work answers in: its own, else the nearest parent's."""
    work = calling_work_item(session)
    while work is not None:
        if work.discord_thread_id:
            return work.discord_thread_id
        parent_id = (work.context or {}).get("parent_work_item_id")
        work = session.get(WorkItem, parent_id) if parent_id and parent_id != work.id else None
    return None


def inherit_reply_config(
    context: dict[str, Any], caller: WorkItem | None, *, parent_pointer: bool = False
) -> dict[str, Any]:
    """Give work a caller spawns the caller's reply handling, unless it declares its own."""
    if caller is None:
        return context
    caller_context = caller.context or {}
    updated = dict(context)
    if not any(key in updated for key in (*REPLY_CONFIG_KEYS, "reply_followup_disabled")):
        for key in REPLY_CONFIG_KEYS:
            if isinstance(caller_context.get(key), dict):
                updated[key] = caller_context[key]
    if "reply_memory" not in updated and isinstance(caller_context.get("reply_memory"), dict):
        updated["reply_memory"] = caller_context["reply_memory"]
    if parent_pointer:
        updated.setdefault("parent_work_item_id", caller.id)
    return updated


def resolve_instruction(
    *, task_instruction: str | None, task_template_path: str | None, template_base_dir: str | None
) -> str:
    instruction = optional_string(task_instruction)
    template_path = optional_string(task_template_path)
    if bool(instruction) == bool(template_path):
        raise ValueError("Provide exactly one of task_instruction or task_template_path.")
    if template_path:
        base_dir = optional_string(template_base_dir)
        return read_template_file(template_path, base_dir=Path(base_dir) if base_dir else None)
    return required(instruction, "task_instruction")


def read_text_file(path: str | Path, *, max_chars: int) -> str:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"File does not exist: {resolved}")
    return resolved.read_text(encoding="utf-8", errors="replace")[:max_chars]


def required(value: Any, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{field_name} is required.")
    return text


def optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def string_list(value: Any) -> list[str]:
    if value is None:
        return []
    values = [value] if isinstance(value, str) else value if isinstance(value, list) else []
    return [str(item).strip() for item in values if str(item).strip()]


def clamp(value: int | None, *, default: int = 20, maximum: int = 100) -> int:
    return default if value is None else max(1, min(int(value), maximum))


def iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def memory_data(memory: Memory | None, *, full: bool = False) -> dict[str, Any] | None:
    if memory is None:
        return None
    return {
        "id": memory.id,
        "namespace": memory.namespace,
        "kind": memory.kind,
        "content": memory.content if full else truncate(memory.content, 1200),
        "tags": memory.tags or [],
        "canonical_key": memory.canonical_key,
        "pinned": memory.pinned,
        "ttl_days": memory.ttl_days,
        "importance": memory.importance,
        "work_item_id": memory.work_item_id,
        "archived_at": iso(memory.archived_at),
        "created_at": iso(memory.created_at),
        "updated_at": iso(memory.updated_at),
    }


def memory_ack(memory: Memory) -> dict[str, Any]:
    """A short confirmation of a write, without echoing the content back."""
    return {
        "id": memory.id,
        "namespace": memory.namespace,
        "kind": memory.kind,
        "canonical_key": memory.canonical_key,
        "chars": len(memory.content),
        "updated_at": iso(memory.updated_at),
    }


def artifact_data(artifact: Artifact, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": artifact.id,
        "kind": artifact.kind,
        "title": artifact.title,
        "local_path": artifact.local_path,
        "content_type": artifact.content_type,
        "size_bytes": artifact.size_bytes,
        "tags": artifact.tags or [],
        "work_item_id": artifact.work_item_id,
        "workflow_run_id": artifact.workflow_run_id,
        "source_kind": artifact.source_kind,
        "created_at": iso(artifact.created_at),
    }
    if full:
        data.update({"sha256": artifact.sha256, "summary": artifact.summary, "archived_at": iso(artifact.archived_at)})
    return data


def work_data(work: WorkItem, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": work.id,
        "title": work.title,
        "lane": work.lane,
        "status": work.status,
        "worker_kind": work.worker_kind,
        "priority": work.priority,
        "attempt_count": work.attempt_count,
        "max_attempts": work.max_attempts,
        "source_kind": work.source_kind,
        "workflow_run_id": work.workflow_run_id,
        "schedule_id": work.schedule_id,
        "discord_thread_id": work.discord_thread_id,
        "not_before": iso(work.not_before),
        "created_at": iso(work.created_at),
        "updated_at": iso(work.updated_at),
    }
    if full:
        data.update(
            {
                "task_instruction": work.task_instruction,
                "runtime_contract": work.runtime_contract or {},
                "context": work.context or {},
            }
        )
    return data


def event_data(event: WorkEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "entity_kind": event.entity_kind,
        "entity_id": event.entity_id,
        "source": event.source,
        "summary": event.summary,
        "payload": event.payload or {},
        "created_at": iso(event.created_at),
    }


def schedule_data(schedule: Schedule, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": schedule.id,
        "name": schedule.name,
        "enabled": schedule.enabled,
        "schedule_type": schedule.schedule_type,
        "expression": schedule.expression,
        "timezone": schedule.timezone,
        "worker_kind": schedule.worker_kind,
        "catchup_policy": schedule.catchup_policy,
        "last_evaluated_at": iso(schedule.last_evaluated_at),
    }
    if full:
        data.update({"payload": schedule.payload or {}, "runtime_contract": schedule.runtime_contract or {}})
    return data


def workflow_definition_data(definition: WorkflowDefinition) -> dict[str, Any]:
    nodes = (definition.definition or {}).get("nodes") or []
    return {
        "id": definition.id,
        "name": definition.name,
        "version": definition.version,
        "enabled": definition.enabled,
        "node_count": len(nodes) if isinstance(nodes, list) else 0,
    }


def workflow_run_data(run: WorkflowRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "name": run.name,
        "status": run.status,
        "workflow_definition_id": run.workflow_definition_id,
        "discord_thread_id": run.discord_thread_id,
        "started_at": iso(run.started_at),
        "ended_at": iso(run.ended_at),
    }


def _json_default(value: Any) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)
