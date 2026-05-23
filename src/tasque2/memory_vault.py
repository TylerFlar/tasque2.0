from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from tasque2.config import get_settings
from tasque2.models import Memory

SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_.-]+")


def mirror_memory(memory: Memory) -> Path | None:
    """Write a readable Markdown mirror of a Memory row.

    The database remains authoritative. The vault is for inspection, editing
    reference, and agent-readable local context.
    """
    try:
        vault_root = get_settings().resolved_memory_vault_dir
        relative_path = memory_vault_relative_path(memory)
        path = vault_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_memory_markdown(memory), encoding="utf-8")
        return path
    except Exception:
        return None


def memory_vault_relative_path(memory: Memory) -> Path:
    namespace = _safe_name(memory.namespace or "global")
    kind = _safe_name(memory.kind or "note")
    key = memory.canonical_key or memory.id
    stem = _safe_name(key)[:120] or memory.id
    return Path(namespace) / kind / f"{stem}.md"


def render_memory_markdown(memory: Memory) -> str:
    metadata: dict[str, Any] = {
        "id": memory.id,
        "namespace": memory.namespace,
        "kind": memory.kind,
        "tags": memory.tags or [],
        "canonical_key": memory.canonical_key,
        "source_kind": memory.source_kind,
        "source_id": memory.source_id,
        "work_item_id": memory.work_item_id,
        "pinned": memory.pinned,
        "ttl_days": memory.ttl_days,
        "superseded_by": memory.superseded_by,
        "archived_at": memory.archived_at.isoformat() if memory.archived_at else None,
        "created_at": memory.created_at.isoformat(),
        "updated_at": memory.updated_at.isoformat(),
    }
    compact_metadata = {key: value for key, value in metadata.items() if value not in (None, [], "")}
    front_matter = json.dumps(compact_metadata, indent=2, sort_keys=True)
    return f"---\n{front_matter}\n---\n\n{memory.content.strip()}\n"


def _safe_name(value: str) -> str:
    cleaned = SAFE_NAME_RE.sub("-", value.strip()).strip(".-")
    return cleaned or "item"
