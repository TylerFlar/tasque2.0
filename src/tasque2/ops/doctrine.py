"""Canonical documents as files: export them for editing, apply the edits back safely.

``export_doctrine`` writes each active canonical memory to ``<dir>/<namespace>/<key>.md`` and
records the exported content's hash in ``<dir>/snapshot.json``. ``apply_doctrine`` makes the live
documents match the directory: edited files replace their documents, deleted files archive
theirs, and new files become new documents. It touches a document only while the live copy
still has the exported hash, so one a worker changed after the export is reported as drifted
instead of being overwritten. Replaced and archived versions stay on record.

Documents that hold credentials (a key or tag naming a credential, password or secret) are never
written to disk unless the export asks for them, and apply leaves any document it did not export
alone.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.memory import MemoryBudgetExceeded, MemoryService
from tasque2.models import Memory

SNAPSHOT_FILE = "snapshot.json"
SECRET_MARKERS = ("credential", "password", "secret")
_SAFE_FILENAME_CHARS = "-_.~ ()[],'!&+=@"


@dataclass(frozen=True)
class DoctrineChange:
    namespace: str
    canonical_key: str
    status: str
    detail: str = ""


def export_doctrine(
    session: Session, directory: Path, *, namespaces: list[str] | None = None, include_secrets: bool = False
) -> list[dict[str, Any]]:
    statement = (
        select(Memory)
        .where(Memory.canonical_key.is_not(None), Memory.archived_at.is_(None))
        .order_by(Memory.namespace, Memory.canonical_key)
    )
    if namespaces:
        statement = statement.where(Memory.namespace.in_(namespaces))
    entries: list[dict[str, Any]] = []
    for memory in session.scalars(statement).all():
        if not include_secrets and holds_secrets(memory):
            continue
        path = document_path(directory, memory.namespace, memory.canonical_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(memory.content, encoding="utf-8", newline="\n")
        entries.append(
            {
                "namespace": memory.namespace,
                "canonical_key": memory.canonical_key,
                "memory_id": memory.id,
                "kind": memory.kind,
                "pinned": memory.pinned,
                "tags": memory.tags or [],
                "sha256": content_hash(memory.content),
                "chars": len(memory.content),
            }
        )
    directory.mkdir(parents=True, exist_ok=True)
    (directory / SNAPSHOT_FILE).write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")
    return entries


def holds_secrets(memory: Memory) -> bool:
    names = [memory.canonical_key or "", *(memory.tags or [])]
    return any(marker in name.lower() for name in names for marker in SECRET_MARKERS)


def apply_doctrine(session: Session, directory: Path, *, dry_run: bool = False) -> list[DoctrineChange]:
    """Make the live documents match the directory.

    A snapshot entry whose file changed replaces the live document; one whose file was deleted
    archives it. A file with no snapshot entry becomes a new document. Any live document that
    changed since the export is left alone and reported as drifted.
    """
    snapshot = json.loads((directory / SNAPSHOT_FILE).read_text(encoding="utf-8"))
    service = MemoryService(session)
    changes: list[DoctrineChange] = []
    known = {(entry["namespace"], entry["canonical_key"]) for entry in snapshot}
    for entry in snapshot:
        namespace, key = entry["namespace"], entry["canonical_key"]
        path = document_path(directory, namespace, key)
        live = service.get_canonical(namespace=namespace, canonical_key=key)
        if live is None:
            changes.append(DoctrineChange(namespace, key, "missing_memory"))
            continue
        if content_hash(live.content) != entry["sha256"]:
            changes.append(DoctrineChange(namespace, key, "drifted", "changed since the export; merge by hand"))
            continue
        if not path.is_file():
            if not dry_run:
                service.archive_memory(live.id)
            changes.append(DoctrineChange(namespace, key, "retired", f"{len(live.content)} chars archived"))
            continue
        content = read_document(path)
        if live.content == content:
            changes.append(DoctrineChange(namespace, key, "unchanged"))
            continue
        detail = f"{len(live.content)} -> {len(content)} chars"
        if not dry_run:
            try:
                service.upsert_canonical(
                    namespace=namespace,
                    canonical_key=key,
                    kind=live.kind,
                    content=content,
                    tags=list(live.tags or []),
                    source_kind="doctrine_apply",
                    pinned=live.pinned,
                    ttl_days=live.ttl_days,
                )
            except MemoryBudgetExceeded as exc:
                changes.append(DoctrineChange(namespace, key, "over_budget", str(exc)))
                continue
        changes.append(DoctrineChange(namespace, key, "applied", detail))
    for path in sorted(directory.glob("*/*.md")):
        namespace, key = path.parent.name, unquote(path.stem)
        if (namespace, key) in known:
            continue
        if service.get_canonical(namespace=namespace, canonical_key=key) is not None:
            changes.append(DoctrineChange(namespace, key, "unmanaged", "a live document exists; export it first"))
            continue
        content = read_document(path)
        if not dry_run:
            try:
                service.upsert_canonical(
                    namespace=namespace,
                    canonical_key=key,
                    kind="doctrine",
                    content=content,
                    tags=[namespace, key],
                    source_kind="doctrine_apply",
                    pinned=True,
                )
            except MemoryBudgetExceeded as exc:
                changes.append(DoctrineChange(namespace, key, "over_budget", str(exc)))
                continue
        changes.append(DoctrineChange(namespace, key, "created", f"{len(content)} chars"))
    return changes


def document_path(directory: Path, namespace: str, canonical_key: str) -> Path:
    """Where a document lives in the directory; characters a filename cannot hold are percent-encoded."""
    return directory / namespace / f"{quote(canonical_key, safe=_SAFE_FILENAME_CHARS)}.md"


def read_document(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
