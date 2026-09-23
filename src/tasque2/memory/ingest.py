"""Turn text sources into searchable memories: one summary row plus content chunks.

The source (an artifact, a file, pasted text) stays authoritative; the ingested rows let
workers find it through memory search.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactService
from tasque2.memory.service import MemoryService
from tasque2.models import Artifact, Memory, WorkItem
from tasque2.text import compress_text

TEXT_SUFFIXES = {
    ".css",
    ".csv",
    ".htm",
    ".html",
    ".ini",
    ".js",
    ".json",
    ".jsonl",
    ".log",
    ".md",
    ".py",
    ".rst",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}
DEFAULT_CHUNK_CHARS = 12_000
DEFAULT_MAX_ARTIFACT_BYTES = 2_000_000


@dataclass(frozen=True)
class MemoryIngestResult:
    source_memory_id: str
    chunk_memory_ids: list[str]
    source_kind: str
    source_id: str
    skipped: bool = False
    reason: str | None = None

    @property
    def memory_ids(self) -> list[str]:
        return [self.source_memory_id, *self.chunk_memory_ids]


class MemoryIngestService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.memory = MemoryService(session)

    def ingest_text(
        self,
        *,
        namespace: str,
        title: str,
        content: str,
        source_kind: str,
        source_id: str,
        tags: list[str] | None = None,
        work_item_id: str | None = None,
        chunk_chars: int = DEFAULT_CHUNK_CHARS,
        force: bool = False,
    ) -> MemoryIngestResult:
        source_kind = _required(source_kind, "source_kind")
        source_id = _required(source_id, "source_id")
        namespace = _required(namespace, "namespace")
        title = _required(title, "title")
        content = content.replace("\r\n", "\n").replace("\r", "\n").strip()
        if not content:
            raise ValueError("content cannot be empty.")

        existing = self._source_memory(source_kind=source_kind, source_id=source_id)
        if existing is not None and not force:
            return MemoryIngestResult(
                source_memory_id=existing.id,
                chunk_memory_ids=[m.id for m in self._chunk_memories(source_kind=source_kind, source_id=source_id)],
                source_kind=source_kind,
                source_id=source_id,
                skipped=True,
                reason="already_ingested",
            )

        base_tags = _dedupe(["ingested", *list(tags or [])])
        chunks = _chunk_text(content, chunk_chars=max(1000, int(chunk_chars or DEFAULT_CHUNK_CHARS)))
        source_key = _source_key(source_kind, source_id)
        source_memory = self.memory.upsert_canonical(
            namespace=namespace,
            canonical_key=source_key,
            kind="source_summary",
            content=_summary_content(
                title=title, source_kind=source_kind, source_id=source_id, chunk_count=len(chunks), content=content
            ),
            tags=_dedupe([*base_tags, "source_summary", source_kind]),
            source_kind=source_kind,
            source_id=source_id,
            work_item_id=work_item_id,
        )
        chunk_ids: list[str] = []
        for index, chunk in enumerate(chunks, start=1):
            digest = hashlib.sha256(chunk.encode("utf-8")).hexdigest()[:16]
            memory = self.memory.upsert_canonical(
                namespace=namespace,
                canonical_key=f"{source_key}:chunk:{index}",
                kind="source_chunk",
                content=(
                    f"# {title} chunk {index}/{len(chunks)}\n\n"
                    f"- source_kind: {source_kind}\n- source_id: {source_id}\n- chunk_sha256: {digest}\n\n"
                    f"## Content\n{chunk.strip()}"
                ),
                tags=_dedupe([*base_tags, "source_chunk", source_kind]),
                source_kind=source_kind,
                source_id=source_id,
                work_item_id=work_item_id,
            )
            chunk_ids.append(memory.id)
        for stale in self._chunk_memories(source_kind=source_kind, source_id=source_id):
            if stale.namespace == namespace and stale.id not in chunk_ids:
                self.memory.archive_memory(stale.id)
        return MemoryIngestResult(
            source_memory_id=source_memory.id,
            chunk_memory_ids=chunk_ids,
            source_kind=source_kind,
            source_id=source_id,
        )

    def ingest_artifact(
        self,
        artifact_id: str,
        *,
        namespace: str | None = None,
        tags: list[str] | None = None,
        max_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
        force: bool = False,
    ) -> MemoryIngestResult | None:
        """Ingest a text artifact; returns None for binary or oversized files."""
        artifact = ArtifactService(self.session).get_artifact(_required(artifact_id, "artifact_id"))
        if not is_text_artifact(artifact):
            return None
        path = Path(artifact.local_path)
        if not path.is_file():
            raise FileNotFoundError(f"Artifact file does not exist: {artifact.local_path}")
        if (artifact.size_bytes or 0) > max_bytes or path.stat().st_size > max_bytes:
            return None
        work_item = self.session.get(WorkItem, artifact.work_item_id) if artifact.work_item_id else None
        return self.ingest_text(
            namespace=namespace or _work_namespace(work_item) or "global",
            title=artifact.title,
            content=path.read_text(encoding="utf-8", errors="replace"),
            source_kind="artifact",
            source_id=artifact.id,
            tags=_dedupe([*(artifact.tags or []), "artifact", *list(tags or [])]),
            work_item_id=artifact.work_item_id,
            force=force,
        )

    def _source_memory(self, *, source_kind: str, source_id: str) -> Memory | None:
        return self.session.scalar(
            select(Memory)
            .where(
                Memory.source_kind == source_kind,
                Memory.source_id == source_id,
                Memory.kind == "source_summary",
                Memory.archived_at.is_(None),
            )
            .order_by(Memory.created_at.desc())
        )

    def _chunk_memories(self, *, source_kind: str, source_id: str) -> list[Memory]:
        return list(
            self.session.scalars(
                select(Memory)
                .where(
                    Memory.source_kind == source_kind,
                    Memory.source_id == source_id,
                    Memory.kind == "source_chunk",
                    Memory.archived_at.is_(None),
                )
                .order_by(Memory.canonical_key.asc())
            ).all()
        )


def is_text_artifact(artifact: Artifact) -> bool:
    content_type = (artifact.content_type or "").casefold()
    if content_type.startswith("text/") or any(t in content_type for t in ("json", "xml", "yaml", "markdown")):
        return True
    return Path(artifact.title or artifact.local_path).suffix.casefold() in TEXT_SUFFIXES


def _source_key(source_kind: str, source_id: str) -> str:
    digest = hashlib.sha256(f"{source_kind}:{source_id}".encode()).hexdigest()[:16]
    return f"source:{source_kind}:{digest}"


def _summary_content(*, title: str, source_kind: str, source_id: str, chunk_count: int, content: str) -> str:
    preview = compress_text(content, max_chars=1800, preserve_lines=40)
    return (
        f"# {title}\n\n- source_kind: {source_kind}\n- source_id: {source_id}\n- chunks: {chunk_count}\n\n"
        f"## Preview\n{preview}"
    ).strip()


def _chunk_text(content: str, *, chunk_chars: int) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for paragraph in content.split("\n\n"):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        paragraph_len = len(paragraph) + 2
        if current and current_len + paragraph_len > chunk_chars:
            chunks.append("\n\n".join(current))
            current, current_len = [], 0
        if paragraph_len > chunk_chars:
            chunks.extend(paragraph[i : i + chunk_chars] for i in range(0, len(paragraph), chunk_chars))
            continue
        current.append(paragraph)
        current_len += paragraph_len
    if current:
        chunks.append("\n\n".join(current))
    return chunks or [content[:chunk_chars]]


def _work_namespace(work_item: WorkItem | None) -> str | None:
    if work_item is None:
        return None
    context = work_item.context or {}
    if context.get("memory_namespace"):
        return str(context["memory_namespace"])
    namespaces = context.get("memory_namespaces")
    if isinstance(namespaces, list) and namespaces:
        return str(namespaces[0])
    return None


def _required(value: object, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required.")
    return text


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in result:
            result.append(text)
    return result
