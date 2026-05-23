from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactService
from tasque2.compression import compress_text
from tasque2.memory import MemoryService
from tasque2.models import Artifact, DiscordMessage, Memory, WorkItem

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
AUTO_INGEST_ARTIFACT_KINDS = {"discord_attachment", "worker_file", "worker_report"}
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


@dataclass(frozen=True)
class AutoIngestResult:
    ingested_sources: int
    skipped_sources: int
    memory_ids: list[str]


class MemoryIngestService:
    """Turn local text sources into searchable Tasque memories.

    The original source artifacts/messages remain authoritative. Ingested
    memories provide a compact summary plus searchable chunks so workers can
    find relevant context without hand-parsing paths or raw database rows.
    """

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

        existing = self._existing_source_memory(source_kind=source_kind, source_id=source_id)
        if existing is not None and not force:
            chunk_ids = [
                memory.id
                for memory in self._source_chunk_memories(source_kind=source_kind, source_id=source_id)
            ]
            return MemoryIngestResult(
                source_memory_id=existing.id,
                chunk_memory_ids=chunk_ids,
                source_kind=source_kind,
                source_id=source_id,
                skipped=True,
                reason="already_ingested",
            )

        clean_tags = _dedupe(["ingested", *list(tags or [])])
        chunks = _chunk_text(content, chunk_chars=max(1000, int(chunk_chars or DEFAULT_CHUNK_CHARS)))
        source_key = _source_key(source_kind, source_id)
        summary_content = _source_summary_content(
            title=title,
            source_kind=source_kind,
            source_id=source_id,
            chunk_count=len(chunks),
            content=content,
        )
        source_memory = self.memory.upsert_canonical(
            namespace=namespace,
            canonical_key=source_key,
            kind="source_summary",
            content=summary_content,
            tags=_dedupe([*clean_tags, "source_summary", source_kind]),
            source_kind=source_kind,
            source_id=source_id,
            work_item_id=work_item_id,
        )

        chunk_memory_ids: list[str] = []
        for index, chunk in enumerate(chunks, start=1):
            digest = hashlib.sha256(chunk.encode("utf-8")).hexdigest()[:16]
            memory = self.memory.upsert_canonical(
                namespace=namespace,
                canonical_key=f"{source_key}:chunk:{index}",
                kind="source_chunk",
                content=_chunk_content(
                    title=title,
                    source_kind=source_kind,
                    source_id=source_id,
                    index=index,
                    count=len(chunks),
                    digest=digest,
                    chunk=chunk,
                ),
                tags=_dedupe([*clean_tags, "source_chunk", source_kind]),
                source_kind=source_kind,
                source_id=source_id,
                work_item_id=work_item_id,
            )
            chunk_memory_ids.append(memory.id)

        return MemoryIngestResult(
            source_memory_id=source_memory.id,
            chunk_memory_ids=chunk_memory_ids,
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
        artifact = ArtifactService(self.session).get_artifact(_required(artifact_id, "artifact_id"))
        if not _is_text_artifact(artifact):
            return None
        path = Path(artifact.local_path)
        if not path.is_file():
            raise FileNotFoundError(f"Artifact file does not exist: {artifact.local_path}")
        if artifact.size_bytes is not None and artifact.size_bytes > max_bytes:
            return None
        raw = path.read_bytes()
        if len(raw) > max_bytes:
            return None
        content = raw.decode("utf-8", errors="replace")
        work_item = self.session.get(WorkItem, artifact.work_item_id) if artifact.work_item_id else None
        return self.ingest_text(
            namespace=namespace or _artifact_namespace(work_item) or "global",
            title=artifact.title,
            content=content,
            source_kind="artifact",
            source_id=artifact.id,
            tags=_dedupe([*(artifact.tags or []), "artifact", *list(tags or [])]),
            work_item_id=artifact.work_item_id,
            force=force,
        )

    def auto_ingest_pending(self, *, limit: int = 25) -> AutoIngestResult:
        memory_ids: list[str] = []
        ingested = 0
        skipped = 0

        for artifact in self._pending_artifacts(limit=limit):
            result = self.ingest_artifact(artifact.id)
            if result is None:
                skipped += 1
                continue
            if result.skipped:
                skipped += 1
            else:
                ingested += 1
                memory_ids.extend(result.memory_ids)

        remaining = max(0, limit - ingested - skipped)
        for message in self._pending_discord_messages(limit=remaining):
            result = self.ingest_text(
                namespace="discord",
                title=f"Discord message from {message.author or 'unknown'}",
                content=message.content_preview,
                source_kind="discord_message",
                source_id=f"message:{message.discord_message_id}",
                tags=["discord", "message", message.direction],
                work_item_id=message.work_item_id,
            )
            if result.skipped:
                skipped += 1
            else:
                ingested += 1
                memory_ids.extend(result.memory_ids)

        return AutoIngestResult(
            ingested_sources=ingested,
            skipped_sources=skipped,
            memory_ids=memory_ids,
        )

    def _existing_source_memory(self, *, source_kind: str, source_id: str) -> Memory | None:
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

    def _source_chunk_memories(self, *, source_kind: str, source_id: str) -> list[Memory]:
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

    def _pending_artifacts(self, *, limit: int) -> list[Artifact]:
        rows = self.session.scalars(
            select(Artifact)
            .where(Artifact.archived_at.is_(None), Artifact.kind.in_(AUTO_INGEST_ARTIFACT_KINDS))
            .order_by(Artifact.created_at.desc())
            .limit(limit * 4)
        ).all()
        pending: list[Artifact] = []
        for artifact in rows:
            if len(pending) >= limit:
                break
            if not _is_text_artifact(artifact):
                continue
            if self._existing_source_memory(source_kind="artifact", source_id=artifact.id) is None:
                pending.append(artifact)
        return pending

    def _pending_discord_messages(self, *, limit: int) -> list[DiscordMessage]:
        if limit <= 0:
            return []
        rows = self.session.scalars(
            select(DiscordMessage)
            .where(DiscordMessage.direction == "inbound")
            .order_by(DiscordMessage.created_at.desc())
            .limit(limit * 4)
        ).all()
        pending: list[DiscordMessage] = []
        for message in rows:
            if len(pending) >= limit:
                break
            if not message.content_preview.strip():
                continue
            if (
                self._existing_source_memory(
                    source_kind="discord_message",
                    source_id=f"message:{message.discord_message_id}",
                )
                is None
            ):
                pending.append(message)
        return pending


def _source_key(source_kind: str, source_id: str) -> str:
    digest = hashlib.sha256(f"{source_kind}:{source_id}".encode()).hexdigest()[:16]
    return f"source:{source_kind}:{digest}"


def _source_summary_content(
    *,
    title: str,
    source_kind: str,
    source_id: str,
    chunk_count: int,
    content: str,
) -> str:
    preview = compress_text(content, max_chars=1800, preserve_lines=40)
    return (
        f"# {title}\n\n"
        f"- source_kind: {source_kind}\n"
        f"- source_id: {source_id}\n"
        f"- chunks: {chunk_count}\n\n"
        "## Preview\n"
        f"{preview}"
    ).strip()


def _chunk_content(
    *,
    title: str,
    source_kind: str,
    source_id: str,
    index: int,
    count: int,
    digest: str,
    chunk: str,
) -> str:
    return (
        f"# {title} chunk {index}/{count}\n\n"
        f"- source_kind: {source_kind}\n"
        f"- source_id: {source_id}\n"
        f"- chunk_sha256: {digest}\n\n"
        "## Content\n"
        f"{chunk.strip()}"
    ).strip()


def _chunk_text(content: str, *, chunk_chars: int) -> list[str]:
    paragraphs = content.split("\n\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        paragraph_len = len(paragraph) + 2
        if current and current_len + paragraph_len > chunk_chars:
            chunks.append("\n\n".join(current))
            current = []
            current_len = 0
        if paragraph_len > chunk_chars:
            chunks.extend(_hard_wrap(paragraph, chunk_chars))
            continue
        current.append(paragraph)
        current_len += paragraph_len
    if current:
        chunks.append("\n\n".join(current))
    return chunks or [content[:chunk_chars]]


def _hard_wrap(value: str, chunk_chars: int) -> list[str]:
    return [value[index : index + chunk_chars] for index in range(0, len(value), chunk_chars)]


def _is_text_artifact(artifact: Artifact) -> bool:
    content_type = (artifact.content_type or "").casefold()
    if content_type.startswith("text/"):
        return True
    if any(token in content_type for token in ("json", "xml", "yaml", "markdown")):
        return True
    return Path(artifact.title or artifact.local_path).suffix.casefold() in TEXT_SUFFIXES


def _artifact_namespace(work_item: WorkItem | None) -> str | None:
    if work_item is None:
        return None
    context = work_item.context or {}
    namespace = context.get("memory_namespace")
    if namespace:
        return str(namespace)
    namespaces = context.get("memory_namespaces")
    if isinstance(namespaces, list) and namespaces:
        return str(namespaces[0])
    return None


def _required(value: Any, name: str) -> str:
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
