"""Artifact storage: files under ``data/artifacts`` with a metadata row per file."""

from __future__ import annotations

import hashlib
import mimetypes
import shutil
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import Artifact, new_id, utc_now

_LIST_SCAN_BATCH = 500


class ArtifactStore:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or get_settings().resolved_artifact_dir

    def write_text(
        self,
        session: Session,
        *,
        kind: str,
        title: str,
        content: str,
        suffix: str = ".txt",
        work_item_id: str | None = None,
        attempt_id: str | None = None,
        workflow_run_id: str | None = None,
        tags: list[str] | None = None,
        source_kind: str | None = None,
        source_id: str | None = None,
    ) -> Artifact:
        encoded = content.encode("utf-8")
        path = self._new_path(suffix)
        path.write_bytes(encoded)
        return self._record(
            session,
            kind=kind,
            title=title,
            path=path,
            size=len(encoded),
            sha256=hashlib.sha256(encoded).hexdigest(),
            content_type="text/markdown; charset=utf-8" if suffix == ".md" else "text/plain; charset=utf-8",
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            workflow_run_id=workflow_run_id,
            tags=tags,
            source_kind=source_kind,
            source_id=source_id,
        )

    def write_bytes(
        self,
        session: Session,
        *,
        kind: str,
        title: str,
        content: bytes,
        suffix: str | None = None,
        work_item_id: str | None = None,
        attempt_id: str | None = None,
        workflow_run_id: str | None = None,
        content_type: str | None = None,
        tags: list[str] | None = None,
        source_kind: str | None = None,
        source_id: str | None = None,
    ) -> Artifact:
        path = self._new_path(suffix or Path(title).suffix)
        path.write_bytes(content)
        return self._record(
            session,
            kind=kind,
            title=title,
            path=path,
            size=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            content_type=content_type or mimetypes.guess_type(title)[0],
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            workflow_run_id=workflow_run_id,
            tags=tags,
            source_kind=source_kind,
            source_id=source_id,
        )

    def capture_file(
        self,
        session: Session,
        *,
        path: str | Path,
        kind: str,
        title: str | None = None,
        work_item_id: str | None = None,
        attempt_id: str | None = None,
        workflow_run_id: str | None = None,
        content_type: str | None = None,
        tags: list[str] | None = None,
        source_kind: str | None = None,
        source_id: str | None = None,
    ) -> Artifact:
        source = Path(path).expanduser().resolve()
        if not source.is_file():
            raise FileNotFoundError(f"Artifact source file does not exist: {source}")
        artifact_title = title or source.name
        target = self._new_path(source.suffix)
        digest = hashlib.sha256()
        with source.open("rb") as reader, target.open("wb") as writer:
            while chunk := reader.read(1024 * 1024):
                digest.update(chunk)
                writer.write(chunk)
        shutil.copystat(source, target)
        return self._record(
            session,
            kind=kind,
            title=artifact_title,
            path=target,
            size=target.stat().st_size,
            sha256=digest.hexdigest(),
            content_type=content_type
            or mimetypes.guess_type(artifact_title)[0]
            or mimetypes.guess_type(source.name)[0],
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            workflow_run_id=workflow_run_id,
            tags=tags,
            source_kind=source_kind,
            source_id=source_id,
        )

    def _new_path(self, suffix: str | None) -> Path:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        return self.base_dir / f"{new_id()}{_safe_suffix(suffix)}"

    def _record(self, session: Session, *, path: Path, size: int, sha256: str, **fields) -> Artifact:
        artifact = Artifact(
            local_path=str(path),
            size_bytes=size,
            sha256=sha256,
            kind=fields["kind"],
            title=fields["title"],
            content_type=fields.get("content_type"),
            work_item_id=fields.get("work_item_id"),
            attempt_id=fields.get("attempt_id"),
            workflow_run_id=fields.get("workflow_run_id"),
            tags=list(fields.get("tags") or []),
            source_kind=fields.get("source_kind"),
            source_id=fields.get("source_id"),
        )
        session.add(artifact)
        session.flush()
        return artifact


class ArtifactService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_artifacts(
        self,
        *,
        kind: str | None = None,
        tag: list[str] | None = None,
        work_item_id: str | None = None,
        source_kind: str | None = None,
        query: str | None = None,
        include_archived: bool = False,
        limit: int = 20,
    ) -> list[Artifact]:
        statement = select(Artifact).order_by(Artifact.created_at.desc(), Artifact.id)
        if not include_archived:
            statement = statement.where(Artifact.archived_at.is_(None))
        if kind is not None:
            statement = statement.where(Artifact.kind == kind)
        if work_item_id is not None:
            statement = statement.where(Artifact.work_item_id == work_item_id)
        if source_kind is not None:
            statement = statement.where(Artifact.source_kind == source_kind)
        if not tag and not query:
            return list(self.session.scalars(statement.limit(limit)).all())
        # Tags and free text are matched here, so scan newest-first in pages until enough rows match.
        wanted = set(tag or [])
        needle = (query or "").casefold()
        matches: list[Artifact] = []
        offset = 0
        while len(matches) < limit:
            rows = self.session.scalars(statement.offset(offset).limit(_LIST_SCAN_BATCH)).all()
            matches.extend(artifact for artifact in rows if _matches(artifact, wanted, needle))
            if len(rows) < _LIST_SCAN_BATCH:
                break
            offset += _LIST_SCAN_BATCH
        return matches[:limit]

    def get_artifact(self, artifact_id: str) -> Artifact:
        artifact = self.session.get(Artifact, artifact_id)
        if artifact is None:
            raise KeyError(f"Unknown artifact: {artifact_id}")
        return artifact

    def archive_artifact(self, artifact_id: str) -> Artifact:
        artifact = self.get_artifact(artifact_id)
        if artifact.archived_at is None:
            artifact.archived_at = utc_now()
            self.session.flush()
        return artifact


@dataclass(frozen=True)
class PruneResult:
    pruned: int
    bytes_freed: int

    @property
    def megabytes_freed(self) -> float:
        return self.bytes_freed / 1048576


def prune_artifacts(
    session: Session,
    *,
    kinds: list[str] | None = None,
    older_than_days: int | None = None,
    limit: int = 2000,
) -> PruneResult:
    """Delete the files of aged-out artifacts and archive their rows.

    Rows survive, archived, as the record of what each run produced; rows whose file is
    already gone are archived too, so the store converges instead of being rescanned.
    """
    settings = get_settings()
    window = settings.artifact_retention_days if older_than_days is None else older_than_days
    target_kinds = kinds if kinds is not None else settings.artifact_retention_kind_list
    if window <= 0 or not target_kinds:
        return PruneResult(pruned=0, bytes_freed=0)
    cutoff = utc_now() - timedelta(days=window)
    rows = session.scalars(
        select(Artifact)
        .where(Artifact.kind.in_(target_kinds), Artifact.archived_at.is_(None), Artifact.created_at < cutoff)
        .order_by(Artifact.created_at.asc())
        .limit(limit)
    ).all()
    freed = 0
    now = utc_now()
    for artifact in rows:
        path = Path(artifact.local_path) if artifact.local_path else None
        if path is not None:
            try:
                if path.is_file():
                    freed += path.stat().st_size
                    path.unlink()
            except OSError:
                pass
        artifact.archived_at = now
    if rows:
        session.flush()
    return PruneResult(pruned=len(rows), bytes_freed=freed)


def _matches(artifact: Artifact, wanted_tags: set[str], needle: str) -> bool:
    tags = artifact.tags or []
    if not wanted_tags.issubset(tags):
        return False
    if not needle:
        return True
    return any(
        needle in text.casefold()
        for text in (artifact.title, artifact.kind, artifact.local_path, " ".join(tags), artifact.summary or "")
    )


def _safe_suffix(value: str | None) -> str:
    if not value:
        return ""
    suffix = value if value.startswith(".") else f".{value}"
    return "".join(ch for ch in suffix if ch.isalnum() or ch in {".", "_", "-"})[:40]
