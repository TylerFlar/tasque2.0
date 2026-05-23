from __future__ import annotations

import hashlib
import mimetypes
import shutil
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import Artifact, new_id
from tasque2.repo import WorkRepository


class ArtifactStore:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or (get_settings().resolved_data_dir / "artifacts")

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
        self.base_dir.mkdir(parents=True, exist_ok=True)
        artifact_id = new_id()
        path = self.base_dir / f"{artifact_id}{suffix}"
        encoded = content.encode("utf-8")
        path.write_bytes(encoded)
        return WorkRepository(session).record_artifact(
            kind=kind,
            title=title,
            local_path=str(path),
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            workflow_run_id=workflow_run_id,
            content_type="text/plain; charset=utf-8",
            size_bytes=len(encoded),
            sha256=hashlib.sha256(encoded).hexdigest(),
            tags=tags or [],
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
        self.base_dir.mkdir(parents=True, exist_ok=True)
        artifact_id = new_id()
        path = self.base_dir / f"{artifact_id}{_safe_suffix(suffix or Path(title).suffix)}"
        path.write_bytes(content)
        return WorkRepository(session).record_artifact(
            kind=kind,
            title=title,
            local_path=str(path),
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            workflow_run_id=workflow_run_id,
            content_type=content_type or _guess_content_type(title),
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            tags=tags or [],
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
        source_path = Path(path).expanduser().resolve()
        if not source_path.is_file():
            raise FileNotFoundError(f"Artifact source file does not exist: {source_path}")

        self.base_dir.mkdir(parents=True, exist_ok=True)
        artifact_id = new_id()
        artifact_title = title or source_path.name
        target = self.base_dir / f"{artifact_id}{_safe_suffix(source_path.suffix)}"
        digest = hashlib.sha256()
        with source_path.open("rb") as source, target.open("wb") as destination:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
                destination.write(chunk)
        shutil.copystat(source_path, target)

        return WorkRepository(session).record_artifact(
            kind=kind,
            title=artifact_title,
            local_path=str(target),
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            workflow_run_id=workflow_run_id,
            content_type=content_type or _guess_content_type(artifact_title),
            size_bytes=target.stat().st_size,
            sha256=digest.hexdigest(),
            tags=tags or [],
            source_kind=source_kind,
            source_id=source_id,
        )


def _safe_suffix(value: str | None) -> str:
    if not value:
        return ""
    suffix = value if value.startswith(".") else f".{value}"
    suffix = "".join(character for character in suffix if character.isalnum() or character in {".", "_", "-"})
    return suffix[:40]


def _guess_content_type(title: str) -> str | None:
    return mimetypes.guess_type(title)[0]


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
        statement = select(Artifact).order_by(Artifact.created_at.desc()).limit(limit * 4)
        if not include_archived:
            statement = statement.where(Artifact.archived_at.is_(None))
        if kind is not None:
            statement = statement.where(Artifact.kind == kind)
        if work_item_id is not None:
            statement = statement.where(Artifact.work_item_id == work_item_id)
        if source_kind is not None:
            statement = statement.where(Artifact.source_kind == source_kind)

        rows = list(self.session.scalars(statement).all())
        if tag:
            wanted = set(tag)
            rows = [artifact for artifact in rows if wanted.issubset(set(artifact.tags or []))]
        if query:
            needle = query.casefold()
            rows = [
                artifact
                for artifact in rows
                if needle in artifact.title.casefold()
                or needle in artifact.kind.casefold()
                or needle in artifact.local_path.casefold()
                or needle in " ".join(artifact.tags or []).casefold()
                or needle in str(artifact.summary or "").casefold()
            ]
        return rows[:limit]

    def archive_artifact(self, artifact_id: str) -> Artifact:
        from tasque2.models import utc_now

        artifact = self.get_artifact(artifact_id)
        if artifact.archived_at is None:
            artifact.archived_at = utc_now()
            self.session.flush()
        return artifact

    def get_artifact(self, artifact_id: str) -> Artifact:
        artifact = self.session.get(Artifact, artifact_id)
        if artifact is None:
            raise KeyError(f"Unknown artifact: {artifact_id}")
        return artifact
