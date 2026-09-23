from __future__ import annotations

from pathlib import Path
from typing import Any

from tasque2.artifacts import ArtifactService, ArtifactStore
from tasque2.db import session_scope
from tasque2.mcp.tools._shared import (
    artifact_data,
    calling_work_item,
    clamp,
    optional_string,
    read_text_file,
    required,
    run_json,
    string_list,
)


def artifact_list(
    query: str | None = None,
    kind: str | None = None,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    limit: int = 20,
    intent: str = "",
) -> str:
    """List stored artifacts (files) by text match, kind, tags, or owning work item."""
    return run_json(lambda: _list(query, kind, tags, work_item_id, limit), intent=intent)


def artifact_get(artifact_id: str, include_text: bool = False, max_chars: int = 20000, intent: str = "") -> str:
    """Fetch an artifact's metadata and local path, optionally with its text."""
    return run_json(lambda: _get(artifact_id, include_text, max_chars), intent=intent)


def artifact_read_text(
    artifact_id: str | None = None, path: str | None = None, max_chars: int = 20000, intent: str = ""
) -> str:
    """Read the text of an artifact or a local file."""
    return run_json(lambda: _read(artifact_id, path, max_chars), intent=intent)


def artifact_capture_file(
    path: str,
    kind: str = "worker_file",
    title: str | None = None,
    tags: list[str] | None = None,
    discord_upload: bool = False,
) -> str:
    """Copy a local file into artifact storage so it outlives the run.

    ``discord_upload`` sends it to the user with this run's result.
    """
    return run_json(lambda: _capture(path, kind, title, tags, discord_upload))


def _list(query, kind, tags, work_item_id, limit) -> dict[str, Any]:
    with session_scope() as session:
        rows = ArtifactService(session).list_artifacts(
            query=optional_string(query),
            kind=optional_string(kind),
            tag=string_list(tags),
            work_item_id=optional_string(work_item_id),
            limit=clamp(limit),
        )
        return {"ok": True, "items": [artifact_data(artifact) for artifact in rows]}


def _get(artifact_id: str, include_text: bool, max_chars: int) -> dict[str, Any]:
    with session_scope() as session:
        data = artifact_data(ArtifactService(session).get_artifact(required(artifact_id, "artifact_id")), full=True)
    if include_text:
        data["text"] = read_text_file(data["local_path"], max_chars=clamp(max_chars, default=20000, maximum=200000))
    return {"ok": True, "artifact": data}


def _read(artifact_id: str | None, path: str | None, max_chars: int) -> dict[str, Any]:
    if artifact_id:
        with session_scope() as session:
            path = ArtifactService(session).get_artifact(artifact_id).local_path
    if not path:
        raise ValueError("artifact_id or path is required.")
    return {
        "ok": True,
        "path": str(Path(path).expanduser().resolve()),
        "text": read_text_file(path, max_chars=clamp(max_chars, default=20000, maximum=200000)),
    }


def _capture(path: str, kind: str, title: str | None, tags: list[str] | None, discord_upload: bool) -> dict[str, Any]:
    clean_tags = string_list(tags)
    if discord_upload and "discord_upload" not in clean_tags:
        clean_tags.append("discord_upload")
    with session_scope() as session:
        caller = calling_work_item(session)
        artifact = ArtifactStore().capture_file(
            session,
            path=required(path, "path"),
            kind=required(kind, "kind"),
            title=optional_string(title),
            tags=clean_tags,
            work_item_id=caller.id if caller is not None else None,
            workflow_run_id=caller.workflow_run_id if caller is not None else None,
            source_kind="mcp",
            source_id=str(Path(path).expanduser()),
        )
        return {"ok": True, "artifact": artifact_data(artifact)}
