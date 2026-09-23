"""Image tools: fetch, crop, compose, save, find, and send images as artifacts.

Workers see images with their own Read tool; these tools cover what Read cannot. Grouping
is done with tags the caller chooses, so nothing here is domain-specific.
"""

from __future__ import annotations

import mimetypes
from io import BytesIO
from math import ceil, sqrt
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactService, ArtifactStore
from tasque2.db import session_scope
from tasque2.mcp.tools._shared import calling_work_item, clamp, optional_string, required, run_json, string_list
from tasque2.models import Artifact

MAX_FETCH_BYTES = 25 * 1024 * 1024


def image_fetch(url: str, label: str | None = None, tags: list[str] | None = None) -> str:
    """Download an image URL into an artifact; Read the returned local_path to see it."""
    return run_json(lambda: _fetch(url, label, tags))


def image_crop(
    source: str,
    box: list[float],
    normalized: bool = False,
    label: str | None = None,
    tags: list[str] | None = None,
    send: bool = False,
) -> str:
    """Crop ``box`` = [left, top, right, bottom] (pixels, or 0-1 fractions when ``normalized``)
    out of an image path or artifact id into a new artifact."""
    return run_json(lambda: _crop(source, box, normalized, label, tags, send))


def image_compose(
    sources: list[str],
    labels: list[str] | None = None,
    columns: int | None = None,
    label: str | None = None,
    tags: list[str] | None = None,
    send: bool = False,
) -> str:
    """Tile several images (paths or artifact ids) into one captioned grid on white."""
    return run_json(lambda: _compose(sources, labels, columns, label, tags, send))


def image_save(
    source: str,
    label: str | None = None,
    tags: list[str] | None = None,
    kind: str = "image",
    send: bool = False,
) -> str:
    """Keep an image (path or artifact id) as a tagged artifact you can find later."""
    return run_json(lambda: _save(source, label, tags, kind, send))


def image_find(
    query: str | None = None, tags: list[str] | None = None, kind: str | None = None, limit: int = 20
) -> str:
    """Find stored images by tags (all must match) or a text match on title, tags, or path."""
    return run_json(lambda: _find(query, tags, kind, limit))


def image_send(
    artifact_id: str | None = None, tags: list[str] | None = None, query: str | None = None, limit: int = 10
) -> str:
    """Mark stored images to be sent with this run's result; returns their ids."""
    return run_json(lambda: _send(artifact_id, tags, query, limit))


def _store(
    session: Session,
    *,
    data: bytes,
    title: str,
    kind: str,
    tags: list[str],
    source_kind: str,
    source_id: str | None,
    content_type: str,
) -> Artifact:
    caller = calling_work_item(session)
    return ArtifactStore().write_bytes(
        session,
        kind=kind,
        title=title,
        content=data,
        suffix=Path(title).suffix,
        content_type=content_type,
        tags=tags,
        work_item_id=caller.id if caller is not None else None,
        workflow_run_id=caller.workflow_run_id if caller is not None else None,
        source_kind=source_kind,
        source_id=source_id,
    )


def _fetch(url: str, label: str | None, tags: list[str] | None) -> dict[str, Any]:
    import httpx

    target = required(url, "url")
    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        response = client.get(target, headers={"User-Agent": "Mozilla/5.0 (tasque2)"})
        response.raise_for_status()
    content_type = (response.headers.get("content-type") or "").split(";")[0].strip().lower()
    if not content_type.startswith("image/"):
        raise ValueError(f"URL did not return an image (content-type={content_type or 'unknown'}).")
    if len(response.content) > MAX_FETCH_BYTES:
        raise ValueError("Image is larger than 25 MB.")
    extension = mimetypes.guess_extension(content_type) or ".img"
    with session_scope() as session:
        artifact = _store(
            session,
            data=response.content,
            title=f"{_slug(label or 'web-image')}{extension}",
            kind="web_image",
            tags=string_list(tags),
            source_kind="image_fetch",
            source_id=target[:240],
            content_type=content_type,
        )
        return {"ok": True, "artifact_id": artifact.id, "local_path": artifact.local_path, "content_type": content_type}


def _crop(source, box, normalized, label, tags, send) -> dict[str, Any]:
    with session_scope() as session:
        path, source_id = _resolve(session, source)
        data, size = _crop_png(path, box, normalized=normalized)
        artifact = _store(
            session,
            data=data,
            title=f"{_slug(label or path.stem)}.png",
            kind="image_crop",
            tags=_with_send(tags, send),
            source_kind="image_crop",
            source_id=source_id,
            content_type="image/png",
        )
        return {
            "ok": True,
            "artifact_id": artifact.id,
            "local_path": artifact.local_path,
            "width": size[0],
            "height": size[1],
        }


def _compose(sources, labels, columns, label, tags, send) -> dict[str, Any]:
    from PIL import Image, ImageDraw, ImageFont

    if not isinstance(sources, list) or not sources:
        raise ValueError("sources must be a non-empty list of image paths or artifact ids.")
    captions = [str(item) for item in labels] if isinstance(labels, list) else []
    cell, gap, caption_height = 400, 20, (30 if captions else 0)
    with session_scope() as session:
        tiles = []
        for source in sources:
            path, _ = _resolve(session, str(source))
            with Image.open(path) as image:
                tile = image.convert("RGB")
                tile.thumbnail((cell, cell))
                tiles.append(tile.copy())
        cols = max(1, int(columns)) if columns else max(1, ceil(sqrt(len(tiles))))
        rows = ceil(len(tiles) / cols)
        canvas = Image.new(
            "RGB", (gap + cols * (cell + gap), gap + rows * (cell + caption_height + gap)), (255, 255, 255)
        )
        draw = ImageDraw.Draw(canvas)
        font = ImageFont.load_default()
        for index, tile in enumerate(tiles):
            row, col = divmod(index, cols)
            x0, y0 = gap + col * (cell + gap), gap + row * (cell + caption_height + gap)
            canvas.paste(tile, (x0 + (cell - tile.width) // 2, y0 + (cell - tile.height) // 2))
            if index < len(captions):
                draw.text((x0, y0 + cell + 6), captions[index][:42], fill=(60, 60, 60), font=font)
        buffer = BytesIO()
        canvas.save(buffer, format="PNG")
        artifact = _store(
            session,
            data=buffer.getvalue(),
            title=f"{_slug(label or 'collage')}.png",
            kind="collage",
            tags=_with_send(tags, send),
            source_kind="image_compose",
            source_id=None,
            content_type="image/png",
        )
        return {"ok": True, "artifact_id": artifact.id, "local_path": artifact.local_path, "tiles": len(tiles)}


def _save(source, label, tags, kind, send) -> dict[str, Any]:
    with session_scope() as session:
        path, source_id = _resolve(session, source)
        caller = calling_work_item(session)
        artifact = ArtifactStore().capture_file(
            session,
            path=path,
            kind=optional_string(kind) or "image",
            title=f"{_slug(label) if label else path.stem}{path.suffix}",
            tags=_with_send(tags, send),
            work_item_id=caller.id if caller is not None else None,
            workflow_run_id=caller.workflow_run_id if caller is not None else None,
            source_kind="image_save",
            source_id=source_id,
        )
        return {"ok": True, "artifact_id": artifact.id, "local_path": artifact.local_path, "tags": artifact.tags}


def _find(query, tags, kind, limit) -> dict[str, Any]:
    with session_scope() as session:
        rows = ArtifactService(session).list_artifacts(
            kind=optional_string(kind), tag=string_list(tags) or None, query=optional_string(query), limit=clamp(limit)
        )
        items = [
            {
                "artifact_id": artifact.id,
                "label": artifact.title,
                "local_path": artifact.local_path,
                "content_type": artifact.content_type,
                "tags": artifact.tags or [],
            }
            for artifact in rows
        ]
        return {"ok": True, "count": len(items), "items": items}


def _send(artifact_id, tags, query, limit) -> dict[str, Any]:
    with session_scope() as session:
        service = ArtifactService(session)
        if artifact_id:
            targets = [service.get_artifact(required(artifact_id, "artifact_id"))]
        elif tags or query:
            targets = service.list_artifacts(
                tag=string_list(tags) or None, query=optional_string(query), limit=clamp(limit)
            )
        else:
            raise ValueError("Provide artifact_id, tags, or query.")
        caller = calling_work_item(session)
        for artifact in targets:
            if "discord_upload" not in (artifact.tags or []):
                artifact.tags = [*(artifact.tags or []), "discord_upload"]
            if caller is not None and artifact.work_item_id is None:
                artifact.work_item_id = caller.id
        session.flush()
        ids = [artifact.id for artifact in targets]
        return {"ok": bool(ids), "artifact_ids": ids, "error": None if ids else "No matching images."}


def _resolve(session: Session, source: str) -> tuple[Path, str | None]:
    text = required(source, "source")
    artifact = session.get(Artifact, text)
    if artifact is not None:
        path = Path(artifact.local_path)
        if not path.is_file():
            raise FileNotFoundError(f"Artifact {text} has no file on disk: {path}")
        return path, artifact.id
    path = Path(text).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"source is neither a known artifact id nor an existing file: {source}")
    return path, None


def _crop_png(path: Path, box: Any, *, normalized: bool) -> tuple[bytes, tuple[int, int]]:
    from PIL import Image

    if not isinstance(box, list | tuple) or len(box) != 4:
        raise ValueError("box must be [left, top, right, bottom].")
    with Image.open(path) as image:
        if image.mode not in ("RGB", "RGBA"):
            image = image.convert("RGB")
        width, height = image.size
        left, top, right, bottom = (float(value) for value in box)
        if normalized:
            left, right, top, bottom = left * width, right * width, top * height, bottom * height
        crop = (max(0, round(left)), max(0, round(top)), min(width, round(right)), min(height, round(bottom)))
        if crop[2] <= crop[0] or crop[3] <= crop[1]:
            raise ValueError(f"Empty crop box {crop} for a {width}x{height} image.")
        cropped = image.crop(crop)
        buffer = BytesIO()
        cropped.save(buffer, format="PNG")
        return buffer.getvalue(), cropped.size


def _with_send(tags: list[str] | None, send: bool) -> list[str]:
    clean = string_list(tags)
    if send and "discord_upload" not in clean:
        clean.append("discord_upload")
    return clean


def _slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value).strip())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-")[:60] or "item"
