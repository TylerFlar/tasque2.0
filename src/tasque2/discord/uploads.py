"""Fit file uploads into Discord's per-file and per-message limits.

Oversized images are re-encoded as JPEG under the per-file cap; anything still too large is
left out with a note naming its artifact. Files that only exceed one message's budget move
to a continuation message, so every file that fits the per-file cap is delivered.
"""

from __future__ import annotations

import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

MAX_FILE_BYTES = 9_500_000
MAX_REQUEST_BYTES = 24_000_000
MAX_FILES_PER_MESSAGE = 10
_SHRINKABLE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tiff"}


@dataclass(frozen=True)
class DiscordFileUpload:
    path: str
    filename: str | None = None
    artifact_id: str | None = None

    @property
    def display_name(self) -> str:
        return self.filename or Path(self.path).name


UploadBatch = tuple[list[DiscordFileUpload], list[str], list[Path]]


def batch_uploads(
    attachments: Sequence[DiscordFileUpload],
    *,
    max_file_bytes: int = MAX_FILE_BYTES,
    max_request_bytes: int = MAX_REQUEST_BYTES,
) -> list[UploadBatch]:
    """One ``(uploads, notes, temp_paths)`` triple per message, in order.

    Callers delete the temp paths after sending.
    """
    batches: list[UploadBatch] = []
    remaining = list(attachments)
    while remaining:
        sendable, notes, temps, deferred = _fit_one_message(
            remaining, max_file_bytes=max_file_bytes, max_request_bytes=max_request_bytes
        )
        if sendable or notes:
            batches.append((sendable, notes, temps))
        if not sendable and deferred:
            head, *deferred = deferred
            kept_as = head.artifact_id or head.path
            batches.append(([], [f"{head.display_name} exceeds the message budget — kept as artifact {kept_as}"], []))
        remaining = deferred
    return batches


def shrink_image(path: Path, *, max_bytes: int) -> Path | None:
    """Re-encode an image as JPEG under ``max_bytes``; None when it cannot be done."""
    try:
        from PIL import Image

        with Image.open(path) as source:
            image = source.convert("RGB")
    except Exception:  # noqa: BLE001 - unreadable images are left out with a note
        return None
    handle = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
    handle.close()
    target = Path(handle.name)
    quality = 88
    for _ in range(8):
        image.save(target, format="JPEG", quality=quality, optimize=True)
        if target.stat().st_size <= max_bytes:
            return target
        if quality > 55:
            quality -= 12
        else:
            width, height = image.size
            if min(width, height) < 320:
                break
            image = image.resize((max(1, int(width * 0.75)), max(1, int(height * 0.75))))
    target.unlink(missing_ok=True)
    return None


def _fit_one_message(
    attachments: Sequence[DiscordFileUpload],
    *,
    max_file_bytes: int,
    max_request_bytes: int,
) -> tuple[list[DiscordFileUpload], list[str], list[Path], list[DiscordFileUpload]]:
    sendable: list[DiscordFileUpload] = []
    notes: list[str] = []
    temps: list[Path] = []
    deferred: list[DiscordFileUpload] = []
    total = 0
    for original in attachments:
        if len(sendable) >= MAX_FILES_PER_MESSAGE:
            deferred.append(original)
            continue
        upload = original
        path = Path(upload.path)
        try:
            size = path.stat().st_size
        except OSError:
            notes.append(f"attachment unavailable: {upload.display_name}")
            continue
        shrunk: Path | None = None
        if size > max_file_bytes:
            if path.suffix.lower() in _SHRINKABLE_SUFFIXES:
                shrunk = shrink_image(path, max_bytes=max_file_bytes)
            if shrunk is None:
                notes.append(
                    f"{upload.display_name} ({size / 1_000_000:.1f} MB) exceeds Discord's upload cap — "
                    f"kept as artifact {upload.artifact_id or upload.path}"
                )
                continue
            size = shrunk.stat().st_size
            upload = DiscordFileUpload(
                path=str(shrunk),
                filename=(Path(upload.display_name).stem or "image") + ".jpg",
                artifact_id=upload.artifact_id,
            )
        if sendable and total + size > max_request_bytes:
            if shrunk is not None:
                shrunk.unlink(missing_ok=True)
            deferred.append(original)
            continue
        if shrunk is not None:
            temps.append(shrunk)
            notes.append(f"{original.display_name} downscaled to fit Discord's upload cap")
        total += size
        sendable.append(upload)
    return sendable, notes, temps, deferred
