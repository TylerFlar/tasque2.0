"""Lane context files: a lane's packet and reply configuration, read from disk where it is used.

A work item, schedule payload or thread owner may carry ``context_file``: the path of a lane's
``context.json``, relative to the project directory. Wherever that context is used, the file is
loaded and its keys override the stored copy, so editing the file changes the lane's next run
and next reply without touching stored rows. Keys the file does not define keep their stored
values.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from tasque2.config import get_settings

logger = logging.getLogger(__name__)

CONTEXT_FILE_KEY = "context_file"


def context_file_path(reference: str) -> Path:
    path = Path(reference).expanduser()
    if not path.is_absolute():
        path = get_settings().resolved_project_dir / path
    return path.resolve()


def load_context_file(reference: str) -> dict[str, Any]:
    path = context_file_path(reference)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must hold a JSON object.")
    return data


def effective_context(context: dict[str, Any] | None) -> dict[str, Any]:
    """The context with its lane file applied; an unreadable file leaves the stored copy in force."""
    stored = dict(context or {})
    reference = stored.get(CONTEXT_FILE_KEY)
    if not isinstance(reference, str) or not reference.strip():
        return stored
    try:
        lane = load_context_file(reference)
    except (OSError, ValueError) as exc:
        logger.warning("Lane context file %s is unavailable (%s); using the stored context", reference, exc)
        return stored
    return {**stored, **lane, CONTEXT_FILE_KEY: reference}
