"""The fault ledger and its neighbours: what went wrong, kept where health checks and the Workshop read it.

Ingestors, digests, gates and tick bookkeeping catch their own exceptions so one bad pass never
breaks a run, which also means a real code fault leaves no trace beyond a log line on stderr.
``FaultLedgerHandler`` appends each ERROR record to ``data/runtime/faults.jsonl`` as one JSON line
with a signature: a short hash of the logger, the exception type and the innermost frame inside
Tasque or an extension. The same bug in the same place keeps its signature across occurrences and
line edits, so ``fault_summary`` can group them and say how often and on how many days each one
happened.

Two more ledgers sit next to it, in the same format:

- ``warnings.jsonl``: every WARNING record (``WarningLedgerHandler``), signed by its logger and its
  message before the values were filled in, so one warning keeps one signature however its values vary;
- ``tool_errors.jsonl``: every Tasque tool call that answered ``{ok: false}`` or raised
  (``record_tool_error``, written by the MCP server).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import traceback
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any

from tasque2.config import get_settings
from tasque2.models import utc_now

FAULTS_FILE = "faults.jsonl"
WARNINGS_FILE = "warnings.jsonl"
TOOL_ERRORS_FILE = "tool_errors.jsonl"
MAX_BYTES = 5 * 1024 * 1024
MESSAGE_CHARS = 600
TRACEBACK_CHARS = 3000
_PACKAGE_DIR = Path(__file__).resolve().parents[1]
_NUMBERS = re.compile(r"\d+")
_append_lock = threading.Lock()


def faults_path() -> Path:
    return get_settings().resolved_data_dir / "runtime" / FAULTS_FILE


def warnings_path() -> Path:
    return get_settings().resolved_data_dir / "runtime" / WARNINGS_FILE


def tool_errors_path() -> Path:
    return get_settings().resolved_data_dir / "runtime" / TOOL_ERRORS_FILE


def _clip(text: Any, limit: int) -> str:
    flat = str(text or "")
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _owned_roots() -> list[Path]:
    roots = [_PACKAGE_DIR]
    try:
        roots.append(get_settings().resolved_extensions_dir)
    except Exception:  # noqa: BLE001 - settings trouble must not break logging
        pass
    return roots


def _relative(filename: str, roots: list[Path]) -> str | None:
    try:
        path = Path(filename).resolve()
    except (OSError, ValueError):
        return None
    for root in roots:
        if path.is_relative_to(root):
            prefix = "tasque2" if root == _PACKAGE_DIR else root.name
            return f"{prefix}/{path.relative_to(root).as_posix()}"
    return None


def innermost_frame(record: logging.LogRecord) -> dict[str, Any]:
    """The deepest traceback frame inside Tasque or an extension, else the logging call site."""
    roots = _owned_roots()
    tb: TracebackType | None = record.exc_info[2] if record.exc_info else None
    found: dict[str, Any] | None = None
    while tb is not None:
        code = tb.tb_frame.f_code
        relative = _relative(code.co_filename, roots)
        if relative is not None:
            found = {"file": relative, "line": tb.tb_lineno, "function": code.co_name}
        tb = tb.tb_next
    if found is not None:
        return found
    relative = _relative(record.pathname, roots) or Path(record.pathname).name
    return {"file": relative, "line": record.lineno, "function": record.funcName}


def fault_signature(logger_name: str, exc_type: str | None, frame: dict[str, Any]) -> str:
    # Line numbers stay out: an edit elsewhere in the file must not split one fault into two.
    key = f"{logger_name}|{exc_type or '-'}|{frame.get('file')}:{frame.get('function')}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def warning_signature(logger_name: str, template: str) -> str:
    """One warning, however its values vary: the logger and the message as written, numbers left out."""
    key = f"{logger_name}|{_NUMBERS.sub('#', template)}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def _at(record: logging.LogRecord) -> str:
    return datetime.fromtimestamp(record.created, tz=utc_now().tzinfo).isoformat()


def fault_entry(record: logging.LogRecord) -> dict[str, Any]:
    exc_type = exc_message = None
    trace = None
    if record.exc_info and record.exc_info[0] is not None:
        exc_type = record.exc_info[0].__name__
        exc_message = _clip(record.exc_info[1], MESSAGE_CHARS)
        trace = _clip("".join(traceback.format_exception(*record.exc_info)), TRACEBACK_CHARS)
    frame = innermost_frame(record)
    return {
        "at": _at(record),
        "logger": record.name,
        "level": record.levelname,
        "message": _clip(record.getMessage(), MESSAGE_CHARS),
        "exc_type": exc_type,
        "exc_message": exc_message,
        "frame": frame,
        "signature": fault_signature(record.name, exc_type, frame),
        "traceback": trace,
    }


def warning_entry(record: logging.LogRecord) -> dict[str, Any]:
    return {
        "at": _at(record),
        "logger": record.name,
        "message": _clip(record.getMessage(), MESSAGE_CHARS),
        "frame": innermost_frame(record),
        "signature": warning_signature(record.name, str(record.msg)),
    }


def _rotated(path: Path) -> Path:
    return path.with_name(f"{path.stem}.1{path.suffix}")


def append_entry(path: Path, entry: dict[str, Any]) -> None:
    """Append one JSON line; a file past ``MAX_BYTES`` is rotated first (one older file is kept)."""
    line = json.dumps(entry, ensure_ascii=False, default=str)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _append_lock:
        if path.exists() and path.stat().st_size > MAX_BYTES:
            path.replace(_rotated(path))
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def read_entries(path: Path, *, since: datetime | None = None) -> Iterator[dict[str, Any]]:
    """A ledger's entries, oldest first, from the rotated and the current file."""
    for candidate in (_rotated(path), path):
        try:
            lines = candidate.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                entry = json.loads(line)
                at = datetime.fromisoformat(entry["at"])
            except (ValueError, KeyError, TypeError):
                continue
            if since is None or at >= since:
                yield entry


class _LedgerHandler(logging.Handler):
    """Append records to a ledger; never raises into the code that logged."""

    below: int | None = None

    def emit(self, record: logging.LogRecord) -> None:
        if self.below is not None and record.levelno >= self.below:
            return
        try:
            append_entry(self._path(), self._entry(record))
        except Exception:  # noqa: BLE001 - the ledger must never break the caller
            self.handleError(record)

    def _path(self) -> Path:
        raise NotImplementedError

    def _entry(self, record: logging.LogRecord) -> dict[str, Any]:
        raise NotImplementedError


class FaultLedgerHandler(_LedgerHandler):
    """ERROR records into the fault ledger."""

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)

    def _path(self) -> Path:
        return faults_path()

    def _entry(self, record: logging.LogRecord) -> dict[str, Any]:
        return fault_entry(record)


class WarningLedgerHandler(_LedgerHandler):
    """WARNING records (errors go to the fault ledger) into the warning ledger."""

    below = logging.ERROR

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)

    def _path(self) -> Path:
        return warnings_path()

    def _entry(self, record: logging.LogRecord) -> dict[str, Any]:
        return warning_entry(record)


def record_tool_error(tool: str, error_type: str, error: Any, *, work_item_id: str | None = None) -> None:
    """A Tasque tool call that answered ``{ok: false}`` or raised; never raises itself."""
    entry = {
        "at": utc_now().isoformat(),
        "tool": tool,
        "error_type": error_type,
        "error": _clip(error, MESSAGE_CHARS),
        "work_item_id": work_item_id,
    }
    try:
        append_entry(tool_errors_path(), entry)
    except Exception:  # noqa: BLE001 - a tool call never fails for its record
        logging.getLogger(__name__).debug("Could not record a tool error", exc_info=True)


def read_faults(*, since: datetime | None = None, path: Path | None = None) -> Iterator[dict[str, Any]]:
    """Fault ledger entries, oldest first, from the current and the rotated file."""
    return read_entries(path or faults_path(), since=since)


def read_warnings(*, since: datetime | None = None) -> Iterator[dict[str, Any]]:
    return read_entries(warnings_path(), since=since)


def read_tool_errors(*, since: datetime | None = None) -> Iterator[dict[str, Any]]:
    return read_entries(tool_errors_path(), since=since)


def fault_summary(*, days: int = 7, now: datetime | None = None) -> list[dict[str, Any]]:
    """Faults in the window grouped by signature, most frequent first."""
    now = now or utc_now()
    groups: dict[str, dict[str, Any]] = {}
    for entry in read_faults(since=now - timedelta(days=days)):
        group = groups.setdefault(
            entry["signature"],
            {
                "signature": entry["signature"],
                "logger": entry["logger"],
                "exc_type": entry.get("exc_type"),
                "frame": entry.get("frame"),
                "count": 0,
                "days": set(),
                "first_at": entry["at"],
            },
        )
        group["count"] += 1
        group["days"].add(entry["at"][:10])
        group["last_at"] = entry["at"]
        group["message"] = entry.get("exc_message") or entry.get("message")
    out = []
    for group in groups.values():
        group["days"] = len(group["days"])
        out.append(group)
    out.sort(key=lambda group: (group["count"], group["last_at"]), reverse=True)
    return out


def recurring(group: dict[str, Any]) -> bool:
    """A fault worth a person's attention: it happened twice or on two days."""
    return group["count"] >= 2 or group["days"] >= 2
