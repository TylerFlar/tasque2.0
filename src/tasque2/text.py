"""Small text utilities shared by prompts, traces, and Discord rendering."""

from __future__ import annotations

import re

_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
_WHITESPACE_RE = re.compile(r"[ \t]+")


def compress_text(text: str, *, max_chars: int = 12000, preserve_lines: int = 160) -> str:
    """Compact noisy tool or provider output while keeping its head and tail.

    Raw output stays in artifacts; this is the shorter view used in prompts and traces.
    """
    if not text:
        return ""
    cleaned = _ANSI_RE.sub("", text.replace("\r\n", "\n").replace("\r", "\n"))
    lines = _collapse_repeated(_dedupe_adjacent([_compact_line(line) for line in cleaned.splitlines()]))
    if len(lines) > preserve_lines:
        head = lines[: preserve_lines // 2]
        tail = lines[-preserve_lines // 2 :]
        omitted = len(lines) - len(head) - len(tail)
        lines = [*head, f"[omitted {omitted} middle line(s)]", *tail]
    compact = "\n".join(lines).strip()
    if len(compact) <= max_chars:
        return compact
    marker_room = len(f"\n[omitted {len(compact)} character(s)]\n")
    head_size = max(1, (max_chars - marker_room) // 2)
    tail_size = max(1, max_chars - marker_room - head_size)
    omitted = len(compact) - head_size - tail_size
    return f"{compact[:head_size].rstrip()}\n[omitted {omitted} character(s)]\n{compact[-tail_size:].lstrip()}"


def one_line(value: str, *, limit: int = 1000) -> str:
    return " ".join(compress_text(value, max_chars=limit, preserve_lines=20).split())


def truncate(value: str, limit: int, *, marker: str = "\n[truncated]") -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - len(marker))] + marker


def _compact_line(line: str) -> str:
    line = _WHITESPACE_RE.sub(" ", line.strip())
    return _UUID_RE.sub(lambda match: match.group(0)[:8] + "...", line)


def _dedupe_adjacent(lines: list[str]) -> list[str]:
    result: list[str] = []
    for line in lines:
        if result and line == result[-1]:
            continue
        result.append(line)
    return result


def _collapse_repeated(lines: list[str]) -> list[str]:
    result: list[str] = []
    seen: dict[str, int] = {}
    for line in lines:
        if not line:
            result.append(line)
            continue
        seen[line] = seen.get(line, 0) + 1
        if seen[line] <= 3:
            result.append(line)
        elif seen[line] == 4:
            result.append(f"[repeated line suppressed: {line[:160]}]")
    return result
