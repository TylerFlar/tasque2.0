from __future__ import annotations

import re

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
WHITESPACE_RE = re.compile(r"[ \t]+")


def compress_text(
    text: str,
    *,
    max_chars: int = 12000,
    preserve_lines: int = 160,
) -> str:
    """Compact noisy tool/provider output while preserving useful evidence.

    This is deliberately rule-based and audit-friendly: raw output remains in
    artifacts, while prompts/traces get a shorter signal-bearing view.
    """
    if not text:
        return ""

    cleaned = ANSI_RE.sub("", text.replace("\r\n", "\n").replace("\r", "\n"))
    lines = [_compact_line(line) for line in cleaned.splitlines()]
    lines = _dedupe_adjacent(lines)
    lines = _collapse_repeated(lines)

    if len(lines) > preserve_lines:
        head = lines[: preserve_lines // 2]
        tail = lines[-preserve_lines // 2 :]
        omitted = len(lines) - len(head) - len(tail)
        lines = [*head, f"[tasque compressed: omitted {omitted} middle line(s)]", *tail]

    compact = "\n".join(line for line in lines if line or line == "").strip()
    if len(compact) <= max_chars:
        return compact
    head_size = max(1, max_chars // 2 - 80)
    tail_size = max(1, max_chars - head_size - 120)
    omitted = len(compact) - head_size - tail_size
    return (
        compact[:head_size].rstrip()
        + f"\n[tasque compressed: omitted {omitted} character(s)]\n"
        + compact[-tail_size:].lstrip()
    )


def _compact_line(line: str) -> str:
    line = WHITESPACE_RE.sub(" ", line.strip())
    return UUID_RE.sub(lambda match: match.group(0)[:8] + "...", line)


def _dedupe_adjacent(lines: list[str]) -> list[str]:
    result: list[str] = []
    previous = object()
    for line in lines:
        if line == previous:
            continue
        result.append(line)
        previous = line
    return result


def _collapse_repeated(lines: list[str]) -> list[str]:
    result: list[str] = []
    seen_counts: dict[str, int] = {}
    for line in lines:
        if not line:
            result.append(line)
            continue
        seen_counts[line] = seen_counts.get(line, 0) + 1
        if seen_counts[line] <= 3:
            result.append(line)
        elif seen_counts[line] == 4:
            result.append(f"[tasque compressed: repeated line suppressed: {line[:160]}]")
    return result
