"""Relevance-aware excerpting for large memory documents.

The old context builder compressed every memory to a fixed head+tail window, so a
14k-char ledger reached the worker as its first and last ~20 lines with the
middle deleted — exactly where the signal lived. :func:`select_relevant_excerpt`
instead splits a document into sections and keeps the ones most relevant to the
run (with an optional recency/position bias for append-style logs), assembled in
original order. It is pure-lexical and deterministic: no embedding calls per
context build, so it is cheap to run on every worker prompt.
"""

from __future__ import annotations

import re

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_HEADER_RE = re.compile(r"^#{1,6}\s", re.MULTILINE)
_OMISSION = "\n\n[… omitted for brevity — ask for the full memory if needed …]\n\n"


def select_relevant_excerpt(
    content: str,
    query: str,
    *,
    budget_chars: int,
    position_bias: float = 0.0,
) -> tuple[str, bool]:
    """Return ``(excerpt, was_trimmed)`` keeping the most relevant sections.

    ``position_bias`` in [0, 1] favors later sections (use it for chronological
    logs where recent entries matter most); 0 ranks purely by lexical relevance.
    The first section is always kept as it usually carries the document's framing.
    """
    content = content or ""
    if len(content) <= budget_chars:
        return content, False

    sections = _split_sections(content)
    if len(sections) <= 1:
        # Nothing to select between — fall back to a head slice.
        return content[:budget_chars].rstrip() + _OMISSION.rstrip(), True

    query_tokens = set(_TOKEN_RE.findall(query.lower()))
    last_index = len(sections) - 1
    scored: list[tuple[int, float, str]] = []
    for index, section in enumerate(sections):
        relevance = _lexical_overlap(query_tokens, section)
        position = index / last_index if last_index else 0.0
        scored.append((index, relevance + position_bias * position, section))

    keep: set[int] = {0}
    used = len(sections[0])
    # Greedily admit the highest-scoring sections that still fit the budget.
    for index, _score, section in sorted(scored, key=lambda item: item[1], reverse=True):
        if index in keep:
            continue
        if used + len(section) + len(_OMISSION) > budget_chars:
            continue
        keep.add(index)
        used += len(section) + len(_OMISSION)

    pieces: list[str] = []
    previous = -1
    for index in sorted(keep):
        if previous >= 0 and index != previous + 1:
            pieces.append(_OMISSION.strip())
        pieces.append(sections[index].strip())
        previous = index
    if last_index not in keep:
        pieces.append(_OMISSION.strip())
    return "\n\n".join(pieces).strip(), True


def _split_sections(content: str) -> list[str]:
    """Split on markdown headers; fall back to blank-line paragraph grouping."""
    if _HEADER_RE.search(content):
        parts: list[str] = []
        last = 0
        for match in _HEADER_RE.finditer(content):
            start = match.start()
            if start > last:
                chunk = content[last:start].strip()
                if chunk:
                    parts.append(chunk)
            last = start
        tail = content[last:].strip()
        if tail:
            parts.append(tail)
        return parts or [content]
    paragraphs = [block.strip() for block in content.split("\n\n") if block.strip()]
    return paragraphs or [content]


def _lexical_overlap(query_tokens: set[str], section: str) -> float:
    if not query_tokens:
        return 0.0
    section_tokens = set(_TOKEN_RE.findall(section.lower()))
    if not section_tokens:
        return 0.0
    return len(query_tokens & section_tokens) / len(query_tokens)
