"""Relevance-aware excerpting for large memory documents.

A document longer than its delivery budget is split into sections (Markdown headers, or
blank-line paragraphs), and the sections most relevant to the run are kept in their
original order, with an optional bias toward later sections for append-style logs. It is
lexical and deterministic, so it is cheap enough to run on every context packet.
"""

from __future__ import annotations

import re

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_HEADER_RE = re.compile(r"^#{1,6}\s", re.MULTILINE)
_OMISSION = "\n\n[… omitted for brevity — fetch the full memory if needed …]\n\n"


def select_relevant_excerpt(
    content: str,
    query: str,
    *,
    budget_chars: int,
    position_bias: float = 0.0,
) -> tuple[str, bool]:
    """Return ``(excerpt, was_trimmed)`` keeping the most relevant sections.

    ``position_bias`` in [0, 1] favors later sections; 0 ranks purely by lexical overlap
    with ``query``. The first section always stays because it carries the framing.
    """
    content = content or ""
    if len(content) <= budget_chars:
        return content, False

    sections = _split_sections(content)
    if len(sections) <= 1:
        return content[:budget_chars].rstrip() + _OMISSION.rstrip(), True

    query_tokens = set(_TOKEN_RE.findall(query.lower()))
    last_index = len(sections) - 1
    scored: list[tuple[int, float]] = []
    for index, section in enumerate(sections):
        relevance = _lexical_overlap(query_tokens, section)
        position = index / last_index if last_index else 0.0
        scored.append((index, relevance + position_bias * position))

    keep = {0}
    used = len(sections[0])
    for index, _score in sorted(scored, key=lambda item: item[1], reverse=True):
        if index in keep:
            continue
        cost = len(sections[index]) + len(_OMISSION)
        if used + cost > budget_chars:
            continue
        keep.add(index)
        used += cost

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
