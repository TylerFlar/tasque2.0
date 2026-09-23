"""The durable conversation window a reply worker sees."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

MESSAGE_LIMIT = 12
MESSAGE_MAX_CHARS = 1_200
TOTAL_MAX_CHARS = 8_000
ROW_FETCH_CAP = 240


def collapse_conversation(
    messages: Sequence[dict[str, Any]],
    *,
    limit: int = MESSAGE_LIMIT,
    max_chars_per_message: int = MESSAGE_MAX_CHARS,
    total_max_chars: int = TOTAL_MAX_CHARS,
) -> list[dict[str, Any]]:
    """Fold a chronological transcript into a bounded window of logical messages.

    Consecutive outbound rows from the same work item are one reply that Discord's length
    limit split, so they rejoin into one entry with a ``parts`` count. The newest ``limit``
    entries survive, each capped at ``max_chars_per_message`` and the window at
    ``total_max_chars``, dropping oldest first, so a long reply trims its own tail and never
    pushes the user's turns out.
    """
    grouped: list[dict[str, Any]] = []
    for message in messages:
        previous = grouped[-1] if grouped else None
        if previous is not None and _same_reply(previous, message):
            previous["content"] = f"{previous['content']}\n\n{message.get('content') or ''}".strip()
            previous["parts"] = int(previous.get("parts", 1)) + 1
            previous["last_discord_message_id"] = message.get("discord_message_id")
            previous["created_at"] = message.get("created_at") or previous.get("created_at")
            continue
        grouped.append({**message, "content": str(message.get("content") or ""), "parts": 1})

    window = grouped[-limit:] if limit > 0 else []
    for entry in window:
        entry["content"] = _truncate(entry["content"], max_chars_per_message)
    kept: list[dict[str, Any]] = []
    budget = total_max_chars
    for entry in reversed(window):
        cost = len(entry["content"])
        if kept and cost > budget:
            break
        budget -= cost
        kept.append(entry)
    return list(reversed(kept))


def _same_reply(previous: dict[str, Any], message: dict[str, Any]) -> bool:
    work_item_id = message.get("work_item_id")
    return bool(
        work_item_id
        and message.get("direction") == "outbound"
        and previous.get("direction") == "outbound"
        and previous.get("work_item_id") == work_item_id
    )


def _truncate(content: str, max_chars: int) -> str:
    if max_chars <= 0 or len(content) <= max_chars:
        return content
    return f"{content[:max_chars].rstrip()}\n… [trimmed {len(content) - max_chars} chars]"
