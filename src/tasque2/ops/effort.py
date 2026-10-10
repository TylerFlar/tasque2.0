"""How much Tasque asks of its user, per Discord thread: the numbers behind "low effort".

``effort_report`` reads the recorded Discord messages over a window and counts, per thread,
the posts Tasque sent (how many a week, how long, how many ask the user for something) and
the messages the user sent back (how many pushed back). A review compares two windows to see
whether a change made Tasque quieter without making it miss what the user needs.
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import median
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.models import DiscordMessage, DiscordThread, WorkflowRun, WorkItem, utc_now

ASK = re.compile(
    r"\?|\b(?:say ['\"“‘]|reply\b|confirm\b|sign in\b|approve\b|pick (?:one|a|the)\b|which\b|"
    r"let me know\b|your call\b|tell me\b)",
    re.IGNORECASE,
)
PUSHBACK = re.compile(
    r"\b(?:stop|don'?t|already|less|too many|no need|i said|again|why did(?:n'?t)?|wrong|not what)\b",
    re.IGNORECASE,
)


def thread_label(session: Session, thread_id: str | None) -> str:
    """The lane or title of the work (or workflow run) a thread belongs to."""
    if not thread_id:
        return "channel"
    binding = session.scalar(select(DiscordThread).where(DiscordThread.discord_thread_id == thread_id))
    owner = session.get(WorkItem, binding.work_item_id) if binding is not None and binding.work_item_id else None
    if owner is not None:
        return owner.lane or owner.title
    run = session.get(WorkflowRun, binding.workflow_run_id) if binding is not None and binding.workflow_run_id else None
    return run.name if run is not None else "unbound"


def effort_report(session: Session, *, days: int = 30, now: datetime | None = None) -> dict[str, Any]:
    """Per-thread posts, asks, replies and pushback over the last ``days`` days."""
    now = now or utc_now()
    days = max(1, int(days))
    since = now - timedelta(days=days)
    weeks = days / 7
    rows = session.scalars(select(DiscordMessage).where(DiscordMessage.created_at >= since)).all()
    threads: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"posts": 0, "lengths": [], "asks": 0, "inbound": 0, "pushback": 0}
    )
    for message in rows:
        key = message.discord_thread_id or message.discord_channel_id
        entry = threads[key]
        entry["thread_id"] = message.discord_thread_id
        text = message.content_preview or ""
        if message.direction == "outbound":
            entry["posts"] += 1
            entry["lengths"].append(len(text))
            entry["asks"] += bool(ASK.search(text))
        elif message.direction == "inbound":
            entry["inbound"] += 1
            entry["pushback"] += bool(PUSHBACK.search(text))

    out: list[dict[str, Any]] = []
    for entry in threads.values():
        posts = entry["posts"]
        row = {
            "label": thread_label(session, entry.get("thread_id")),
            "thread_id": entry.get("thread_id"),
            "posts": posts,
            "posts_per_week": round(posts / weeks, 1),
            "median_chars": int(median(entry["lengths"])) if entry["lengths"] else 0,
            "ask_share": round(entry["asks"] / posts, 2) if posts else 0.0,
            "inbound": entry["inbound"],
            "pushback": entry["pushback"],
            "_asks": entry["asks"],
        }
        out.append(row)
    out.sort(key=lambda row: (row["posts"], row["inbound"]), reverse=True)

    posts = sum(row["posts"] for row in out)
    asks = sum(row.pop("_asks") for row in out)
    inbound = sum(row["inbound"] for row in out)
    return {
        "window": {"days": days, "since": since.isoformat(), "now": now.isoformat()},
        "totals": {
            "posts": posts,
            "posts_per_week": round(posts / weeks, 1),
            "ask_share": round(asks / posts, 2) if posts else 0.0,
            "inbound": inbound,
            "pushback": sum(row["pushback"] for row in out),
        },
        "threads": out,
    }
