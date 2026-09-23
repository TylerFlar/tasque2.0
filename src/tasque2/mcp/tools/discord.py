from __future__ import annotations

from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select

from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.mcp.tools._shared import clamp, optional_string, run_json
from tasque2.models import DiscordMessage, DiscordThread, WorkItem, utc_now


def discord_history(
    lane: str | None = None,
    thread_id: str | None = None,
    query: str | None = None,
    days: int = 90,
    limit: int = 40,
    include_tasque: bool = False,
    intent: str = "",
) -> str:
    """The user's own Discord messages, newest first and dated: the record of what the user
    asked for. Scope with ``lane`` (every thread the lane owns) or ``thread_id``, keep messages
    containing every word of ``query``, and look back ``days``. ``include_tasque`` adds
    Tasque's own posts for context."""
    return run_json(lambda: _history(lane, thread_id, query, days, limit, include_tasque), intent=intent)


def _history(
    lane: str | None, thread_id: str | None, query: str | None, days: int, limit: int, include_tasque: bool
) -> dict[str, Any]:
    local = ZoneInfo(get_settings().timezone)
    statement = select(DiscordMessage).where(DiscordMessage.created_at >= utc_now() - timedelta(days=max(1, days)))
    if not include_tasque:
        statement = statement.where(DiscordMessage.direction == "inbound")
    if thread := optional_string(thread_id):
        statement = statement.where(DiscordMessage.discord_thread_id == thread)
    if name := optional_string(lane):
        owned = (
            select(DiscordThread.discord_thread_id)
            .join(WorkItem, WorkItem.id == DiscordThread.work_item_id)
            .where(WorkItem.lane == name)
        )
        routed = select(WorkItem.id).where(WorkItem.lane == name)
        statement = statement.where(
            or_(DiscordMessage.discord_thread_id.in_(owned), DiscordMessage.work_item_id.in_(routed))
        )
    for word in (optional_string(query) or "").split():
        statement = statement.where(DiscordMessage.content_preview.ilike(f"%{word}%"))
    statement = statement.order_by(DiscordMessage.created_at.desc()).limit(clamp(limit, default=40))
    with session_scope() as session:
        items = [
            {
                "at": message.created_at.astimezone(local).isoformat(timespec="minutes"),
                "thread_id": message.discord_thread_id,
                "from": "tasque" if message.direction == "outbound" else message.author,
                "content": message.content_preview,
            }
            for message in session.scalars(statement).all()
        ]
    return {"ok": True, "items": items}
