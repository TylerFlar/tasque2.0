from __future__ import annotations

from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select

from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.mcp.tools._shared import clamp, optional_string, required, run_json
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


DISCORD_API = "https://discord.com/api/v10"
THREAD_NAME_LIMIT = 100


def discord_thread_rename(thread_id: str, name: str) -> str:
    """Rename a Discord thread with Tasque's bot (it needs the Manage Threads permission there).

    Thread and channel housekeeping goes through the bot, never through a scripted user account:
    platforms disable user accounts that act like bots."""
    return run_json(lambda: _rename(thread_id, name))


def _rename(thread_id: str, name: str) -> dict[str, Any]:
    import httpx

    thread = required(thread_id, "thread_id")
    title = " ".join(required(name, "name").split())[:THREAD_NAME_LIMIT]
    token = get_settings().discord_token
    if not token:
        raise ValueError("No Discord bot token is configured (TASQUE2_DISCORD_TOKEN).")
    response = httpx.patch(
        f"{DISCORD_API}/channels/{thread}",
        headers={"Authorization": f"Bot {token}"},
        json={"name": title},
        timeout=20,
    )
    if response.status_code >= 400:
        raise ValueError(f"Discord refused the rename ({response.status_code}): {response.text[:300]}")
    return {"ok": True, "thread_id": thread, "name": response.json().get("name", title)}


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
