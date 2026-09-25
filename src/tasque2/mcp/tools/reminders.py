from __future__ import annotations

from typing import Any

from tasque2.db import session_scope
from tasque2.mcp.tools._shared import calling_thread, optional_string, required, run_json
from tasque2.reminders import ReminderService


def reminder_set(text: str, at: str, thread_id: str | None = None) -> str:
    """Keep a reminder the user asked for: at ``at`` (local ``YYYY-MM-DD``, or ``YYYY-MM-DDTHH:MM``)
    Tasque posts ``text`` into the thread, with no model run. A date alone posts late morning.
    ``thread_id`` defaults to the thread this work answers in."""
    return run_json(lambda: _set(text, at, thread_id))


def reminder_list(days: int = 7, intent: str = "") -> str:
    """Reminders due from a day ago to ``days`` ahead, earliest first; ``sent`` marks posted ones."""
    return run_json(lambda: _list(days), intent=intent)


def reminder_cancel(reminder_id: str) -> str:
    """Cancel a reminder that has not posted yet (or remove one that has)."""
    return run_json(lambda: _cancel(reminder_id))


def _set(text: str, at: str, thread_id: str | None) -> dict[str, Any]:
    with session_scope() as session:
        thread = optional_string(thread_id) or calling_thread(session)
        if not thread:
            raise ValueError("This work has no Discord thread; pass thread_id for where the reminder should post.")
        reminder = ReminderService(session).set(required(text, "text"), required(at, "at"), thread_id=thread)
        return {"ok": True, "reminder": reminder.data()}


def _list(days: int) -> dict[str, Any]:
    with session_scope() as session:
        return {"ok": True, "items": [reminder.data() for reminder in ReminderService(session).list(days=days)]}


def _cancel(reminder_id: str) -> dict[str, Any]:
    with session_scope() as session:
        ReminderService(session).cancel(required(reminder_id, "reminder_id"))
        return {"ok": True, "canceled": reminder_id}
