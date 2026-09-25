from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from tasque2.db import session_scope
from tasque2.mcp.tools._shared import calling_thread, calling_work_item, optional_string, run_json
from tasque2.sticky import StickyService


def sticky_get(thread_id: str | None = None, intent: str = "") -> str:
    """A thread's sticky note: the notes kept there for the user and the thread's upcoming
    scheduled runs. ``shown`` is false when the user removed it. ``thread_id`` defaults to the
    thread this work answers in."""
    return run_json(lambda: _get(thread_id), intent=intent)


def sticky_set(notes: str, thread_id: str | None = None, show: bool = False) -> str:
    """Replace the notes on a thread's sticky note, the short message pinned in the thread above
    its upcoming runs: what the user should keep in view there (things to do, replies or emails
    owed, a decision waiting, a date to keep) as a few short Markdown lines, 800 characters at
    most. The notes replace the old ones; an empty string clears them. ``thread_id`` defaults to
    the thread this work answers in. ``show`` brings back a sticky note the user removed; pass it
    only when they ask for it."""
    return run_json(lambda: _set(notes, thread_id, show))


def _get(thread_id: str | None) -> dict[str, Any]:
    with session_scope() as session:
        return {"ok": True, "sticky": StickyService(session).view(_thread(session, thread_id)).data()}


def _set(notes: str, thread_id: str | None, show: bool) -> dict[str, Any]:
    with session_scope() as session:
        caller = calling_work_item(session)
        view = StickyService(session).set_notes(
            _thread(session, thread_id), notes, work_item_id=caller.id if caller else None, show=bool(show)
        )
        result: dict[str, Any] = {"ok": True, "thread_id": view.thread_id, "shown": view.shown}
        result["chars"] = len(view.notes)
        if not view.shown:
            result["note"] = "The user removed this thread's sticky note; the notes are kept but not shown."
        return result


def _thread(session: Session, thread_id: str | None) -> str:
    thread = optional_string(thread_id) or calling_thread(session)
    if not thread:
        raise ValueError("This work has no Discord thread; pass thread_id for the sticky note to use.")
    return thread
