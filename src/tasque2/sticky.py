"""Sticky notes: one short message per Tasque thread, pinned once and kept current in place.

A thread's sticky note shows the notes a worker keeps for the user there (things to do,
replies owed, a decision waiting) above the thread's upcoming runs, which come from the
enabled schedules that post into it. Workers may leave the notes empty. A sticky note shows
in an active Tasque thread that has notes or an upcoming run. The user turns a thread's
sticky note off by deleting its message; a worker brings it back only when the user asks.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.events import record_event
from tasque2.models import DiscordSticky, DiscordThread, Schedule, utc_now
from tasque2.reminders import NOTIFY_WORKER
from tasque2.schedules import ScheduleService

STICKY_NOTES_MAX_CHARS = 800
COMING_UP_LIMIT = 8
LABEL_CHARS = 72
STICKY_ON = "on"
STICKY_OFF = "off"
ACTIVE_THREAD = "active"
_WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_BLANK_LINES = re.compile(r"\n{3,}")


@dataclass(frozen=True)
class UpcomingRun:
    at: datetime
    when: str
    label: str
    if_needed: bool

    @property
    def line(self) -> str:
        return f"{self.when} · {self.label}" + (" (if needed)" if self.if_needed else "")


@dataclass(frozen=True)
class StickyView:
    thread_id: str
    notes: str
    notes_updated_at: datetime | None
    shown: bool
    coming_up: tuple[UpcomingRun, ...]
    more: int = 0

    def lines(self) -> list[str]:
        """The coming-up lines, soonest first."""
        lines = [run.line for run in self.coming_up]
        if self.more:
            lines.append(f"+{self.more} more")
        return lines

    def data(self) -> dict[str, Any]:
        updated = self.notes_updated_at
        return {
            "thread_id": self.thread_id,
            "shown": self.shown,
            "notes": self.notes,
            "notes_updated_at": updated.astimezone(_zone()).isoformat(timespec="minutes") if updated else None,
            "coming_up": self.lines(),
        }

    def text(self) -> str:
        parts = [self.notes] if self.notes else []
        if self.coming_up:
            parts.append("Coming up\n" + "\n".join(self.lines()))
        return "\n\n".join(parts) or "(nothing here)"


class StickyService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def sticky(self, thread_id: str) -> DiscordSticky | None:
        return self.session.scalar(select(DiscordSticky).where(DiscordSticky.discord_thread_id == thread_id))

    def view(self, thread_id: str, *, now: datetime | None = None) -> StickyView:
        """An active thread's sticky note as it stands, posted or not."""
        self._require_active(thread_id)
        return _view(thread_id, self.sticky(thread_id), self.upcoming(now=now).get(thread_id, []))

    def packet_view(self, thread_id: str, *, now: datetime | None = None) -> dict[str, Any] | None:
        """The sticky note as a run's packet carries it; None when the thread is not an active Tasque thread."""
        if not self._active_threads({thread_id}):
            return None
        return self.view(thread_id, now=now).data()

    def showable(self, *, now: datetime | None = None) -> list[StickyView]:
        """The sticky notes to keep posted: each active thread with notes, an upcoming run or a posted
        sticky note, unless the user turned it off."""
        upcoming = self.upcoming(now=now)
        stickies = {sticky.discord_thread_id: sticky for sticky in self.session.scalars(select(DiscordSticky)).all()}
        candidates = set(upcoming) | {
            thread_id for thread_id, sticky in stickies.items() if sticky.notes or sticky.discord_message_id
        }
        views: list[StickyView] = []
        for thread_id in sorted(self._active_threads(candidates)):
            sticky = stickies.get(thread_id)
            if sticky is None or sticky.status == STICKY_ON:
                views.append(_view(thread_id, sticky, upcoming.get(thread_id, [])))
        return views

    def set_notes(
        self,
        thread_id: str,
        notes: str,
        *,
        work_item_id: str | None = None,
        show: bool = False,
        now: datetime | None = None,
    ) -> StickyView:
        """Replace a thread's notes (empty clears them); ``show`` turns back on a sticky note the user removed."""
        text = clean_notes(notes)
        if len(text) > STICKY_NOTES_MAX_CHARS:
            raise ValueError(
                f"A sticky note holds {STICKY_NOTES_MAX_CHARS} characters of notes; these are {len(text)}. "
                "Keep only what the user needs in view."
            )
        self._require_active(thread_id)
        sticky = self.sticky(thread_id) or self._create(thread_id)
        if text != sticky.notes:
            sticky.notes = text
            sticky.notes_updated_at = now or utc_now()
            sticky.notes_work_item_id = work_item_id
            self._event(
                "sticky.notes_updated",
                thread_id,
                summary=f"Sticky notes updated ({len(text)} chars)" if text else "Sticky notes cleared",
                work_item_id=work_item_id,
                payload={"chars": len(text)},
            )
        if show and sticky.status == STICKY_OFF:
            sticky.status = STICKY_ON
            self._event("sticky.shown", thread_id, summary="Sticky note shown again", work_item_id=work_item_id)
        self.session.flush()
        return self.view(thread_id, now=now)

    def record_posted(self, thread_id: str, *, message_id: str, signature: str) -> DiscordSticky:
        sticky = self.sticky(thread_id) or self._create(thread_id)
        sticky.discord_message_id = message_id
        sticky.signature = signature
        sticky.pinned_at = sticky.pin_retry_at = None
        self.session.flush()
        self._event(
            "sticky.posted",
            thread_id,
            summary="Posted the thread's sticky note",
            payload={"discord_message_id": message_id},
        )
        return sticky

    def turn_off(self, thread_id: str) -> None:
        """The sticky note's message is gone from Discord: keep the notes, stop showing it."""
        sticky = self.sticky(thread_id)
        if sticky is None:
            return
        message_id = sticky.discord_message_id
        sticky.status = STICKY_OFF
        sticky.discord_message_id = None
        sticky.signature = None
        sticky.pinned_at = sticky.pin_retry_at = None
        self.session.flush()
        self._event(
            "sticky.removed",
            thread_id,
            summary="The sticky note's message was deleted; it is off",
            payload={"discord_message_id": message_id},
        )

    def upcoming(self, *, now: datetime | None = None) -> dict[str, list[UpcomingRun]]:
        """Each thread's upcoming runs: the next fire of every enabled schedule posting into it, soonest first."""
        zone = _zone()
        now = (now or utc_now()).astimezone(zone)
        schedules = ScheduleService(self.session)
        runs: dict[str, list[UpcomingRun]] = {}
        for schedule in self.session.scalars(select(Schedule).where(Schedule.enabled.is_(True))).all():
            payload = schedule.payload or {}
            thread_id = str(payload.get("discord_thread_id") or "").strip()
            if not thread_id:
                continue
            try:
                at = schedules.next_fire_time(schedule, now=now)
            except Exception:  # noqa: BLE001 - a schedule the poller cannot read stays off the sticky note
                continue
            if at is None:
                continue
            local = at.astimezone(zone)
            runs.setdefault(thread_id, []).append(
                UpcomingRun(
                    at=local,
                    when=_when(schedule, local, now),
                    label=schedule_label(schedule),
                    if_needed=bool(payload.get("gate")),
                )
            )
        for items in runs.values():
            items.sort(key=lambda run: run.at)
        return runs

    def _active_threads(self, thread_ids: set[str]) -> set[str]:
        if not thread_ids:
            return set()
        return set(
            self.session.scalars(
                select(DiscordThread.discord_thread_id).where(
                    DiscordThread.discord_thread_id.in_(sorted(thread_ids)), DiscordThread.status == ACTIVE_THREAD
                )
            ).all()
        )

    def _require_active(self, thread_id: str) -> None:
        if not self._active_threads({thread_id}):
            raise ValueError(f"Thread {thread_id} is not an active Tasque thread.")

    def _create(self, thread_id: str) -> DiscordSticky:
        sticky = DiscordSticky(discord_thread_id=thread_id, notes="", status=STICKY_ON)
        self.session.add(sticky)
        self.session.flush()
        return sticky

    def _event(
        self,
        event_type: str,
        thread_id: str,
        *,
        summary: str,
        work_item_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        record_event(
            self.session,
            event_type=event_type,
            entity_kind="discord_thread",
            entity_id=thread_id,
            work_item_id=work_item_id,
            source="sticky",
            summary=summary,
            payload=payload,
        )


def clean_notes(notes: str | None) -> str:
    """Notes with line endings normalized, trailing spaces and runs of blank lines removed."""
    text = str(notes or "").replace("\r\n", "\n").replace("\r", "\n")
    return _BLANK_LINES.sub("\n\n", "\n".join(line.rstrip() for line in text.split("\n"))).strip()


def schedule_label(schedule: Schedule) -> str:
    """How a sticky note names a run: a reminder by its text, anything else by its schedule's name."""
    label = schedule.name
    if schedule.worker_kind == NOTIFY_WORKER and schedule.schedule_type == "date":
        label = str((schedule.payload or {}).get("task_instruction") or label)
    label = " ".join(label.split())
    return label if len(label) <= LABEL_CHARS else label[: LABEL_CHARS - 1].rstrip() + "…"


def _view(thread_id: str, sticky: DiscordSticky | None, runs: list[UpcomingRun]) -> StickyView:
    return StickyView(
        thread_id=thread_id,
        notes=sticky.notes if sticky is not None else "",
        notes_updated_at=sticky.notes_updated_at if sticky is not None else None,
        shown=sticky is None or sticky.status == STICKY_ON,
        coming_up=tuple(runs[:COMING_UP_LIMIT]),
        more=max(0, len(runs) - COMING_UP_LIMIT),
    )


def _when(schedule: Schedule, at: datetime, now: datetime) -> str:
    """``daily 08:00`` for a once-a-day cron; otherwise the weekday, date and time of the next run."""
    if _daily(schedule):
        return f"daily {at:%H:%M}"
    date = f"{at.month}/{at.day}" if at.year == now.year else f"{at.month}/{at.day}/{at:%y}"
    return f"{_WEEKDAYS[at.weekday()]} {date} {at:%H:%M}"


def _daily(schedule: Schedule) -> bool:
    fields = schedule.expression.split()
    return (
        schedule.schedule_type == "cron"
        and len(fields) == 5
        and fields[0].isdigit()
        and fields[1].isdigit()
        and fields[2:] == ["*", "*", "*"]
    )


def _zone() -> ZoneInfo:
    return ZoneInfo(get_settings().timezone)
