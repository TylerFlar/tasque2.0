"""Reminders: one-shot messages the user asked for, posted at their time without a model run.

A reminder is a date schedule whose worker is ``function.notify``: when it fires, the run
posts its text into the Discord thread it names and costs nothing. A date without a time
posts at ``TASQUE2_REMINDER_DEFAULT_TIME`` (local). Reminders past their time are pruned
after a month.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import Schedule, ScheduleOccurrence, utc_now
from tasque2.schedules import ScheduleService

NOTIFY_WORKER = "function.notify"
REMINDER_LANE = "reminders"
PRUNE_AFTER = timedelta(days=30)


@dataclass(frozen=True)
class Reminder:
    id: str
    at: datetime
    text: str
    thread_id: str | None
    sent: bool

    def data(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "at": self.at.isoformat(timespec="minutes"),
            "text": self.text,
            "thread_id": self.thread_id,
            "sent": self.sent,
        }


class ReminderService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.schedules = ScheduleService(session)

    def set(self, text: str, at: str, *, thread_id: str, now: datetime | None = None) -> Reminder:
        """Keep a reminder that posts ``text`` into ``thread_id`` at ``at`` (local date or date and time)."""
        body = " ".join(text.split())
        if not body:
            raise ValueError("A reminder needs text.")
        when = parse_when(at)
        if when <= (now or utc_now()):
            raise ValueError(f"{when.isoformat(timespec='minutes')} has already passed.")
        schedule = self.schedules.create_schedule(
            name=f"Reminder: {body[:60]}",
            schedule_type="date",
            expression=when.replace(tzinfo=None).isoformat(timespec="minutes"),
            worker_kind=NOTIFY_WORKER,
            payload={
                "title": "Reminder",
                "task_instruction": f"Reminder: {body}",
                "discord_thread_id": thread_id,
                "lane": REMINDER_LANE,
                "max_attempts": 1,
            },
            timezone_name=str(when.tzinfo),
        )
        return self._reminder(schedule)

    def list(self, *, days: int = 7, now: datetime | None = None) -> list[Reminder]:
        """Reminders due from a day ago to ``days`` ahead, earliest first."""
        now = now or utc_now()
        start, end = now - timedelta(days=1), now + timedelta(days=max(0, days))
        reminders = [self._reminder(schedule) for schedule in self._schedules()]
        return sorted((r for r in reminders if start <= r.at <= end), key=lambda r: r.at)

    def cancel(self, reminder_id: str) -> None:
        schedule = self.session.get(Schedule, reminder_id)
        if schedule is None or schedule.worker_kind != NOTIFY_WORKER:
            raise KeyError(f"Unknown reminder: {reminder_id}")
        self.schedules.delete_schedule(schedule.id)

    def prune(self, *, now: datetime | None = None) -> int:
        """Delete reminders whose time passed more than a month ago."""
        cutoff = (now or utc_now()) - PRUNE_AFTER
        stale = [schedule for schedule in self._schedules() if self._when(schedule) < cutoff]
        for schedule in stale:
            self.schedules.delete_schedule(schedule.id)
        return len(stale)

    def _schedules(self) -> list[Schedule]:
        """One-shot notices only: a recurring notify schedule is not a reminder and is never pruned."""
        return list(
            self.session.scalars(
                select(Schedule).where(Schedule.worker_kind == NOTIFY_WORKER, Schedule.schedule_type == "date")
            ).all()
        )

    def _when(self, schedule: Schedule) -> datetime:
        return datetime.fromisoformat(schedule.expression).replace(tzinfo=ZoneInfo(schedule.timezone))

    def _reminder(self, schedule: Schedule) -> Reminder:
        payload = schedule.payload or {}
        sent = self.session.scalar(select(ScheduleOccurrence.id).where(ScheduleOccurrence.schedule_id == schedule.id))
        text = str(payload.get("task_instruction") or "").removeprefix("Reminder: ")
        return Reminder(
            id=schedule.id,
            at=self._when(schedule),
            text=text,
            thread_id=payload.get("discord_thread_id"),
            sent=sent is not None,
        )


def parse_when(value: str) -> datetime:
    """A local date (``2026-09-26``) or date and time (``2026-09-26T09:30``) as an aware datetime."""
    settings = get_settings()
    tz = ZoneInfo(settings.timezone)
    text = str(value or "").strip()
    try:
        if len(text) == 10:
            default = time.fromisoformat(settings.reminder_default_time)
            return datetime.combine(date.fromisoformat(text), default, tzinfo=tz)
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(f"Give the time as YYYY-MM-DD or YYYY-MM-DDTHH:MM, not {value!r}.") from None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=tz)
