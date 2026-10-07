"""Local-calendar helpers for the configured timezone.

Ledger-style tables store calendar dates ("which training day / eating day /
wear day"), not timestamps: an evening session that finishes after midnight
UTC must still file under the local day the user experienced. Core and
extensions share these helpers so every ledger agrees on what "today" means.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

from tasque2.config import get_settings


def local_today(now: datetime | None = None) -> date:
    """Today's date in the configured timezone."""
    return _to_local(now or datetime.now(UTC)).date()


def local_date(value: datetime) -> str:
    """The configured-timezone calendar date for a timestamp (YYYY-MM-DD)."""
    return _to_local(value).date().isoformat()


def _to_local(value: datetime) -> datetime:
    try:
        zone = ZoneInfo(get_settings().timezone)
    except Exception:  # noqa: BLE001 - settings/tz problems must not break date math
        return value
    return value.astimezone(zone)


def quiet_window() -> tuple[time, time] | None:
    """The configured quiet hours as (start, end) local times, or None when they are off.

    ``TASQUE2_QUIET_HOURS`` reads "HH:MM-HH:MM" (default 22:00-08:00); an empty value turns
    quiet hours off. A window may cross midnight.
    """
    raw = (get_settings().quiet_hours or "").strip()
    if not raw:
        return None
    try:
        start_text, end_text = (part.strip() for part in raw.split("-", 1))
        start, end = time.fromisoformat(start_text), time.fromisoformat(end_text)
    except ValueError:
        return None
    return (start, end) if start != end else None


def in_quiet_hours(value: datetime | None = None) -> bool:
    """Whether a moment (default now) falls inside the configured quiet hours, local time."""
    window = quiet_window()
    if window is None:
        return False
    moment = value or datetime.now(UTC)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    local = _to_local(moment).time()
    start, end = window
    return start <= local < end if start < end else local >= start or local < end
