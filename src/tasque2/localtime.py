"""Local-calendar helpers for the configured timezone.

Ledger-style tables store calendar dates ("which training day / eating day /
wear day"), not timestamps: an evening session that finishes after midnight
UTC must still file under the local day the user experienced. Core and
extensions share these helpers so every ledger agrees on what "today" means.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from tasque2.config import get_settings


def local_today(now: datetime | None = None) -> date:
    """Today's date in the configured timezone."""
    return _to_local(now or datetime.now(UTC)).date()


def local_date(value: datetime) -> str:
    """The configured-timezone calendar date for a timestamp (YYYY-MM-DD)."""
    return _to_local(value).date().isoformat()


def next_local_midnight(now: datetime | None = None) -> datetime:
    """When the next local day starts, as an aware UTC moment."""
    local = _to_local(now or datetime.now(UTC))
    start = (local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start.astimezone(UTC)


def _to_local(value: datetime) -> datetime:
    try:
        zone = ZoneInfo(get_settings().timezone)
    except Exception:  # noqa: BLE001 - settings/tz problems must not break date math
        return value
    return value.astimezone(zone)
