"""Retry policy for failed attempts, including provider usage-limit stops.

A worker-reported failure uses the work item's own ``max_attempts``. A transient failure
(the provider crashed, the socket dropped, no result was submitted) gets a floor of
attempts because the task itself never ran to completion. A usage-limit stop is "no
capacity right now": it gets a higher floor and waits for the reset the provider states.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from tasque2.config import get_settings
from tasque2.models import utc_now

TRANSIENT_ERROR_TYPES = frozenset({"TransientProviderError"})
TRANSIENT_RETRY_FLOOR = 3
TRANSIENT_RETRY_DELAY_SECONDS = 30
LIMIT_RETRY_FLOOR = 8
LIMIT_RETRY_FALLBACK_SECONDS = 30 * 60
LIMIT_RETRY_BUFFER_SECONDS = 5 * 60
LIMIT_RETRY_MAX_SECONDS = 7 * 24 * 60 * 60
CAPACITY_GATE_MAX_SECONDS = 6 * 60 * 60

_LIMIT_MESSAGE_RE = re.compile(
    r"\b(?:session|usage|weekly|monthly|rate|hourly|5-hour)[\s-]*limit\b"
    r"|hit\s+your\b[^.\n]{0,40}\blimit\b"
    r"|limit\s+reached\b",
    re.IGNORECASE,
)
_RESET_TIME_RE = re.compile(
    r"\bresets?\b[^0-9\n]{0,15}(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>am|pm)\b",
    re.IGNORECASE,
)
_RESET_DATE_RE = re.compile(
    r"\bresets?\b[^A-Za-z0-9\n]{0,5}(?P<month>[A-Za-z]{3,9})\.?\s+(?P<day>\d{1,2})"
    r"\s*,?\s*(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>am|pm)\b",
    re.IGNORECASE,
)
_TIMEZONE_RE = re.compile(r"\(([A-Za-z]+(?:/[A-Za-z_+\-0-9]+)+)\)")
_MONTHS = {
    name: number
    for number, names in enumerate(
        (
            ("jan", "january"),
            ("feb", "february"),
            ("mar", "march"),
            ("apr", "april"),
            ("may",),
            ("jun", "june"),
            ("jul", "july"),
            ("aug", "august"),
            ("sep", "sept", "september"),
            ("oct", "october"),
            ("nov", "november"),
            ("dec", "december"),
        ),
        start=1,
    )
    for name in names
}
_SCOPE_FALLBACK_SECONDS = (
    (re.compile(r"\bmonthly\b", re.IGNORECASE), 12 * 60 * 60),
    (re.compile(r"\bweekly\b", re.IGNORECASE), 6 * 60 * 60),
)


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    delay_seconds: int
    transient: bool
    limit_delay_seconds: int | None

    @property
    def limit_stop(self) -> bool:
        return self.limit_delay_seconds is not None


def decide_retry(
    *,
    error_type: str | None,
    error_message: str | None,
    attempt_number: int,
    max_attempts: int,
    base_delay_seconds: int = 0,
    now: datetime | None = None,
) -> RetryDecision:
    """Whether a failed attempt goes back to the queue, and after how long."""
    transient = (error_type or "") in TRANSIENT_ERROR_TYPES
    limit_delay = limit_retry_delay_seconds(error_message, now=now) if transient else None
    budget = max_attempts
    if transient:
        budget = max(max_attempts, LIMIT_RETRY_FLOOR if limit_delay is not None else TRANSIENT_RETRY_FLOOR)
    if attempt_number >= budget:
        return RetryDecision(retry=False, delay_seconds=0, transient=transient, limit_delay_seconds=limit_delay)
    delay = base_delay_seconds
    if transient:
        delay = max(delay, limit_delay if limit_delay is not None else TRANSIENT_RETRY_DELAY_SECONDS)
    return RetryDecision(retry=True, delay_seconds=delay, transient=transient, limit_delay_seconds=limit_delay)


def limit_retry_delay_seconds(
    error_message: str | None,
    *,
    now: datetime | None = None,
    default_timezone: str | None = None,
) -> int | None:
    """Retry delay for a provider usage or session limit stop, or None for other errors.

    The reset the message states wins, whether it names a date ("resets Aug 19, 11pm") or
    only a time of day ("resets 11:40am"). With no parseable reset, back off by the
    window's stated scope.
    """
    message = (error_message or "").strip()
    if not message or _LIMIT_MESSAGE_RE.search(message) is None:
        return None
    fallback = _scope_fallback_seconds(message)
    date_match = _RESET_DATE_RE.search(message)
    match = date_match or _RESET_TIME_RE.search(message)
    if match is None:
        return fallback
    clock = _parse_clock(match)
    tzinfo = _reset_timezone(message, default_timezone)
    if clock is None or tzinfo is None:
        return fallback
    hour, minute = clock
    local_now = (now or utc_now()).astimezone(tzinfo)
    if date_match is not None:
        target = _date_target(date_match, local_now, hour, minute)
        if target is None:
            return fallback
    else:
        target = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= local_now:
            target += timedelta(days=1)
    delay = int((target - local_now).total_seconds()) + LIMIT_RETRY_BUFFER_SECONDS
    return max(60, min(delay, LIMIT_RETRY_MAX_SECONDS))


class ProviderCapacityGate:
    """Process-local hold on provider claims after an account-wide limit stop.

    A usage limit applies to every run, so after one stop the daemon stops claiming
    provider work until the stated reset instead of spending an attempt per item on an
    instant failure. Each item's own ``not_before`` still carries its retry, so a restarted
    daemon simply relearns the gate from the next stop.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._until: datetime | None = None

    def hold_until(self, until: datetime) -> None:
        capped = min(until, utc_now() + timedelta(seconds=CAPACITY_GATE_MAX_SECONDS))
        with self._lock:
            if self._until is None or capped > self._until:
                self._until = capped

    def until(self) -> datetime | None:
        with self._lock:
            return self._until

    def is_closed(self, now: datetime | None = None) -> bool:
        until = self.until()
        return until is not None and until > (now or utc_now())

    def reset(self) -> None:
        with self._lock:
            self._until = None


capacity_gate = ProviderCapacityGate()


def _scope_fallback_seconds(message: str) -> int:
    for pattern, seconds in _SCOPE_FALLBACK_SECONDS:
        if pattern.search(message) is not None:
            return seconds
    return LIMIT_RETRY_FALLBACK_SECONDS


def _parse_clock(match: re.Match[str]) -> tuple[int, int] | None:
    hour = int(match.group("hour"))
    minute = int(match.group("minute") or 0)
    if not (1 <= hour <= 12 and 0 <= minute <= 59):
        return None
    ampm = match.group("ampm").lower()
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0
    return hour, minute


def _reset_timezone(message: str, default_timezone: str | None) -> ZoneInfo | None:
    tz_match = _TIMEZONE_RE.search(message)
    candidates = [tz_match.group(1) if tz_match else None, default_timezone, get_settings().timezone]
    for name in candidates:
        if not name:
            continue
        try:
            return ZoneInfo(name)
        except (KeyError, ValueError):
            continue
    return None


def _date_target(match: re.Match[str], local_now: datetime, hour: int, minute: int) -> datetime | None:
    """The absolute reset instant; the unstated year is the one that puts it in the future."""
    month = _MONTHS.get(match.group("month").lower())
    day = int(match.group("day"))
    if month is None or not (1 <= day <= 31):
        return None
    for year in (local_now.year, local_now.year + 1):
        try:
            target = local_now.replace(
                year=year, month=month, day=day, hour=hour, minute=minute, second=0, microsecond=0
            )
        except ValueError:
            continue
        if target > local_now:
            return target
    return None
