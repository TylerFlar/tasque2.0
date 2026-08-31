from __future__ import annotations

import re
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import (
    FailedWork,
    ProviderRun,
    WorkAttempt,
    WorkDependency,
    WorkEvent,
    WorkItem,
    utc_now,
)

TERMINAL_WORK_STATUSES = {"succeeded", "dead_letter", "canceled"}

# Attempt error_type values that represent infrastructure/transient failures
# rather than a genuine agent-reported task outcome. These get a retry floor
# (TRANSIENT_RETRY_FLOOR total attempts) independent of the work item's
# max_attempts, so a single dropped socket or crashed provider no longer
# permanently dead-letters work that was never actually attempted to completion.
TRANSIENT_ERROR_TYPES = frozenset({"TransientProviderError"})
TRANSIENT_RETRY_FLOOR = 3
TRANSIENT_RETRY_DELAY_SECONDS = 30
# A usage-limit stop is "no capacity right now", not an attempt at the task, so
# it gets its own (higher) floor. Three attempts is plenty for a dropped socket
# but nowhere near enough to ride out a weekly window, which is why limit stops
# were the single largest source of dead-lettered work through 2026-08.
LIMIT_RETRY_FLOOR = 8

# Providers surface subscription usage/session-limit stops as transient errors
# whose message states the reset time, e.g. "You've hit your session limit ·
# resets 11:40am (America/Los_Angeles)". Retrying those on the normal
# transient cadence burns the whole retry floor in ~a minute while the limit
# is still in force (that is how a career-apply child dead-lettered on
# 2026-07-08), so limit-shaped failures wait for the stated reset instead.
#
# Longer windows state a *date* as well ("resets Aug 19, 11pm"), and a spend
# cap states no reset at all ("hit your monthly spend limit - raise it at ...").
# Both used to fall through to the 30-minute fallback and burn the retry floor
# inside an hour against a window that stays shut for days -- that is how the
# 2026-08-18 career-apply run failed. So: parse the date form, and scale the
# no-reset fallback to the stated scope.
LIMIT_RETRY_FALLBACK_SECONDS = 30 * 60
LIMIT_RETRY_BUFFER_SECONDS = 5 * 60
# A stated reset may legitimately be days out on a weekly window; only the
# *parsed* target is clamped this far, so same-day session resets are unaffected.
LIMIT_RETRY_MAX_SECONDS = 7 * 24 * 60 * 60

_LIMIT_MESSAGE_RE = re.compile(
    r"\b(?:session|usage|weekly|monthly|rate|hourly|5-hour)[\s-]*limit\b"
    r"|hit\s+your\b[^.\n]{0,40}\blimit\b"
    r"|limit\s+reached\b",
    re.IGNORECASE,
)
_LIMIT_RESET_TIME_RE = re.compile(
    r"\bresets?\b[^0-9\n]{0,15}(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>am|pm)\b",
    re.IGNORECASE,
)
# "resets Aug 19, 11pm" / "resets Jul 28, 7:30pm" -- the weekly-window shape.
_LIMIT_RESET_DATE_RE = re.compile(
    r"\bresets?\b[^A-Za-z0-9\n]{0,5}(?P<month>[A-Za-z]{3,9})\.?\s+(?P<day>\d{1,2})"
    r"\s*,?\s*(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>am|pm)\b",
    re.IGNORECASE,
)
_LIMIT_TZ_RE = re.compile(r"\(([A-Za-z]+(?:/[A-Za-z_+\-0-9]+)+)\)")
_LIMIT_MONTHS = {
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
# When the provider states no reset time, back off by how long the window
# plausibly lasts rather than the flat 30 minutes a session stop deserves.
_LIMIT_SCOPE_FALLBACK_SECONDS: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"\bmonthly\b", re.IGNORECASE), 12 * 60 * 60),
    (re.compile(r"\bweekly\b", re.IGNORECASE), 6 * 60 * 60),
)


def limit_retry_delay_seconds(
    error_message: str | None,
    *,
    now: datetime | None = None,
    default_timezone: str | None = None,
) -> int | None:
    """Retry delay for a provider usage/session-limit stop, if this is one.

    Returns None when the message does not look like a limit stop. When it
    does, prefer the reset stated in the message (plus a small buffer), whether
    that reset names a date ("resets Aug 19, 11pm") or only a time-of-day
    ("resets 11:40am"). With no parseable reset, back off by the window's stated
    scope instead of the normal transient cadence.
    """
    message = (error_message or "").strip()
    if not message or _LIMIT_MESSAGE_RE.search(message) is None:
        return None

    fallback = _limit_scope_fallback_seconds(message)
    date_match = _LIMIT_RESET_DATE_RE.search(message)
    time_match = None if date_match is not None else _LIMIT_RESET_TIME_RE.search(message)
    if date_match is None and time_match is None:
        return fallback

    match = date_match or time_match
    assert match is not None  # narrowed by the check above
    clock = _parse_reset_clock(match)
    if clock is None:
        return fallback
    hour, minute = clock

    tzinfo = _resolve_reset_timezone(message, default_timezone)
    if tzinfo is None:
        return fallback

    local_now = (now or utc_now()).astimezone(tzinfo)
    if date_match is not None:
        target = _reset_target_from_date(date_match, local_now, hour, minute)
        if target is None:
            return fallback
    else:
        target = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= local_now:
            target += timedelta(days=1)
    delay = int((target - local_now).total_seconds()) + LIMIT_RETRY_BUFFER_SECONDS
    return max(60, min(delay, LIMIT_RETRY_MAX_SECONDS))


def _limit_scope_fallback_seconds(message: str) -> int:
    """Backoff for a limit stop that states no reset, scaled to its scope."""
    for pattern, seconds in _LIMIT_SCOPE_FALLBACK_SECONDS:
        if pattern.search(message) is not None:
            return seconds
    return LIMIT_RETRY_FALLBACK_SECONDS


def _parse_reset_clock(match: re.Match[str]) -> tuple[int, int] | None:
    """12-hour clock from a reset match, normalized to 24-hour (hour, minute)."""
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


def _resolve_reset_timezone(message: str, default_timezone: str | None) -> ZoneInfo | None:
    """Timezone the reset is stated in: message first, then caller, then settings."""
    tz_match = _LIMIT_TZ_RE.search(message)
    for tz_name in filter(None, (tz_match.group(1) if tz_match else None, default_timezone)):
        try:
            return ZoneInfo(tz_name)
        except (KeyError, ValueError):
            continue
    try:
        return ZoneInfo(get_settings().timezone)
    except (KeyError, ValueError):
        return None


def _reset_target_from_date(
    match: re.Match[str],
    local_now: datetime,
    hour: int,
    minute: int,
) -> datetime | None:
    """Absolute reset instant for a dated reset, resolving the unstated year.

    The provider states no year, so pick the one that puts the reset ahead of
    now -- which is what rolls "resets Jan 2" forward correctly on Dec 31.
    """
    month = _LIMIT_MONTHS.get(match.group("month").lower())
    day = int(match.group("day"))
    if month is None or not (1 <= day <= 31):
        return None
    for year in (local_now.year, local_now.year + 1):
        try:
            target = local_now.replace(
                year=year, month=month, day=day, hour=hour, minute=minute, second=0, microsecond=0
            )
        except ValueError:  # e.g. Feb 30, or Feb 29 in a common year
            continue
        if target > local_now:
            return target
    return None


# A provider session limit is account-wide, not per-item: once one run is told
# "resets 4:40pm", every other provider run will be told the same thing. Without
# a gate the daemon keeps claiming ready items and burning a real attempt on each
# one for a ~2s $0 failure -- on 2026-08-03 that spent 8 of 10 career-apply
# attempts in 77 seconds against a window that was already closed. The gate is
# process-local on purpose: it is an optimisation, not a correctness boundary
# (each item's own not_before already holds the retry), so a restarted daemon
# simply relearns it from the first limit stop instead of needing schema.
_LIMIT_GATE_MAX_SECONDS = 6 * 60 * 60
_limit_gate_lock = threading.Lock()
_limit_gate_until: datetime | None = None


def note_provider_limit_stop(until: datetime) -> None:
    """Record that provider capacity is exhausted until ``until``."""
    global _limit_gate_until
    ceiling = utc_now() + timedelta(seconds=_LIMIT_GATE_MAX_SECONDS)
    capped = min(until, ceiling)
    with _limit_gate_lock:
        if _limit_gate_until is None or capped > _limit_gate_until:
            _limit_gate_until = capped


def provider_limit_gate_until() -> datetime | None:
    """The time provider work may be claimed again, or None when not gated."""
    with _limit_gate_lock:
        return _limit_gate_until


def reset_provider_limit_gate() -> None:
    """Clear the gate (used by tests and by operators forcing a retry)."""
    global _limit_gate_until
    with _limit_gate_lock:
        _limit_gate_until = None


@dataclass(frozen=True)
class ClaimedWork:
    work_item: WorkItem
    attempt: WorkAttempt


class WorkQueue:
    def __init__(self, session: Session) -> None:
        self.session = session

    def claim_next_ready_work(
        self,
        *,
        lease_owner: str,
        lease_seconds: int | None = None,
        now: datetime | None = None,
    ) -> ClaimedWork | None:
        now = now or utc_now()
        gated_until = provider_limit_gate_until()
        provider_gated = gated_until is not None and gated_until > now
        candidates = self.session.scalars(
            select(WorkItem)
            .where(
                WorkItem.status == "ready",
                or_(WorkItem.not_before.is_(None), WorkItem.not_before <= now),
            )
            .order_by(WorkItem.priority.desc(), WorkItem.created_at.asc())
            .limit(50)
        ).all()

        for work_item in candidates:
            if provider_gated and work_item.worker_kind.startswith("provider."):
                # Provider capacity is exhausted account-wide; claiming this
                # would spend an attempt on a guaranteed instant failure.
                continue
            if self._deadline_passed(work_item, now):
                # Time-sensitive work whose window closed must not run late
                # (e.g. a daily trading step resuming a day after the daemon was
                # down). Abandon it instead of claiming it.
                self._expire_overdue_work_item(work_item, now=now)
                continue
            if self._has_unsatisfied_dependency(work_item):
                continue

            attempt_number = work_item.attempt_count + 1
            attempt = WorkAttempt(
                work_item_id=work_item.id,
                attempt_number=attempt_number,
                status="running",
                lease_owner=lease_owner,
                lease_expires_at=(
                    now + timedelta(seconds=lease_seconds)
                    if lease_seconds is not None
                    else None
                ),
                started_at=now,
                heartbeat_at=now,
                worker_kind=work_item.worker_kind,
            )
            work_item.status = "running"
            work_item.attempt_count = attempt_number
            self.session.add(attempt)
            self.session.flush()
            self._emit_event(
                event_type="work.claimed",
                entity_kind="work_item",
                entity_id=work_item.id,
                work_item_id=work_item.id,
                attempt_id=attempt.id,
                source="queue",
                summary=f"Claimed by {lease_owner}",
                payload={"lease_owner": lease_owner, "attempt_number": attempt_number},
            )
            return ClaimedWork(work_item=work_item, attempt=attempt)

        return None

    def heartbeat_attempt(
        self,
        attempt_id: str,
        *,
        lease_seconds: int | None = None,
        now: datetime | None = None,
    ) -> WorkAttempt:
        now = now or utc_now()
        attempt = self._get_attempt(attempt_id)
        if attempt.status != "running":
            raise ValueError(f"Cannot heartbeat attempt in status {attempt.status!r}.")
        attempt.heartbeat_at = now
        attempt.lease_expires_at = (
            now + timedelta(seconds=lease_seconds) if lease_seconds is not None else None
        )
        self.session.flush()
        self._emit_event(
            event_type="work.heartbeat",
            entity_kind="work_item",
            entity_id=attempt.work_item_id,
            work_item_id=attempt.work_item_id,
            attempt_id=attempt.id,
            source="queue",
            summary="Lease heartbeat recorded",
        )
        return attempt

    def heartbeat_running_attempts(
        self,
        attempt_ids: Sequence[str],
        *,
        lease_seconds: int | None = None,
        now: datetime | None = None,
    ) -> int:
        """Refresh leases for in-flight attempts without emitting an event each time.

        The daemon calls this every tick (seconds apart), so the per-heartbeat
        ``work.heartbeat`` event that :meth:`heartbeat_attempt` writes would bloat
        the event log by thousands of rows a day. The lease itself is the signal
        that matters: a daemon that dies stops refreshing it, and lease recovery
        picks the work back up.
        """
        ids = [str(attempt_id) for attempt_id in attempt_ids if attempt_id]
        if not ids:
            return 0
        now = now or utc_now()
        expires_at = now + timedelta(seconds=lease_seconds) if lease_seconds is not None else None
        attempts = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.id.in_(ids),
                WorkAttempt.status == "running",
            )
        ).all()
        for attempt in attempts:
            attempt.heartbeat_at = now
            attempt.lease_expires_at = expires_at
        self.session.flush()
        return len(attempts)

    def complete_attempt(
        self,
        attempt_id: str,
        *,
        summary: str,
        produces: dict[str, Any] | None = None,
        report_artifact_id: str | None = None,
        now: datetime | None = None,
    ) -> WorkAttempt:
        now = now or utc_now()
        attempt = self._get_attempt(attempt_id)
        work_item = attempt.work_item
        self.session.refresh(work_item)

        if work_item.status in {"cancel_requested", "canceled"}:
            attempt.status = "canceled"
            work_item.status = "canceled"
            event_type = "work.canceled"
            event_summary = "Work was canceled before completion was recorded"
        else:
            attempt.status = "succeeded"
            work_item.status = "succeeded"
            event_type = "work.succeeded"
            event_summary = summary

        attempt.ended_at = now
        attempt.summary = summary
        attempt.produces = produces or {}
        attempt.report_artifact_id = report_artifact_id
        self._close_sibling_running_attempts(
            attempt,
            now=now,
            summary="Another attempt finished this WorkItem first.",
        )
        self.session.flush()
        self._emit_event(
            event_type=event_type,
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            attempt_id=attempt.id,
            source="queue",
            summary=event_summary,
            payload={"produces": produces or {}, "report_artifact_id": report_artifact_id},
        )
        return attempt

    def fail_attempt(
        self,
        attempt_id: str,
        *,
        error_type: str,
        error_message: str,
        now: datetime | None = None,
    ) -> WorkAttempt:
        now = now or utc_now()
        attempt = self._get_attempt(attempt_id)
        work_item = attempt.work_item
        self.session.refresh(work_item)
        attempt.status = "failed"
        attempt.ended_at = now
        attempt.error_type = error_type
        attempt.error_message = error_message
        self._transition_after_failure(
            attempt,
            now=now,
            event_type="work.failed",
            summary=error_message,
        )
        return attempt

    def recover_expired_leases(self, *, now: datetime | None = None) -> int:
        now = now or utc_now()
        attempts = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.status == "running",
                WorkAttempt.lease_expires_at.is_not(None),
                WorkAttempt.lease_expires_at < now,
            )
        ).all()

        for attempt in attempts:
            attempt.status = "expired"
            attempt.ended_at = now
            attempt.error_type = "LeaseExpired"
            attempt.error_message = "The work attempt lease expired before completion."
            # The subprocess behind an expired lease is gone with it. Without
            # this its ProviderRun row stays "running" forever and shows up as
            # phantom in-flight work in status output.
            self._close_running_provider_runs(attempt, now=now, marker="lease_expired")
            self._transition_after_failure(
                attempt,
                now=now,
                event_type="work.lease_expired",
                summary=attempt.error_message,
            )

        return len(attempts)

    def _close_running_provider_runs(
        self,
        attempt: WorkAttempt,
        *,
        now: datetime,
        marker: str,
        extra: dict[str, object] | None = None,
    ) -> list[ProviderRun]:
        """Mark this attempt's still-'running' provider runs as orphaned."""
        provider_runs = list(
            self.session.scalars(
                select(ProviderRun).where(
                    ProviderRun.attempt_id == attempt.id,
                    ProviderRun.status == "running",
                )
            ).all()
        )
        for provider_run in provider_runs:
            provider_run.status = "orphaned"
            provider_run.ended_at = now
            provider_run.usage = {**(provider_run.usage or {}), marker: True, **(extra or {})}
        return provider_runs

    def recover_orphaned_attempts(
        self,
        *,
        lease_owner: str,
        orphaned_before: datetime,
        now: datetime | None = None,
    ) -> int:
        """Requeue daemon-owned attempts left running by a prior process.

        This is deliberately separate from lease expiry. Workers do not have a
        wall-clock timeout, but a restarted daemon needs a way to recover rows
        that were claimed by an older process and can no longer complete.
        """
        now = now or utc_now()
        attempts = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.status == "running",
                WorkAttempt.lease_owner == lease_owner,
                WorkAttempt.lease_expires_at.is_(None),
                or_(
                    WorkAttempt.heartbeat_at.is_(None),
                    WorkAttempt.heartbeat_at < orphaned_before,
                ),
            )
        ).all()

        for attempt in attempts:
            attempt.status = "orphaned"
            attempt.ended_at = now
            attempt.error_type = "OrphanedAttempt"
            attempt.error_message = (
                f"The {lease_owner!r} process restarted before this attempt completed."
            )

            work_item = attempt.work_item
            if work_item.status == "running":
                work_item.status = "ready"
                work_item.not_before = None
                work_item.max_attempts = max(work_item.max_attempts, work_item.attempt_count + 1)

            provider_runs = self._close_running_provider_runs(
                attempt, now=now, marker="orphaned_recovered"
            )

            self._emit_event(
                event_type="work.orphaned_attempt_recovered",
                entity_kind="work_item",
                entity_id=work_item.id,
                work_item_id=work_item.id,
                attempt_id=attempt.id,
                source="queue",
                summary="Recovered orphaned running attempt after daemon restart",
                payload={
                    "lease_owner": lease_owner,
                    "orphaned_before": orphaned_before.isoformat(),
                    "provider_run_ids": [provider_run.id for provider_run in provider_runs],
                },
            )

        self.session.flush()
        return len(attempts)

    def expire_overdue_work(self, *, now: datetime | None = None) -> int:
        """Dead-letter ready/paused work whose ``deadline_at`` passed unrun.

        ``deadline_at`` marks the latest time a work item may still *start*.
        Once it passes, running the item would execute stale, time-sensitive
        work -- e.g. a scheduled trading step resuming hours late after the
        daemon was down -- so we abandon it to the dead-letter queue instead of
        letting a worker claim it. Running attempts are left untouched: lease
        and orphan recovery own in-flight work.
        """
        now = now or utc_now()
        overdue = self.session.scalars(
            select(WorkItem).where(
                WorkItem.status.in_(("ready", "paused")),
                WorkItem.deadline_at.is_not(None),
                WorkItem.deadline_at < now,
            )
        ).all()
        for work_item in overdue:
            self._expire_overdue_work_item(work_item, now=now)
        self.session.flush()
        return len(overdue)

    def _deadline_passed(self, work_item: WorkItem, now: datetime) -> bool:
        return work_item.deadline_at is not None and work_item.deadline_at < now

    def _expire_overdue_work_item(self, work_item: WorkItem, *, now: datetime) -> None:
        deadline = work_item.deadline_at
        work_item.status = "dead_letter"
        failed = FailedWork(
            work_item_id=work_item.id,
            attempt_id=None,
            status="unresolved",
            error_type="DeadlineExceeded",
            error_message=(
                f"Work deadline {deadline.isoformat()} passed before it could run."
                if deadline is not None
                else "Work deadline passed before it could run."
            ),
            retry_count=work_item.attempt_count,
        )
        self.session.add(failed)
        self.session.flush()
        self._emit_event(
            event_type="work.deadline_exceeded",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            source="queue",
            summary="Work expired: deadline passed before it could run",
            payload={
                "deadline_at": deadline.isoformat() if deadline is not None else None,
                "failed_work_id": failed.id,
            },
        )

    def _close_sibling_running_attempts(
        self,
        attempt: WorkAttempt,
        *,
        now: datetime,
        summary: str,
    ) -> None:
        sibling_attempts = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.work_item_id == attempt.work_item_id,
                WorkAttempt.id != attempt.id,
                WorkAttempt.status == "running",
            )
        ).all()
        for sibling in sibling_attempts:
            sibling.status = "orphaned"
            sibling.ended_at = now
            sibling.error_type = "SupersededAttempt"
            sibling.error_message = summary

            provider_runs = self._close_running_provider_runs(
                sibling,
                now=now,
                marker="superseded",
                extra={"superseded_by_attempt_id": attempt.id},
            )

            self._emit_event(
                event_type="work.sibling_attempt_superseded",
                entity_kind="work_item",
                entity_id=attempt.work_item_id,
                work_item_id=attempt.work_item_id,
                attempt_id=sibling.id,
                source="queue",
                summary=summary,
                payload={
                    "completed_attempt_id": attempt.id,
                    "provider_run_ids": [provider_run.id for provider_run in provider_runs],
                },
            )

    def pause_work(self, work_item_id: str) -> WorkItem:
        work_item = self._get_work_item(work_item_id)
        if work_item.status in TERMINAL_WORK_STATUSES:
            return work_item
        work_item.status = "paused"
        self.session.flush()
        self._emit_event(
            event_type="work.paused",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            source="queue",
            summary="Work paused",
        )
        return work_item

    def resume_work(self, work_item_id: str) -> WorkItem:
        work_item = self._get_work_item(work_item_id)
        if work_item.status != "paused":
            return work_item
        work_item.status = "ready"
        self.session.flush()
        self._emit_event(
            event_type="work.resumed",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            source="queue",
            summary="Work resumed",
        )
        return work_item

    def request_cancel(self, work_item_id: str) -> WorkItem:
        work_item = self._get_work_item(work_item_id)
        if work_item.status in TERMINAL_WORK_STATUSES:
            return work_item
        if work_item.status == "running":
            work_item.status = "cancel_requested"
            event_type = "work.cancel_requested"
            summary = "Cancellation requested"
        else:
            work_item.status = "canceled"
            event_type = "work.canceled"
            summary = "Work canceled"
        self.session.flush()
        self._emit_event(
            event_type=event_type,
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            source="queue",
            summary=summary,
        )
        return work_item

    def retry_dead_letter(self, work_item_id: str) -> WorkItem:
        work_item = self._get_work_item(work_item_id)
        if work_item.status != "dead_letter":
            return work_item

        now = utc_now()
        work_item.status = "ready"
        work_item.not_before = None
        work_item.max_attempts = max(work_item.max_attempts, work_item.attempt_count + 1)
        failed_rows = self.session.scalars(
            select(FailedWork).where(
                FailedWork.work_item_id == work_item.id,
                FailedWork.status == "unresolved",
            )
        ).all()
        for failed in failed_rows:
            failed.status = "retrying"
            failed.resolved_at = now
            failed.resolution_note = "Operator requested retry."
        self.session.flush()
        self._emit_event(
            event_type="work.retry_requested",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            source="queue",
            summary="Dead-letter work returned to ready state",
        )
        return work_item

    def _effective_max_attempts(self, attempt: WorkAttempt, work_item: WorkItem) -> int:
        """Attempt budget for this failure.

        Genuine agent-reported failures use the work item's configured
        max_attempts. Transient/infra failures (TRANSIENT_ERROR_TYPES) are
        guaranteed at least TRANSIENT_RETRY_FLOOR attempts regardless, so a
        single dropped socket cannot permanently dead-letter a max_attempts=1
        work item that never ran to completion. Usage-limit stops get the larger
        LIMIT_RETRY_FLOOR: each one costs an attempt but ran no task, and a
        weekly window outlasts the transient floor several times over.
        """
        if (attempt.error_type or "") in TRANSIENT_ERROR_TYPES:
            if limit_retry_delay_seconds(attempt.error_message) is not None:
                return max(work_item.max_attempts, LIMIT_RETRY_FLOOR)
            return max(work_item.max_attempts, TRANSIENT_RETRY_FLOOR)
        return work_item.max_attempts

    def _transition_after_failure(
        self,
        attempt: WorkAttempt,
        *,
        now: datetime,
        event_type: str,
        summary: str,
    ) -> None:
        work_item = attempt.work_item
        self._emit_event(
            event_type=event_type,
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            attempt_id=attempt.id,
            source="queue",
            summary=summary,
            payload={
                "attempt_number": attempt.attempt_number,
                "error_type": attempt.error_type,
                "error_message": attempt.error_message,
            },
        )

        if work_item.status in {"cancel_requested", "canceled"}:
            attempt.status = "canceled"
            work_item.status = "canceled"
            self._emit_event(
                event_type="work.canceled",
                entity_kind="work_item",
                entity_id=work_item.id,
                work_item_id=work_item.id,
                attempt_id=attempt.id,
                source="queue",
                summary="Work canceled after attempt ended",
            )
        elif attempt.attempt_number < self._effective_max_attempts(attempt, work_item):
            is_transient = (attempt.error_type or "") in TRANSIENT_ERROR_TYPES
            base_delay = int((work_item.retry_policy or {}).get("delay_seconds", 0))
            if is_transient:
                limit_delay = limit_retry_delay_seconds(attempt.error_message, now=now)
                if limit_delay is not None:
                    # Account-wide capacity stop: hold every other provider claim
                    # until the stated reset instead of burning an attempt each.
                    note_provider_limit_stop(now + timedelta(seconds=limit_delay))
                transient_delay = (
                    limit_delay if limit_delay is not None else TRANSIENT_RETRY_DELAY_SECONDS
                )
                retry_delay = max(base_delay, transient_delay)
            else:
                retry_delay = base_delay
            work_item.status = "ready"
            work_item.not_before = now + timedelta(seconds=retry_delay)
            self._emit_event(
                event_type="work.retry_scheduled",
                entity_kind="work_item",
                entity_id=work_item.id,
                work_item_id=work_item.id,
                attempt_id=attempt.id,
                source="queue",
                summary="Work returned to ready state for retry",
                payload={"delay_seconds": retry_delay, "transient": is_transient},
            )
        else:
            work_item.status = "dead_letter"
            failed = FailedWork(
                work_item_id=work_item.id,
                attempt_id=attempt.id,
                status="unresolved",
                error_type=attempt.error_type,
                error_message=attempt.error_message,
                retry_count=work_item.attempt_count,
            )
            self.session.add(failed)
            self.session.flush()
            self._emit_event(
                event_type="work.dead_lettered",
                entity_kind="work_item",
                entity_id=work_item.id,
                work_item_id=work_item.id,
                attempt_id=attempt.id,
                source="queue",
                summary="Work moved to dead letter after retries were exhausted",
                payload={"failed_work_id": failed.id},
            )

        self.session.flush()

    def _has_unsatisfied_dependency(self, work_item: WorkItem) -> bool:
        dependencies = self.session.scalars(
            select(WorkDependency).where(WorkDependency.blocked_work_item_id == work_item.id)
        ).all()
        for dependency in dependencies:
            if dependency.dependency_workflow_node_id is not None:
                return True
            if dependency.dependency_work_item_id is None:
                continue
            upstream = self.session.get(WorkItem, dependency.dependency_work_item_id)
            if upstream is None or upstream.status != dependency.condition:
                return True
        return False

    def _get_work_item(self, work_item_id: str) -> WorkItem:
        work_item = self.session.get(WorkItem, work_item_id)
        if work_item is None:
            raise KeyError(f"Unknown work item: {work_item_id}")
        return work_item

    def _get_attempt(self, attempt_id: str) -> WorkAttempt:
        attempt = self.session.get(WorkAttempt, attempt_id)
        if attempt is None:
            raise KeyError(f"Unknown work attempt: {attempt_id}")
        return attempt

    def _emit_event(
        self,
        *,
        event_type: str,
        entity_kind: str,
        entity_id: str,
        work_item_id: str | None = None,
        attempt_id: str | None = None,
        source: str,
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WorkEvent:
        workflow_run_id = None
        if work_item_id is not None:
            work_item = self.session.get(WorkItem, work_item_id)
            if work_item is not None:
                workflow_run_id = work_item.workflow_run_id
        event = WorkEvent(
            event_type=event_type,
            entity_kind=entity_kind,
            entity_id=entity_id,
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            workflow_run_id=workflow_run_id,
            source=source,
            summary=summary,
            payload=payload or {},
        )
        self.session.add(event)
        self.session.flush()
        return event
