"""Durable schedules: cron, interval, and one-shot date triggers that launch work or workflows."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from croniter import croniter
from opentelemetry.trace import SpanKind
from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.events import record_event
from tasque2.models import Schedule, ScheduleOccurrence, WorkflowDefinition, utc_now
from tasque2.telemetry import instruments, span
from tasque2.templates import read_template_file
from tasque2.work.repository import WorkRepository
from tasque2.workflows import WorkflowService

logger = logging.getLogger(__name__)

_INTERVAL_RE = re.compile(r"^\s*(seconds|minutes|hours|days)\s*=\s*(\d+)\s*$")
WORKFLOW_SCHEDULE_TARGETS = {"workflow", "workflow.run"}
SCHEDULE_TYPES = ("cron", "interval", "date")
CATCHUP_POLICIES = ("skip", "coalesce", "all")


class ScheduleService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_schedule(
        self,
        *,
        name: str,
        schedule_type: str,
        expression: str,
        worker_kind: str,
        payload: dict[str, Any],
        timezone_name: str | None = None,
        runtime_contract: dict[str, Any] | None = None,
        catchup_policy: str = "coalesce",
        misfire_grace_seconds: int | None = None,
        max_backfill: int = 10,
        max_active_runs: int = 1,
        enabled: bool = True,
    ) -> Schedule:
        _validate_catchup_policy(catchup_policy)
        timezone_name = timezone_name or get_settings().timezone
        _validate_timing(schedule_type, expression, timezone_name)
        schedule = Schedule(
            name=name,
            enabled=enabled,
            schedule_type=schedule_type,
            expression=expression,
            timezone=timezone_name,
            payload=payload,
            worker_kind=worker_kind,
            runtime_contract=runtime_contract or {},
            catchup_policy=catchup_policy,
            misfire_grace_seconds=misfire_grace_seconds,
            max_backfill=max_backfill,
            max_active_runs=max_active_runs,
        )
        self.session.add(schedule)
        self.session.flush()
        self._event(
            "schedule.created",
            schedule,
            summary=f"Created schedule: {name}",
            payload={"schedule_type": schedule_type, "expression": expression, "worker_kind": worker_kind},
        )
        return schedule

    def enable_schedule(
        self, schedule_id: str, *, now: datetime | None = None, resume_from_now: bool = True
    ) -> Schedule:
        schedule = self._get(schedule_id)
        if not schedule.enabled:
            schedule.enabled = True
            if resume_from_now:
                schedule.last_evaluated_at = _aware(now or utc_now())
            self.session.flush()
            self._event(
                "schedule.enabled",
                schedule,
                summary=f"Enabled schedule: {schedule.name}",
                payload={"resume_from_now": resume_from_now},
            )
        return schedule

    def disable_schedule(self, schedule_id: str) -> Schedule:
        schedule = self._get(schedule_id)
        if schedule.enabled:
            schedule.enabled = False
            self.session.flush()
            self._event("schedule.disabled", schedule, summary=f"Disabled schedule: {schedule.name}")
        return schedule

    def delete_schedule(self, schedule_id: str) -> None:
        schedule = self._get(schedule_id)
        self._event("schedule.deleted", schedule, summary=f"Deleted schedule: {schedule.name}")
        self.session.delete(schedule)
        self.session.flush()

    def update_schedule(
        self,
        schedule_id: str,
        *,
        name: str | None = None,
        schedule_type: str | None = None,
        expression: str | None = None,
        worker_kind: str | None = None,
        payload: dict[str, Any] | None = None,
        timezone_name: str | None = None,
        runtime_contract: dict[str, Any] | None = None,
        catchup_policy: str | None = None,
        misfire_grace_seconds: int | None = None,
        max_backfill: int | None = None,
        max_active_runs: int | None = None,
        enabled: bool | None = None,
    ) -> Schedule:
        schedule = self._get(schedule_id)
        timing_changed = schedule_type is not None or expression is not None or timezone_name is not None
        if timing_changed:
            _validate_timing(
                schedule.schedule_type if schedule_type is None else schedule_type,
                schedule.expression if expression is None else expression,
                schedule.timezone if timezone_name is None else timezone_name,
            )
        if catchup_policy is not None:
            _validate_catchup_policy(catchup_policy)
        if name is not None:
            schedule.name = name
        if schedule_type is not None:
            schedule.schedule_type = schedule_type
        if expression is not None:
            schedule.expression = expression
        if worker_kind is not None:
            schedule.worker_kind = worker_kind
        if payload is not None:
            schedule.payload = payload
        if timezone_name is not None:
            schedule.timezone = timezone_name
        if runtime_contract is not None:
            schedule.runtime_contract = runtime_contract
        if catchup_policy is not None:
            schedule.catchup_policy = catchup_policy
        if misfire_grace_seconds is not None:
            schedule.misfire_grace_seconds = misfire_grace_seconds
        if max_backfill is not None:
            schedule.max_backfill = max_backfill
        if max_active_runs is not None:
            schedule.max_active_runs = max_active_runs
        if enabled is not None:
            schedule.enabled = enabled
        if timing_changed:
            # A new cadence starts from now: its next slot fires, not the most recent past one.
            schedule.last_evaluated_at = utc_now()
        self.session.flush()
        self._event(
            "schedule.updated",
            schedule,
            summary=f"Updated schedule: {schedule.name}",
            payload={
                "schedule_type": schedule.schedule_type,
                "expression": schedule.expression,
                "worker_kind": schedule.worker_kind,
            },
        )
        return schedule

    def fire_schedule_now(self, schedule_id: str, *, now: datetime | None = None) -> ScheduleOccurrence:
        schedule = self._get(schedule_id)
        occurrence = self.enqueue_occurrence(schedule, _aware(now or utc_now()))
        if occurrence is None:
            raise ValueError("Schedule occurrence already exists for this fire time.")
        self._event(
            "schedule.fired_now",
            schedule,
            summary=f"Fired schedule now: {schedule.name}",
            payload={"schedule_occurrence_id": occurrence.id},
            work_item_id=occurrence.work_item_id,
            workflow_run_id=occurrence.workflow_run_id,
        )
        return occurrence

    def poll_due_schedules(self, *, now: datetime | None = None) -> int:
        """Launch every due occurrence of every enabled schedule.

        A schedule that cannot launch is logged and left due, so it retries on the next poll
        and never holds up the others.
        """
        now = _aware(now or utc_now())
        schedules = self.session.scalars(
            select(Schedule).where(Schedule.enabled.is_(True)).order_by(Schedule.created_at)
        ).all()
        enqueued = 0
        for schedule in schedules:
            try:
                for scheduled_for in self.due_times(schedule, now=now):
                    if self.enqueue_occurrence(schedule, scheduled_for) is not None:
                        enqueued += 1
            except Exception:  # noqa: BLE001 - one broken schedule must not stop the rest
                logger.exception("Schedule %s (%s) could not fire; it stays due", schedule.name, schedule.id)
                continue
            schedule.last_evaluated_at = now
        self.session.flush()
        return enqueued

    def due_times(self, schedule: Schedule, *, now: datetime | None = None) -> list[datetime]:
        now = _aware(now or utc_now())
        tz = ZoneInfo(schedule.timezone)
        start = _aware(schedule.last_evaluated_at or schedule.created_at)
        if schedule.schedule_type == "date":
            scheduled_for = _parse_date(schedule.expression, tz)
            if scheduled_for <= now and not self._occurrence_exists(schedule, scheduled_for):
                return [scheduled_for]
            return []
        if schedule.schedule_type == "interval":
            times = list(_interval_times(schedule.expression, anchor=_aware(schedule.created_at), start=start, end=now))
        elif schedule.schedule_type == "cron":
            times = list(_cron_times(schedule.expression, tz, start=start, end=now))
        else:
            raise ValueError(f"Unsupported schedule type: {schedule.schedule_type!r}")
        if schedule.misfire_grace_seconds is not None:
            grace = timedelta(seconds=schedule.misfire_grace_seconds)
            times = [time for time in times if now - time <= grace]
        return _apply_catchup(schedule, times)

    def next_fire_time(self, schedule: Schedule, *, now: datetime | None = None) -> datetime | None:
        """When the schedule fires next, or None for a date schedule whose time has passed."""
        now = _aware(now or utc_now())
        tz = ZoneInfo(schedule.timezone)
        if schedule.schedule_type == "date":
            scheduled_for = _parse_date(schedule.expression, tz)
            return scheduled_for.astimezone(tz) if scheduled_for > now else None
        if schedule.schedule_type == "interval":
            anchor = _aware(schedule.created_at)
            interval = _interval(schedule.expression)
            return (anchor + (max(now - anchor, timedelta(0)) // interval + 1) * interval).astimezone(tz)
        upcoming = croniter(schedule.expression, now.astimezone(tz)).get_next(datetime)
        return upcoming if upcoming.tzinfo is not None else upcoming.replace(tzinfo=tz)

    def enqueue_occurrence(self, schedule: Schedule, scheduled_for: datetime) -> ScheduleOccurrence | None:
        """Launch one occurrence; None when that fire time already has one.

        The work or workflow is resolved (template read, payload checked, definition found)
        before anything is written, so a launch that fails leaves no occurrence behind.
        """
        scheduled_for = _aware(scheduled_for)
        dedupe_key = _dedupe_key(schedule.id, scheduled_for)
        if self.session.scalar(select(ScheduleOccurrence).where(ScheduleOccurrence.dedupe_key == dedupe_key)):
            return None
        is_workflow = schedule.worker_kind in WORKFLOW_SCHEDULE_TARGETS
        with span(
            "tasque.schedule.fire",
            kind=SpanKind.PRODUCER,
            attributes={
                "tasque.schedule.id": schedule.id,
                "tasque.schedule.name": schedule.name,
                "tasque.schedule.scheduled_for": scheduled_for.isoformat(),
                "tasque.schedule.target": "workflow" if is_workflow else schedule.worker_kind,
            },
        ):
            if is_workflow:
                definition, run_input = self._workflow_target(schedule)
                occurrence = self._new_occurrence(schedule, scheduled_for, dedupe_key)
                self._start_workflow(schedule, occurrence, scheduled_for, definition, run_input)
            else:
                fields = _work_fields(schedule)
                occurrence = self._new_occurrence(schedule, scheduled_for, dedupe_key)
                self._enqueue_work(schedule, occurrence, scheduled_for, dedupe_key, fields)
        instruments().schedule_occurrences.add(
            1, {"tasque.schedule.name": schedule.name, "tasque.schedule.target": "workflow" if is_workflow else "work"}
        )
        return occurrence

    def _new_occurrence(self, schedule: Schedule, scheduled_for: datetime, dedupe_key: str) -> ScheduleOccurrence:
        occurrence = ScheduleOccurrence(
            schedule_id=schedule.id,
            scheduled_for=scheduled_for,
            status="pending",
            dedupe_key=dedupe_key,
        )
        self.session.add(occurrence)
        self.session.flush()
        return occurrence

    def _enqueue_work(
        self,
        schedule: Schedule,
        occurrence: ScheduleOccurrence,
        scheduled_for: datetime,
        dedupe_key: str,
        fields: dict[str, Any],
    ) -> None:
        work = WorkRepository(self.session).create_work_item(
            **fields,
            idempotency_key=dedupe_key,
            source_kind="schedule",
            source_id=schedule.id,
            schedule_id=schedule.id,
            schedule_occurrence_id=occurrence.id,
        )
        occurrence.work_item_id = work.id
        occurrence.status = "enqueued"
        self.session.flush()
        self._event(
            "schedule.occurrence_enqueued",
            schedule,
            summary=f"Enqueued scheduled work: {schedule.name}",
            payload={
                "schedule_occurrence_id": occurrence.id,
                "scheduled_for": scheduled_for.isoformat(),
                "work_item_id": work.id,
            },
            work_item_id=work.id,
        )

    def _start_workflow(
        self,
        schedule: Schedule,
        occurrence: ScheduleOccurrence,
        scheduled_for: datetime,
        definition: WorkflowDefinition,
        run_input: dict[str, Any],
    ) -> None:
        payload = schedule.payload or {}
        run = WorkflowService(self.session).start_run(
            workflow_definition_id=definition.id,
            name=str(payload.get("run_name") or schedule.name),
            input={
                **run_input,
                "schedule_id": schedule.id,
                "schedule_occurrence_id": occurrence.id,
                "scheduled_for": scheduled_for.isoformat(),
            },
            discord_thread_id=_optional_str(payload.get("discord_thread_id")),
        )
        occurrence.workflow_run_id = run.id
        occurrence.status = "enqueued"
        self.session.flush()
        self._event(
            "schedule.workflow_started",
            schedule,
            summary=f"Started scheduled workflow: {schedule.name}",
            payload={
                "schedule_occurrence_id": occurrence.id,
                "scheduled_for": scheduled_for.isoformat(),
                "workflow_run_id": run.id,
            },
            workflow_run_id=run.id,
        )

    def _workflow_target(self, schedule: Schedule) -> tuple[WorkflowDefinition, dict[str, Any]]:
        """The enabled definition a workflow schedule starts, and the run input from its payload."""
        payload = schedule.payload or {}
        run_input = payload.get("input") or {}
        if not isinstance(run_input, dict):
            raise ValueError("Schedule payload 'input' must be an object.")
        if payload.get("workflow_definition_id"):
            reference = str(payload["workflow_definition_id"])
            definition = self.session.get(WorkflowDefinition, reference)
        else:
            workflow_name = payload.get("workflow_name")
            if not workflow_name:
                raise ValueError("Scheduled workflow payload requires workflow_definition_id or workflow_name.")
            version = str(payload.get("workflow_version", "1"))
            reference = f"{workflow_name}@{version}"
            definition = self.session.scalar(
                select(WorkflowDefinition).where(
                    WorkflowDefinition.name == str(workflow_name),
                    WorkflowDefinition.version == version,
                )
            )
        if definition is None:
            raise ValueError(f"Unknown workflow definition: {reference}")
        if not definition.enabled:
            raise ValueError(f"Workflow definition {definition.name}@{definition.version} is disabled.")
        return definition, run_input

    def _occurrence_exists(self, schedule: Schedule, scheduled_for: datetime) -> bool:
        return (
            self.session.scalar(
                select(ScheduleOccurrence).where(
                    ScheduleOccurrence.dedupe_key == _dedupe_key(schedule.id, scheduled_for)
                )
            )
            is not None
        )

    def _get(self, schedule_id: str) -> Schedule:
        schedule = self.session.get(Schedule, schedule_id)
        if schedule is None:
            raise KeyError(f"Unknown schedule: {schedule_id}")
        return schedule

    def _event(
        self,
        event_type: str,
        schedule: Schedule,
        *,
        summary: str,
        payload: dict[str, Any] | None = None,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
    ) -> None:
        record_event(
            self.session,
            event_type=event_type,
            entity_kind="schedule",
            entity_id=schedule.id,
            schedule_id=schedule.id,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source="scheduler",
            summary=summary,
            payload=payload,
        )


def _work_fields(schedule: Schedule) -> dict[str, Any]:
    """The fields of the work item a work schedule queues, its template read at fire time."""
    payload = schedule.payload or {}
    return {
        "title": str(payload.get("title", schedule.name)),
        "task_instruction": _task_instruction(schedule),
        "worker_kind": schedule.worker_kind,
        "runtime_contract": schedule.runtime_contract or {},
        "context": dict(payload.get("context") or {}),
        "retry_policy": dict(payload.get("retry_policy") or {}),
        "priority": int(payload.get("priority", 0)),
        "max_attempts": int(payload.get("max_attempts", 1)),
        "discord_thread_id": _optional_str(payload.get("discord_thread_id")),
        "visible": _payload_visible(payload),
        "lane": _optional_str(payload.get("lane")) or schedule.name,
    }


def _task_instruction(schedule: Schedule) -> str:
    payload = schedule.payload or {}
    template_path = payload.get("task_template_path")
    if template_path:
        base_dir = _optional_str(payload.get("template_base_dir"))
        return read_template_file(str(template_path), base_dir=Path(base_dir) if base_dir else None)
    return str(payload.get("task_instruction") or schedule.name)


def _apply_catchup(schedule: Schedule, times: list[datetime]) -> list[datetime]:
    if not times:
        return []
    if schedule.catchup_policy in {"skip", "coalesce"}:
        return [times[-1]]
    if schedule.catchup_policy == "all":
        return times[: schedule.max_backfill]
    raise ValueError(f"Unsupported catchup policy: {schedule.catchup_policy!r}")


def _interval_times(expression: str, *, anchor: datetime, start: datetime, end: datetime) -> Iterable[datetime]:
    """Fire times on the grid ``anchor + k * interval`` that fall in (start, end]."""
    interval = _interval(expression)
    elapsed = max(start - anchor, timedelta(0)) // interval
    current = anchor + (elapsed + 1) * interval
    while current <= end:
        yield current.astimezone(UTC)
        current += interval


def _interval(expression: str) -> timedelta:
    match = _INTERVAL_RE.match(expression)
    if match is None:
        raise ValueError("Interval expression must look like seconds=60, minutes=5, hours=1, or days=1.")
    unit, raw_value = match.groups()
    if int(raw_value) <= 0:
        raise ValueError("Interval value must be positive.")
    return timedelta(**{unit: int(raw_value)})


def _cron_times(expression: str, tz: ZoneInfo, *, start: datetime, end: datetime) -> Iterable[datetime]:
    iterator = croniter(expression, start.astimezone(tz))
    while True:
        next_local = iterator.get_next(datetime)
        if next_local.tzinfo is None:
            next_local = next_local.replace(tzinfo=tz)
        next_utc = next_local.astimezone(UTC)
        if next_utc > end:
            break
        yield next_utc


def _parse_date(expression: str, tz: ZoneInfo) -> datetime:
    value = datetime.fromisoformat(expression.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=tz)
    return value.astimezone(UTC)


def _dedupe_key(schedule_id: str, scheduled_for: datetime) -> str:
    return f"schedule:{schedule_id}:{scheduled_for.astimezone(UTC).isoformat()}"


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _validate_timing(schedule_type: str, expression: str, timezone_name: str) -> None:
    """Refuse a type, expression, or timezone the poller could not evaluate."""
    if schedule_type not in SCHEDULE_TYPES:
        raise ValueError("schedule_type must be one of: cron, interval, date.")
    tz = ZoneInfo(timezone_name)
    if schedule_type == "cron" and not croniter.is_valid(expression):
        raise ValueError(f"Invalid cron expression: {expression!r}.")
    if schedule_type == "interval":
        _interval(expression)
    if schedule_type == "date":
        _parse_date(expression, tz)


def _validate_catchup_policy(catchup_policy: str) -> None:
    if catchup_policy not in CATCHUP_POLICIES:
        raise ValueError("catchup_policy must be one of: skip, coalesce, all.")


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _payload_visible(payload: dict[str, Any]) -> bool:
    """``visible: false`` keeps a schedule's runs out of Discord; results still land in artifacts."""
    value = payload.get("visible", True)
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)
