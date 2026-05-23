from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from croniter import croniter
from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import Schedule, ScheduleOccurrence, WorkEvent, WorkflowDefinition, utc_now
from tasque2.repo import WorkRepository
from tasque2.templates import read_template_file
from tasque2.workflows import WorkflowService

_INTERVAL_RE = re.compile(r"^\s*(seconds|minutes|hours|days)\s*=\s*(\d+)\s*$")
WORKFLOW_SCHEDULE_TARGETS = {"workflow", "workflow.run"}


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
        self._validate_schedule_type(schedule_type)
        self._validate_catchup_policy(catchup_policy)
        timezone_name = timezone_name or get_settings().timezone
        self._get_timezone(timezone_name)
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
        self._emit_event(
            event_type="schedule.created",
            entity_id=schedule.id,
            schedule_id=schedule.id,
            summary=f"Created schedule: {name}",
            payload={
                "schedule_type": schedule_type,
                "expression": expression,
                "worker_kind": worker_kind,
                "catchup_policy": catchup_policy,
            },
        )
        return schedule

    def enable_schedule(
        self,
        schedule_id: str,
        *,
        now: datetime | None = None,
        resume_from_now: bool = True,
    ) -> Schedule:
        schedule = self._get_schedule(schedule_id)
        was_enabled = schedule.enabled
        schedule.enabled = True
        if not was_enabled and resume_from_now:
            schedule.last_evaluated_at = self._ensure_aware(now or utc_now()).astimezone(UTC)
        self.session.flush()
        if not was_enabled:
            self._emit_event(
                event_type="schedule.enabled",
                entity_id=schedule.id,
                schedule_id=schedule.id,
                summary=f"Enabled schedule: {schedule.name}",
                payload={"resume_from_now": resume_from_now},
            )
        return schedule

    def disable_schedule(self, schedule_id: str) -> Schedule:
        schedule = self._get_schedule(schedule_id)
        was_enabled = schedule.enabled
        schedule.enabled = False
        self.session.flush()
        if was_enabled:
            self._emit_event(
                event_type="schedule.disabled",
                entity_id=schedule.id,
                schedule_id=schedule.id,
                summary=f"Disabled schedule: {schedule.name}",
            )
        return schedule

    def delete_schedule(self, schedule_id: str) -> None:
        schedule = self._get_schedule(schedule_id)
        self._emit_event(
            event_type="schedule.deleted",
            entity_id=schedule.id,
            schedule_id=schedule.id,
            summary=f"Deleted schedule: {schedule.name}",
        )
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
        schedule = self._get_schedule(schedule_id)
        schedule_changed = False
        if name is not None:
            schedule.name = name
        if schedule_type is not None:
            self._validate_schedule_type(schedule_type)
            schedule.schedule_type = schedule_type
            schedule_changed = True
        if expression is not None:
            schedule.expression = expression
            schedule_changed = True
        if worker_kind is not None:
            schedule.worker_kind = worker_kind
        if payload is not None:
            schedule.payload = payload
        if timezone_name is not None:
            self._get_timezone(timezone_name)
            schedule.timezone = timezone_name
            schedule_changed = True
        if runtime_contract is not None:
            schedule.runtime_contract = runtime_contract
        if catchup_policy is not None:
            self._validate_catchup_policy(catchup_policy)
            schedule.catchup_policy = catchup_policy
        if misfire_grace_seconds is not None:
            schedule.misfire_grace_seconds = misfire_grace_seconds
        if max_backfill is not None:
            schedule.max_backfill = max_backfill
        if max_active_runs is not None:
            schedule.max_active_runs = max_active_runs
        if enabled is not None:
            schedule.enabled = enabled
        if schedule_changed:
            schedule.last_evaluated_at = None

        self.session.flush()
        self._emit_event(
            event_type="schedule.updated",
            entity_id=schedule.id,
            schedule_id=schedule.id,
            summary=f"Updated schedule: {schedule.name}",
            payload={
                "schedule_type": schedule.schedule_type,
                "expression": schedule.expression,
                "worker_kind": schedule.worker_kind,
            },
        )
        return schedule

    def fire_schedule_now(self, schedule_id: str, *, now: datetime | None = None) -> ScheduleOccurrence:
        schedule = self._get_schedule(schedule_id)
        now = self._ensure_aware(now or utc_now()).astimezone(UTC)
        occurrence = self.enqueue_occurrence(schedule, now)
        if occurrence is None:
            raise ValueError("Schedule occurrence already exists for this fire time.")
        self._emit_event(
            event_type="schedule.fired_now",
            entity_id=schedule.id,
            schedule_id=schedule.id,
            work_item_id=occurrence.work_item_id,
            workflow_run_id=occurrence.workflow_run_id,
            summary=f"Fired schedule now: {schedule.name}",
            payload={"schedule_occurrence_id": occurrence.id},
        )
        return occurrence

    def poll_due_schedules(self, *, now: datetime | None = None) -> int:
        now = self._ensure_aware(now or utc_now()).astimezone(UTC)
        schedules = self.session.scalars(
            select(Schedule).where(Schedule.enabled.is_(True)).order_by(Schedule.created_at)
        ).all()

        enqueued = 0
        for schedule in schedules:
            due_times = self.due_times(schedule, now=now)
            for scheduled_for in due_times:
                if self.enqueue_occurrence(schedule, scheduled_for) is not None:
                    enqueued += 1
            schedule.last_evaluated_at = now
        self.session.flush()
        return enqueued

    def due_times(self, schedule: Schedule, *, now: datetime | None = None) -> list[datetime]:
        now = self._ensure_aware(now or utc_now()).astimezone(UTC)
        timezone_info = self._get_timezone(schedule.timezone)
        start = schedule.last_evaluated_at or schedule.created_at
        start = self._ensure_aware(start).astimezone(UTC)

        if schedule.schedule_type == "date":
            scheduled_for = self._parse_date(schedule.expression, timezone_info)
            if scheduled_for <= now and not self._occurrence_exists(schedule, scheduled_for):
                return [scheduled_for]
            return []

        if schedule.schedule_type == "interval":
            times = list(self._iter_interval_times(schedule.expression, start=start, end=now))
        elif schedule.schedule_type == "cron":
            times = list(self._iter_cron_times(schedule.expression, timezone_info, start=start, end=now))
        else:
            raise ValueError(f"Unsupported schedule type: {schedule.schedule_type!r}")

        times = [time for time in times if self._within_misfire_grace(schedule, time, now)]
        return self._apply_catchup_policy(schedule, times)

    def enqueue_occurrence(
        self,
        schedule: Schedule,
        scheduled_for: datetime,
    ) -> ScheduleOccurrence | None:
        scheduled_for = self._ensure_aware(scheduled_for).astimezone(UTC)
        dedupe_key = self._dedupe_key(schedule.id, scheduled_for)
        existing = self.session.scalar(
            select(ScheduleOccurrence).where(ScheduleOccurrence.dedupe_key == dedupe_key)
        )
        if existing is not None:
            return None

        occurrence = ScheduleOccurrence(
            schedule_id=schedule.id,
            scheduled_for=scheduled_for,
            status="pending",
            dedupe_key=dedupe_key,
        )
        self.session.add(occurrence)
        self.session.flush()

        if schedule.worker_kind in WORKFLOW_SCHEDULE_TARGETS:
            self._start_workflow_occurrence(schedule, occurrence, scheduled_for)
        else:
            self._enqueue_work_occurrence(schedule, occurrence, scheduled_for, dedupe_key)
        return occurrence

    def _enqueue_work_occurrence(
        self,
        schedule: Schedule,
        occurrence: ScheduleOccurrence,
        scheduled_for: datetime,
        dedupe_key: str,
    ) -> None:
        payload = schedule.payload or {}
        repo = WorkRepository(self.session)
        work = repo.create_work_item(
            title=str(payload.get("title", schedule.name)),
            task_instruction=self._scheduled_task_instruction(schedule),
            worker_kind=schedule.worker_kind,
            runtime_contract=schedule.runtime_contract or {},
            context=dict(payload.get("context") or {}),
            retry_policy=dict(payload.get("retry_policy") or {}),
            priority=int(payload.get("priority", 0)),
            max_attempts=int(payload.get("max_attempts", 1)),
            idempotency_key=dedupe_key,
            source_kind="schedule",
            source_id=schedule.id,
            schedule_id=schedule.id,
            schedule_occurrence_id=occurrence.id,
        )
        occurrence.work_item_id = work.id
        occurrence.status = "enqueued"
        self.session.flush()
        self._emit_event(
            event_type="schedule.occurrence_enqueued",
            entity_id=schedule.id,
            schedule_id=schedule.id,
            work_item_id=work.id,
            summary=f"Enqueued scheduled work: {schedule.name}",
            payload={
                "schedule_occurrence_id": occurrence.id,
                "scheduled_for": scheduled_for.isoformat(),
                "work_item_id": work.id,
            },
        )

    def _start_workflow_occurrence(
        self,
        schedule: Schedule,
        occurrence: ScheduleOccurrence,
        scheduled_for: datetime,
    ) -> None:
        payload = schedule.payload or {}
        run_input = self._payload_object(payload, "input")
        run_input.update(self._schedule_input(schedule, occurrence, scheduled_for))
        run = WorkflowService(self.session).start_run(
            workflow_definition_id=self._workflow_definition_id(payload),
            name=str(payload.get("run_name") or schedule.name),
            input=run_input,
            discord_thread_id=_optional_str(payload.get("discord_thread_id")),
        )
        occurrence.workflow_run_id = run.id
        occurrence.status = "enqueued"
        self.session.flush()
        self._emit_event(
            event_type="schedule.workflow_started",
            entity_id=schedule.id,
            schedule_id=schedule.id,
            workflow_run_id=run.id,
            summary=f"Started scheduled workflow: {schedule.name}",
            payload={
                "schedule_occurrence_id": occurrence.id,
                "scheduled_for": scheduled_for.isoformat(),
                "workflow_run_id": run.id,
            },
        )

    def _apply_catchup_policy(self, schedule: Schedule, times: list[datetime]) -> list[datetime]:
        if not times:
            return []
        if schedule.catchup_policy == "skip":
            return [times[-1]] if times[-1] == max(times) else []
        if schedule.catchup_policy == "coalesce":
            return [times[-1]]
        if schedule.catchup_policy == "all":
            return times[: schedule.max_backfill]
        raise ValueError(f"Unsupported catchup policy: {schedule.catchup_policy!r}")

    def _iter_interval_times(
        self,
        expression: str,
        *,
        start: datetime,
        end: datetime,
    ) -> Iterable[datetime]:
        interval = self._parse_interval(expression)
        current = start + interval
        while current <= end:
            yield current.astimezone(UTC)
            current = current + interval

    def _iter_cron_times(
        self,
        expression: str,
        timezone_info: ZoneInfo,
        *,
        start: datetime,
        end: datetime,
    ) -> Iterable[datetime]:
        start_local = start.astimezone(timezone_info)
        end_utc = end.astimezone(UTC)
        iterator = croniter(expression, start_local)
        while True:
            next_local = iterator.get_next(datetime)
            if next_local.tzinfo is None:
                next_local = next_local.replace(tzinfo=timezone_info)
            next_utc = next_local.astimezone(UTC)
            if next_utc > end_utc:
                break
            yield next_utc

    def _parse_date(self, expression: str, timezone_info: ZoneInfo) -> datetime:
        value = datetime.fromisoformat(expression.replace("Z", "+00:00"))
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone_info)
        return value.astimezone(UTC)

    def _parse_interval(self, expression: str) -> timedelta:
        match = _INTERVAL_RE.match(expression)
        if match is None:
            raise ValueError("Interval expression must look like seconds=60, minutes=5, hours=1, or days=1.")
        unit, raw_value = match.groups()
        value = int(raw_value)
        if value <= 0:
            raise ValueError("Interval value must be positive.")
        return timedelta(**{unit: value})

    def _within_misfire_grace(self, schedule: Schedule, scheduled_for: datetime, now: datetime) -> bool:
        if schedule.misfire_grace_seconds is None:
            return True
        grace = timedelta(seconds=schedule.misfire_grace_seconds)
        return now - scheduled_for <= grace

    def _occurrence_exists(self, schedule: Schedule, scheduled_for: datetime) -> bool:
        return (
            self.session.scalar(
                select(ScheduleOccurrence).where(
                    ScheduleOccurrence.dedupe_key == self._dedupe_key(schedule.id, scheduled_for)
                )
            )
            is not None
        )

    def _dedupe_key(self, schedule_id: str, scheduled_for: datetime) -> str:
        return f"schedule:{schedule_id}:{scheduled_for.astimezone(UTC).isoformat()}"

    def _get_schedule(self, schedule_id: str) -> Schedule:
        schedule = self.session.get(Schedule, schedule_id)
        if schedule is None:
            raise KeyError(f"Unknown schedule: {schedule_id}")
        return schedule

    def _workflow_definition_id(self, payload: dict[str, Any]) -> str:
        workflow_definition_id = payload.get("workflow_definition_id")
        if workflow_definition_id:
            return str(workflow_definition_id)

        workflow_name = payload.get("workflow_name")
        if workflow_name:
            version = str(payload.get("workflow_version", "1"))
            definition = self.session.scalar(
                select(WorkflowDefinition).where(
                    WorkflowDefinition.name == str(workflow_name),
                    WorkflowDefinition.version == version,
                )
            )
            if definition is not None:
                return definition.id
            raise ValueError(f"Unknown workflow definition: {workflow_name}@{version}")

        raise ValueError("Scheduled workflow payload requires workflow_definition_id or workflow_name.")

    def _payload_object(self, payload: dict[str, Any], key: str) -> dict[str, Any]:
        value = payload.get(key) or {}
        if not isinstance(value, dict):
            raise ValueError(f"Schedule payload {key!r} must be an object.")
        return dict(value)

    def _required_string(self, payload: dict[str, Any], key: str) -> str:
        value = payload.get(key)
        if value is None or str(value).strip() == "":
            raise ValueError(f"Schedule payload requires {key!r}.")
        return str(value)

    def _scheduled_task_instruction(self, schedule: Schedule) -> str:
        payload = schedule.payload or {}
        template_path = payload.get("task_template_path") or payload.get("instruction_template_path")
        if template_path:
            base_dir = _optional_str(payload.get("template_base_dir"))
            return read_template_file(
                str(template_path),
                base_dir=Path(base_dir) if base_dir else None,
            )
        return str(payload.get("task_instruction") or payload.get("instruction") or schedule.name)

    def _schedule_input(
        self,
        schedule: Schedule,
        occurrence: ScheduleOccurrence,
        scheduled_for: datetime,
    ) -> dict[str, Any]:
        return {
            "schedule_id": schedule.id,
            "schedule_occurrence_id": occurrence.id,
            "scheduled_for": scheduled_for.isoformat(),
        }

    def _validate_schedule_type(self, schedule_type: str) -> None:
        if schedule_type not in {"cron", "interval", "date"}:
            raise ValueError("schedule_type must be one of: cron, interval, date.")

    def _validate_catchup_policy(self, catchup_policy: str) -> None:
        if catchup_policy not in {"skip", "coalesce", "all"}:
            raise ValueError("catchup_policy must be one of: skip, coalesce, all.")

    def _get_timezone(self, timezone_name: str) -> ZoneInfo:
        return ZoneInfo(timezone_name)

    def _ensure_aware(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    def _emit_event(
        self,
        *,
        event_type: str,
        entity_id: str,
        schedule_id: str,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WorkEvent:
        event = WorkEvent(
            event_type=event_type,
            entity_kind="schedule",
            entity_id=entity_id,
            schedule_id=schedule_id,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source="scheduler",
            summary=summary,
            payload=payload or {},
        )
        self.session.add(event)
        self.session.flush()
        return event


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
