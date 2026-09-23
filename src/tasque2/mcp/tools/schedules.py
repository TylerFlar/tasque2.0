from __future__ import annotations

from typing import Any

from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.mcp.tools._shared import (
    calling_work_item,
    clamp,
    inherit_reply_config,
    optional_string,
    required,
    run_json,
    schedule_data,
)
from tasque2.models import Schedule
from tasque2.schedules import ScheduleService


def schedule_create_work(
    name: str,
    schedule_type: str,
    expression: str,
    task_instruction: str | None = None,
    task_template_path: str | None = None,
    template_base_dir: str | None = None,
    worker_kind: str = "provider.default",
    runtime_contract: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    timezone_name: str | None = None,
    catchup_policy: str = "coalesce",
    enabled: bool = True,
    discord_thread_id: str | None = None,
    lane: str | None = None,
) -> str:
    """Create a recurring or one-time schedule that queues a work item each time it fires.

    ``schedule_type`` is ``cron`` (for example "0 9 * * FRI"), ``interval`` ("hours=6"),
    or ``date`` (an ISO datetime, fires once). ``discord_thread_id`` posts each result into
    that thread.
    """
    return run_json(
        lambda: _create(
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            task_instruction=task_instruction,
            task_template_path=task_template_path,
            template_base_dir=template_base_dir,
            worker_kind=worker_kind,
            runtime_contract=runtime_contract,
            context=context,
            timezone_name=timezone_name,
            catchup_policy=catchup_policy,
            enabled=enabled,
            discord_thread_id=discord_thread_id,
            lane=lane,
        )
    )


def schedule_list(enabled: bool | None = None, limit: int = 20, intent: str = "") -> str:
    """List schedules, newest first."""
    return run_json(lambda: _list(enabled, limit), intent=intent)


def schedule_get(schedule_id: str, intent: str = "") -> str:
    """Fetch one schedule with its payload and contract."""
    return run_json(lambda: _get(schedule_id), intent=intent)


def schedule_update(
    schedule_id: str,
    name: str | None = None,
    schedule_type: str | None = None,
    expression: str | None = None,
    timezone_name: str | None = None,
    context: dict[str, Any] | None = None,
    catchup_policy: str | None = None,
    enabled: bool | None = None,
    discord_thread_id: str | None = None,
) -> str:
    """Change a schedule's cadence, context, or thread; only the fields you pass change."""
    return run_json(
        lambda: _update(
            schedule_id=schedule_id,
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            timezone_name=timezone_name,
            context=context,
            catchup_policy=catchup_policy,
            enabled=enabled,
            discord_thread_id=discord_thread_id,
        )
    )


def schedule_set_enabled(schedule_id: str, enabled: bool) -> str:
    """Pause or resume a schedule without deleting it."""
    return run_json(lambda: _set_enabled(schedule_id, enabled))


def schedule_delete(schedule_id: str) -> str:
    """Delete a schedule permanently."""
    return run_json(lambda: _delete(schedule_id))


def schedule_fire_now(schedule_id: str) -> str:
    """Run a schedule once now, as its own run, in addition to its cadence."""
    return run_json(lambda: _fire(schedule_id))


def _create(**fields: Any) -> dict[str, Any]:
    instruction = optional_string(fields["task_instruction"])
    template = optional_string(fields["task_template_path"])
    if bool(instruction) == bool(template):
        raise ValueError("Provide exactly one of task_instruction or task_template_path.")
    if fields["context"] is not None and not isinstance(fields["context"], dict):
        raise ValueError("context must be an object or omitted.")
    payload: dict[str, Any] = {"title": required(fields["name"], "name")}
    if template:
        payload["task_template_path"] = template
        if optional_string(fields["template_base_dir"]):
            payload["template_base_dir"] = optional_string(fields["template_base_dir"])
    else:
        payload["task_instruction"] = instruction
    if optional_string(fields["discord_thread_id"]):
        payload["discord_thread_id"] = optional_string(fields["discord_thread_id"])
    if optional_string(fields["lane"]):
        payload["lane"] = optional_string(fields["lane"])
    with session_scope() as session:
        payload["context"] = inherit_reply_config(dict(fields["context"] or {}), calling_work_item(session))
        schedule = ScheduleService(session).create_schedule(
            name=payload["title"],
            schedule_type=required(fields["schedule_type"], "schedule_type"),
            expression=required(fields["expression"], "expression"),
            worker_kind=required(fields["worker_kind"], "worker_kind"),
            payload=payload,
            timezone_name=optional_string(fields["timezone_name"]),
            runtime_contract=dict(fields["runtime_contract"] or {}),
            catchup_policy=required(fields["catchup_policy"], "catchup_policy"),
            enabled=bool(fields["enabled"]),
        )
        return {"ok": True, "schedule": schedule_data(schedule, full=True)}


def _list(enabled: bool | None, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(Schedule).order_by(Schedule.created_at.desc()).limit(clamp(limit))
        if enabled is not None:
            statement = statement.where(Schedule.enabled.is_(bool(enabled)))
        return {"ok": True, "items": [schedule_data(schedule) for schedule in session.scalars(statement).all()]}


def _get(schedule_id: str) -> dict[str, Any]:
    with session_scope() as session:
        schedule = session.get(Schedule, required(schedule_id, "schedule_id"))
        if schedule is None:
            raise KeyError(f"Unknown schedule: {schedule_id}")
        return {"ok": True, "schedule": schedule_data(schedule, full=True)}


def _update(**fields: Any) -> dict[str, Any]:
    schedule_id = required(fields["schedule_id"], "schedule_id")
    if fields["context"] is not None and not isinstance(fields["context"], dict):
        raise ValueError("context must be an object or omitted.")
    name = optional_string(fields["name"])
    with session_scope() as session:
        existing = session.get(Schedule, schedule_id)
        if existing is None:
            raise KeyError(f"Unknown schedule: {schedule_id}")
        payload = dict(existing.payload or {})
        if fields["context"] is not None:
            payload["context"] = dict(fields["context"])
        if fields["discord_thread_id"] is not None:
            thread_id = optional_string(fields["discord_thread_id"])
            if thread_id:
                payload["discord_thread_id"] = thread_id
            else:
                payload.pop("discord_thread_id", None)
        if name and payload.get("title") == existing.name:
            payload["title"] = name
        service = ScheduleService(session)
        schedule = service.update_schedule(
            schedule_id,
            name=name,
            schedule_type=optional_string(fields["schedule_type"]),
            expression=optional_string(fields["expression"]),
            payload=payload if payload != (existing.payload or {}) else None,
            timezone_name=optional_string(fields["timezone_name"]),
            catchup_policy=optional_string(fields["catchup_policy"]),
        )
        if fields["enabled"] is not None:
            schedule = (
                service.enable_schedule(schedule_id) if fields["enabled"] else service.disable_schedule(schedule_id)
            )
        return {"ok": True, "schedule": schedule_data(schedule, full=True)}


def _set_enabled(schedule_id: str, enabled: bool) -> dict[str, Any]:
    with session_scope() as session:
        service = ScheduleService(session)
        schedule_id = required(schedule_id, "schedule_id")
        schedule = service.enable_schedule(schedule_id) if enabled else service.disable_schedule(schedule_id)
        return {"ok": True, "schedule": schedule_data(schedule)}


def _delete(schedule_id: str) -> dict[str, Any]:
    with session_scope() as session:
        ScheduleService(session).delete_schedule(required(schedule_id, "schedule_id"))
        return {"ok": True, "deleted_schedule_id": schedule_id}


def _fire(schedule_id: str) -> dict[str, Any]:
    with session_scope() as session:
        occurrence = ScheduleService(session).fire_schedule_now(required(schedule_id, "schedule_id"))
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "occurrence_id": occurrence.id,
            "work_item_id": occurrence.work_item_id,
            "workflow_run_id": occurrence.workflow_run_id,
        }
