from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer
from sqlalchemy import select

from tasque2.cli._common import (
    PlainTable,
    app,
    cli_session_scope,
    cli_span,
    console,
    echo,
    emit_json,
    existing_file,
    fail,
    model_profile,
    parse_json_object,
    runtime_contract,
)
from tasque2.models import Schedule, WorkflowDefinition
from tasque2.schedules import ScheduleService

TYPE_HELP = 'cron ("0 9 * * FRI"), interval ("hours=6"), or date (ISO datetime, fires once).'


@app.command("schedule-create")
def schedule_create(
    name: Annotated[str, typer.Argument(help="Schedule name; also the title of the work it queues.")],
    schedule_type: Annotated[str, typer.Option("--type", help=TYPE_HELP)],
    expression: Annotated[str, typer.Option("--expr")],
    task: Annotated[str | None, typer.Option("--task", help="Instruction for each run.")] = None,
    template: Annotated[
        Path | None, typer.Option("--template", "-t", help="Markdown instruction file, read at each fire.")
    ] = None,
    lane: Annotated[str | None, typer.Option("--lane")] = None,
    profile: Annotated[str | None, typer.Option("--profile", help="Model profile: low, medium, high, ultra.")] = None,
    context_json: Annotated[str | None, typer.Option("--context-json")] = None,
    thread: Annotated[str | None, typer.Option("--thread", help="Discord thread id for every result.")] = None,
    worker_kind: Annotated[str, typer.Option("--worker-kind", "-w")] = "provider.default",
    timezone_name: Annotated[str | None, typer.Option("--timezone")] = None,
    catchup_policy: Annotated[
        str, typer.Option("--catchup-policy", help="After downtime: coalesce or skip (fire once), or all (replay).")
    ] = "coalesce",
    max_backfill: Annotated[int, typer.Option("--max-backfill")] = 10,
    disabled: Annotated[bool, typer.Option("--disabled", help="Create it switched off.")] = False,
) -> None:
    """Create a schedule that queues a work item each time it fires."""
    if bool(task) == bool(template):
        raise typer.BadParameter("Provide exactly one of --task or --template.")
    payload: dict[str, Any] = {"title": name}
    if template is not None:
        payload["task_template_path"] = str(existing_file(template, option="--template"))
    else:
        payload["task_instruction"] = task
    if lane:
        payload["lane"] = lane
    if thread:
        payload["discord_thread_id"] = thread
    context = parse_json_object(context_json, option="--context-json")
    if context:
        payload["context"] = context
    with cli_session_scope() as session:
        schedule = ScheduleService(session).create_schedule(
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            worker_kind=worker_kind,
            payload=payload,
            runtime_contract=runtime_contract(profile=profile),
            timezone_name=timezone_name,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
            enabled=not disabled,
        )
        console.print(schedule.id)


@app.command("schedule-workflow-create")
def schedule_workflow_create(
    name: Annotated[str, typer.Argument(help="Schedule name.")],
    schedule_type: Annotated[str, typer.Option("--type", help=TYPE_HELP)],
    expression: Annotated[str, typer.Option("--expr")],
    workflow: Annotated[str, typer.Option("--workflow", help="Workflow definition name or id.")],
    run_name: Annotated[str | None, typer.Option("--run-name")] = None,
    input_json: Annotated[str | None, typer.Option("--input-json")] = None,
    timezone_name: Annotated[str | None, typer.Option("--timezone")] = None,
    catchup_policy: Annotated[str, typer.Option("--catchup-policy")] = "coalesce",
    max_backfill: Annotated[int, typer.Option("--max-backfill")] = 10,
    disabled: Annotated[bool, typer.Option("--disabled")] = False,
) -> None:
    """Create a schedule that starts a workflow run each time it fires."""
    with cli_session_scope() as session:
        definition = session.get(WorkflowDefinition, workflow) or session.scalar(
            select(WorkflowDefinition)
            .where(WorkflowDefinition.name == workflow)
            .order_by(WorkflowDefinition.created_at.desc())
        )
        if definition is None:
            raise fail(f"Unknown workflow: {workflow}")
        payload: dict[str, Any] = {"workflow_definition_id": definition.id}
        if run_name:
            payload["run_name"] = run_name
        run_input = parse_json_object(input_json, option="--input-json")
        if run_input:
            payload["input"] = run_input
        schedule = ScheduleService(session).create_schedule(
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            worker_kind="workflow",
            payload=payload,
            timezone_name=timezone_name,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
            enabled=not disabled,
        )
        console.print(schedule.id)


@app.command("schedule-list")
def schedule_list(
    enabled_only: Annotated[bool, typer.Option("--enabled", help="Only enabled schedules.")] = False,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
) -> None:
    """List schedules with their next fire time."""
    with cli_session_scope() as session:
        statement = select(Schedule).order_by(Schedule.name).limit(limit)
        if enabled_only:
            statement = statement.where(Schedule.enabled.is_(True))
        service = ScheduleService(session)
        table = PlainTable("Id", "On", "Expression", "Profile", "Next", "Name")
        for schedule in session.scalars(statement).all():
            upcoming = service.next_fire_time(schedule) if schedule.enabled else None
            table.add_row(
                schedule.id[:8],
                "yes" if schedule.enabled else "no",
                f"{schedule.schedule_type} {schedule.expression}",
                str((schedule.runtime_contract or {}).get("model_profile") or ""),
                upcoming.strftime("%m-%d %H:%M %Z") if upcoming else "",
                schedule.name,
            )
        console.print(table)


@app.command("schedule-show")
def schedule_show(
    schedule_id: Annotated[str, typer.Argument()],
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Show one schedule in full."""
    with cli_session_scope() as session:
        schedule = _resolve(session, schedule_id)
        data = {
            "id": schedule.id,
            "name": schedule.name,
            "enabled": schedule.enabled,
            "schedule_type": schedule.schedule_type,
            "expression": schedule.expression,
            "timezone": schedule.timezone,
            "worker_kind": schedule.worker_kind,
            "catchup_policy": schedule.catchup_policy,
            "runtime_contract": schedule.runtime_contract or {},
            "payload": schedule.payload or {},
            "last_evaluated_at": schedule.last_evaluated_at,
        }
        if as_json:
            emit_json(data)
            return
        table = PlainTable("Field", "Value")
        for key, value in data.items():
            table.add_row(key, str(value))
        console.print(table)


@app.command("schedule-edit")
def schedule_edit(
    schedule_id: Annotated[str, typer.Argument()],
    name: Annotated[
        str | None, typer.Option("--name", help="Rename; the work it queues takes the new name as its title.")
    ] = None,
    schedule_type: Annotated[str | None, typer.Option("--type")] = None,
    expression: Annotated[str | None, typer.Option("--expr")] = None,
    task: Annotated[str | None, typer.Option("--task")] = None,
    template: Annotated[Path | None, typer.Option("--template", "-t")] = None,
    profile: Annotated[str | None, typer.Option("--profile")] = None,
    contract_json: Annotated[str | None, typer.Option("--contract-json", help="Replace the runtime contract.")] = None,
    payload_json: Annotated[str | None, typer.Option("--payload-json", help="Replace the whole payload.")] = None,
    timezone_name: Annotated[str | None, typer.Option("--timezone")] = None,
    catchup_policy: Annotated[str | None, typer.Option("--catchup-policy")] = None,
    max_backfill: Annotated[int | None, typer.Option("--max-backfill")] = None,
) -> None:
    """Change a schedule's timing, instruction, payload, or runtime contract."""
    if task and template:
        raise typer.BadParameter("Provide only one of --task or --template.")
    if payload_json and (task or template):
        raise typer.BadParameter("--payload-json replaces the payload; do not combine it with --task or --template.")
    with cli_session_scope() as session:
        existing = _resolve(session, schedule_id)
        payload = None
        if payload_json:
            payload = parse_json_object(payload_json, option="--payload-json")
        elif task or template:
            payload = dict(existing.payload or {})
            payload.pop("task_instruction", None)
            payload.pop("task_template_path", None)
            if template:
                payload["task_template_path"] = str(existing_file(template, option="--template"))
            else:
                payload["task_instruction"] = task
        if name:
            payload = {**(dict(existing.payload or {}) if payload is None else payload), "title": name}
        contract = None
        if contract_json:
            contract = parse_json_object(contract_json, option="--contract-json")
        elif profile:
            contract = {**(existing.runtime_contract or {}), "model_profile": model_profile(profile)}
        schedule = ScheduleService(session).update_schedule(
            existing.id,
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            payload=payload,
            runtime_contract=contract,
            timezone_name=timezone_name,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
        )
        console.print(f"{schedule.id}: updated")


@app.command("schedule-enable")
def schedule_enable(
    schedule_id: Annotated[str, typer.Argument()],
    resume_from_now: Annotated[
        bool, typer.Option("--resume-from-now/--catch-up", help="Skip or replay fires missed while it was off.")
    ] = True,
) -> None:
    """Switch a schedule on."""
    with cli_session_scope() as session:
        schedule = ScheduleService(session).enable_schedule(
            _resolve(session, schedule_id).id, resume_from_now=resume_from_now
        )
        echo(f"{schedule.name}: enabled")


@app.command("schedule-disable")
def schedule_disable(schedule_id: Annotated[str, typer.Argument()]) -> None:
    """Switch a schedule off without deleting it."""
    with cli_session_scope() as session:
        schedule = ScheduleService(session).disable_schedule(_resolve(session, schedule_id).id)
        echo(f"{schedule.name}: disabled")


@app.command("schedule-delete")
def schedule_delete(
    schedule_id: Annotated[str, typer.Argument()],
    yes: Annotated[bool, typer.Option("--yes", help="Confirm the deletion.")] = False,
) -> None:
    """Delete a schedule permanently."""
    with cli_session_scope() as session:
        schedule = _resolve(session, schedule_id)
        if not yes:
            raise fail(f"This deletes schedule {schedule.name!r}; pass --yes to confirm.")
        ScheduleService(session).delete_schedule(schedule.id)
        echo(f"{schedule.name}: deleted")


@app.command("schedule-fire-now")
def schedule_fire_now(schedule_id: Annotated[str, typer.Argument()]) -> None:
    """Fire a schedule once now, in addition to its cadence."""
    with cli_span("schedule-fire-now"), cli_session_scope() as session:
        occurrence = ScheduleService(session).fire_schedule_now(_resolve(session, schedule_id).id)
        console.print(f"occurrence: {occurrence.id}")
        if occurrence.work_item_id:
            console.print(f"work item: {occurrence.work_item_id}")
        if occurrence.workflow_run_id:
            console.print(f"workflow run: {occurrence.workflow_run_id}")


def _resolve(session, value: str) -> Schedule:
    """A schedule by id, id prefix, or exact name."""
    schedule = session.get(Schedule, value)
    if schedule is not None:
        return schedule
    matches = session.scalars(select(Schedule).where(Schedule.id.startswith(value))).all() if len(value) >= 6 else []
    if not matches:
        matches = session.scalars(select(Schedule).where(Schedule.name == value)).all()
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise fail(f"Unknown schedule: {value}")
    raise fail(f"{value!r} matches {len(matches)} schedules; use the full id.")
