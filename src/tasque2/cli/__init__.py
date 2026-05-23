from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table
from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactService, ArtifactStore
from tasque2.config import get_settings
from tasque2.daemon import TasqueDaemon
from tasque2.db import session_scope
from tasque2.discord_adapter import DiscordService
from tasque2.discord_output import DiscordOutputService, FakeDiscordOutputGateway
from tasque2.health import health_report_to_dict, run_doctor
from tasque2.memory import MemoryService
from tasque2.memory_ingest import MemoryIngestService
from tasque2.migrations import schema_status, upgrade_database
from tasque2.models import (
    ProviderRun,
    Schedule,
    WorkAttempt,
    WorkDependency,
    WorkEvent,
    WorkflowDefinition,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
)
from tasque2.ops import BackupService, JobResetService
from tasque2.queue import WorkQueue
from tasque2.repo import WorkRepository
from tasque2.reports import ReportService, report_to_json
from tasque2.runbooks import run_local_smoke
from tasque2.runtime import WorkRunner
from tasque2.scheduler import ScheduleService
from tasque2.status import get_system_status
from tasque2.workflows import WorkflowService

app = typer.Typer(no_args_is_help=True)
console = Console()

PROVIDER_SMOKE_SCHEMA = {
    "type": "object",
    "properties": {
        "ok": {"type": "boolean"},
        "provider": {"type": "string"},
        "message": {"type": "string"},
    },
    "required": ["ok", "provider", "message"],
    "additionalProperties": True,
}


@contextmanager
def cli_session_scope() -> Iterator[Session]:
    upgrade_database()
    with session_scope() as session:
        yield session


def ensure_cli_database_ready() -> None:
    upgrade_database()


@app.command("init-db")
def init_db() -> None:
    """Create or upgrade the local SQLite schema."""
    status = upgrade_database()
    console.print(f"[green]Database schema is ready.[/green] {status.database_path}")
    console.print(f"revision: {status.current_display}")


@app.command("db-status")
def db_status() -> None:
    """Show Alembic migration status for the local database."""
    status = schema_status()
    table = Table("Field", "Value")
    table.add_row("database", str(status.database_path))
    table.add_row("current", status.current_display)
    table.add_row("head", status.head_display)
    table.add_row("is_current", str(status.is_current))
    console.print(table)


@app.command("doctor")
def doctor(
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON.")] = False,
    migrate: Annotated[
        bool,
        typer.Option("--migrate/--no-migrate", help="Run startup migrations before checking."),
    ] = True,
    strict: Annotated[bool, typer.Option("--strict", help="Exit non-zero when failures are found.")] = False,
) -> None:
    """Check local Tasque readiness."""
    report = run_doctor(migrate=migrate)
    if as_json:
        console.file.write(json.dumps(health_report_to_dict(report), indent=2, sort_keys=True) + "\n")
    else:
        table = Table("Check", "Status", "Summary")
        for check in report.checks:
            table.add_row(check.name, check.status, check.summary)
        console.print(table)
        console.print(f"overall: {report.overall_status}")

    if strict and report.has_failures:
        raise typer.Exit(code=1)


@app.command("runbook-smoke")
def runbook_smoke(
    title: Annotated[
        str,
        typer.Option("--title", help="Title for the smoke run."),
    ] = "Local runbook smoke",
    max_ticks: Annotated[int, typer.Option("--max-ticks", help="Maximum daemon ticks to run.")] = 10,
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON.")] = False,
) -> None:
    """Run a deterministic local end-to-end smoke run."""
    with cli_session_scope() as session:
        result = run_local_smoke(session, title=title, max_ticks=max_ticks)
        if as_json:
            console.file.write(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n")
            return

        table = Table("Field", "Value")
        for key, value in result.to_dict().items():
            if key != "tick_results":
                table.add_row(key, str(value))
        console.print(table)


@app.command("queue")
def queue_work(
    title: Annotated[str, typer.Argument(help="Short work title.")],
    task_instruction: Annotated[str, typer.Argument(help="Instruction for the worker.")],
    worker_kind: Annotated[
        str,
        typer.Option("--worker-kind", "-w", help="Registered worker runtime."),
    ] = "manual",
    priority: Annotated[int, typer.Option("--priority", "-p")] = 0,
    max_attempts: Annotated[int, typer.Option("--max-attempts")] = 1,
    idempotency_key: Annotated[str | None, typer.Option("--idempotency-key")] = None,
) -> None:
    """Queue a one-shot WorkItem."""
    with cli_session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title=title,
            task_instruction=task_instruction,
            worker_kind=worker_kind,
            priority=priority,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            source_kind="cli",
            source_id=idempotency_key,
        )
        console.print(work.id)


@app.command("list")
def list_work(limit: Annotated[int, typer.Option("--limit", "-n")] = 20) -> None:
    """List recent WorkItems."""
    with cli_session_scope() as session:
        rows = session.scalars(
            select(WorkItem).order_by(WorkItem.created_at.desc()).limit(limit)
        ).all()

        table = Table("Id", "Status", "Worker", "Priority", "Title")
        for work in rows:
            table.add_row(work.id, work.status, work.worker_kind, str(work.priority), work.title)
        console.print(table)


@app.command("show")
def show_work(work_item_id: Annotated[str, typer.Argument(help="WorkItem id.")]) -> None:
    """Show a WorkItem and its event timeline."""
    with cli_session_scope() as session:
        repo = WorkRepository(session)
        work = repo.get_work_item(work_item_id)
        if work is None:
            raise typer.Exit(code=1)

        console.print(f"[bold]{work.title}[/bold]")
        console.print(f"id: {work.id}")
        console.print(f"status: {work.status}")
        console.print(f"worker: {work.worker_kind}")
        console.print(f"priority: {work.priority}")
        console.print()

        table = Table("Event", "Source", "Summary")
        for event in repo.list_events_for_work(work.id):
            table.add_row(event.event_type, event.source, event.summary or "")
        console.print(table)


@app.command("events")
def events(
    work_item_id: Annotated[str | None, typer.Option("--work-item-id")] = None,
    workflow_run_id: Annotated[str | None, typer.Option("--workflow-run-id")] = None,
    entity_kind: Annotated[str | None, typer.Option("--entity-kind")] = None,
    entity_id: Annotated[str | None, typer.Option("--entity-id")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
) -> None:
    """List recent WorkEvents for debugging."""
    with cli_session_scope() as session:
        statement = select(WorkEvent).order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc()).limit(limit)
        if work_item_id:
            statement = statement.where(WorkEvent.work_item_id == work_item_id)
        if workflow_run_id:
            statement = statement.where(WorkEvent.workflow_run_id == workflow_run_id)
        if entity_kind:
            statement = statement.where(WorkEvent.entity_kind == entity_kind)
        if entity_id:
            statement = statement.where(WorkEvent.entity_id == entity_id)
        rows = session.scalars(statement).all()
        table = Table("When", "Type", "Entity", "Source", "Summary")
        for event in rows:
            table.add_row(
                event.created_at.isoformat(),
                event.event_type,
                f"{event.entity_kind}:{event.entity_id}",
                event.source,
                event.summary or "",
            )
        console.print(table)


@app.command("diagnose-work")
def diagnose_work(work_item_id: Annotated[str, typer.Argument(help="WorkItem id.")]) -> None:
    """Explain why a WorkItem is or is not runnable."""
    with cli_session_scope() as session:
        work = session.get(WorkItem, work_item_id)
        if work is None:
            raise typer.Exit(code=1)
        lines = [
            f"id: {work.id}",
            f"title: {work.title}",
            f"status: {work.status}",
            f"worker: {work.worker_kind}",
            f"attempts: {work.attempt_count}/{work.max_attempts}",
        ]
        if work.not_before:
            lines.append(f"not_before: {work.not_before.isoformat()}")
        dependencies = session.scalars(
            select(WorkDependency).where(WorkDependency.blocked_work_item_id == work.id)
        ).all()
        if dependencies:
            lines.append("dependencies:")
            for dependency in dependencies:
                upstream = (
                    session.get(WorkItem, dependency.dependency_work_item_id)
                    if dependency.dependency_work_item_id
                    else None
                )
                upstream_status = upstream.status if upstream is not None else "(missing)"
                dependency_id = (
                    dependency.dependency_work_item_id
                    or dependency.dependency_workflow_node_id
                )
                lines.append(
                    f"- requires {dependency_id} to be {dependency.condition}; "
                    f"current={upstream_status}"
                )
        latest_attempt = session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work.id)
            .order_by(WorkAttempt.attempt_number.desc())
        )
        if latest_attempt is not None:
            lines.append(f"latest_attempt: {latest_attempt.status}")
            if latest_attempt.error_message:
                lines.append(f"latest_error: {latest_attempt.error_message}")
        console.print("\n".join(lines))


@app.command("run-next")
def run_next(
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "cli",
    lease_seconds: Annotated[int | None, typer.Option("--lease-seconds")] = None,
) -> None:
    """Claim and run the next ready WorkItem with the local function runtime."""
    with cli_session_scope() as session:
        outcome = WorkRunner(
            session,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
        ).run_next()
        if outcome is None:
            console.print("No ready work.")
            return
        console.print(f"{outcome.status}: {outcome.work_item_id}")
        console.print(outcome.summary)


@app.command("pause")
def pause_work(work_item_id: Annotated[str, typer.Argument(help="WorkItem id.")]) -> None:
    """Pause a non-terminal WorkItem."""
    with cli_session_scope() as session:
        work = WorkQueue(session).pause_work(work_item_id)
        console.print(f"{work.id}: {work.status}")


@app.command("resume")
def resume_work(work_item_id: Annotated[str, typer.Argument(help="WorkItem id.")]) -> None:
    """Resume a paused WorkItem."""
    with cli_session_scope() as session:
        work = WorkQueue(session).resume_work(work_item_id)
        console.print(f"{work.id}: {work.status}")


@app.command("cancel")
def cancel_work(work_item_id: Annotated[str, typer.Argument(help="WorkItem id.")]) -> None:
    """Cancel or request cancellation of a WorkItem."""
    with cli_session_scope() as session:
        work = WorkQueue(session).request_cancel(work_item_id)
        console.print(f"{work.id}: {work.status}")


@app.command("retry")
def retry_work(work_item_id: Annotated[str, typer.Argument(help="WorkItem id.")]) -> None:
    """Return dead-lettered work to the ready queue."""
    with cli_session_scope() as session:
        work = WorkQueue(session).retry_dead_letter(work_item_id)
        console.print(f"{work.id}: {work.status}")


@app.command("schedule-create")
def schedule_create(
    name: Annotated[str, typer.Argument(help="Schedule name.")],
    schedule_type: Annotated[
        str,
        typer.Option("--type", help="cron, interval, or date."),
    ],
    expression: Annotated[str, typer.Option("--expr", help="Cron, interval, or ISO date expression.")],
    task_instruction: Annotated[
        str | None,
        typer.Option("--task", help="Instruction for enqueued work."),
    ] = None,
    task_template_path: Annotated[
        Path | None,
        typer.Option("--task-template", help="Markdown template file for enqueued work."),
    ] = None,
    worker_kind: Annotated[str, typer.Option("--worker-kind", "-w")] = "manual",
    timezone_name: Annotated[str | None, typer.Option("--timezone")] = None,
    catchup_policy: Annotated[str, typer.Option("--catchup-policy")] = "coalesce",
    max_backfill: Annotated[int, typer.Option("--max-backfill")] = 10,
) -> None:
    """Create a durable recurring or future WorkItem schedule."""
    if bool(task_instruction) == bool(task_template_path):
        raise typer.BadParameter("Provide exactly one of --task or --task-template.")
    payload = {"title": name}
    if task_template_path is not None:
        if not task_template_path.is_file():
            raise typer.BadParameter(f"Template file does not exist: {task_template_path}")
        payload["task_template_path"] = str(task_template_path.resolve())
    else:
        payload["task_instruction"] = task_instruction or ""

    with cli_session_scope() as session:
        schedule = ScheduleService(session).create_schedule(
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            worker_kind=worker_kind,
            timezone_name=timezone_name,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
            payload=payload,
        )
        console.print(schedule.id)


@app.command("schedule-workflow-create")
def schedule_workflow_create(
    name: Annotated[str, typer.Argument(help="Schedule name.")],
    schedule_type: Annotated[str, typer.Option("--type", help="cron, interval, or date.")],
    expression: Annotated[str, typer.Option("--expr", help="Cron, interval, or ISO date expression.")],
    workflow_definition_id: Annotated[str, typer.Option("--workflow-definition-id")],
    run_name: Annotated[str | None, typer.Option("--run-name")] = None,
    input_json: Annotated[str | None, typer.Option("--input-json")] = None,
    timezone_name: Annotated[str | None, typer.Option("--timezone")] = None,
    catchup_policy: Annotated[str, typer.Option("--catchup-policy")] = "coalesce",
    max_backfill: Annotated[int, typer.Option("--max-backfill")] = 10,
) -> None:
    """Create a schedule that starts WorkflowRuns directly."""
    payload = {"workflow_definition_id": workflow_definition_id}
    if run_name:
        payload["run_name"] = run_name
    if input_json:
        payload["input"] = _parse_json_object(input_json)

    with cli_session_scope() as session:
        schedule = ScheduleService(session).create_schedule(
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            worker_kind="workflow",
            timezone_name=timezone_name,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
            payload=payload,
        )
        console.print(schedule.id)


@app.command("schedule-list")
def schedule_list(limit: Annotated[int, typer.Option("--limit", "-n")] = 20) -> None:
    """List schedules."""
    with cli_session_scope() as session:
        schedules = session.scalars(
            select(Schedule).order_by(Schedule.created_at.desc()).limit(limit)
        ).all()
        table = Table("Id", "Enabled", "Type", "Target", "Expression", "Policy", "Name")
        for schedule in schedules:
            table.add_row(
                schedule.id,
                str(schedule.enabled),
                schedule.schedule_type,
                schedule.worker_kind,
                schedule.expression,
                schedule.catchup_policy,
                schedule.name,
            )
        console.print(table)


@app.command("schedule-disable")
def schedule_disable(schedule_id: Annotated[str, typer.Argument(help="Schedule id.")]) -> None:
    """Disable a schedule so future polls skip it."""
    with cli_session_scope() as session:
        schedule = ScheduleService(session).disable_schedule(schedule_id)
        console.print(f"{schedule.id}: enabled={schedule.enabled}")


@app.command("schedule-enable")
def schedule_enable(
    schedule_id: Annotated[str, typer.Argument(help="Schedule id.")],
    resume_from_now: Annotated[
        bool,
        typer.Option("--resume-from-now/--catch-up", help="Reset recurring catch-up baseline."),
    ] = True,
) -> None:
    """Enable a schedule."""
    with cli_session_scope() as session:
        schedule = ScheduleService(session).enable_schedule(
            schedule_id,
            resume_from_now=resume_from_now,
        )
        console.print(f"{schedule.id}: enabled={schedule.enabled}")


@app.command("schedule-pause")
def schedule_pause(schedule_id: Annotated[str, typer.Argument(help="Schedule id.")]) -> None:
    """Alias for schedule-disable."""
    with cli_session_scope() as session:
        schedule = ScheduleService(session).disable_schedule(schedule_id)
        console.print(f"{schedule.id}: enabled={schedule.enabled}")


@app.command("schedule-resume")
def schedule_resume(
    schedule_id: Annotated[str, typer.Argument(help="Schedule id.")],
    resume_from_now: Annotated[
        bool,
        typer.Option("--resume-from-now/--catch-up", help="Reset recurring catch-up baseline."),
    ] = True,
) -> None:
    """Alias for schedule-enable."""
    with cli_session_scope() as session:
        schedule = ScheduleService(session).enable_schedule(
            schedule_id,
            resume_from_now=resume_from_now,
        )
        console.print(f"{schedule.id}: enabled={schedule.enabled}")


@app.command("schedule-delete")
def schedule_delete(schedule_id: Annotated[str, typer.Argument(help="Schedule id.")]) -> None:
    """Delete a schedule and future occurrences."""
    with cli_session_scope() as session:
        ScheduleService(session).delete_schedule(schedule_id)
        console.print(f"{schedule_id}: deleted")


@app.command("schedule-fire-now")
def schedule_fire_now(schedule_id: Annotated[str, typer.Argument(help="Schedule id.")]) -> None:
    """Launch one schedule occurrence immediately."""
    with cli_session_scope() as session:
        occurrence = ScheduleService(session).fire_schedule_now(schedule_id)
        console.print(f"occurrence: {occurrence.id}")
        if occurrence.work_item_id:
            console.print(f"work_item: {occurrence.work_item_id}")
        if occurrence.workflow_run_id:
            console.print(f"workflow_run: {occurrence.workflow_run_id}")


@app.command("schedule-edit")
def schedule_edit(
    schedule_id: Annotated[str, typer.Argument(help="Schedule id.")],
    name: Annotated[str | None, typer.Option("--name")] = None,
    schedule_type: Annotated[str | None, typer.Option("--type")] = None,
    expression: Annotated[str | None, typer.Option("--expr")] = None,
    worker_kind: Annotated[str | None, typer.Option("--worker-kind", "-w")] = None,
    task_instruction: Annotated[str | None, typer.Option("--task")] = None,
    task_template_path: Annotated[Path | None, typer.Option("--task-template")] = None,
    payload_json: Annotated[str | None, typer.Option("--payload-json")] = None,
    timezone_name: Annotated[str | None, typer.Option("--timezone")] = None,
    catchup_policy: Annotated[str | None, typer.Option("--catchup-policy")] = None,
    max_backfill: Annotated[int | None, typer.Option("--max-backfill")] = None,
) -> None:
    """Edit schedule metadata, timing, target worker, or payload."""
    with cli_session_scope() as session:
        existing = session.get(Schedule, schedule_id)
        if existing is None:
            raise typer.Exit(code=1)
        payload = None
        if payload_json:
            if task_instruction or task_template_path:
                raise typer.BadParameter("--payload-json cannot be combined with --task or --task-template.")
            payload = _parse_json_object(payload_json)
        elif task_instruction or task_template_path:
            if task_instruction and task_template_path:
                raise typer.BadParameter("Provide only one of --task or --task-template.")
            payload = dict(existing.payload or {})
            payload.pop("task_instruction", None)
            payload.pop("instruction", None)
            payload.pop("task_template_path", None)
            payload.pop("instruction_template_path", None)
            if task_template_path:
                if not task_template_path.is_file():
                    raise typer.BadParameter(f"Template file does not exist: {task_template_path}")
                payload["task_template_path"] = str(task_template_path.resolve())
            else:
                payload["task_instruction"] = task_instruction
            if name:
                payload["title"] = name
            else:
                payload.setdefault("title", existing.name)
        schedule = ScheduleService(session).update_schedule(
            schedule_id,
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            worker_kind=worker_kind,
            payload=payload,
            timezone_name=timezone_name,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
        )
        console.print(f"{schedule.id}: updated")


@app.command("schedule-poll")
def schedule_poll() -> None:
    """Evaluate enabled schedules and launch due work or workflows."""
    with cli_session_scope() as session:
        enqueued = ScheduleService(session).poll_due_schedules()
        console.print(f"Launched {enqueued} scheduled occurrence(s).")


@app.command("workflow-start-file")
def workflow_start_file(
    path: Annotated[Path, typer.Argument(help="JSON workflow definition file.")],
    name: Annotated[str | None, typer.Option("--name")] = None,
) -> None:
    """Load a workflow JSON file and start a run."""
    with cli_session_scope() as session:
        service = WorkflowService(session)
        definition = service.load_definition_file(path)
        run = service.start_run(workflow_definition_id=definition.id, name=name)
        console.print(run.id)


@app.command("workflow-create-file")
def workflow_create_file(
    path: Annotated[Path, typer.Argument(help="JSON workflow definition file.")],
) -> None:
    """Load or update a WorkflowDefinition without starting a run."""
    with cli_session_scope() as session:
        definition = WorkflowService(session).load_definition_file(path)
        console.print(definition.id)


@app.command("workflow-validate-file")
def workflow_validate_file(
    path: Annotated[Path, typer.Argument(help="JSON workflow definition file.")],
) -> None:
    """Validate a workflow JSON file without writing to the database."""
    service = WorkflowService(None)  # type: ignore[arg-type]
    data = service.parse_definition_file(path)
    console.print(f"valid: {data['name']}@{data.get('version', '1')}")


@app.command("workflow-list")
def workflow_list(limit: Annotated[int, typer.Option("--limit", "-n")] = 20) -> None:
    """List WorkflowDefinitions."""
    with cli_session_scope() as session:
        definitions = session.scalars(
            select(WorkflowDefinition).order_by(WorkflowDefinition.created_at.desc()).limit(limit)
        ).all()
        table = Table("Id", "Enabled", "Name", "Version", "Nodes")
        for definition in definitions:
            nodes = definition.definition.get("nodes", [])
            table.add_row(
                definition.id,
                str(definition.enabled),
                definition.name,
                definition.version,
                str(len(nodes) if isinstance(nodes, list) else 0),
            )
        console.print(table)


@app.command("workflow-tick")
def workflow_tick() -> None:
    """Reconcile workflow runs and enqueue ready nodes."""
    with cli_session_scope() as session:
        changed = WorkflowService(session).tick_runs()
        console.print(f"Changed {changed} workflow run(s).")


@app.command("workflow-show")
def workflow_show(workflow_run_id: Annotated[str, typer.Argument(help="WorkflowRun id.")]) -> None:
    """Show workflow run status and materialized nodes."""
    with cli_session_scope() as session:
        run = session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise typer.Exit(code=1)
        console.print(f"[bold]{run.name}[/bold]")
        console.print(f"id: {run.id}")
        console.print(f"status: {run.status}")
        console.print()

        nodes = session.scalars(
            select(WorkflowNode)
            .where(WorkflowNode.workflow_run_id == run.id)
            .order_by(WorkflowNode.created_at)
        ).all()
        table = Table("Key", "Kind", "Status", "WorkItem", "Failure")
        for node in nodes:
            table.add_row(
                node.node_key,
                node.kind,
                node.status,
                node.work_item_id or "",
                node.failure_reason or "",
            )
        console.print(table)


@app.command("workflow-answer")
def workflow_answer(
    workflow_run_id: Annotated[str, typer.Argument(help="WorkflowRun id.")],
    node_key: Annotated[str, typer.Argument(help="Gate node key.")],
    answer: Annotated[str, typer.Argument(help="Gate answer.")],
) -> None:
    """Answer an awaiting workflow gate."""
    with cli_session_scope() as session:
        node = WorkflowService(session).answer_gate(
            workflow_run_id=workflow_run_id,
            node_key=node_key,
            answer=answer,
        )
        console.print(f"{node.node_key}: {node.status}")


@app.command("memory-add")
def memory_add(
    content: Annotated[str, typer.Argument(help="Memory content.")],
    namespace: Annotated[str, typer.Option("--namespace", "-n")] = "global",
    kind: Annotated[str, typer.Option("--kind", "-k")] = "note",
    tags: Annotated[list[str] | None, typer.Option("--tag")] = None,
) -> None:
    """Create a curated memory."""
    with cli_session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace=namespace,
            kind=kind,
            content=content,
            tags=tags or [],
        )
        console.print(memory.id)


@app.command("memory-search")
def memory_search(
    query: Annotated[str | None, typer.Argument(help="FTS query.")] = None,
    namespace: Annotated[str | None, typer.Option("--namespace", "-n")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 10,
) -> None:
    """Search curated memories."""
    with cli_session_scope() as session:
        memories = MemoryService(session).search(
            query=query,
            namespace=namespace,
            tags=tag or [],
            limit=limit,
        )
        table = Table("Id", "Namespace", "Kind", "Tags", "Content")
        for memory in memories:
            table.add_row(
                memory.id,
                memory.namespace,
                memory.kind,
                ", ".join(memory.tags or []),
                memory.content,
            )
        console.print(table)


@app.command("memory-archive")
def memory_archive(memory_id: Annotated[str, typer.Argument(help="Memory id.")]) -> None:
    """Archive a curated memory."""
    with cli_session_scope() as session:
        memory = MemoryService(session).archive_memory(memory_id)
        console.print(f"{memory.id}: archived")


@app.command("memory-ingest-text")
def memory_ingest_text(
    path: Annotated[Path, typer.Argument(help="Text file to ingest.")],
    namespace: Annotated[str, typer.Option("--namespace", "-n")] = "global",
    title: Annotated[str | None, typer.Option("--title")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
) -> None:
    """Ingest a local text file into searchable memory chunks."""
    content = path.expanduser().resolve().read_text(encoding="utf-8", errors="replace")
    with cli_session_scope() as session:
        result = MemoryIngestService(session).ingest_text(
            namespace=namespace,
            title=title or path.name,
            content=content,
            source_kind="cli_file",
            source_id=str(path.expanduser().resolve()),
            tags=tag or [],
        )
        console.print(f"ingested {len(result.memory_ids)} memories")


@app.command("memory-ingest-artifact")
def memory_ingest_artifact(
    artifact_id: Annotated[str, typer.Argument(help="Artifact id.")],
    namespace: Annotated[str | None, typer.Option("--namespace", "-n")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
) -> None:
    """Ingest a text artifact into searchable memory chunks."""
    with cli_session_scope() as session:
        result = MemoryIngestService(session).ingest_artifact(
            artifact_id,
            namespace=namespace,
            tags=tag or [],
        )
        if result is None:
            console.print("artifact skipped: not text or too large")
        else:
            console.print(f"ingested {len(result.memory_ids)} memories")


@app.command("memory-ingest-pending")
def memory_ingest_pending(
    limit: Annotated[int, typer.Option("--limit", "-n")] = 25,
) -> None:
    """Ingest pending text artifacts and inbound Discord messages."""
    with cli_session_scope() as session:
        result = MemoryIngestService(session).auto_ingest_pending(limit=limit)
        console.print(
            f"ingested_sources={result.ingested_sources} "
            f"skipped_sources={result.skipped_sources} memories={len(result.memory_ids)}"
        )


@app.command("artifact-list")
def artifact_list(
    query: Annotated[str | None, typer.Argument(help="Search text.")] = None,
    kind: Annotated[str | None, typer.Option("--kind")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
    work_item_id: Annotated[str | None, typer.Option("--work-item-id")] = None,
    source_kind: Annotated[str | None, typer.Option("--source-kind")] = None,
    include_archived: Annotated[bool, typer.Option("--include-archived")] = False,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 20,
) -> None:
    """List/search artifact metadata."""
    with cli_session_scope() as session:
        artifacts = ArtifactService(session).list_artifacts(
            query=query,
            kind=kind,
            tag=tag or [],
            work_item_id=work_item_id,
            source_kind=source_kind,
            include_archived=include_archived,
            limit=limit,
        )
        table = Table("Id", "Kind", "Tags", "Size", "Title", "Path")
        for artifact in artifacts:
            table.add_row(
                artifact.id,
                artifact.kind,
                ", ".join(artifact.tags or []),
                str(artifact.size_bytes or ""),
                artifact.title,
                artifact.local_path,
            )
        console.print(table)


@app.command("artifact-show")
def artifact_show(artifact_id: Annotated[str, typer.Argument(help="Artifact id.")]) -> None:
    """Show artifact metadata."""
    with cli_session_scope() as session:
        artifact = ArtifactService(session).get_artifact(artifact_id)
        table = Table("Field", "Value")
        for key, value in {
            "id": artifact.id,
            "kind": artifact.kind,
            "title": artifact.title,
            "local_path": artifact.local_path,
            "content_type": artifact.content_type or "",
            "size_bytes": artifact.size_bytes or "",
            "sha256": artifact.sha256 or "",
            "tags": ", ".join(artifact.tags or []),
            "workflow_run_id": artifact.workflow_run_id or "",
            "work_item_id": artifact.work_item_id or "",
            "attempt_id": artifact.attempt_id or "",
            "source": f"{artifact.source_kind or ''}:{artifact.source_id or ''}",
        }.items():
            table.add_row(key, str(value))
        console.print(table)


@app.command("artifact-capture")
def artifact_capture(
    path: Annotated[Path, typer.Argument(help="Local file to copy into artifact storage.")],
    kind: Annotated[str, typer.Option("--kind")] = "file",
    title: Annotated[str | None, typer.Option("--title")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
    work_item_id: Annotated[str | None, typer.Option("--work-item-id")] = None,
    discord_upload: Annotated[
        bool,
        typer.Option("--discord-upload", help="Tag artifact for Discord upload."),
    ] = False,
) -> None:
    """Copy a local file into artifact storage."""
    tags = list(tag or [])
    if discord_upload and "discord_upload" not in tags:
        tags.append("discord_upload")
    with cli_session_scope() as session:
        artifact = ArtifactStore().capture_file(
            session,
            path=path,
            kind=kind,
            title=title,
            tags=tags,
            work_item_id=work_item_id,
            source_kind="cli",
            source_id=str(path),
        )
        console.print(artifact.id)


@app.command("artifact-archive")
def artifact_archive(artifact_id: Annotated[str, typer.Argument(help="Artifact id.")]) -> None:
    """Archive artifact metadata without deleting the local file."""
    with cli_session_scope() as session:
        artifact = ArtifactService(session).archive_artifact(artifact_id)
        console.print(f"{artifact.id}: archived")


@app.command("discord-intake")
def discord_intake(
    discord_message_id: Annotated[str, typer.Argument(help="Discord message id.")],
    discord_channel_id: Annotated[str, typer.Argument(help="Discord channel id.")],
    author: Annotated[str, typer.Argument(help="Discord author.")],
    content: Annotated[str, typer.Argument(help="Message content.")],
    worker_kind: Annotated[str, typer.Option("--worker-kind", "-w")] = "manual",
) -> None:
    """Simulate Discord intake and queue a WorkItem."""
    with cli_session_scope() as session:
        work = DiscordService(session).ingest_intake_message(
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            author=author,
            content=content,
            worker_kind=worker_kind,
        )
        console.print(work.id)


@app.command("discord-bot")
def discord_bot() -> None:
    """Run only the Discord bot adapter for debugging."""
    from tasque2.discord_bot import run_bot

    run_bot()


@app.command("discord-output-simulate")
def discord_output_simulate(
    ops_channel_id: Annotated[str, typer.Option("--ops-channel-id")] = "local-ops",
    jobs_channel_id: Annotated[str, typer.Option("--jobs-channel-id")] = "local-jobs",
    chains_channel_id: Annotated[str, typer.Option("--chains-channel-id")] = "local-chains",
    dlq_channel_id: Annotated[str, typer.Option("--dlq-channel-id")] = "local-dlq",
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
) -> None:
    """Simulate Discord output posting using fake local ids."""
    gateway = FakeDiscordOutputGateway()
    with cli_session_scope() as session:
        posted = asyncio.run(
            DiscordOutputService(session).post_pending_updates(
                parent_channel_id=ops_channel_id,
                gateway=gateway,
                ops_channel_id=ops_channel_id,
                jobs_channel_id=jobs_channel_id,
                chains_channel_id=chains_channel_id,
                dlq_channel_id=dlq_channel_id,
                limit=limit,
            )
        )
        console.print(f"Posted {posted} update(s).")
        console.print(f"Created {len(gateway.created_threads)} thread(s).")
        console.print(f"Sent {len(gateway.sent_messages)} message(s).")


@app.command("provider-smoke")
def provider_smoke(
    provider: Annotated[str, typer.Argument(help="codex or claude.")],
    prompt: Annotated[str | None, typer.Option("--prompt")] = None,
    cwd: Annotated[Path | None, typer.Option("--cwd")] = None,
    model: Annotated[str | None, typer.Option("--model")] = None,
    expect_json: Annotated[bool, typer.Option("--expect-json/--no-expect-json")] = True,
) -> None:
    """Run a persisted provider smoke WorkItem."""
    allowed = {"codex", "claude"}
    if get_settings().allow_test_providers:
        allowed.update({"fake", "subprocess"})
    if provider not in allowed:
        raise typer.BadParameter(f"provider must be one of: {', '.join(sorted(allowed))}")

    task_instruction = prompt or (
        f"Run a provider smoke check for provider {provider!r}. Submit the result through "
        "submit_worker_result with summary 'provider smoke passed', a short report, and "
        "produces containing ok=true and the provider name."
    )
    runtime_contract: dict[str, object] = {
        "expect_json": expect_json,
        "cwd": str((cwd or Path.cwd()).resolve()),
    }
    if model:
        runtime_contract["model"] = model
    if provider == "subprocess":
        runtime_contract["argv"] = [
            sys.executable,
            "-c",
            (
                "import os; "
                "from tasque2 import result_inbox; "
                "result_inbox.deposit("
                "result_token=os.environ['TASQUE2_RESULT_TOKEN'], "
                "agent_kind='worker', "
                "payload={"
                "'summary': 'provider smoke passed', "
                "'report': 'Subprocess provider smoke passed.', "
                "'produces': {'ok': True, 'provider': 'subprocess'}"
                "}); "
                "print('submitted provider smoke result')"
            ),
        ]

    with cli_session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title=f"Provider smoke: {provider}",
            task_instruction=task_instruction,
            worker_kind=f"provider.{provider}",
            runtime_contract=runtime_contract,
            max_attempts=1,
            source_kind="provider_smoke",
            source_id=provider,
        )
        outcome = WorkRunner(session, lease_owner="provider-smoke").run_next()
        if outcome is None:
            console.print("No ready provider smoke work.")
            return

        attempt = session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work.id)
            .order_by(WorkAttempt.attempt_number.desc())
        )
        provider_run = None
        if attempt is not None:
            provider_run = session.scalar(
                select(ProviderRun).where(ProviderRun.attempt_id == attempt.id)
            )

        console.print(f"{outcome.status}: {outcome.work_item_id}")
        console.print(outcome.summary)
        if provider_run is not None:
            table = Table("Field", "Value")
            table.add_row("provider_run_id", provider_run.id)
            table.add_row("status", provider_run.status)
            table.add_row("provider", provider_run.provider)
            table.add_row("session", provider_run.provider_session_id or "")
            table.add_row("stdout_artifact", provider_run.stdout_artifact_id or "")
            table.add_row("stderr_artifact", provider_run.stderr_artifact_id or "")
            table.add_row("raw_artifact", provider_run.raw_stream_artifact_id or "")
            console.print(table)


@app.command("daemon-once")
def daemon_once(
    max_work_items: Annotated[int, typer.Option("--max-work-items")] = 10,
) -> None:
    """Run one daemon tick: recover, schedule, execute, reconcile."""
    with cli_session_scope() as session:
        result = TasqueDaemon(session).run_once(max_work_items=max_work_items)
        console.print(result)


@app.command("daemon")
def daemon(
    interval_seconds: Annotated[float, typer.Option("--interval-seconds")] = 5.0,
    max_work_items: Annotated[int, typer.Option("--max-work-items")] = 10,
) -> None:
    """Run the local Tasque service: Discord, scheduler, workflows, and workers."""
    from tasque2.discord_bot import run_bot

    ensure_cli_database_ready()
    run_bot(
        start_daemon=True,
        daemon_interval_seconds=interval_seconds,
        daemon_max_work_items=max_work_items,
    )


@app.command("backup-create")
def backup_create(
    destination: Annotated[Path | None, typer.Argument(help="Backup directory.")] = None,
    include_artifacts: Annotated[bool, typer.Option("--artifacts/--no-artifacts")] = True,
) -> None:
    """Create a SQLite/artifact backup directory."""
    ensure_cli_database_ready()
    result = BackupService().create_backup(destination=destination, include_artifacts=include_artifacts)
    console.print(f"backup_dir: {result.backup_dir}")
    console.print(f"database: {result.database_path}")
    console.print(f"manifest: {result.manifest_path}")
    if result.artifacts_dir:
        console.print(f"artifacts: {result.artifacts_dir}")


@app.command("backup-restore")
def backup_restore(
    backup_dir: Annotated[Path, typer.Argument(help="Backup directory.")],
    force: Annotated[bool, typer.Option("--force", help="Required to overwrite current database.")] = False,
) -> None:
    """Restore a backup directory into the configured data path."""
    result = BackupService().restore_backup(backup_dir, force=force)
    status = upgrade_database()
    console.print(f"restored_database: {result.restored_database_path}")
    if result.restored_artifacts_dir:
        console.print(f"restored_artifacts: {result.restored_artifacts_dir}")
    if result.previous_database_backup:
        console.print(f"previous_database_backup: {result.previous_database_backup}")
    console.print(f"revision: {status.current_display}")


@app.command("reset-jobs")
def reset_jobs(
    yes: Annotated[bool, typer.Option("--yes", help="Confirm deleting current job/run history.")] = False,
    backup: Annotated[bool, typer.Option("--backup/--no-backup")] = True,
    include_workflows: Annotated[
        bool,
        typer.Option("--workflows/--no-workflows", help="Also clear standalone workflow run history."),
    ] = True,
) -> None:
    """Clear current WorkItems, DLQ records, and optional standalone workflow runs."""
    ensure_cli_database_ready()
    if not yes:
        console.print("Refusing to reset jobs without --yes.")
        raise typer.Exit(code=1)

    backup_dir = None
    if backup:
        backup_dir = BackupService().create_backup().backup_dir

    with session_scope() as session:
        result = JobResetService(session).reset_jobs(
            include_standalone_workflows=include_workflows,
        )

    if backup_dir is not None:
        console.print(f"backup_dir: {backup_dir}")
    console.print(f"work_items_deleted: {result.work_items_deleted}")
    console.print(f"workflow_runs_deleted: {result.workflow_runs_deleted}")
    console.print(f"discord_messages_deleted: {result.discord_messages_deleted}")
    console.print(f"discord_threads_deleted: {result.discord_threads_deleted}")
    console.print(f"workflow_events_deleted: {result.workflow_events_deleted}")
    console.print(f"schedule_occurrences_unlinked: {result.schedule_occurrences_unlinked}")
    console.print(f"agent_results_deleted: {result.agent_results_deleted}")


@app.command("report-work")
def report_work(
    work_item_id: Annotated[str, typer.Argument(help="WorkItem id.")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Render a WorkItem report."""
    with cli_session_scope() as session:
        report = ReportService(session).work_report(work_item_id)
        if as_json:
            console.print(Syntax(report_to_json(report), "json"))
        else:
            console.print(report.body)


@app.command("status")
def status() -> None:
    """Show local system status counts."""
    with cli_session_scope() as session:
        snapshot = get_system_status(session)
        table = Table("Area", "Status", "Count")
        table.add_row("summary", "ready_work", str(snapshot.ready_work))
        table.add_row("summary", "running_work", str(snapshot.running_work))
        for key, value in snapshot.work_items.items():
            table.add_row("work", key, str(value))
        for key, value in snapshot.work_attempts.items():
            table.add_row("attempt", key, str(value))
        table.add_row("failed_work", "unresolved", str(snapshot.failed_work_unresolved))
        table.add_row("schedules", "enabled", str(snapshot.schedules_enabled))
        for key, value in snapshot.workflow_runs.items():
            table.add_row("workflow", key, str(value))
        console.print(table)


def _parse_json_object(raw: str) -> dict[str, object]:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise typer.BadParameter("Value must be a JSON object.")
    return parsed
