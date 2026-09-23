from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.text import Text
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.cli._common import (
    PlainTable,
    app,
    cli_session_scope,
    cli_span,
    console,
    echo,
    emit_json,
    fail,
    runtime_contract,
)
from tasque2.config import Settings, get_settings
from tasque2.daemon import control
from tasque2.models import ProviderRun, Schedule, WorkAttempt, WorkflowDefinition, WorkItem
from tasque2.ops.status import get_system_status
from tasque2.ops.usage import usage_by_lane
from tasque2.schedules import WORKFLOW_SCHEDULE_TARGETS, ScheduleService

WORK_NODE_KINDS = ("work", "fan_out")


@app.command("migrate")
def migrate() -> None:
    """Create or upgrade the database schema."""
    from tasque2.migrations import upgrade_database

    status = upgrade_database()
    echo(f"{status.database_path}: {status.current_display}")


@app.command("db-status")
def db_status() -> None:
    """Show the database's migration state."""
    from tasque2.migrations import schema_status

    status = schema_status()
    table = PlainTable("Field", "Value")
    table.add_row("database", str(status.database_path))
    table.add_row("current", status.current_display)
    table.add_row("head", status.head_display)
    table.add_row("up to date", str(status.is_current))
    console.print(table)


@app.command("doctor")
def doctor(
    as_json: Annotated[bool, typer.Option("--json")] = False,
    migrate: Annotated[bool, typer.Option("--migrate/--no-migrate", help="Upgrade the schema first.")] = True,
    strict: Annotated[bool, typer.Option("--strict", help="Exit non-zero on any failed check.")] = False,
) -> None:
    """Check configuration, storage, providers, Discord, telemetry, and the daemon."""
    from tasque2.ops.doctor import run_doctor

    report = run_doctor(migrate=migrate)
    if as_json:
        emit_json(report.as_dict())
    else:
        table = PlainTable("Check", "Status", "Summary")
        colors = {"ok": "green", "warn": "yellow", "fail": "red"}
        for check in report.checks:
            table.add_row(check.name, Text(check.status, style=colors.get(check.status, "white")), check.summary)
        console.print(table)
    if strict and report.has_failures:
        raise typer.Exit(code=1)


@app.command("status")
def status() -> None:
    """Show counts of work, attempts, schedules, and workflow runs."""
    with cli_session_scope() as session:
        snapshot = get_system_status(session)
        table = PlainTable("Area", "Status", "Count")
        table.add_row("work", "ready", str(snapshot.ready_work))
        table.add_row("work", "running", str(snapshot.running_work))
        for key, value in sorted(snapshot.work_items.items()):
            if key not in {"ready", "running"}:
                table.add_row("work", key, str(value))
        table.add_row("dead letters", "unresolved", str(snapshot.failed_work_unresolved))
        table.add_row("schedules", "enabled", str(snapshot.schedules_enabled))
        for key, value in sorted(snapshot.workflow_runs.items()):
            table.add_row("workflow runs", key, str(value))
        console.print(table)


@app.command("usage")
def usage(
    days: Annotated[int, typer.Option("--days", "-d")] = 14,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Tokens, estimated cost, and runtime per lane and model."""
    with cli_session_scope() as session:
        rows = usage_by_lane(session, days=days)
    if as_json:
        emit_json([row.__dict__ | {"prompt_tokens": row.prompt_tokens} for row in rows])
        return
    table = PlainTable("Lane", "Model", "Runs", "Failed", "Prompt tok", "Cache %", "Output tok", "Est. $", "Minutes")
    for row in rows:
        cache_share = row.cache_read_tokens / row.prompt_tokens * 100 if row.prompt_tokens else 0.0
        table.add_row(
            row.lane,
            row.model,
            str(row.runs),
            str(row.failed),
            f"{row.prompt_tokens:,}",
            f"{cache_share:.0f}",
            f"{row.output_tokens:,}",
            f"{row.estimated_cost_usd:.2f}",
            f"{row.seconds / 60:.0f}",
        )
    console.print(table)
    console.print(
        f"Estimated cost over {days} days: ${sum(row.estimated_cost_usd for row in rows):.2f} (API list prices)"
    )


@app.command("lanes")
def lanes() -> None:
    """Each lane's cadence and model profile: every schedule and every workflow node that runs work."""
    settings = get_settings()
    with cli_session_scope() as session:
        table = PlainTable("Lane", "Cadence", "Profile", "Model", "MCP servers")
        schedules = session.scalars(select(Schedule).where(Schedule.enabled.is_(True)).order_by(Schedule.name)).all()
        workflow_schedules = [schedule for schedule in schedules if schedule.worker_kind in WORKFLOW_SCHEDULE_TARGETS]
        for schedule in schedules:
            if schedule.worker_kind not in WORKFLOW_SCHEDULE_TARGETS:
                contract = schedule.runtime_contract or {}
                table.add_row(
                    schedule.name, schedule.expression, *_lane_columns(schedule.worker_kind, contract, settings)
                )
        for definition in session.scalars(
            select(WorkflowDefinition).where(WorkflowDefinition.enabled.is_(True)).order_by(WorkflowDefinition.name)
        ).all():
            cadence = "; ".join(
                schedule.expression for schedule in workflow_schedules if _starts_workflow(schedule, definition)
            )
            for node in (definition.definition or {}).get("nodes", []):
                if not isinstance(node, dict) or node.get("kind", "work") not in WORK_NODE_KINDS:
                    continue
                columns = _lane_columns(_node_worker_kind(node), node.get("runtime_contract") or {}, settings)
                table.add_row(f"{definition.name}/{node.get('key')}", cadence or "on demand", *columns)
        console.print(table)


@app.command("smoke")
def smoke(as_json: Annotated[bool, typer.Option("--json")] = False) -> None:
    """Run a deterministic local end-to-end check with the fake provider."""
    from tasque2.ops.smoke import run_smoke

    if not get_settings().allow_test_providers:
        raise fail("The smoke run uses the fake provider; set TASQUE2_ALLOW_TEST_PROVIDERS=true.")
    with cli_span("smoke"), cli_session_scope() as session:
        _refuse_while_daemon_alive(session)
        pending = _pending_work(session)
        if pending:
            raise fail(f"The smoke run's ticks would also run {pending}; run it on an idle database.")
        result = run_smoke(session)
    if as_json:
        emit_json(result.as_dict())
        return
    table = PlainTable("Field", "Value")
    for key, value in result.as_dict().items():
        table.add_row(key, str(value))
    console.print(table)


@app.command("provider-smoke")
def provider_smoke(
    provider: Annotated[str, typer.Argument(help="claude or codex.")],
    profile: Annotated[str, typer.Option("--profile", help="Model profile to test.")] = "low",
    model: Annotated[str | None, typer.Option("--model")] = None,
    prompt: Annotated[str | None, typer.Option("--prompt")] = None,
) -> None:
    """Run one real provider call end to end and show what it recorded."""
    from tasque2.work.repository import WorkRepository
    from tasque2.work.runner import WorkRunner

    allowed = {"claude", "codex"} | ({"fake", "subprocess"} if get_settings().allow_test_providers else set())
    if provider not in allowed:
        raise typer.BadParameter(f"provider must be one of: {', '.join(sorted(allowed))}")
    instruction = prompt or (
        "Provider smoke check. Submit your result with summary 'provider smoke passed', a one-line report, "
        'and produces {"ok": true}.'
    )
    extra: dict[str, Any] = {}
    if provider == "subprocess":
        extra["argv"] = [
            sys.executable,
            "-c",
            "import os; from tasque2.worker import results; results.deposit("
            "result_token=os.environ['TASQUE2_RESULT_TOKEN'], payload={'summary': 'provider smoke passed', "
            "'report': 'Subprocess provider smoke passed.', 'produces': {'ok': True}})",
        ]
    with cli_span("provider-smoke"), cli_session_scope() as session:
        _refuse_while_daemon_alive(session)
        highest = session.scalar(select(func.max(WorkItem.priority)).where(WorkItem.status == "ready"))
        work = WorkRepository(session).create_work_item(
            title=f"Provider smoke: {provider}",
            task_instruction=instruction,
            worker_kind=f"provider.{provider}",
            runtime_contract=runtime_contract(profile=profile, model=model, extra=extra),
            priority=(highest or 0) + 1,
            source_kind="provider_smoke",
            source_id=provider,
            lane="provider-smoke",
            visible=False,
        )
        session.commit()
        outcome = WorkRunner(session, lease_owner="provider-smoke").run_next()
        if outcome is None or outcome.work_item_id != work.id:
            raise fail("The smoke item was not the next ready item; drain the queue or stop the daemon first.")
        echo(f"{outcome.status}: {outcome.summary}")
        attempt = session.scalar(
            select(WorkAttempt).where(WorkAttempt.work_item_id == work.id).order_by(WorkAttempt.attempt_number.desc())
        )
        run = session.scalar(select(ProviderRun).where(ProviderRun.attempt_id == attempt.id)) if attempt else None
        if run is not None:
            table = PlainTable("Field", "Value")
            usage_data = run.usage or {}
            for key in (
                "model",
                "input_tokens",
                "cache_read_tokens",
                "cache_write_tokens",
                "output_tokens",
                "estimated_cost_usd",
                "messages",
            ):
                table.add_row(key, str(usage_data.get(key, "")))
            table.add_row("status", run.status)
            table.add_row("stream artifact", run.stdout_artifact_id or "")
            console.print(table)


@app.command("telemetry-check")
def telemetry_check() -> None:
    """Send a test span, metric and log record to the configured OpenTelemetry endpoint."""
    import logging

    from tasque2.logs import configure_logging
    from tasque2.telemetry import TelemetryMode, configure_telemetry, flush_telemetry, instruments, span

    configure_logging()
    mode = configure_telemetry("cli")
    if mode is TelemetryMode.OFF:
        raise fail(
            "Telemetry is off. Set OTEL_EXPORTER_OTLP_ENDPOINT (for example http://localhost:4318) "
            "or TASQUE2_TELEMETRY=console."
        )
    with span("tasque.telemetry.check") as current:
        instruments().memory_operations.add(0, {"tasque.memory.operation": "telemetry_check"})
        logging.getLogger("tasque2.telemetry").info("Telemetry check")
        trace_id = format(current.get_span_context().trace_id, "032x")
    flush_telemetry()
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "(signal-specific endpoints)")
    console.print(f"mode: {mode.value}\nendpoint: {endpoint}\ntrace id: {trace_id}")


@app.command("discord-output-simulate")
def discord_output_simulate(limit: Annotated[int, typer.Option("--limit", "-n")] = 50) -> None:
    """Render pending Discord output against a fake gateway and report what would be posted."""
    from tasque2.discord.gateway import FakeDiscordGateway
    from tasque2.discord.output import DiscordOutputService, OutputChannels

    gateway = FakeDiscordGateway()
    channels = OutputChannels(ops="local-ops", jobs="local-jobs", chains="local-chains", dlq="local-dlq")
    with cli_session_scope() as session:
        posted = DiscordOutputService(session).post_pending_updates(gateway=gateway, channels=channels, limit=limit)
        session.rollback()
    threads, messages = len(gateway.created_threads), len(gateway.sent_messages)
    console.print(f"would post {posted} update(s): {threads} thread(s), {messages} message(s)")


@app.command("backup-create")
def backup_create(
    destination: Annotated[Path | None, typer.Argument(help="Backup directory.")] = None,
    artifacts: Annotated[bool, typer.Option("--artifacts/--no-artifacts")] = True,
) -> None:
    """Back up the database (and artifacts) to a directory."""
    from tasque2.migrations import upgrade_database
    from tasque2.ops.backup import BackupService

    upgrade_database()
    try:
        result = BackupService().create_backup(destination=destination, include_artifacts=artifacts)
    except OSError as exc:
        raise fail(str(exc)) from None
    echo(f"backup: {result.backup_dir}")


@app.command("backup-restore")
def backup_restore(
    backup_dir: Annotated[Path, typer.Argument()],
    force: Annotated[bool, typer.Option("--force", help="Required: overwrites the current database.")] = False,
) -> None:
    """Restore a backup over the current database; the current one is kept alongside."""
    from tasque2.migrations import upgrade_database
    from tasque2.ops.backup import BackupService

    if not force:
        raise fail("Restoring overwrites the current database; pass --force to confirm.")
    try:
        result = BackupService().restore_backup(backup_dir, force=True)
    except OSError as exc:
        raise fail(str(exc)) from None
    status = upgrade_database()
    echo(f"restored: {result.restored_database_path} ({status.current_display})")
    if result.previous_database_backup:
        echo(f"previous database kept at: {result.previous_database_backup}")


@app.command("reset-jobs")
def reset_jobs(
    yes: Annotated[bool, typer.Option("--yes", help="Confirm deleting work and run history.")] = False,
    backup: Annotated[bool, typer.Option("--backup/--no-backup")] = True,
    workflows: Annotated[bool, typer.Option("--workflows/--no-workflows")] = True,
) -> None:
    """Delete all work items, dead letters, and (optionally) workflow runs."""
    from tasque2.db import session_scope
    from tasque2.migrations import upgrade_database
    from tasque2.ops.backup import BackupService, JobResetService

    if not yes:
        raise fail("This deletes all work and run history; pass --yes to confirm.")
    upgrade_database()
    if backup:
        echo(f"backup: {BackupService().create_backup().backup_dir}")
    with session_scope() as session:
        result = JobResetService(session).reset_jobs(include_standalone_workflows=workflows)
    for key, value in result.__dict__.items():
        console.print(f"{key}: {value}")


def _lane_columns(worker_kind: str, contract: dict[str, Any], settings: Settings) -> tuple[str, str, str]:
    profile = str(contract.get("model_profile") or settings.default_model_profile)
    servers = contract.get("mcp_servers")
    return (
        profile,
        _lane_model(worker_kind, profile, contract, settings),
        ", ".join(servers) if isinstance(servers, list) else "(default)",
    )


def _lane_model(worker_kind: str, profile: str, contract: dict[str, Any], settings: Settings) -> str:
    if not worker_kind.startswith("provider."):
        return f"({worker_kind})"
    from tasque2.providers import provider_name_for_worker_kind

    try:
        choice = settings.model_choice(
            provider_name_for_worker_kind(worker_kind),
            profile,
            model=contract.get("model"),
            effort=contract.get("effort"),
        )
    except ValueError as exc:
        return f"invalid: {exc}"
    return (choice.model or "default") + (f" / {choice.effort}" if choice.effort else "")


def _node_worker_kind(node: dict[str, Any]) -> str:
    """The worker kind a node's work runs with; a fan-out node's children may name their own."""
    worker_kind = str(node.get("worker_kind") or "manual")
    if node.get("kind") == "fan_out":
        return str(node.get("child_worker_kind") or worker_kind)
    return worker_kind


def _starts_workflow(schedule: Schedule, definition: WorkflowDefinition) -> bool:
    payload = schedule.payload or {}
    if payload.get("workflow_definition_id"):
        return payload["workflow_definition_id"] == definition.id
    return (
        payload.get("workflow_name") == definition.name
        and str(payload.get("workflow_version", "1")) == definition.version
    )


def _refuse_while_daemon_alive(session: Session) -> None:
    reason = control.live_daemon_reason()
    if reason is not None:
        raise fail(f"A daemon is alive ({reason}) and would race this command for work; stop it first.")


def _pending_work(session: Session) -> str | None:
    """Work a tick would pick up right now: ready items and schedules that are due."""
    ready = session.scalar(select(func.count()).select_from(WorkItem).where(WorkItem.status == "ready")) or 0
    service = ScheduleService(session)
    due = [
        schedule.name
        for schedule in session.scalars(select(Schedule).where(Schedule.enabled.is_(True))).all()
        if service.due_times(schedule)
    ]
    parts = [f"{ready} ready work item(s)"] if ready else []
    if due:
        parts.append(f"the due schedule(s) {', '.join(due)}")
    return " and ".join(parts) or None
