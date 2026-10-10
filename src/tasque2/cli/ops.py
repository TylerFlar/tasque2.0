from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
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
from tasque2.models import (
    DiscordSticky,
    DiscordThread,
    ProviderRun,
    Schedule,
    WorkAttempt,
    WorkflowDefinition,
    WorkflowRun,
    WorkItem,
)
from tasque2.ops.status import get_system_status
from tasque2.ops.usage import usage_by_lane
from tasque2.schedules import WORKFLOW_SCHEDULE_TARGETS, ScheduleService

WORK_NODE_KINDS = ("work", "fan_out")


@app.command("migrate")
def migrate(
    to: Annotated[str | None, typer.Option("--to", help="Step back to this revision (an undo).")] = None,
) -> None:
    """Create or upgrade the database schema (or, with --to, step it back)."""
    from tasque2.migrations import downgrade_database, upgrade_database

    status = downgrade_database(to) if to else upgrade_database()
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


@app.command("stickies")
def stickies(as_json: Annotated[bool, typer.Option("--json")] = False) -> None:
    """Each thread's sticky note as Discord shows it: the notes kept for the user, then the thread's upcoming runs."""
    from tasque2.sticky import STICKY_OFF, StickyService

    with cli_session_scope() as session:
        views = StickyService(session).showable()
        off = session.scalars(
            select(DiscordSticky.discord_thread_id)
            .where(DiscordSticky.status == STICKY_OFF)
            .order_by(DiscordSticky.discord_thread_id)
        ).all()
        if as_json:
            emit_json({"stickies": [view.data() for view in views], "off": list(off)})
            return
        for view in views:
            echo(f"== {view.thread_id} ({_thread_label(session, view.thread_id)})\n{view.text()}\n")
        for thread_id in off:
            echo(f"== {thread_id} ({_thread_label(session, thread_id)}): off, its message was deleted")
        if not views and not off:
            echo("No thread has a sticky note.")


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
        posted = DiscordOutputService(session, dry_run=True).post_pending_updates(
            gateway=gateway, channels=channels, limit=limit
        )
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


@app.command("backup-init")
def backup_init(
    repository: Annotated[str, typer.Argument(help="Where the restic repository lives, e.g. F:/TasqueBackups/restic.")],
    restic_binary: Annotated[str | None, typer.Option("--restic", help="Path to the restic binary.")] = None,
) -> None:
    """Set up scheduled backups: a starter data/backup.toml, a stored password and the repository."""
    from tasque2.ops import backup_runner

    path = backup_runner.write_starter_config(repository)
    if restic_binary:
        text = path.read_text(encoding="utf-8")
        binary = Path(restic_binary).as_posix()
        path.write_text(text.replace('restic = "restic"', f'restic = "{binary}"', 1), encoding="utf-8")
    try:
        config = backup_runner.load_config(path)
    except backup_runner.BackupConfigError as exc:
        raise fail(str(exc)) from None
    _password, created = backup_runner.ensure_password()
    result = backup_runner.init_repository(config)
    if not result.ok:
        raise fail(f"restic init failed: {result.stderr.strip() or result.stdout.strip()}")
    echo(f"config: {path}")
    echo(f"repository: {config.repository}")
    where = f"{backup_runner.KEYRING_SERVICE}/{backup_runner.KEYRING_USER} in the system credential store"
    echo(f"password: {'created and stored' if created else 'already stored'} as {where}")
    if created:
        echo("Copy that password into your password manager: without it the backups cannot be read.")


@app.command("backup-run")
def backup_run(
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show what would be backed up; change nothing.")] = False,
    maintenance: Annotated[bool, typer.Option("--maintenance", help="Prune and check now.")] = False,
    restore_test: Annotated[bool, typer.Option("--restore-test", help="Restore and verify the database now.")] = False,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Run a backup now (the daemon's tasque-backup schedule does this nightly)."""
    from tasque2.migrations import upgrade_database
    from tasque2.ops.backup_runner import run_backup

    upgrade_database()
    run = run_backup(dry_run=dry_run, force_maintenance=maintenance, force_restore_test=restore_test)
    if as_json:
        emit_json(run)
    else:
        _print_backup_run(run)
    if not run["ok"]:
        raise typer.Exit(code=1)


@app.command("backup-verify")
def backup_verify(as_json: Annotated[bool, typer.Option("--json")] = False) -> None:
    """Check the repository and restore-test the latest database snapshot, without backing up."""
    from tasque2.ops import backup_runner

    try:
        config = backup_runner.load_config()
    except backup_runner.BackupConfigError as exc:
        raise fail(str(exc)) from None
    out = {"maintenance": backup_runner.maintenance(config), "restore_test": backup_runner.restore_test(config)}
    state = backup_runner.read_state()
    now = datetime.now(UTC).isoformat()
    state.update(
        last_check_at=now,
        last_check_ok=out["maintenance"]["ok"],
        last_restore_test_at=now,
        last_restore_test_ok=out["restore_test"]["ok"],
    )
    backup_runner.write_state(state)
    if as_json:
        emit_json(out)
    else:
        for name, part in out.items():
            details = json.dumps({key: value for key, value in part.items() if key != "ok"})
            echo(f"{name}: {'ok' if part['ok'] else 'FAILED'} {details}")
    if not all(part["ok"] for part in out.values()):
        raise typer.Exit(code=1)


@app.command("backup-status")
def backup_status(as_json: Annotated[bool, typer.Option("--json")] = False) -> None:
    """When the last backup ran, whether it worked, and what needs attention."""
    from tasque2.ops.backup_runner import backup_health, read_state

    health = backup_health()
    if as_json:
        emit_json({"health": health, "state": read_state()})
        return
    if not health["configured"]:
        echo("backups are not configured: run `tasque2 backup-init <repository>`")
        for line in health["attention"]:
            echo(f"attention: {line}")
        return
    echo(f"last good backup: {health['last_success_at'] or 'never'} ({health['age_hours']} h ago)")
    echo(f"last snapshot: {health['last_snapshot_id'] or '-'}")
    echo(f"last check: {health['last_check_at'] or 'never'}")
    echo(f"last restore test: {health['last_restore_test_at'] or 'never'}")
    for line in health["attention"] or ["nothing needs attention"]:
        echo(f"attention: {line}")


@app.command("privacy-scan")
def privacy_scan(
    base: Annotated[str, typer.Option("--base", help="Commit the range starts after.")] = "origin/main",
    head: Annotated[str, typer.Option("--head", help="Commit the range ends at.")] = "HEAD",
    repo: Annotated[Path | None, typer.Option("--repo", help="Repository (default: the project).")] = None,
) -> None:
    """Refuse personal data in what a push would publish: emails, long ids, phones, home paths, denylist terms."""
    from tasque2.ops.privacy import scan_range

    root = (repo or get_settings().resolved_project_dir).resolve()
    findings = scan_range(root, base, head)
    for finding in findings:
        echo(f"{finding.where}: {finding.kind} {finding.match!r}  | {finding.line}")
    if findings:
        raise fail(f"{len(findings)} possible personal detail(s) in {base}..{head}; nothing should be pushed.")
    echo(f"privacy scan clean: {base}..{head}")


@app.command("privacy-hook-install")
def privacy_hook_install(
    repo: Annotated[Path | None, typer.Option("--repo", help="Repository (default: the project).")] = None,
) -> None:
    """Put the privacy scan in front of every git push from this repository."""
    from tasque2.ops.privacy import install_hook

    path = install_hook((repo or get_settings().resolved_project_dir).resolve())
    echo(f"pre-push hook: {path}")


@app.command("effort")
def effort(
    days: Annotated[int, typer.Option("--days", "-d", help="Window in days.")] = 30,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """What Tasque asks of its user, per thread: posts a week, ask share, pushback."""
    from tasque2.ops.effort import effort_report

    with cli_session_scope() as session:
        report = effort_report(session, days=days)
    if as_json:
        emit_json(report)
        return
    totals = report["totals"]
    echo(
        f"{report['window']['days']} days: {totals['posts']} posts ({totals['posts_per_week']}/week), "
        f"{totals['ask_share']:.0%} ask something; {totals['inbound']} messages back, {totals['pushback']} pushback"
    )
    table = PlainTable("Thread", "Posts/wk", "Median chars", "Asks", "Inbound", "Pushback")
    for row in report["threads"]:
        table.add_row(
            row["label"],
            str(row["posts_per_week"]),
            str(row["median_chars"]),
            f"{row['ask_share']:.0%}",
            str(row["inbound"]),
            str(row["pushback"]),
        )
    console.print(table)


def _print_backup_run(run: dict[str, Any]) -> None:
    backup = run.get("backup") or {}
    status = "ok" if run["ok"] else "FAILED"
    added = backup.get("data_added")
    size = f", {added / 1_000_000:.1f} MB added" if isinstance(added, int | float) else ""
    dry = " (dry run)" if run.get("dry_run") else ""
    echo(f"backup {status}{dry}: snapshot {backup.get('snapshot_id') or '-'}{size}, {backup.get('seconds', 0)} s")
    if run.get("error") or backup.get("error"):
        echo(f"error: {run.get('error') or backup.get('error')}")
    database = run.get("database") or {}
    echo(f"database: {database.get('error') or database.get('check') or '-'}")
    for source in run.get("missing_sources") or []:
        echo(f"missing source (skipped): {source}")
    for part in ("maintenance", "restore_test"):
        if part in run:
            echo(f"{part}: {'ok' if run[part]['ok'] else 'FAILED'}")


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


def _thread_label(session: Session, thread_id: str) -> str:
    """The lane or title of the work (or workflow run) a thread belongs to."""
    binding = session.scalar(select(DiscordThread).where(DiscordThread.discord_thread_id == thread_id))
    owner = session.get(WorkItem, binding.work_item_id) if binding is not None and binding.work_item_id else None
    if owner is not None:
        return owner.lane or owner.title
    run = session.get(WorkflowRun, binding.workflow_run_id) if binding is not None and binding.workflow_run_id else None
    return run.name if run is not None else "unbound"


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
