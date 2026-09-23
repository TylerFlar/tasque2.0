from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.markup import escape
from sqlalchemy import select

from tasque2.cli._common import (
    PlainTable,
    app,
    cli_session_scope,
    cli_span,
    console,
    echo,
    existing_file,
    fail,
    parse_json_object,
    runtime_contract,
)
from tasque2.daemon import control
from tasque2.models import ProviderRun, WorkAttempt, WorkDependency, WorkEvent, WorkItem
from tasque2.ops.reports import ReportService, report_to_json
from tasque2.templates import read_template_file
from tasque2.work.queue import WorkQueue
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner


@app.command("queue")
def queue_work(
    title: Annotated[str, typer.Argument(help="Short title.")],
    instruction: Annotated[str | None, typer.Argument(help="Instruction for the worker.")] = None,
    template: Annotated[Path | None, typer.Option("--template", "-t", help="Markdown instruction file.")] = None,
    lane: Annotated[str | None, typer.Option("--lane", help="Lane the work belongs to.")] = None,
    profile: Annotated[str | None, typer.Option("--profile", help="Model profile: low, medium, high, ultra.")] = None,
    worker_kind: Annotated[str, typer.Option("--worker-kind", "-w")] = "provider.default",
    context_json: Annotated[str | None, typer.Option("--context-json", help="Task context as a JSON object.")] = None,
    thread: Annotated[str | None, typer.Option("--thread", help="Discord thread id to post the result into.")] = None,
    priority: Annotated[int, typer.Option("--priority", "-p")] = 0,
    max_attempts: Annotated[int, typer.Option("--max-attempts")] = 1,
    idempotency_key: Annotated[str | None, typer.Option("--idempotency-key")] = None,
) -> None:
    """Queue one work item from an instruction or a template file."""
    if bool(instruction) == bool(template):
        raise typer.BadParameter("Provide exactly one of an instruction argument or --template.")
    text = read_template_file(existing_file(template, option="--template")) if template else str(instruction)
    context = parse_json_object(context_json, option="--context-json")
    with cli_span("queue"), cli_session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title=title,
            task_instruction=text,
            worker_kind=worker_kind,
            runtime_contract=runtime_contract(profile=profile),
            context=context,
            priority=priority,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            source_kind="cli",
            source_id=idempotency_key,
            discord_thread_id=thread,
            lane=lane,
        )
        console.print(work.id)


@app.command("list")
def list_work(
    status: Annotated[list[str] | None, typer.Option("--status", "-s")] = None,
    lane: Annotated[str | None, typer.Option("--lane")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 20,
) -> None:
    """List recent work items."""
    with cli_session_scope() as session:
        statement = select(WorkItem).order_by(WorkItem.created_at.desc()).limit(limit)
        if status:
            statement = statement.where(WorkItem.status.in_(status))
        if lane:
            statement = statement.where(WorkItem.lane == lane)
        table = PlainTable("Id", "Status", "Lane", "Attempts", "Created", "Title")
        for work in session.scalars(statement).all():
            table.add_row(
                work.id,
                work.status,
                work.lane or "",
                f"{work.attempt_count}/{work.max_attempts}",
                work.created_at.strftime("%Y-%m-%d %H:%M"),
                work.title,
            )
        console.print(table)


@app.command("show")
def show_work(work_item_id: Annotated[str, typer.Argument(help="Work item id.")]) -> None:
    """Show a work item: state, why it is or is not runnable, its last run, and its events."""
    with cli_session_scope() as session:
        work = session.get(WorkItem, work_item_id)
        if work is None:
            raise fail(f"Unknown work item: {work_item_id}")
        lines = [
            f"id: {work.id}",
            f"status: {work.status}",
            f"lane: {work.lane or ''}",
            f"worker: {work.worker_kind}",
            f"attempts: {work.attempt_count}/{work.max_attempts}",
        ]
        if work.not_before:
            lines.append(f"not before: {work.not_before.isoformat()}")
        if work.runtime_contract:
            lines.append(f"contract: {work.runtime_contract}")
        for dependency in session.scalars(
            select(WorkDependency).where(WorkDependency.blocked_work_item_id == work.id)
        ).all():
            upstream = (
                session.get(WorkItem, dependency.dependency_work_item_id)
                if dependency.dependency_work_item_id
                else None
            )
            target = dependency.dependency_work_item_id or dependency.dependency_workflow_node_id
            lines.append(
                f"waits for {target} to be {dependency.condition} (now {upstream.status if upstream else 'missing'})"
            )
        attempt = session.scalar(
            select(WorkAttempt).where(WorkAttempt.work_item_id == work.id).order_by(WorkAttempt.attempt_number.desc())
        )
        if attempt is not None:
            lines.append(f"last attempt: #{attempt.attempt_number} {attempt.status}")
            if attempt.error_message:
                lines.append(f"last error: {attempt.error_message}")
            run = session.scalar(select(ProviderRun).where(ProviderRun.attempt_id == attempt.id))
            if run is not None:
                usage = run.usage or {}
                lines.append(
                    f"provider: {run.provider} {usage.get('model') or run.model or ''} "
                    f"input={usage.get('input_tokens', 0)} cache_read={usage.get('cache_read_tokens', 0)} "
                    f"output={usage.get('output_tokens', 0)} cost=${float(usage.get('estimated_cost_usd') or 0):.2f}"
                )
        console.print(f"[bold]{escape(work.title)}[/bold]")
        echo("\n".join(lines))
        table = PlainTable("When", "Event", "Summary")
        for event in WorkRepository(session).list_events_for_work(work.id):
            table.add_row(event.created_at.strftime("%m-%d %H:%M:%S"), event.event_type, event.summary or "")
        console.print(table)


@app.command("events")
def events(
    work_item_id: Annotated[str | None, typer.Option("--work-item-id")] = None,
    workflow_run_id: Annotated[str | None, typer.Option("--workflow-run-id")] = None,
    event_type: Annotated[str | None, typer.Option("--type")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
) -> None:
    """List recent events."""
    with cli_session_scope() as session:
        statement = select(WorkEvent).order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc()).limit(limit)
        if work_item_id:
            statement = statement.where(WorkEvent.work_item_id == work_item_id)
        if workflow_run_id:
            statement = statement.where(WorkEvent.workflow_run_id == workflow_run_id)
        if event_type:
            statement = statement.where(WorkEvent.event_type == event_type)
        table = PlainTable("When", "Type", "Entity", "Summary")
        for event in session.scalars(statement).all():
            table.add_row(
                event.created_at.strftime("%m-%d %H:%M:%S"),
                event.event_type,
                f"{event.entity_kind}:{event.entity_id}",
                event.summary or "",
            )
        console.print(table)


@app.command("run-next")
def run_next(
    force: Annotated[bool, typer.Option("--force", help="Run even while a daemon is alive.")] = False,
) -> None:
    """Claim and run the next ready work item in this process."""
    with cli_span("run-next"), cli_session_scope() as session:
        reason = control.live_daemon_reason()
        if reason is not None and not force:
            raise fail(f"A daemon is alive ({reason}) and claims ready work itself. Pass --force to run one anyway.")
        outcome = WorkRunner(session, lease_owner="cli").run_next()
        if outcome is None:
            console.print("No ready work.")
            return
        console.print(f"{outcome.status}: {outcome.work_item_id}")
        echo(outcome.summary)


@app.command("pause")
def pause_work(work_item_id: Annotated[str, typer.Argument()]) -> None:
    """Pause a work item that has not finished."""
    _transition(work_item_id, "pause")


@app.command("resume")
def resume_work(work_item_id: Annotated[str, typer.Argument()]) -> None:
    """Resume a paused work item."""
    _transition(work_item_id, "resume")


@app.command("cancel")
def cancel_work(work_item_id: Annotated[str, typer.Argument()]) -> None:
    """Cancel a work item, or request cancellation when it is running."""
    _transition(work_item_id, "cancel")


@app.command("retry")
def retry_work(work_item_id: Annotated[str, typer.Argument()]) -> None:
    """Return a dead-lettered work item to the queue."""
    _transition(work_item_id, "retry")


@app.command("report-work")
def report_work(
    work_item_id: Annotated[str, typer.Argument()],
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Render a work item's report."""
    with cli_session_scope() as session:
        report = ReportService(session).work_report(work_item_id)
    echo(report_to_json(report) if as_json else report.body)


def _transition(work_item_id: str, action: str) -> None:
    with cli_session_scope() as session:
        queue = WorkQueue(session)
        operations = {
            "pause": queue.pause_work,
            "resume": queue.resume_work,
            "cancel": queue.request_cancel,
            "retry": queue.retry_dead_letter,
        }
        work = operations[action](work_item_id)
        console.print(f"{work.id}: {work.status}")
