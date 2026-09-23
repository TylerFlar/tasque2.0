from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any

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
    error_message,
    existing_file,
    fail,
    parse_json_object,
)
from tasque2.models import WorkflowDefinition, WorkflowNode, WorkflowRun
from tasque2.workflows import WorkflowService, parse_definition_file


@app.command("workflow-register")
def workflow_register(
    paths: Annotated[list[Path], typer.Argument(help="Workflow JSON files.")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show each node's changes without registering.")] = False,
) -> None:
    """Register or update workflow definitions from JSON files, showing what changes per node."""
    files = [existing_file(path, option="path") for path in paths]
    with cli_session_scope() as session:
        service = WorkflowService(session)
        for path in files:
            data = service.parse_definition_file(path)
            name, version = str(data["name"]), str(data.get("version", "1"))
            stored = session.scalar(
                select(WorkflowDefinition).where(WorkflowDefinition.name == name, WorkflowDefinition.version == version)
            )
            echo(f"{path.name}: {name}@{version} " + (f"(registered {stored.id[:8]})" if stored else "(new)"))
            for line in node_changes(stored.definition if stored else None, dict(data["definition"])):
                echo(f"  {line}")
            if not dry_run:
                definition = service.load_definition_file(path)
                echo(f"  registered {definition.id}")
        if dry_run:
            session.rollback()


_TEXT_KEYS = ("task_instruction", "child_task_instruction_template")
_COMPARED_KEYS = (
    "kind",
    "title",
    "task_template_path",
    "child_task_template_path",
    "worker_kind",
    "child_worker_kind",
    "runtime_contract",
    "context",
    "depends_on",
    "items",
    "items_from_output",
    "max_attempts",
)


def node_changes(stored: dict[str, Any] | None, parsed: dict[str, Any]) -> list[str]:
    """One line per node: new (+), removed (-), changed (~, with what changed) or unchanged (=)."""
    before = {str(node.get("key")): node for node in (stored or {}).get("nodes", []) if isinstance(node, dict)}
    after = {str(node.get("key")): node for node in parsed.get("nodes", []) if isinstance(node, dict)}
    lines: list[str] = []
    for key, node in after.items():
        old = before.get(key)
        if old is None:
            lines.append(f"+ {key}")
            continue
        changes = [
            _field_change(field, old.get(field), node.get(field))
            for field in _COMPARED_KEYS
            if old.get(field) != node.get(field)
        ]
        changes += [
            f"{field}: {len(str(old.get(field) or '')):,} -> {len(str(node.get(field) or '')):,} chars"
            for field in _TEXT_KEYS
            if str(old.get(field) or "") != str(node.get(field) or "")
        ]
        lines.append(f"~ {key}: " + "; ".join(changes) if changes else f"= {key}")
    lines.extend(f"- {key}" for key in before.keys() - after.keys())
    return lines


def _field_change(field: str, before: Any, after: Any) -> str:
    old, new = json.dumps(before, ensure_ascii=False), json.dumps(after, ensure_ascii=False)
    if len(old) + len(new) > 200:
        return f"{field} changed ({len(old):,} -> {len(new):,} chars)"
    return f"{field}: {old} -> {new}"


@app.command("workflow-validate")
def workflow_validate(paths: Annotated[list[Path], typer.Argument(help="Workflow JSON files.")]) -> None:
    """Check workflow JSON files without registering them."""
    failed = False
    for path in paths:
        try:
            data = parse_definition_file(path)
        except (OSError, ValueError, KeyError) as exc:
            console.print(f"[red]{escape(f'{path}: {error_message(exc)}')}[/red]")
            failed = True
            continue
        echo(f"{path}: {data['name']}@{data.get('version', '1')} ok")
    if failed:
        raise typer.Exit(code=1)


@app.command("workflow-start")
def workflow_start(
    workflow: Annotated[str, typer.Argument(help="Definition name or id, or a workflow JSON file.")],
    input_json: Annotated[str | None, typer.Option("--input-json")] = None,
    run_name: Annotated[str | None, typer.Option("--run-name")] = None,
    thread: Annotated[str | None, typer.Option("--thread", help="Discord thread id for the run.")] = None,
) -> None:
    """Start a workflow run."""
    with cli_span("workflow-start"), cli_session_scope() as session:
        service = WorkflowService(session)
        if Path(workflow).suffix == ".json" and Path(workflow).is_file():
            definition = service.load_definition_file(Path(workflow))
        else:
            definition = session.get(WorkflowDefinition, workflow) or session.scalar(
                select(WorkflowDefinition)
                .where(WorkflowDefinition.name == workflow)
                .order_by(WorkflowDefinition.created_at.desc())
            )
        if definition is None:
            raise fail(f"Unknown workflow: {workflow}")
        run = service.start_run(
            workflow_definition_id=definition.id,
            name=run_name,
            input=parse_json_object(input_json, option="--input-json"),
            discord_thread_id=thread,
        )
        console.print(run.id)


@app.command("workflow-list")
def workflow_list() -> None:
    """List workflow definitions."""
    with cli_session_scope() as session:
        table = PlainTable("Id", "On", "Name", "Version", "Nodes")
        for definition in session.scalars(select(WorkflowDefinition).order_by(WorkflowDefinition.name)).all():
            nodes = (definition.definition or {}).get("nodes", [])
            table.add_row(
                definition.id[:8],
                "yes" if definition.enabled else "no",
                definition.name,
                definition.version,
                str(len(nodes) if isinstance(nodes, list) else 0),
            )
        console.print(table)


@app.command("workflow-runs")
def workflow_runs(
    status: Annotated[list[str] | None, typer.Option("--status", "-s")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 20,
) -> None:
    """List recent workflow runs."""
    with cli_session_scope() as session:
        statement = select(WorkflowRun).order_by(WorkflowRun.created_at.desc()).limit(limit)
        if status:
            statement = statement.where(WorkflowRun.status.in_(status))
        table = PlainTable("Id", "Status", "Started", "Name")
        for run in session.scalars(statement).all():
            started = run.started_at.strftime("%m-%d %H:%M") if run.started_at else ""
            table.add_row(run.id, run.status, started, run.name)
        console.print(table)


@app.command("workflow-show")
def workflow_show(workflow_run_id: Annotated[str, typer.Argument()]) -> None:
    """Show a workflow run and its nodes."""
    with cli_session_scope() as session:
        run = session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise fail(f"Unknown workflow run: {workflow_run_id}")
        console.print(f"[bold]{escape(run.name)}[/bold]")
        echo(f"id: {run.id}\nstatus: {run.status}")
        table = PlainTable("Node", "Kind", "Status", "Work item", "Failure")
        nodes = session.scalars(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id).order_by(WorkflowNode.created_at)
        ).all()
        for node in nodes:
            table.add_row(node.node_key, node.kind, node.status, node.work_item_id or "", node.failure_reason or "")
        console.print(table)


@app.command("workflow-answer")
def workflow_answer(
    workflow_run_id: Annotated[str, typer.Argument()],
    node_key: Annotated[str, typer.Argument(help="Gate node key.")],
    answer: Annotated[str, typer.Argument()],
) -> None:
    """Answer a workflow gate that is waiting for input."""
    with cli_session_scope() as session:
        node = WorkflowService(session).answer_gate(workflow_run_id=workflow_run_id, node_key=node_key, answer=answer)
        echo(f"{node.node_key}: {node.status}")


@app.command("workflow-cancel")
def workflow_cancel(workflow_run_id: Annotated[str, typer.Argument()]) -> None:
    """Cancel a workflow run and its unfinished work."""
    with cli_session_scope() as session:
        run = WorkflowService(session).cancel_run(workflow_run_id)
        console.print(f"{run.id}: {run.status}")
