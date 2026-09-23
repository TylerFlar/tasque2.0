from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.text import Text

from tasque2.cli._common import PlainTable, app, cli_session_scope, console, echo, fail


@app.command("doctrine-export")
def doctrine_export(
    directory: Annotated[Path, typer.Argument(help="Directory to write <namespace>/<key>.md files into.")],
    namespace: Annotated[list[str] | None, typer.Option("--namespace", "-n")] = None,
    include_secrets: Annotated[
        bool, typer.Option("--include-secrets", help="Also write documents that hold credentials.")
    ] = False,
) -> None:
    """Write canonical documents to files for editing, with a snapshot for doctrine-apply."""
    from tasque2.ops.doctrine import export_doctrine

    target = directory.expanduser().resolve()
    with cli_session_scope() as session:
        entries = export_doctrine(session, target, namespaces=namespace or None, include_secrets=include_secrets)
    echo(f"exported {len(entries)} documents to {target}")


@app.command("doctrine-apply")
def doctrine_apply(
    directory: Annotated[Path, typer.Argument(help="A directory written by doctrine-export.")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show what would change.")] = False,
) -> None:
    """Apply edited documents; any document changed since the export is skipped as drifted."""
    from tasque2.ops.doctrine import SNAPSHOT_FILE, apply_doctrine

    source = directory.expanduser().resolve()
    if not (source / SNAPSHOT_FILE).is_file():
        raise fail(f"{source} has no {SNAPSHOT_FILE}; export the documents with doctrine-export first.")
    with cli_session_scope() as session:
        changes = apply_doctrine(session, source, dry_run=dry_run)
        if dry_run:
            session.rollback()
    table = PlainTable("Document", "Status", "Detail")
    colors = {"applied": "green", "created": "green", "retired": "cyan", "unchanged": "dim", "drifted": "yellow"}
    for change in changes:
        status = (
            f"would be {change.status}"
            if dry_run and change.status in {"applied", "created", "retired"}
            else change.status
        )
        table.add_row(
            f"{change.namespace}/{change.canonical_key}",
            Text(status, style=colors.get(change.status, "red")),
            change.detail,
        )
    console.print(table)


@app.command("lanes-apply")
def lanes_apply(
    manifest: Annotated[Path, typer.Argument(help="JSON manifest of schedule, workflow and thread settings.")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show what would change.")] = False,
) -> None:
    """Apply a lane manifest: tiers, lane-file bindings, and schedules on or off."""
    from tasque2.ops.lanes import LaneManifestError, apply_lane_tiers, load_manifest

    if not manifest.is_file():
        raise fail(f"Manifest not found: {manifest}")
    with cli_session_scope() as session:
        try:
            changes = apply_lane_tiers(session, load_manifest(manifest), dry_run=dry_run)
        except LaneManifestError as exc:
            raise fail(str(exc)) from None
        if dry_run:
            session.rollback()
    table = PlainTable("Target", "Field", "From", "To")
    for change in changes:
        table.add_row(
            change.target,
            change.field,
            change.old or "default",
            change.new or "default",
            style="" if change.changed else "dim",
        )
    console.print(table)
    changed = sum(1 for change in changes if change.changed)
    echo(f"{'would change' if dry_run else 'changed'} {changed} of {len(changes)} settings")


@app.command("packet")
def packet(
    work_item_id: Annotated[str | None, typer.Argument(help="An existing work item.")] = None,
    schedule: Annotated[str | None, typer.Option("--schedule", help="What this schedule's next run would see.")] = None,
    thread: Annotated[str | None, typer.Option("--thread", help="What a reply in this thread would see.")] = None,
    as_json: Annotated[bool, typer.Option("--json", help="Print the whole packet.")] = False,
) -> None:
    """Show the context packet a run starts from: its sections, pinned documents and their sizes.

    With --schedule or --thread nothing is queued or recorded: the run is built and rolled back.
    """
    from sqlalchemy import select

    from tasque2.cli._common import emit_json
    from tasque2.discord.routing import DiscordService
    from tasque2.models import Schedule, WorkflowNode, WorkItem, utc_now
    from tasque2.schedules import ScheduleService
    from tasque2.worker.context import WorkerContextBuilder
    from tasque2.worker.prompt import render_user_prompt
    from tasque2.workflows import WorkflowService

    if sum(value is not None for value in (work_item_id, schedule, thread)) != 1:
        raise typer.BadParameter("Give exactly one of a work item id, --schedule or --thread.")
    with cli_session_scope() as session:
        if work_item_id:
            items = [session.get(WorkItem, work_item_id)]
        elif schedule:
            row = session.scalar(select(Schedule).where((Schedule.id == schedule) | (Schedule.name == schedule)))
            if row is None:
                raise fail(f"Unknown schedule: {schedule}")
            occurrence = ScheduleService(session).fire_schedule_now(row.id)
            if occurrence.work_item_id:
                items = [session.get(WorkItem, occurrence.work_item_id)]
            else:
                WorkflowService(session).tick_runs()
                nodes = session.scalars(
                    select(WorkflowNode).where(WorkflowNode.workflow_run_id == occurrence.workflow_run_id)
                ).all()
                items = [session.get(WorkItem, node.work_item_id) for node in nodes if node.work_item_id]
        else:
            result = DiscordService(session).handle_thread_reply(
                discord_message_id=f"packet-preview-{utc_now().timestamp()}",
                discord_channel_id=str(thread),
                discord_thread_id=str(thread),
                author="packet-preview",
                content="Status check.",
            )
            if result.entity_id is None:
                raise fail(f"Thread {thread} does not route replies to work ({result.action}).")
            items = [session.get(WorkItem, result.entity_id)]
        builder = WorkerContextBuilder(session)
        for item in items:
            if item is None:
                raise fail("Unknown work item.")
            context_packet = builder.build_for_work(item)
            prompt = render_user_prompt(
                task_instruction=item.task_instruction,
                context_packet=context_packet,
                result_token="preview",
                scratch_dir=None,
            )
            if as_json:
                emit_json({"work_item_id": item.id, "prompt_chars": len(prompt), "packet": context_packet})
                continue
            _print_packet(item, context_packet, len(prompt))
        session.rollback()


def _print_packet(item, context_packet: dict, prompt_chars: int) -> None:
    import json

    def size(value) -> int:
        return len(json.dumps(value, ensure_ascii=False, default=str))

    template_chars = len(item.task_instruction or "")
    echo(f"{item.title} [{item.lane or 'no lane'}]: prompt {prompt_chars:,} chars, template {template_chars:,}")
    sections = PlainTable("Section", "Chars")
    for key, value in sorted(context_packet.items(), key=lambda pair: -size(pair[1])):
        sections.add_row(key, f"{size(value):,}")
    console.print(sections)
    memories = PlainTable("Memory", "Delivered", "Full", "How")
    for memory in context_packet.get("memories") or []:
        delivered = len(memory.get("content") or "")
        full = memory.get("content_chars", delivered)
        memories.add_row(
            f"{memory['namespace']}/{memory.get('canonical_key') or memory['kind']}",
            f"{delivered:,}",
            f"{full:,}",
            "excerpt" if memory.get("content_compacted") else "whole",
        )
    console.print(memories)
