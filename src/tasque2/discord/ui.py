"""Discord embeds and buttons: the ops panel, workflow status panels, sticky notes, and work controls."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.models import WorkflowNode, WorkflowRun, WorkItem
from tasque2.ops.reports import ReportService
from tasque2.ops.status import SystemStatus
from tasque2.sticky import StickyView
from tasque2.work.queue import WorkQueue
from tasque2.workflows import WorkflowService

CUSTOM_ID_PREFIX = "t2"
CONTROL_PANEL_ENTITY_ID = "discord-control-panel"
CONTROL_PANEL_VERSION = 3
EMBED_DESCRIPTION_LIMIT = 4096
EMBED_FIELD_LIMIT = 1024
COLOR_OK = 0x2ECC71
COLOR_WARN = 0xF1C40F
COLOR_ALERT = 0xE74C3C
COLOR_RUNNING = 0x5865F2
COLOR_IDLE = 0x95A5A6
_NODE_MARKERS = {
    "pending": ".",
    "enqueued": "ready",
    "ready": "ready",
    "running": ">",
    "awaiting_input": "?",
    "succeeded": "ok",
    "failed": "x",
    "failed_tolerated": "!",
    "canceled": "#",
}


@dataclass(frozen=True)
class DiscordUIAction:
    scope: str
    action: str
    entity_id: str | None = None


def make_custom_id(scope: str, action: str, entity_id: str | None = None) -> str:
    value = f"{CUSTOM_ID_PREFIX}:{scope}:{action}" + (f":{entity_id}" if entity_id is not None else "")
    if len(value) > 100:
        raise ValueError("Discord component custom_id must be 100 characters or fewer.")
    return value


def parse_custom_id(custom_id: str) -> DiscordUIAction | None:
    parts = custom_id.split(":")
    if len(parts) not in {3, 4} or parts[0] != CUSTOM_ID_PREFIX:
        return None
    return DiscordUIAction(scope=parts[1], action=parts[2], entity_id=parts[3] if len(parts) == 4 else None)


def is_modal_action(action: DiscordUIAction) -> bool:
    return action.scope == "workflow" and action.action == "answer"


def build_work_controls_view(work_item: WorkItem) -> Any | None:
    buttons: list[tuple[str, str, str, int | None]] = []
    if work_item.status in {"ready", "running", "cancel_requested"}:
        buttons += [("Pause", "secondary", "pause", None), ("Cancel", "danger", "cancel", None)]
    elif work_item.status == "paused":
        buttons += [("Resume", "success", "resume", None), ("Cancel", "danger", "cancel", None)]
    elif work_item.status == "dead_letter":
        buttons.append(("Retry", "success", "retry", None))
    buttons += [("Show", "secondary", "show", 1), ("Report", "secondary", "report", 1)]
    return _view("work", work_item.id, buttons)


def build_workflow_controls_view(run: WorkflowRun) -> Any | None:
    buttons: list[tuple[str, str, str, int | None]] = []
    if run.status == "awaiting_input":
        buttons += [
            ("Answer", "success", "answer", None),
            ("Pause", "secondary", "pause", None),
            ("Cancel", "danger", "cancel", None),
        ]
    elif run.status == "paused":
        buttons += [("Resume", "success", "resume", None), ("Cancel", "danger", "cancel", None)]
    elif run.status not in {"completed", "failed", "canceled"}:
        buttons += [("Pause", "secondary", "pause", None), ("Cancel", "danger", "cancel", None)]
    return _view("workflow", run.id, buttons)


def build_ops_embed(status: SystemStatus) -> dict[str, Any]:
    ready, running = status.work_items.get("ready", 0), status.work_items.get("running", 0)
    paused, dead = status.work_items.get("paused", 0), status.work_items.get("dead_letter", 0)
    jobs = " - ".join(
        part
        for part in (
            f"ready **{ready}**",
            f"running **{running}**",
            f"paused **{paused}**" if paused else "",
            f"dead letter **{dead}**" if dead else "",
        )
        if part
    )
    active_runs = {
        key: value
        for key, value in status.workflow_runs.items()
        if key not in {"completed", "failed", "canceled"} and value
    }
    fields = [
        {"name": "Jobs", "value": jobs if (ready or running or paused or dead) else "_(idle)_", "inline": False},
        {
            "name": "In flight",
            "value": f"running **{status.running_work}**" if status.running_work else "_(none)_",
            "inline": False,
        },
        {"name": "Workflows", "value": _format_counts(active_runs) if active_runs else "_(idle)_", "inline": False},
        {"name": "Schedules", "value": f"enabled **{status.schedules_enabled}**", "inline": False},
        {
            "name": "DLQ",
            "value": f"unresolved **{status.failed_work_unresolved}**" if status.failed_work_unresolved else "_(none)_",
            "inline": False,
        },
    ]
    if status.failed_work_unresolved:
        color = COLOR_ALERT
    elif status.ready_work or status.running_work or active_runs:
        color = COLOR_WARN
    else:
        color = COLOR_OK
    return {
        "title": "tasque ops panel",
        "color": color,
        "fields": fields,
        "footer": {"text": "updates when state changes"},
    }


def build_workflow_status_panel_embed(run: WorkflowRun, nodes: list[WorkflowNode]) -> dict[str, Any]:
    counts = _count_by_status(nodes)
    parts = [_summary_line(counts)]
    in_flight = [node.node_key for node in nodes if effective_node_status(node) in {"running", "awaiting_input"}]
    if in_flight:
        parts.append("Now: " + ", ".join(f"`{key}`" for key in in_flight[:5]))
    tree = _tree_lines(nodes)
    parts.append(_truncate_lines(tree, EMBED_DESCRIPTION_LIMIT - 600) if tree else "_(no nodes yet)_")
    failed = [node for node in nodes if effective_node_status(node) in {"failed", "canceled"}]
    if failed:
        parts.append(f"Failure on `{failed[0].node_key}`: {(failed[0].failure_reason or failed[0].status)[:200]}")
    fields = [
        {"name": "chain_id", "value": run.id, "inline": True},
        {"name": "status", "value": run.status, "inline": True},
    ]
    if run.started_at:
        fields.append({"name": "started", "value": run.started_at.isoformat(timespec="seconds"), "inline": True})
    if run.ended_at:
        fields.append({"name": "ended", "value": run.ended_at.isoformat(timespec="seconds"), "inline": True})
    if run.status == "completed":
        color = COLOR_OK
    elif run.status in {"failed", "canceled"} or counts.get("failed"):
        color = COLOR_ALERT
    elif run.status in {"awaiting_input", "paused"}:
        color = COLOR_WARN
    else:
        color = COLOR_RUNNING if run.status == "active" else COLOR_IDLE
    return {
        "title": f"Chain: {run.name} - {run.status}"[:256],
        "description": "\n\n".join(parts)[:EMBED_DESCRIPTION_LIMIT],
        "color": color,
        "fields": fields,
    }


def build_sticky_embed(view: StickyView) -> dict[str, Any]:
    """A thread's sticky note: the notes kept for the user, then the thread's upcoming runs."""
    embed: dict[str, Any] = {"title": "Sticky note", "color": COLOR_IDLE}
    lines = view.lines()
    if view.notes:
        embed["description"] = view.notes[:EMBED_DESCRIPTION_LIMIT]
    elif not lines:
        embed["description"] = "_(nothing here)_"
    if lines:
        embed["fields"] = [{"name": "Coming up", "value": "\n".join(lines)[:EMBED_FIELD_LIMIT], "inline": False}]
    if view.notes_updated_at is not None:
        embed["footer"] = {"text": "notes updated"}
        embed["timestamp"] = view.notes_updated_at.isoformat()
    return embed


class DiscordUIService:
    """Button and modal actions from the work and workflow controls."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def handle_action(self, action: DiscordUIAction) -> str:
        if not action.entity_id:
            raise ValueError(f"{action.scope}:{action.action} requires an entity id.")
        if action.scope == "work":
            return self._work_action(action.action, action.entity_id)
        if action.scope == "workflow":
            return self._workflow_action(action.action, action.entity_id)
        return f"Unknown Tasque action: {action.scope}:{action.action}"

    def answer_gate(self, *, workflow_run_id: str, answer: str, node_key: str | None = None) -> str:
        key = (node_key or "").strip()
        if not key:
            gates = self.session.scalars(
                select(WorkflowNode).where(
                    WorkflowNode.workflow_run_id == workflow_run_id,
                    WorkflowNode.kind == "gate",
                    WorkflowNode.status == "awaiting_input",
                )
            ).all()
            if len(gates) != 1:
                raise ValueError("Name the gate: there is not exactly one open gate.")
            key = gates[0].node_key
        node = WorkflowService(self.session).answer_gate(workflow_run_id=workflow_run_id, node_key=key, answer=answer)
        return f"Answered workflow gate `{node.node_key}`."

    def _work_action(self, action: str, work_item_id: str) -> str:
        queue = WorkQueue(self.session)
        transitions = {
            "pause": queue.pause_work,
            "resume": queue.resume_work,
            "cancel": queue.request_cancel,
            "retry": queue.retry_dead_letter,
        }
        if action in transitions:
            work = transitions[action](work_item_id)
            return f"`{work.id}` is {work.status}."
        if action == "show":
            work = self.session.get(WorkItem, work_item_id)
            if work is None:
                raise KeyError(f"Unknown work item: {work_item_id}")
            return _truncate(
                f"Work: {work.title}\nID: {work.id}\nLane: {work.lane or '-'}\nStatus: {work.status}\n"
                f"Worker: {work.worker_kind}\nAttempts: {work.attempt_count}/{work.max_attempts}"
            )
        if action == "report":
            return _truncate(ReportService(self.session).work_report(work_item_id).body)
        return f"Unknown work action: {action}"

    def _workflow_action(self, action: str, workflow_run_id: str) -> str:
        service = WorkflowService(self.session)
        transitions = {"pause": service.pause_run, "resume": service.resume_run, "cancel": service.cancel_run}
        if action in transitions:
            run = transitions[action](workflow_run_id)
            return f"`{run.id}` is {run.status}."
        if action == "report":
            return _truncate(ReportService(self.session).workflow_report(workflow_run_id).body)
        return f"Unknown workflow action: {action}"


def effective_node_status(node: WorkflowNode) -> str:
    work = node.work_item
    if work is None or node.status != "enqueued":
        return node.status
    if work.status == "dead_letter":
        return "failed_tolerated" if (node.definition or {}).get("tolerate_failure") else "failed"
    if work.status in {"succeeded", "canceled", "running", "ready"}:
        return work.status
    return node.status


def _view(scope: str, entity_id: str, buttons: list[tuple[str, str, str, int | None]]) -> Any | None:
    try:
        import discord
    except ImportError:
        return None
    view = discord.ui.View(timeout=None)
    for label, style, action, row in buttons:
        view.add_item(
            discord.ui.Button(
                label=label,
                style=getattr(discord.ButtonStyle, style),
                custom_id=make_custom_id(scope, action, entity_id),
                row=row,
            )
        )
    return view if view.children else None


def _count_by_status(nodes: list[WorkflowNode]) -> dict[str, int]:
    counts: dict[str, int] = {"total": len(nodes)}
    for node in nodes:
        status = effective_node_status(node)
        counts[status] = counts.get(status, 0) + 1
    return counts


def _summary_line(counts: dict[str, int]) -> str:
    parts = [f"step **{counts.get('succeeded', 0)}/{counts.get('total', 0)}**"]
    for label, value in (
        ("in flight", counts.get("running", 0) + counts.get("awaiting_input", 0)),
        ("ready", counts.get("enqueued", 0) + counts.get("ready", 0)),
        ("pending", counts.get("pending", 0)),
        ("failed", counts.get("failed", 0)),
        ("canceled", counts.get("canceled", 0)),
    ):
        if value:
            parts.append(f"{label} **{value}**")
    return " - ".join(parts)


def _tree_lines(nodes: list[WorkflowNode]) -> list[str]:
    ids = {node.id for node in nodes}
    children: dict[str | None, list[WorkflowNode]] = {}
    for node in nodes:
        children.setdefault(node.parent_node_id if node.parent_node_id in ids else None, []).append(node)
    for siblings in children.values():
        siblings.sort(
            key=lambda node: (
                node.fanout_index if node.fanout_index is not None else -1,
                node.created_at,
                node.node_key,
            )
        )
    lines: list[str] = []

    def append(node: WorkflowNode, level: int, visiting: frozenset[str]) -> None:
        if node.id in visiting:
            return
        suffix = f" (err: {node.failure_reason.splitlines()[0][:60]})" if node.failure_reason else ""
        marker = _NODE_MARKERS.get(effective_node_status(node), "?")
        lines.append(f"{'  ' * level}{marker} `{node.node_key}` _{node.kind}_{suffix}")
        for child in children.get(node.id, []):
            append(child, level + 1, visiting | {node.id})

    for root in children.get(None, []):
        append(root, 0, frozenset())
    return lines


def _truncate_lines(lines: list[str], limit: int) -> str:
    output: list[str] = []
    used = 0
    for line in lines:
        if used + len(line) + 1 > limit - 32:
            output.append(f"... +{len(lines) - len(output)} more steps")
            break
        output.append(line)
        used += len(line) + 1
    return "\n".join(output)


def _format_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items())) or "(none)"


def _truncate(content: str, limit: int = 1900) -> str:
    return content if len(content) <= limit else content[: limit - 20] + "\n[truncated]"
