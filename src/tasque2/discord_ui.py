from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactService
from tasque2.daemon import TasqueDaemon
from tasque2.health import health_report_to_dict, run_doctor
from tasque2.memory import MemoryService
from tasque2.models import Schedule, WorkflowNode, WorkflowRun, WorkItem
from tasque2.queue import WorkQueue
from tasque2.repo import WorkRepository
from tasque2.reports import ReportService
from tasque2.runtime import WorkRunner
from tasque2.scheduler import ScheduleService
from tasque2.status import SystemStatus, get_system_status
from tasque2.workflows import WorkflowService

CUSTOM_ID_PREFIX = "t2"
CONTROL_PANEL_ENTITY_ID = "discord-control-panel"
CONTROL_PANEL_VERSION = 3
STATIC_CUSTOM_IDS = {
    "status": "t2:system:status",
    "doctor": "t2:system:doctor",
    "run_next": "t2:system:run_next",
    "daemon_tick": "t2:system:daemon_tick",
    "workflow_tick": "t2:workflow:tick",
    "work_new": "t2:work:new",
    "schedule_new": "t2:schedule:new",
    "schedule_poll": "t2:schedule:poll",
    "schedule_list": "t2:schedule:list",
    "artifact_search": "t2:artifact:search",
}


@dataclass(frozen=True)
class DiscordUIAction:
    scope: str
    action: str
    entity_id: str | None = None


@dataclass(frozen=True)
class DiscordUIResult:
    content: str
    refresh_panel: bool = False


def make_custom_id(scope: str, action: str, entity_id: str | None = None) -> str:
    value = f"{CUSTOM_ID_PREFIX}:{scope}:{action}"
    if entity_id is not None:
        value = f"{value}:{entity_id}"
    if len(value) > 100:
        raise ValueError("Discord component custom_id must be 100 characters or fewer.")
    return value


def parse_custom_id(custom_id: str) -> DiscordUIAction | None:
    parts = custom_id.split(":")
    if len(parts) not in {3, 4} or parts[0] != CUSTOM_ID_PREFIX:
        return None
    return DiscordUIAction(
        scope=parts[1],
        action=parts[2],
        entity_id=parts[3] if len(parts) == 4 else None,
    )


def is_modal_action(action: DiscordUIAction) -> bool:
    if action.scope == "work" and action.action == "new":
        return True
    if action.scope == "workflow" and action.action == "answer":
        return True
    if action.scope == "schedule" and action.action == "new":
        return True
    return action.scope == "artifact" and action.action == "search"


def build_control_panel_view() -> Any | None:
    return None


def build_work_controls_view(work_item: WorkItem) -> Any | None:
    try:
        import discord
    except Exception:
        return None

    view = discord.ui.View(timeout=None)
    if work_item.status in {"ready", "running", "cancel_requested"}:
        _add_button(
            view,
            "Pause",
            discord.ButtonStyle.secondary,
            make_custom_id("work", "pause", work_item.id),
        )
        _add_button(
            view,
            "Cancel",
            discord.ButtonStyle.danger,
            make_custom_id("work", "cancel", work_item.id),
        )
    elif work_item.status == "paused":
        _add_button(
            view,
            "Resume",
            discord.ButtonStyle.success,
            make_custom_id("work", "resume", work_item.id),
        )
        _add_button(
            view,
            "Cancel",
            discord.ButtonStyle.danger,
            make_custom_id("work", "cancel", work_item.id),
        )
    elif work_item.status == "dead_letter":
        _add_button(
            view,
            "Retry",
            discord.ButtonStyle.success,
            make_custom_id("work", "retry", work_item.id),
        )

    _add_button(
        view,
        "Show",
        discord.ButtonStyle.secondary,
        make_custom_id("work", "show", work_item.id),
        row=1,
    )
    _add_button(
        view,
        "Report",
        discord.ButtonStyle.secondary,
        make_custom_id("work", "report", work_item.id),
        row=1,
    )
    return view if view.children else None


def build_workflow_controls_view(run: WorkflowRun) -> Any | None:
    try:
        import discord
    except Exception:
        return None

    view = discord.ui.View(timeout=None)
    if run.status == "awaiting_input":
        _add_button(
            view,
            "Answer",
            discord.ButtonStyle.success,
            make_custom_id("workflow", "answer", run.id),
        )
        _add_button(
            view,
            "Pause",
            discord.ButtonStyle.secondary,
            make_custom_id("workflow", "pause", run.id),
        )
        _add_button(
            view,
            "Cancel",
            discord.ButtonStyle.danger,
            make_custom_id("workflow", "cancel", run.id),
        )
    elif run.status == "paused":
        _add_button(
            view,
            "Resume",
            discord.ButtonStyle.success,
            make_custom_id("workflow", "resume", run.id),
        )
        _add_button(
            view,
            "Cancel",
            discord.ButtonStyle.danger,
            make_custom_id("workflow", "cancel", run.id),
        )
    elif run.status not in {"completed", "failed", "canceled"}:
        _add_button(
            view,
            "Pause",
            discord.ButtonStyle.secondary,
            make_custom_id("workflow", "pause", run.id),
        )
        _add_button(
            view,
            "Cancel",
            discord.ButtonStyle.danger,
            make_custom_id("workflow", "cancel", run.id),
        )

    return view if view.children else None


COLOR_OK = 0x2ECC71
COLOR_WARN = 0xF1C40F
COLOR_ALERT = 0xE74C3C
COLOR_RUNNING = 0x5865F2
COLOR_IDLE = 0x95A5A6
EMBED_DESCRIPTION_LIMIT = 4096


def build_ops_embed(status: SystemStatus) -> dict[str, Any]:
    fields = [
        {
            "name": "Jobs",
            "value": _format_jobs_block(status),
            "inline": False,
        },
        {
            "name": "In flight",
            "value": _format_in_flight_block(status),
            "inline": False,
        },
        {
            "name": "Workflows",
            "value": _format_workflows_block(status),
            "inline": False,
        },
        {
            "name": "Schedules",
            "value": f"enabled **{status.schedules_enabled}**",
            "inline": False,
        },
        {
            "name": "DLQ",
            "value": _format_dlq_block(status),
            "inline": False,
        },
    ]
    return {
        "title": "tasque ops panel",
        "color": _ops_color(status),
        "fields": fields,
        "footer": {"text": "updates when state changes"},
    }


def build_workflow_status_panel_embed(
    run: WorkflowRun,
    nodes: list[WorkflowNode],
) -> dict[str, Any]:
    counts = _count_by_status(nodes)
    in_flight = [
        node.node_key
        for node in nodes
        if _effective_workflow_node_status(node) in {"running", "awaiting_input"}
    ]
    description_parts = [_workflow_summary_line(counts)]
    if in_flight:
        description_parts.append("Now: " + ", ".join(f"`{key}`" for key in in_flight[:5]))

    tree = _workflow_tree_lines(nodes)
    if tree:
        description_parts.append(_truncate_lines(tree, EMBED_DESCRIPTION_LIMIT - 600))
    else:
        description_parts.append("_(no nodes yet - chain just started)_")

    failed = [
        node
        for node in nodes
        if _effective_workflow_node_status(node) in {"failed", "canceled"}
    ]
    if failed:
        first = failed[0]
        detail = first.failure_reason or first.status
        description_parts.append(f"Failure on `{first.node_key}`: {detail[:200]}")

    fields = [
        {"name": "chain_id", "value": run.id, "inline": True},
        {"name": "status", "value": run.status, "inline": True},
    ]
    if run.started_at:
        fields.append(
            {
                "name": "started",
                "value": run.started_at.isoformat(timespec="seconds"),
                "inline": True,
            }
        )
    if run.ended_at:
        fields.append(
            {
                "name": "ended",
                "value": run.ended_at.isoformat(timespec="seconds"),
                "inline": True,
            }
        )

    return {
        "title": f"Chain: {run.name} - {run.status}"[:256],
        "description": "\n\n".join(description_parts)[:EMBED_DESCRIPTION_LIMIT],
        "color": _workflow_color(run.status, counts),
        "fields": fields,
    }


def _quiet_status_line(status: SystemStatus) -> str:
    active = status.ready_work + status.running_work
    if active == 0 and status.failed_work_unresolved == 0:
        return "Nothing needs attention right now."
    parts = []
    if status.ready_work:
        parts.append(f"{status.ready_work} ready")
    if status.running_work:
        parts.append(f"{status.running_work} running")
    if status.failed_work_unresolved:
        parts.append(f"{status.failed_work_unresolved} failed")
    return "Current state: " + ", ".join(parts) + "."


def _workflow_color(status: str, counts: dict[str, int]) -> int:
    if status == "completed":
        return COLOR_OK
    if status in {"failed", "canceled"} or counts.get("failed", 0):
        return COLOR_ALERT
    if status in {"awaiting_input", "paused"}:
        return COLOR_WARN
    if status == "active":
        return COLOR_RUNNING
    return COLOR_IDLE


def _workflow_summary_line(counts: dict[str, int]) -> str:
    total = counts.get("total", 0)
    done = counts.get("succeeded", 0)
    active = counts.get("running", 0) + counts.get("awaiting_input", 0)
    ready = counts.get("enqueued", 0) + counts.get("ready", 0)
    pending = counts.get("pending", 0)
    failed = counts.get("failed", 0)
    canceled = counts.get("canceled", 0)
    parts = [f"step **{done}/{total}**"]
    if active:
        parts.append(f"in flight **{active}**")
    if ready:
        parts.append(f"ready **{ready}**")
    if pending:
        parts.append(f"pending **{pending}**")
    if failed:
        parts.append(f"failed **{failed}**")
    if canceled:
        parts.append(f"canceled **{canceled}**")
    return " - ".join(parts)


def _workflow_tree_lines(nodes: list[WorkflowNode]) -> list[str]:
    node_ids = {node.id for node in nodes}
    children_by_parent: dict[str | None, list[WorkflowNode]] = {}
    for node in nodes:
        parent_id = node.parent_node_id if node.parent_node_id in node_ids else None
        children_by_parent.setdefault(parent_id, []).append(node)

    def sort_key(node: WorkflowNode) -> tuple[int, object, str]:
        fanout_order = node.fanout_index if node.fanout_index is not None else -1
        return (fanout_order, node.created_at, node.node_key)

    for siblings in children_by_parent.values():
        siblings.sort(key=sort_key)

    lines: list[str] = []

    def append_node(node: WorkflowNode, level: int, visiting: set[str]) -> None:
        if node.id in visiting:
            return
        indent = "  " * level
        suffix = ""
        if node.failure_reason:
            suffix = f" (err: {node.failure_reason.splitlines()[0][:60]})"
        status = _effective_workflow_node_status(node)
        lines.append(f"{indent}{_workflow_node_marker(status)} `{node.node_key}` _{node.kind}_{suffix}")
        for child in children_by_parent.get(node.id, []):
            append_node(child, level + 1, visiting | {node.id})

    for node in children_by_parent.get(None, []):
        append_node(node, 0, set())
    return lines


def _workflow_node_marker(status: str) -> str:
    return {
        "pending": ".",
        "enqueued": "ready",
        "ready": "ready",
        "running": ">",
        "awaiting_input": "?",
        "succeeded": "ok",
        "failed": "x",
        "canceled": "#",
    }.get(status, "?")


def _truncate_lines(lines: list[str], limit: int) -> str:
    output: list[str] = []
    used = 0
    for line in lines:
        added = len(line) + 1
        if used + added > limit - 32:
            output.append(f"... +{len(lines) - len(output)} more steps")
            break
        output.append(line)
        used += added
    return "\n".join(output)


def _ops_color(status: SystemStatus) -> int:
    if status.failed_work_unresolved > 0:
        return COLOR_ALERT
    if (
        status.ready_work > 0
        or status.running_work > 0
        or _active_workflow_count(status) > 0
    ):
        return COLOR_WARN
    return COLOR_OK


def _format_jobs_block(status: SystemStatus) -> str:
    ready = status.work_items.get("ready", 0)
    running = status.work_items.get("running", 0)
    paused = status.work_items.get("paused", 0)
    dead = status.work_items.get("dead_letter", 0)
    canceled = status.work_items.get("canceled", 0)
    if ready == 0 and running == 0 and paused == 0 and dead == 0:
        return "_(idle)_"
    parts = [f"ready **{ready}**", f"running **{running}**"]
    if paused:
        parts.append(f"paused **{paused}**")
    if dead:
        parts.append(f"dead letter **{dead}**")
    if canceled:
        parts.append(f"canceled **{canceled}**")
    return " - ".join(parts)


def _format_in_flight_block(status: SystemStatus) -> str:
    if status.running_work == 0:
        return "_(none)_"
    return f"running **{status.running_work}**"


def _format_workflows_block(status: SystemStatus) -> str:
    active_statuses = {
        key: value
        for key, value in status.workflow_runs.items()
        if key not in {"completed", "failed", "canceled"} and value
    }
    if not active_statuses:
        return "_(idle)_"
    return _format_counts(active_statuses)


def _format_dlq_block(status: SystemStatus) -> str:
    if status.failed_work_unresolved == 0:
        return "_(none)_"
    return f"unresolved **{status.failed_work_unresolved}**"


def _active_workflow_count(status: SystemStatus) -> int:
    return sum(
        value
        for key, value in status.workflow_runs.items()
        if key not in {"completed", "failed", "canceled"}
    )


def _format_utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _add_button(view: Any, label: str, style: Any, custom_id: str, *, row: int | None = None) -> None:
    import discord

    view.add_item(discord.ui.Button(label=label, style=style, custom_id=custom_id, row=row))


class DiscordUIService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def handle_action(self, action: DiscordUIAction) -> DiscordUIResult:
        if action.scope == "system":
            return self._handle_system_action(action)
        if action.scope == "workflow":
            return self._handle_workflow_action(action)
        if action.scope == "work":
            return self._handle_work_action(action)
        if action.scope == "schedule":
            return self._handle_schedule_action(action)
        if action.scope == "artifact":
            return self._handle_artifact_action(action)
        return DiscordUIResult(f"Unknown Tasque action: {action.scope}:{action.action}")

    def create_work(
        self,
        *,
        title: str,
        task_instruction: str,
        worker_kind: str = "manual",
        priority: int = 0,
        discord_interaction_id: str | None = None,
    ) -> DiscordUIResult:
        work = WorkRepository(self.session).create_work_item(
            title=title.strip(),
            task_instruction=task_instruction.strip(),
            worker_kind=worker_kind.strip() or "manual",
            priority=priority,
            source_kind="discord_ui",
            source_id=discord_interaction_id,
            idempotency_key=(
                f"discord:ui:new_work:{discord_interaction_id}"
                if discord_interaction_id
                else None
            ),
        )
        return DiscordUIResult(
            f"Queued work `{work.id}`: {work.title} ({work.worker_kind}).",
            refresh_panel=True,
        )

    def create_schedule(
        self,
        *,
        name: str,
        schedule_type: str,
        expression: str,
        task_instruction: str,
        worker_kind: str = "manual",
    ) -> DiscordUIResult:
        schedule = ScheduleService(self.session).create_schedule(
            name=name.strip(),
            schedule_type=schedule_type.strip(),
            expression=expression.strip(),
            worker_kind=worker_kind.strip() or "manual",
            payload={"title": name.strip(), "task_instruction": task_instruction.strip()},
        )
        return DiscordUIResult(
            f"Created schedule `{schedule.id}`: {schedule.name} ({schedule.schedule_type}).",
            refresh_panel=True,
        )

    def search_artifacts(
        self,
        *,
        query: str | None = None,
        tags: list[str] | None = None,
        limit: int = 10,
    ) -> DiscordUIResult:
        artifacts = ArtifactService(self.session).list_artifacts(
            query=query or None,
            tag=tags or [],
            limit=limit,
        )
        return DiscordUIResult(render_artifact_list(artifacts))

    def handle_workflow_text_action(
        self,
        *,
        workflow_run_id: str,
        action: str,
        value: str,
        node_key: str | None = None,
    ) -> DiscordUIResult:
        if action != "answer":
            raise ValueError(f"Unsupported workflow text action: {action}")
        key = (node_key or "").strip() or self._single_awaiting_gate_key(workflow_run_id)
        node = WorkflowService(self.session).answer_gate(
            workflow_run_id=workflow_run_id,
            node_key=key,
            answer=value,
        )
        return DiscordUIResult(f"Answered workflow gate `{node.node_key}`.", refresh_panel=True)

    def _handle_system_action(self, action: DiscordUIAction) -> DiscordUIResult:
        if action.action == "status":
            return DiscordUIResult(render_status(get_system_status(self.session)))
        if action.action == "doctor":
            report = run_doctor(migrate=False)
            return DiscordUIResult(render_doctor(report=health_report_to_dict(report)))
        if action.action == "run_next":
            outcome = WorkRunner(self.session, lease_owner="discord-ui").run_next()
            if outcome is None:
                return DiscordUIResult("No ready work.", refresh_panel=True)
            return DiscordUIResult(
                f"{outcome.status}: `{outcome.work_item_id}`\n{outcome.summary}",
                refresh_panel=True,
            )
        if action.action == "daemon_tick":
            result = TasqueDaemon(self.session).run_once()
            return DiscordUIResult(
                "\n".join(
                    [
                        "Daemon tick complete.",
                        f"Recovered leases: {result.recovered_leases}",
                        f"Scheduled launches: {result.scheduled_work}",
                        f"Workflow changes: {result.workflow_runs_changed}",
                        f"Work ran: {result.work_items_ran}",
                    ]
                ),
                refresh_panel=True,
            )
        return DiscordUIResult(f"Unknown system action: {action.action}")

    def _handle_work_action(self, action: DiscordUIAction) -> DiscordUIResult:
        work_item_id = _require_entity_id(action)
        queue = WorkQueue(self.session)
        if action.action == "pause":
            work = queue.pause_work(work_item_id)
            return DiscordUIResult(f"`{work.id}` is {work.status}.", refresh_panel=True)
        if action.action == "resume":
            work = queue.resume_work(work_item_id)
            return DiscordUIResult(f"`{work.id}` is {work.status}.", refresh_panel=True)
        if action.action == "cancel":
            work = queue.request_cancel(work_item_id)
            return DiscordUIResult(f"`{work.id}` is {work.status}.", refresh_panel=True)
        if action.action == "retry":
            work = queue.retry_dead_letter(work_item_id)
            return DiscordUIResult(f"`{work.id}` is {work.status}.", refresh_panel=True)
        if action.action == "show":
            work = self.session.get(WorkItem, work_item_id)
            if work is None:
                raise KeyError(f"Unknown work item: {work_item_id}")
            return DiscordUIResult(render_work_summary(work))
        if action.action == "report":
            report = ReportService(self.session).work_report(work_item_id)
            return DiscordUIResult(_truncate(report.body))
        return DiscordUIResult(f"Unknown work action: {action.action}")

    def _handle_workflow_action(self, action: DiscordUIAction) -> DiscordUIResult:
        if action.action == "tick":
            changed = WorkflowService(self.session).tick_runs()
            return DiscordUIResult(f"Workflow tick changed {changed} run(s).", refresh_panel=True)

        workflow_run_id = _require_entity_id(action)
        service = WorkflowService(self.session)
        if action.action == "pause":
            run = service.pause_run(workflow_run_id)
            return DiscordUIResult(f"`{run.id}` is {run.status}.", refresh_panel=True)
        if action.action == "resume":
            run = service.resume_run(workflow_run_id)
            return DiscordUIResult(f"`{run.id}` is {run.status}.", refresh_panel=True)
        if action.action == "cancel":
            run = service.cancel_run(workflow_run_id)
            return DiscordUIResult(f"`{run.id}` is {run.status}.", refresh_panel=True)
        if action.action == "show":
            run = self.session.get(WorkflowRun, workflow_run_id)
            if run is None:
                raise KeyError(f"Unknown workflow run: {workflow_run_id}")
            return DiscordUIResult(render_workflow_summary(run, self._workflow_nodes(run.id)))
        if action.action == "report":
            report = ReportService(self.session).workflow_report(workflow_run_id)
            return DiscordUIResult(_truncate(report.body))
        return DiscordUIResult(f"Unknown workflow action: {action.action}")

    def _handle_schedule_action(self, action: DiscordUIAction) -> DiscordUIResult:
        if action.action == "poll":
            count = ScheduleService(self.session).poll_due_schedules()
            return DiscordUIResult(f"Launched {count} scheduled occurrence(s).", refresh_panel=True)
        if action.action == "list":
            schedules = self.session.scalars(
                select(Schedule).order_by(Schedule.created_at.desc()).limit(10)
            ).all()
            return DiscordUIResult(render_schedule_list(list(schedules)))
        return DiscordUIResult(f"Unknown schedule action: {action.action}")

    def _handle_artifact_action(self, action: DiscordUIAction) -> DiscordUIResult:
        if action.action == "search":
            return self.search_artifacts(limit=10)
        return DiscordUIResult(f"Unknown artifact action: {action.action}")

    def _single_awaiting_gate_key(self, workflow_run_id: str) -> str:
        gates = self.session.scalars(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == workflow_run_id,
                WorkflowNode.kind == "gate",
                WorkflowNode.status == "awaiting_input",
            )
        ).all()
        if len(gates) != 1:
            raise ValueError("Workflow answer needs a gate key because there is not exactly one open gate.")
        return gates[0].node_key

    def _workflow_nodes(self, workflow_run_id: str) -> list[WorkflowNode]:
        return list(
            self.session.scalars(
                select(WorkflowNode)
                .where(WorkflowNode.workflow_run_id == workflow_run_id)
                .order_by(WorkflowNode.created_at)
            ).all()
        )


def render_status(status: SystemStatus) -> str:
    lines = [
        "Tasque status",
        f"Ready work: {status.ready_work}",
        f"Running work: {status.running_work}",
        f"Unresolved failed work: {status.failed_work_unresolved}",
        f"Enabled schedules: {status.schedules_enabled}",
        "",
        f"Work: {_format_counts(status.work_items)}",
        f"Attempts: {_format_counts(status.work_attempts)}",
        f"Workflow runs: {_format_counts(status.workflow_runs)}",
    ]
    return _truncate("\n".join(lines))


def render_doctor(*, report: dict[str, Any]) -> str:
    checks = report.get("checks", [])
    lines = [f"Doctor: {report.get('overall_status', 'unknown')}"]
    if isinstance(checks, list):
        for check in checks:
            if not isinstance(check, dict):
                continue
            lines.append(f"{check.get('status', '?')} {check.get('name', '?')}: {check.get('summary', '')}")
    return _truncate("\n".join(lines))


def render_work_summary(work: WorkItem) -> str:
    return _truncate(
        "\n".join(
            [
                f"Work: {work.title}",
                f"ID: {work.id}",
                f"Status: {work.status}",
                f"Worker: {work.worker_kind}",
                f"Priority: {work.priority}",
                f"Attempts: {work.attempt_count}/{work.max_attempts}",
            ]
        )
    )


def render_workflow_summary(run: WorkflowRun, nodes: list[WorkflowNode]) -> str:
    counts = _count_by_status(nodes)
    lines = [
        f"Workflow: {run.name}",
        f"ID: {run.id}",
        f"Status: {run.status}",
        f"Nodes: {_format_counts(counts)}",
    ]
    waiting = [node for node in nodes if _effective_workflow_node_status(node) == "awaiting_input"]
    if waiting:
        lines.append("")
        for node in waiting:
            prompt = node.definition.get("prompt") or "Workflow is awaiting input."
            lines.append(f"Waiting: {node.node_key}: {prompt}")
    failed = [node for node in nodes if _effective_workflow_node_status(node) == "failed"]
    if failed:
        lines.append("")
        for node in failed:
            lines.append(f"Failed: {node.node_key}: {node.failure_reason or ''}")
    return _truncate("\n".join(lines))


def render_schedule_list(schedules: list[Schedule]) -> str:
    if not schedules:
        return "No schedules."
    lines = ["Recent schedules"]
    for schedule in schedules:
        enabled = "enabled" if schedule.enabled else "disabled"
        lines.append(
            f"`{schedule.id}` {enabled} {schedule.schedule_type} {schedule.expression} "
            f"-> {schedule.worker_kind}: {schedule.name}"
        )
    return _truncate("\n".join(lines))


def render_artifact_list(artifacts: list) -> str:
    if not artifacts:
        return "No matching artifacts."
    lines = ["Artifacts"]
    for artifact in artifacts[:10]:
        tags = ", ".join(artifact.tags or [])
        size = f", {artifact.size_bytes} bytes" if artifact.size_bytes is not None else ""
        lines.append(
            f"`{artifact.id}` {artifact.kind}{size} [{tags}] {artifact.title}: {artifact.local_path}"
        )
    return _truncate("\n".join(lines))


def create_memory_from_discord(
    session: Session,
    *,
    content: str,
    namespace: str = "global",
    tags: list[str] | None = None,
) -> DiscordUIResult:
    memory = MemoryService(session).create_memory(
        namespace=namespace,
        kind="note",
        content=content,
        tags=tags or ["discord"],
        source_kind="discord_ui",
    )
    return DiscordUIResult(f"Created memory `{memory.id}`.")


def _require_entity_id(action: DiscordUIAction) -> str:
    if not action.entity_id:
        raise ValueError(f"{action.scope}:{action.action} requires an entity id.")
    return action.entity_id


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "(none)"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _count_by_status(nodes: list[WorkflowNode]) -> dict[str, int]:
    counts: dict[str, int] = {"total": len(nodes)}
    for node in nodes:
        status = _effective_workflow_node_status(node)
        counts[status] = counts.get(status, 0) + 1
    return counts


def _effective_workflow_node_status(node: WorkflowNode) -> str:
    work = node.work_item
    if work is None or node.status != "enqueued":
        return node.status
    if work.status == "succeeded":
        return "succeeded"
    if work.status == "dead_letter":
        return "failed"
    if work.status == "canceled":
        return "canceled"
    if work.status == "running":
        return "running"
    if work.status == "ready":
        return "ready"
    return node.status


def _truncate(content: str, limit: int = 1900) -> str:
    if len(content) <= limit:
        return content
    return content[: limit - 20] + "\n[truncated]"
