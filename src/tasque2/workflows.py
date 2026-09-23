"""Workflow definitions and runs: a DAG of work, fan-out, join, and gate nodes.

A definition is JSON (usually loaded from a ``*.workflow.json`` file). Starting a run
materializes its nodes and edges; each daemon tick advances runs by enqueuing work for
nodes whose dependencies are satisfied and folding finished work back into node state.
Node templates are read from disk when the node is enqueued, so edits to a template take
effect on the next run without re-registering the definition.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from opentelemetry.trace import SpanKind
from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.events import record_event
from tasque2.models import (
    WorkAttempt,
    WorkflowDefinition,
    WorkflowEdge,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
    utc_now,
)
from tasque2.telemetry import current_traceparent, instruments, span
from tasque2.templates import read_template_file, resolve_template_path
from tasque2.work.queue import TERMINAL_WORK_STATUSES, WorkQueue
from tasque2.work.repository import WorkRepository

logger = logging.getLogger(__name__)

ACTIVE_RUN_STATUSES = {"active", "awaiting_input"}
TERMINAL_RUN_STATUSES = {"completed", "failed", "canceled"}
TERMINAL_NODE_STATUSES = {"succeeded", "failed", "failed_tolerated", "canceled"}
COMPLETED_NODE_STATUSES = {"succeeded", "failed_tolerated"}
NODE_KINDS = {"work", "fan_out", "join", "gate"}
_TEMPLATE_KEYS = (
    ("task_template_path", "task_instruction"),
    ("child_task_template_path", "child_task_instruction_template"),
)
_ITEM_KEY_RE = re.compile(r"\{item\[([^\]]+)\]\}")


class WorkflowService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_definition(
        self,
        *,
        name: str,
        version: str,
        definition: dict[str, Any],
        enabled: bool = True,
    ) -> WorkflowDefinition:
        """Create a definition, or replace the stored one with the same name and version."""
        validate_definition(definition)
        existing = self.session.scalar(
            select(WorkflowDefinition).where(WorkflowDefinition.name == name, WorkflowDefinition.version == version)
        )
        if existing is not None:
            existing.definition = definition
            existing.enabled = enabled
            self.session.flush()
            return existing
        workflow_definition = WorkflowDefinition(name=name, version=version, definition=definition, enabled=enabled)
        self.session.add(workflow_definition)
        self.session.flush()
        record_event(
            self.session,
            event_type="workflow.definition_created",
            entity_kind="workflow_definition",
            entity_id=workflow_definition.id,
            source="workflow",
            summary=f"Created workflow definition: {name}@{version}",
        )
        return workflow_definition

    def load_definition_file(self, path: Path) -> WorkflowDefinition:
        data = self.parse_definition_file(path)
        return self.create_definition(
            name=str(data["name"]),
            version=str(data.get("version", "1")),
            definition=dict(data["definition"]),
            enabled=bool(data.get("enabled", True)),
        )

    def parse_definition_file(self, path: Path) -> dict[str, Any]:
        return parse_definition_file(path)

    def start_run(
        self,
        *,
        workflow_definition_id: str,
        name: str | None = None,
        input: dict[str, Any] | None = None,
        discord_thread_id: str | None = None,
    ) -> WorkflowRun:
        definition = self.session.get(WorkflowDefinition, workflow_definition_id)
        if definition is None:
            raise KeyError(f"Unknown workflow definition: {workflow_definition_id}")
        if not definition.enabled:
            raise ValueError("Workflow definition is disabled.")
        with span(
            "tasque.workflow.start",
            kind=SpanKind.PRODUCER,
            attributes={"tasque.workflow.name": definition.name, "tasque.workflow.version": definition.version},
        ) as current:
            run = WorkflowRun(
                workflow_definition_id=definition.id,
                name=name or definition.name,
                status="active",
                input=input or {},
                state={},
                discord_thread_id=discord_thread_id,
                started_at=utc_now(),
                traceparent=current_traceparent(),
            )
            self.session.add(run)
            self.session.flush()
            current.set_attribute("tasque.workflow.run.id", run.id)
            self._materialize(run, definition.definition)
            self.session.flush()
            self._event("workflow.run_started", run, summary=f"Started workflow run: {run.name}")
        return run

    def tick_runs(self) -> int:
        """Advance every active run once; returns how many runs changed."""
        runs = self.session.scalars(
            select(WorkflowRun).where(WorkflowRun.status.in_(ACTIVE_RUN_STATUSES)).order_by(WorkflowRun.created_at)
        ).all()
        changed = 0
        for run in runs:
            before = self._fingerprint(run)
            self._sync_enqueued_nodes(run)
            if run.status == "active":
                self._start_ready_nodes(run)
            self._finalize_if_done(run)
            if self._fingerprint(run) != before:
                changed += 1
        self.session.flush()
        return changed

    def answer_gate(self, *, workflow_run_id: str, node_key: str, answer: str) -> WorkflowNode:
        """Answer an open gate; a gate that is not waiting, or a finished run, is refused unchanged."""
        node = self.session.scalar(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == workflow_run_id, WorkflowNode.node_key == node_key
            )
        )
        if node is None:
            raise KeyError(f"Unknown workflow node: {workflow_run_id}:{node_key}")
        if node.kind != "gate":
            raise ValueError("Only gate nodes can be answered.")
        run = node.workflow_run
        if node.status != "awaiting_input" or run.status in TERMINAL_RUN_STATUSES:
            raise ValueError(f"Gate {node_key} is not waiting for an answer (gate {node.status}, run {run.status}).")
        node.status = "succeeded"
        node.output = {"answer": answer}
        if run.status == "awaiting_input":
            run.status = "active"
        self.session.flush()
        self._event(
            "workflow.gate_answered",
            run,
            entity=("workflow_node", node.id),
            summary=f"Gate answered: {node_key}",
            payload={"node_key": node_key},
        )
        return node

    def pause_run(self, workflow_run_id: str) -> WorkflowRun:
        run = self._get_run(workflow_run_id)
        if run.status in TERMINAL_RUN_STATUSES:
            return run
        run.status = "paused"
        queue = WorkQueue(self.session)
        paused = [queue.pause_work(item.id).id for item in self._run_work(run.id, statuses=("ready",))]
        self.session.flush()
        self._event("workflow.run_paused", run, summary=f"Paused workflow run: {run.name}", payload={"paused": paused})
        return run

    def resume_run(self, workflow_run_id: str) -> WorkflowRun:
        run = self._get_run(workflow_run_id)
        if run.status != "paused":
            return run
        queue = WorkQueue(self.session)
        resumed = [queue.resume_work(item.id).id for item in self._run_work(run.id, statuses=("paused",))]
        run.status = "awaiting_input" if self._awaiting_gate(run.id) is not None else "active"
        self.session.flush()
        self._event(
            "workflow.run_resumed", run, summary=f"Resumed workflow run: {run.name}", payload={"resumed": resumed}
        )
        return run

    def cancel_run(self, workflow_run_id: str) -> WorkflowRun:
        run = self._get_run(workflow_run_id)
        if run.status in TERMINAL_RUN_STATUSES:
            return run
        run.status = "canceled"
        run.ended_at = utc_now()
        queue = WorkQueue(self.session)
        active = self.session.scalars(
            select(WorkItem).where(WorkItem.workflow_run_id == run.id, WorkItem.status.not_in(TERMINAL_WORK_STATUSES))
        ).all()
        canceled = [queue.request_cancel(item.id).id for item in active]
        for node in self._nodes(run.id):
            if node.status not in TERMINAL_NODE_STATUSES:
                node.status = "canceled"
                node.failure_reason = "Workflow run canceled."
        self.session.flush()
        self._event(
            "workflow.run_canceled", run, summary=f"Canceled workflow run: {run.name}", payload={"canceled": canceled}
        )
        instruments().workflow_runs.add(1, {"tasque.workflow.name": run.name, "tasque.workflow.outcome": "canceled"})
        return run

    def _materialize(self, run: WorkflowRun, definition: dict[str, Any]) -> None:
        by_key: dict[str, WorkflowNode] = {}
        for node_def in definition["nodes"]:
            node = WorkflowNode(
                workflow_run_id=run.id,
                node_key=str(node_def["key"]),
                kind=str(node_def.get("kind", "work")),
                status="pending",
                definition=dict(node_def),
                input={},
                output={},
            )
            self.session.add(node)
            self.session.flush()
            by_key[node.node_key] = node
        for node_def in definition["nodes"]:
            for dependency in node_def.get("depends_on", []):
                self.session.add(
                    WorkflowEdge(
                        workflow_run_id=run.id,
                        from_node_id=by_key[str(dependency)].id,
                        to_node_id=by_key[str(node_def["key"])].id,
                        condition="succeeded",
                    )
                )

    def _sync_enqueued_nodes(self, run: WorkflowRun) -> None:
        nodes = self.session.scalars(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.status == "enqueued")
        ).all()
        for node in nodes:
            work_item = node.work_item
            if work_item is None:
                continue
            if work_item.status == "succeeded":
                node.status = "succeeded"
                node.output = self._latest_produces(work_item.id)
                self._event(
                    "workflow.node_succeeded",
                    run,
                    entity=("workflow_node", node.id),
                    summary=f"Workflow node succeeded: {node.node_key}",
                )
            elif work_item.status in {"dead_letter", "canceled"}:
                if work_item.status == "dead_letter" and (node.definition or {}).get("tolerate_failure"):
                    node.status = "failed_tolerated"
                    node.failure_reason = "Work item ended with status dead_letter (tolerated)."
                    node.output = {"tolerated_failure": True}
                    event_type = "workflow.node_failure_tolerated"
                    summary = f"Workflow node failed (tolerated): {node.node_key}"
                else:
                    node.status = "failed" if work_item.status == "dead_letter" else "canceled"
                    node.failure_reason = f"Work item ended with status {work_item.status}."
                    event_type = "workflow.node_failed"
                    summary = node.failure_reason
                self._event(
                    event_type,
                    run,
                    entity=("workflow_node", node.id),
                    summary=summary,
                    payload={"node_key": node.node_key, "work_item_id": work_item.id},
                )

    def _start_ready_nodes(self, run: WorkflowRun) -> None:
        pending = self.session.scalars(
            select(WorkflowNode)
            .where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.status == "pending")
            .order_by(WorkflowNode.created_at)
        ).all()
        for node in pending:
            if not self._dependencies_satisfied(node):
                continue
            try:
                self._start_node(run, node)
            except Exception as exc:  # noqa: BLE001 - a node that cannot start fails its run, not the tick
                logger.exception("Workflow run %s: node %s could not start", run.id, node.node_key)
                node.status = "failed"
                node.failure_reason = f"Node could not start: {exc}"
                self._event(
                    "workflow.node_failed",
                    run,
                    entity=("workflow_node", node.id),
                    summary=node.failure_reason,
                    payload={"node_key": node.node_key},
                )

    def _start_node(self, run: WorkflowRun, node: WorkflowNode) -> None:
        if node.kind == "work":
            self._enqueue_work_node(run, node)
        elif node.kind == "fan_out":
            self._expand_fan_out(run, node)
        elif node.kind == "join":
            self._complete_join(run, node)
        elif node.kind == "gate":
            node.status = "awaiting_input"
            run.status = "awaiting_input"
            self._event(
                "workflow.gate_waiting",
                run,
                entity=("workflow_node", node.id),
                summary=f"Waiting for gate input: {node.node_key}",
                payload={"prompt": node.definition.get("prompt")},
            )
        else:
            node.status = "failed"
            node.failure_reason = f"Unsupported workflow node kind: {node.kind}"

    def _enqueue_work_node(self, run: WorkflowRun, node: WorkflowNode) -> None:
        node_def = node.definition
        context = {**dict(run.input), **dict(node_def.get("context") or {})}
        work_item = WorkRepository(self.session).create_work_item(
            title=str(node_def.get("title", node.node_key)),
            task_instruction=node_instruction(node_def) or node.node_key,
            worker_kind=str(node_def.get("worker_kind", "manual")),
            runtime_contract=dict(node_def.get("runtime_contract") or {}),
            context=context,
            retry_policy=dict(node_def.get("retry_policy") or {}),
            priority=int(node_def.get("priority", 0)),
            max_attempts=int(node_def.get("max_attempts", 1)),
            deadline_at=_node_deadline(node_def),
            idempotency_key=f"workflow:{run.id}:{node.node_key}",
            source_kind="workflow",
            source_id=run.id,
            workflow_run_id=run.id,
            workflow_node_id=node.id,
            discord_thread_id=run.discord_thread_id,
            lane=str(run.input.get("lane") or run.definition.name),
            traceparent=run.traceparent,
        )
        node.work_item_id = work_item.id
        node.status = "enqueued"
        self.session.flush()
        self._event(
            "workflow.node_enqueued",
            run,
            entity=("workflow_node", node.id),
            work_item_id=work_item.id,
            summary=f"Workflow node enqueued: {node.node_key}",
        )

    def _expand_fan_out(self, run: WorkflowRun, node: WorkflowNode) -> None:
        node_def = node.definition
        tolerate = bool(node_def.get("tolerate_child_failures"))
        items = node_def.get("items")
        if items is None:
            items = self._fan_out_items(run, node_def)
        if not isinstance(items, list):
            node.status = "failed"
            node.failure_reason = "fan_out items must be a list."
            return
        downstream = self.session.scalars(select(WorkflowEdge).where(WorkflowEdge.from_node_id == node.id)).all()
        child_ids: list[str] = []
        for index, item in enumerate(items):
            child_key = f"{node.node_key}.{index}"
            child = self.session.scalar(
                select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.node_key == child_key)
            )
            if child is None:
                child = WorkflowNode(
                    workflow_run_id=run.id,
                    node_key=child_key,
                    kind="work",
                    status="pending",
                    definition=_fan_out_child_definition(node_def, item, index),
                    input={"item": item, "index": index},
                    output={},
                    parent_node_id=node.id,
                    fanout_index=index,
                )
                self.session.add(child)
                self.session.flush()
                self.session.add(
                    WorkflowEdge(
                        workflow_run_id=run.id, from_node_id=node.id, to_node_id=child.id, condition="succeeded"
                    )
                )
                for edge in downstream:
                    self.session.add(
                        WorkflowEdge(
                            workflow_run_id=run.id,
                            from_node_id=child.id,
                            to_node_id=edge.to_node_id,
                            condition="finished" if tolerate else "succeeded",
                        )
                    )
            child_ids.append(child.id)
        node.status = "succeeded"
        node.output = {"child_node_ids": child_ids, "count": len(child_ids)}
        self.session.flush()
        self._event(
            "workflow.fan_out_expanded",
            run,
            entity=("workflow_node", node.id),
            summary=f"Expanded fan-out node: {node.node_key}",
            payload={"count": len(child_ids)},
        )

    def _complete_join(self, run: WorkflowRun, node: WorkflowNode) -> None:
        outputs: dict[str, Any] = {}
        for edge in self.session.scalars(select(WorkflowEdge).where(WorkflowEdge.to_node_id == node.id)).all():
            upstream = self.session.get(WorkflowNode, edge.from_node_id)
            if upstream is not None:
                outputs[upstream.node_key] = upstream.output
        node.status = "succeeded"
        node.output = {"dependencies": outputs}
        self.session.flush()
        self._event(
            "workflow.join_completed",
            run,
            entity=("workflow_node", node.id),
            summary=f"Completed join node: {node.node_key}",
        )

    def _fan_out_items(self, run: WorkflowRun, node_def: dict[str, Any]) -> Any:
        if "items_from_output" not in node_def:
            return run.input.get(str(node_def.get("items_from", "items")), [])
        reference = node_def["items_from_output"]
        if isinstance(reference, str):
            node_key, _, path = reference.partition(".")
        elif isinstance(reference, dict):
            node_key, path = str(reference.get("node") or ""), str(reference.get("path") or "")
        else:
            return []
        if not node_key:
            return []
        node = self.session.scalar(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.node_key == node_key)
        )
        if node is None:
            return []
        return _output_path(node.output or {}, path)

    def _dependencies_satisfied(self, node: WorkflowNode) -> bool:
        for edge in self.session.scalars(select(WorkflowEdge).where(WorkflowEdge.to_node_id == node.id)).all():
            upstream = self.session.get(WorkflowNode, edge.from_node_id)
            if upstream is None:
                return False
            if edge.condition == "finished":
                if upstream.status not in COMPLETED_NODE_STATUSES:
                    return False
            elif upstream.status != edge.condition:
                return False
        return True

    def _finalize_if_done(self, run: WorkflowRun) -> None:
        nodes = self._nodes(run.id)
        if any(node.status == "failed" for node in nodes):
            run.status = "failed"
        elif all(node.status in COMPLETED_NODE_STATUSES for node in nodes):
            run.status = "completed"
            run.state = {"outputs": {node.node_key: node.output for node in nodes}}
        else:
            return
        run.ended_at = utc_now()
        self._event(f"workflow.run_{run.status}", run, summary=f"Workflow run {run.status}: {run.name}")
        instruments().workflow_runs.add(1, {"tasque.workflow.name": run.name, "tasque.workflow.outcome": run.status})

    def _get_run(self, workflow_run_id: str) -> WorkflowRun:
        run = self.session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise KeyError(f"Unknown workflow run: {workflow_run_id}")
        return run

    def _nodes(self, workflow_run_id: str) -> list[WorkflowNode]:
        return list(
            self.session.scalars(
                select(WorkflowNode)
                .where(WorkflowNode.workflow_run_id == workflow_run_id)
                .order_by(WorkflowNode.created_at)
            ).all()
        )

    def _run_work(self, workflow_run_id: str, *, statuses: tuple[str, ...]) -> list[WorkItem]:
        return list(
            self.session.scalars(
                select(WorkItem).where(WorkItem.workflow_run_id == workflow_run_id, WorkItem.status.in_(statuses))
            ).all()
        )

    def _awaiting_gate(self, workflow_run_id: str) -> WorkflowNode | None:
        return self.session.scalar(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == workflow_run_id,
                WorkflowNode.kind == "gate",
                WorkflowNode.status == "awaiting_input",
            )
        )

    def _latest_produces(self, work_item_id: str) -> dict[str, Any]:
        attempt = self.session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work_item_id)
            .order_by(WorkAttempt.attempt_number.desc())
        )
        return (attempt.produces or {}) if attempt is not None else {}

    def _fingerprint(self, run: WorkflowRun) -> tuple[Any, ...]:
        nodes = self.session.scalars(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id).order_by(WorkflowNode.node_key)
        ).all()
        return run.status, tuple((node.node_key, node.status, node.work_item_id) for node in nodes)

    def _event(
        self,
        event_type: str,
        run: WorkflowRun,
        *,
        summary: str,
        entity: tuple[str, str] | None = None,
        work_item_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        entity_kind, entity_id = entity or ("workflow_run", run.id)
        record_event(
            self.session,
            event_type=event_type,
            entity_kind=entity_kind,
            entity_id=entity_id,
            work_item_id=work_item_id,
            workflow_run_id=run.id,
            source="workflow",
            summary=summary,
            payload=payload,
        )


def parse_definition_file(path: Path) -> dict[str, Any]:
    """Load and validate a workflow JSON file, resolving node template paths.

    Template paths are stored absolute, with the template's text at registration as the
    fallback for a file that goes missing.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Workflow file must contain a JSON object.")
    if "name" not in data:
        raise ValueError("Workflow file requires name.")
    if not isinstance(data.get("definition"), dict):
        raise ValueError("Workflow file requires definition object.")
    base_dir = path.resolve().parent
    definition = dict(data["definition"])
    nodes = []
    for raw_node in definition.get("nodes", []):
        node = dict(raw_node)
        for path_key, text_key in _TEMPLATE_KEYS:
            if node.get(path_key):
                resolved = resolve_template_path(str(node[path_key]), base_dir=base_dir)
                node[path_key] = str(resolved)
                node[text_key] = read_template_file(resolved)
        nodes.append(node)
    definition["nodes"] = nodes
    validate_definition(definition)
    data["definition"] = definition
    return data


def validate_definition(definition: dict[str, Any]) -> None:
    nodes = definition.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("Workflow definition requires a non-empty nodes list.")
    keys: set[str] = set()
    for node in nodes:
        if "key" not in node:
            raise ValueError("Each workflow node requires a key.")
        key = str(node["key"])
        if key in keys:
            raise ValueError(f"Duplicate workflow node key: {key}")
        keys.add(key)
        kind = str(node.get("kind", "work"))
        if kind not in NODE_KINDS:
            raise ValueError(f"Unsupported workflow node kind: {kind}")
    for node in nodes:
        for dependency in node.get("depends_on", []):
            if str(dependency) not in keys:
                raise ValueError(f"Unknown workflow dependency: {dependency}")


def node_instruction(
    node_def: dict[str, Any], *, path_key: str = "task_template_path", text_key: str = "task_instruction"
) -> str:
    """The node's instruction: its template file as it is on disk, else the stored text."""
    template_path = node_def.get(path_key)
    if template_path:
        for candidate in _template_candidates(str(template_path)):
            if candidate.is_file():
                return candidate.read_text(encoding="utf-8").strip()
    return str(node_def.get(text_key) or "")


def _template_candidates(template_path: str) -> list[Path]:
    path = Path(template_path).expanduser()
    if path.is_absolute():
        return [path]
    return [resolve_template_path(path, base_dir=get_settings().resolved_data_dir / "workflows")]


def _fan_out_child_definition(node_def: dict[str, Any], item: Any, index: int) -> dict[str, Any]:
    title_template = str(node_def.get("child_title_template", node_def.get("title", "Fan-out child {index}")))
    instruction_template = (
        node_instruction(node_def, path_key="child_task_template_path", text_key="child_task_instruction_template")
        or node_instruction(node_def)
        or "Process fan-out item {index}: {item}"
    )
    context = {**dict(node_def.get("context") or {}), "item": item, "index": index}
    return {
        "key": f"{node_def['key']}.{index}",
        "kind": "work",
        "tolerate_failure": bool(node_def.get("tolerate_child_failures")),
        "title": _render_item_template(title_template, item=item, index=index),
        "task_instruction": _render_item_template(instruction_template, item=item, index=index),
        "worker_kind": node_def.get("child_worker_kind", node_def.get("worker_kind", "manual")),
        "runtime_contract": dict(node_def.get("runtime_contract") or {}),
        "context": context,
        "retry_policy": dict(node_def.get("retry_policy") or {}),
        "priority": int(node_def.get("priority", 0)),
        "max_attempts": int(node_def.get("max_attempts", 1)),
    }


def _node_deadline(node_def: dict[str, Any], *, now: datetime | None = None) -> datetime | None:
    """Latest start time: ``deadline_at`` (ISO-8601) or ``deadline_seconds`` after enqueue."""
    explicit = node_def.get("deadline_at")
    if explicit:
        value = datetime.fromisoformat(str(explicit).replace("Z", "+00:00"))
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    seconds = node_def.get("deadline_seconds")
    if seconds is None:
        return None
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        raise ValueError("Workflow node deadline_seconds must be an integer.") from None
    return (now or utc_now()) + timedelta(seconds=seconds) if seconds > 0 else None


def _output_path(value: Any, path: str) -> Any:
    for part in (part for part in path.split(".") if part):
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, list):
            try:
                value = value[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return value


def _render_item_template(template: str, *, item: Any, index: int) -> str:
    rendered = template.replace("{index}", str(index))
    rendered = _ITEM_KEY_RE.sub(
        lambda match: (
            str(item[match.group(1)]) if isinstance(item, dict) and match.group(1) in item else match.group(0)
        ),
        rendered,
    )
    return rendered.replace("{item}", item if isinstance(item, str) else _json_text(item))


def _json_text(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    except TypeError:
        return str(value)
