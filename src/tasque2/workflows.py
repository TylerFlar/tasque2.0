from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.models import (
    WorkAttempt,
    WorkEvent,
    WorkflowDefinition,
    WorkflowEdge,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
    utc_now,
)
from tasque2.queue import TERMINAL_WORK_STATUSES, WorkQueue
from tasque2.repo import WorkRepository
from tasque2.templates import read_template_file

ACTIVE_RUN_STATUSES = {"active", "awaiting_input"}
TERMINAL_RUN_STATUSES = {"completed", "failed", "canceled"}
TERMINAL_NODE_STATUSES = {"succeeded", "failed", "canceled"}
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
        self._validate_definition(definition)
        existing = self.session.scalar(
            select(WorkflowDefinition).where(
                WorkflowDefinition.name == name,
                WorkflowDefinition.version == version,
            )
        )
        if existing is not None:
            existing.definition = definition
            existing.enabled = enabled
            self.session.flush()
            return existing

        workflow_definition = WorkflowDefinition(
            name=name,
            version=version,
            definition=definition,
            enabled=enabled,
        )
        self.session.add(workflow_definition)
        self.session.flush()
        self._emit_event(
            event_type="workflow.definition_created",
            entity_kind="workflow_definition",
            entity_id=workflow_definition.id,
            summary=f"Created workflow definition: {name}@{version}",
        )
        return workflow_definition

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

        run = WorkflowRun(
            workflow_definition_id=definition.id,
            name=name or definition.name,
            status="active",
            input=input or {},
            state={},
            discord_thread_id=discord_thread_id,
            started_at=utc_now(),
        )
        self.session.add(run)
        self.session.flush()
        self._materialize_nodes(run, definition.definition)
        self.session.flush()
        self._emit_event(
            event_type="workflow.run_started",
            entity_kind="workflow_run",
            entity_id=run.id,
            workflow_run_id=run.id,
            summary=f"Started workflow run: {run.name}",
        )
        return run

    def tick_runs(self) -> int:
        runs = self.session.scalars(
            select(WorkflowRun)
            .where(WorkflowRun.status.in_(ACTIVE_RUN_STATUSES))
            .order_by(WorkflowRun.created_at)
        ).all()

        changed = 0
        for run in runs:
            before = self._run_fingerprint(run)
            self._sync_enqueued_nodes(run)
            if run.status == "active":
                self._start_ready_nodes(run)
            self._finalize_run_if_ready(run)
            if self._run_fingerprint(run) != before:
                changed += 1
        self.session.flush()
        return changed

    def answer_gate(
        self,
        *,
        workflow_run_id: str,
        node_key: str,
        answer: str,
    ) -> WorkflowNode:
        node = self.session.scalar(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == workflow_run_id,
                WorkflowNode.node_key == node_key,
            )
        )
        if node is None:
            raise KeyError(f"Unknown workflow node: {workflow_run_id}:{node_key}")
        if node.kind != "gate":
            raise ValueError("Only gate nodes can be answered.")
        node.status = "succeeded"
        node.output = {"answer": answer}
        node.workflow_run.status = "active"
        self.session.flush()
        self._emit_event(
            event_type="workflow.gate_answered",
            entity_kind="workflow_node",
            entity_id=node.id,
            workflow_run_id=workflow_run_id,
            summary=f"Gate answered: {node_key}",
            payload={"node_key": node_key},
        )
        return node

    def pause_run(self, workflow_run_id: str) -> WorkflowRun:
        run = self._get_run(workflow_run_id)
        if run.status in TERMINAL_RUN_STATUSES:
            return run

        run.status = "paused"
        paused_work_item_ids = self._pause_ready_run_work(run.id)
        self.session.flush()
        self._emit_event(
            event_type="workflow.run_paused",
            entity_kind="workflow_run",
            entity_id=run.id,
            workflow_run_id=run.id,
            summary=f"Paused workflow run: {run.name}",
            payload={"paused_work_item_ids": paused_work_item_ids},
        )
        return run

    def resume_run(self, workflow_run_id: str) -> WorkflowRun:
        run = self._get_run(workflow_run_id)
        if run.status != "paused":
            return run

        resumed_work_item_ids = self._resume_paused_run_work(run.id)
        run.status = "awaiting_input" if self._awaiting_gate(run.id) is not None else "active"
        self.session.flush()
        self._emit_event(
            event_type="workflow.run_resumed",
            entity_kind="workflow_run",
            entity_id=run.id,
            workflow_run_id=run.id,
            summary=f"Resumed workflow run: {run.name}",
            payload={"resumed_work_item_ids": resumed_work_item_ids},
        )
        return run

    def cancel_run(self, workflow_run_id: str) -> WorkflowRun:
        run = self._get_run(workflow_run_id)
        if run.status in TERMINAL_RUN_STATUSES:
            return run

        run.status = "canceled"
        run.ended_at = utc_now()
        canceled_work_item_ids = self._cancel_active_run_work(run.id)
        for node in self._run_nodes(run.id):
            if node.status not in {"succeeded", "failed", "canceled"}:
                node.status = "canceled"
                node.failure_reason = "Workflow run canceled."
        self.session.flush()
        self._emit_event(
            event_type="workflow.run_canceled",
            entity_kind="workflow_run",
            entity_id=run.id,
            workflow_run_id=run.id,
            summary=f"Canceled workflow run: {run.name}",
            payload={"canceled_work_item_ids": canceled_work_item_ids},
        )
        return run

    def load_definition_file(self, path: Path) -> WorkflowDefinition:
        data = self.parse_definition_file(path)
        return self.create_definition(
            name=str(data["name"]),
            version=str(data.get("version", "1")),
            definition=dict(data["definition"]),
            enabled=bool(data.get("enabled", True)),
        )

    def parse_definition_file(self, path: Path) -> dict[str, Any]:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Workflow file must contain a JSON object.")
        if "name" not in data:
            raise ValueError("Workflow file requires name.")
        if "definition" not in data or not isinstance(data["definition"], dict):
            raise ValueError("Workflow file requires definition object.")
        data["definition"] = self._definition_with_template_files(
            dict(data["definition"]),
            base_dir=path.parent,
        )
        self._validate_definition(dict(data["definition"]))
        return data

    def _definition_with_template_files(
        self,
        definition: dict[str, Any],
        *,
        base_dir: Path,
    ) -> dict[str, Any]:
        resolved = dict(definition)
        nodes = []
        for raw_node in resolved.get("nodes", []):
            node = dict(raw_node)
            self._load_node_template_files(node, base_dir=base_dir)
            nodes.append(node)
        resolved["nodes"] = nodes
        return resolved

    def _load_node_template_files(self, node: dict[str, Any], *, base_dir: Path) -> None:
        self._load_template_key(
            node,
            path_keys=("task_template_path", "instruction_template_path"),
            target_key="task_instruction",
            base_dir=base_dir,
        )
        self._load_template_key(
            node,
            path_keys=("task_instruction_template_path",),
            target_key="task_instruction_template",
            base_dir=base_dir,
        )
        self._load_template_key(
            node,
            path_keys=("child_task_template_path", "child_task_instruction_template_path"),
            target_key="child_task_instruction_template",
            base_dir=base_dir,
        )

    def _load_template_key(
        self,
        node: dict[str, Any],
        *,
        path_keys: tuple[str, ...],
        target_key: str,
        base_dir: Path,
    ) -> None:
        for path_key in path_keys:
            template_path = node.get(path_key)
            if template_path:
                node[target_key] = read_template_file(str(template_path), base_dir=base_dir)
                return

    def _materialize_nodes(self, run: WorkflowRun, definition: dict[str, Any]) -> None:
        node_by_key: dict[str, WorkflowNode] = {}
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
            node_by_key[node.node_key] = node

        for node_def in definition["nodes"]:
            to_node = node_by_key[str(node_def["key"])]
            for dependency_key in node_def.get("depends_on", []):
                from_node = node_by_key[str(dependency_key)]
                self.session.add(
                    WorkflowEdge(
                        workflow_run_id=run.id,
                        from_node_id=from_node.id,
                        to_node_id=to_node.id,
                        condition="succeeded",
                    )
                )

    def _sync_enqueued_nodes(self, run: WorkflowRun) -> None:
        nodes = self.session.scalars(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.status == "enqueued",
            )
        ).all()
        for node in nodes:
            if node.work_item_id is None:
                continue
            work_item = node.work_item
            if work_item is None:
                continue
            if work_item.status == "succeeded":
                node.status = "succeeded"
                node.output = self._latest_attempt_output(work_item.id)
                self._emit_event(
                    event_type="workflow.node_succeeded",
                    entity_kind="workflow_node",
                    entity_id=node.id,
                    workflow_run_id=run.id,
                    summary=f"Workflow node succeeded: {node.node_key}",
                )
            elif work_item.status in {"dead_letter", "canceled"}:
                node.status = "failed" if work_item.status == "dead_letter" else "canceled"
                node.failure_reason = f"Work item ended with status {work_item.status}."
                self._emit_event(
                    event_type="workflow.node_failed",
                    entity_kind="workflow_node",
                    entity_id=node.id,
                    workflow_run_id=run.id,
                    summary=node.failure_reason,
                    payload={"node_key": node.node_key, "work_item_id": work_item.id},
                )

    def _start_ready_nodes(self, run: WorkflowRun) -> None:
        pending_nodes = self.session.scalars(
            select(WorkflowNode)
            .where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.status == "pending",
            )
            .order_by(WorkflowNode.created_at)
        ).all()
        for node in pending_nodes:
            if not self._dependencies_satisfied(node):
                continue
            if node.kind in {"work", "native", "model"}:
                self._enqueue_work_node(run, node)
            elif node.kind == "fan_out":
                self._expand_fan_out_node(run, node)
            elif node.kind == "join":
                self._complete_join_node(run, node)
            elif node.kind == "gate":
                node.status = "awaiting_input"
                run.status = "awaiting_input"
                self._emit_event(
                    event_type="workflow.gate_waiting",
                    entity_kind="workflow_node",
                    entity_id=node.id,
                    workflow_run_id=run.id,
                    summary=f"Waiting for gate input: {node.node_key}",
                    payload={"prompt": node.definition.get("prompt")},
                )
            else:
                node.status = "failed"
                node.failure_reason = f"Unsupported workflow node kind: {node.kind}"

    def _enqueue_work_node(self, run: WorkflowRun, node: WorkflowNode) -> None:
        node_def = node.definition
        context = dict(run.input)
        context.update(dict(node_def.get("context") or {}))
        work_item = WorkRepository(self.session).create_work_item(
            title=str(node_def.get("title", node.node_key)),
            task_instruction=str(
                node_def.get("task_instruction") or node_def.get("instruction") or node.node_key
            ),
            worker_kind=str(node_def.get("worker_kind", "manual")),
            runtime_contract=dict(node_def.get("runtime_contract") or {}),
            context=context,
            retry_policy=dict(node_def.get("retry_policy") or {}),
            priority=int(node_def.get("priority", 0)),
            max_attempts=int(node_def.get("max_attempts", 1)),
            idempotency_key=f"workflow:{run.id}:{node.node_key}",
            source_kind="workflow",
            source_id=run.id,
            workflow_run_id=run.id,
            workflow_node_id=node.id,
            discord_thread_id=run.discord_thread_id,
        )
        node.work_item_id = work_item.id
        node.status = "enqueued"
        self.session.flush()
        self._emit_event(
            event_type="workflow.node_enqueued",
            entity_kind="workflow_node",
            entity_id=node.id,
            work_item_id=work_item.id,
            workflow_run_id=run.id,
            summary=f"Workflow node enqueued: {node.node_key}",
        )

    def _expand_fan_out_node(self, run: WorkflowRun, node: WorkflowNode) -> None:
        node_def = node.definition
        items = node_def.get("items")
        if items is None:
            items = self._fan_out_items_from_reference(run, node_def)
        if not isinstance(items, list):
            node.status = "failed"
            node.failure_reason = "fan_out items must be a list."
            return

        child_ids: list[str] = []
        downstream_edges = self.session.scalars(
            select(WorkflowEdge).where(WorkflowEdge.from_node_id == node.id)
        ).all()
        for index, item in enumerate(items):
            child_key = f"{node.node_key}.{index}"
            existing = self.session.scalar(
                select(WorkflowNode).where(
                    WorkflowNode.workflow_run_id == run.id,
                    WorkflowNode.node_key == child_key,
                )
            )
            if existing is not None:
                child = existing
            else:
                child_definition = self._fan_out_child_definition(node_def, item, index)
                child = WorkflowNode(
                    workflow_run_id=run.id,
                    node_key=child_key,
                    kind="work",
                    status="pending",
                    definition=child_definition,
                    input={"item": item, "index": index},
                    output={},
                    parent_node_id=node.id,
                    fanout_index=index,
                )
                self.session.add(child)
                self.session.flush()
                self.session.add(
                    WorkflowEdge(
                        workflow_run_id=run.id,
                        from_node_id=node.id,
                        to_node_id=child.id,
                        condition="succeeded",
                    )
                )
                for edge in downstream_edges:
                    self.session.add(
                        WorkflowEdge(
                            workflow_run_id=run.id,
                            from_node_id=child.id,
                            to_node_id=edge.to_node_id,
                            condition="succeeded",
                        )
                    )
            child_ids.append(child.id)

        node.status = "succeeded"
        node.output = {"child_node_ids": child_ids, "count": len(child_ids)}
        self.session.flush()
        self._emit_event(
            event_type="workflow.fan_out_expanded",
            entity_kind="workflow_node",
            entity_id=node.id,
            workflow_run_id=run.id,
            summary=f"Expanded fan-out node: {node.node_key}",
            payload={"count": len(child_ids)},
        )

    def _complete_join_node(self, run: WorkflowRun, node: WorkflowNode) -> None:
        dependencies = self.session.scalars(
            select(WorkflowEdge).where(WorkflowEdge.to_node_id == node.id)
        ).all()
        outputs: dict[str, Any] = {}
        for edge in dependencies:
            upstream = self.session.get(WorkflowNode, edge.from_node_id)
            if upstream is not None:
                outputs[upstream.node_key] = upstream.output
        node.status = "succeeded"
        node.output = {"dependencies": outputs}
        self.session.flush()
        self._emit_event(
            event_type="workflow.join_completed",
            entity_kind="workflow_node",
            entity_id=node.id,
            workflow_run_id=run.id,
            summary=f"Completed join node: {node.node_key}",
        )

    def _fan_out_child_definition(
        self,
        node_def: dict[str, Any],
        item: Any,
        index: int,
    ) -> dict[str, Any]:
        title_template = str(
            node_def.get("child_title_template", node_def.get("title", "Fan-out child {index}"))
        )
        instruction_template = str(
            node_def.get("child_task_instruction_template")
            or node_def.get("task_instruction_template")
            or node_def.get("task_instruction")
            or "Process fan-out item {index}: {item}"
        )
        context = dict(node_def.get("context") or {})
        context.update({"item": item, "index": index})
        return {
            "key": f"{node_def['key']}.{index}",
            "kind": "work",
            "title": _render_fan_out_template(title_template, item=item, index=index),
            "task_instruction": _render_fan_out_template(
                instruction_template,
                item=item,
                index=index,
            ),
            "worker_kind": node_def.get("child_worker_kind", node_def.get("worker_kind", "manual")),
            "runtime_contract": dict(node_def.get("runtime_contract") or {}),
            "context": context,
            "retry_policy": dict(node_def.get("retry_policy") or {}),
            "priority": int(node_def.get("priority", 0)),
            "max_attempts": int(node_def.get("max_attempts", 1)),
        }

    def _fan_out_items_from_reference(self, run: WorkflowRun, node_def: dict[str, Any]) -> Any:
        if "items_from_output" in node_def:
            reference = node_def["items_from_output"]
            if isinstance(reference, str):
                node_key, _, path = reference.partition(".")
                return self._workflow_node_output_path(
                    run,
                    node_key=node_key,
                    path=path,
                )
            if isinstance(reference, dict):
                return self._workflow_node_output_path(
                    run,
                    node_key=str(reference.get("node") or ""),
                    path=str(reference.get("path") or ""),
                )
            return []
        return run.input.get(str(node_def.get("items_from", "items")), [])

    def _workflow_node_output_path(
        self,
        run: WorkflowRun,
        *,
        node_key: str,
        path: str,
    ) -> Any:
        if not node_key:
            return []
        node = self.session.scalar(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.node_key == node_key,
            )
        )
        if node is None:
            return []
        value: Any = node.output or {}
        if not path:
            return value
        for part in path.split("."):
            if not part:
                continue
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

    def _dependencies_satisfied(self, node: WorkflowNode) -> bool:
        edges = self.session.scalars(
            select(WorkflowEdge).where(WorkflowEdge.to_node_id == node.id)
        ).all()
        for edge in edges:
            upstream = self.session.get(WorkflowNode, edge.from_node_id)
            if upstream is None or upstream.status != edge.condition:
                return False
        return True

    def _finalize_run_if_ready(self, run: WorkflowRun) -> None:
        nodes = self.session.scalars(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id)
        ).all()
        if any(node.status == "failed" for node in nodes):
            run.status = "failed"
            run.ended_at = utc_now()
            self._emit_event(
                event_type="workflow.run_failed",
                entity_kind="workflow_run",
                entity_id=run.id,
                workflow_run_id=run.id,
                summary=f"Workflow run failed: {run.name}",
            )
            return
        if all(node.status == "succeeded" for node in nodes):
            run.status = "completed"
            run.ended_at = utc_now()
            run.state = {"outputs": {node.node_key: node.output for node in nodes}}
            self._emit_event(
                event_type="workflow.run_completed",
                entity_kind="workflow_run",
                entity_id=run.id,
                workflow_run_id=run.id,
                summary=f"Workflow run completed: {run.name}",
            )

    def _get_run(self, workflow_run_id: str) -> WorkflowRun:
        run = self.session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise KeyError(f"Unknown workflow run: {workflow_run_id}")
        return run

    def _run_nodes(self, workflow_run_id: str) -> list[WorkflowNode]:
        return list(
            self.session.scalars(
                select(WorkflowNode)
                .where(WorkflowNode.workflow_run_id == workflow_run_id)
                .order_by(WorkflowNode.created_at)
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

    def _pause_ready_run_work(self, workflow_run_id: str) -> list[str]:
        work_items = self.session.scalars(
            select(WorkItem).where(
                WorkItem.workflow_run_id == workflow_run_id,
                WorkItem.status == "ready",
            )
        ).all()
        queue = WorkQueue(self.session)
        paused_ids = []
        for work_item in work_items:
            queue.pause_work(work_item.id)
            paused_ids.append(work_item.id)
        return paused_ids

    def _resume_paused_run_work(self, workflow_run_id: str) -> list[str]:
        work_items = self.session.scalars(
            select(WorkItem).where(
                WorkItem.workflow_run_id == workflow_run_id,
                WorkItem.status == "paused",
            )
        ).all()
        queue = WorkQueue(self.session)
        resumed_ids = []
        for work_item in work_items:
            queue.resume_work(work_item.id)
            resumed_ids.append(work_item.id)
        return resumed_ids

    def _cancel_active_run_work(self, workflow_run_id: str) -> list[str]:
        work_items = self.session.scalars(
            select(WorkItem).where(
                WorkItem.workflow_run_id == workflow_run_id,
                WorkItem.status.not_in(TERMINAL_WORK_STATUSES),
            )
        ).all()
        queue = WorkQueue(self.session)
        canceled_ids = []
        for work_item in work_items:
            queue.request_cancel(work_item.id)
            canceled_ids.append(work_item.id)
        return canceled_ids

    def _latest_attempt_output(self, work_item_id: str) -> dict[str, Any]:
        attempt = self.session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work_item_id)
            .order_by(WorkAttempt.attempt_number.desc())
        )
        if attempt is None:
            return {}
        return attempt.produces or {}

    def _run_fingerprint(self, run: WorkflowRun) -> tuple[Any, ...]:
        nodes = self.session.scalars(
            select(WorkflowNode)
            .where(WorkflowNode.workflow_run_id == run.id)
            .order_by(WorkflowNode.node_key)
        ).all()
        return (run.status, tuple((node.node_key, node.status, node.work_item_id) for node in nodes))

    def _validate_definition(self, definition: dict[str, Any]) -> None:
        nodes = definition.get("nodes")
        if not isinstance(nodes, list) or not nodes:
            raise ValueError("Workflow definition requires a non-empty nodes list.")
        keys = set()
        for node in nodes:
            if "key" not in node:
                raise ValueError("Each workflow node requires a key.")
            key = str(node["key"])
            if key in keys:
                raise ValueError(f"Duplicate workflow node key: {key}")
            keys.add(key)
        for node in nodes:
            for dependency_key in node.get("depends_on", []):
                if str(dependency_key) not in keys:
                    raise ValueError(f"Unknown workflow dependency: {dependency_key}")

    def _emit_event(
        self,
        *,
        event_type: str,
        entity_kind: str,
        entity_id: str,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WorkEvent:
        event = WorkEvent(
            event_type=event_type,
            entity_kind=entity_kind,
            entity_id=entity_id,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source="workflow",
            summary=summary,
            payload=payload or {},
        )
        self.session.add(event)
        self.session.flush()
        return event


def _render_fan_out_template(template: str, *, item: Any, index: int) -> str:
    rendered = template.replace("{index}", str(index))
    rendered = _ITEM_KEY_RE.sub(lambda match: _item_value(item, match.group(1), match.group(0)), rendered)
    return rendered.replace("{item}", _item_text(item))


def _item_value(item: Any, key: str, fallback: str) -> str:
    if isinstance(item, dict) and key in item:
        return str(item[key])
    return fallback


def _item_text(item: Any) -> str:
    if isinstance(item, str):
        return item
    try:
        return json.dumps(item, ensure_ascii=True, sort_keys=True)
    except TypeError:
        return str(item)
