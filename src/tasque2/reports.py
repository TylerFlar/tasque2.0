from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.models import (
    Artifact,
    ProviderRun,
    WorkAttempt,
    WorkEvent,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
)


@dataclass(frozen=True)
class Report:
    title: str
    body: str
    data: dict[str, Any]


class ReportService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def work_report(self, work_item_id: str) -> Report:
        work = self.session.get(WorkItem, work_item_id)
        if work is None:
            raise KeyError(f"Unknown work item: {work_item_id}")

        attempts = self.session.scalars(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work.id)
            .order_by(WorkAttempt.attempt_number)
        ).all()
        events = self._events(work_item_id=work.id)
        artifacts = self.session.scalars(
            select(Artifact).where(Artifact.work_item_id == work.id).order_by(Artifact.created_at)
        ).all()
        provider_runs = []
        for attempt in attempts:
            provider_runs.extend(
                self.session.scalars(
                    select(ProviderRun).where(ProviderRun.attempt_id == attempt.id)
                ).all()
            )

        data = {
            "work_item": self._work_item_data(work),
            "attempts": [self._attempt_data(attempt) for attempt in attempts],
            "provider_runs": [self._provider_run_data(run) for run in provider_runs],
            "artifacts": [self._artifact_data(artifact) for artifact in artifacts],
            "events": [self._event_data(event) for event in events],
        }
        body = self._render_work_report(data)
        return Report(title=f"Work Report: {work.title}", body=body, data=data)

    def workflow_report(self, workflow_run_id: str) -> Report:
        run = self.session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise KeyError(f"Unknown workflow run: {workflow_run_id}")

        nodes = self.session.scalars(
            select(WorkflowNode)
            .where(WorkflowNode.workflow_run_id == run.id)
            .order_by(WorkflowNode.created_at)
        ).all()
        work_items = self.session.scalars(
            select(WorkItem)
            .where(WorkItem.workflow_run_id == run.id)
            .order_by(WorkItem.created_at)
        ).all()
        events = self._events(workflow_run_id=run.id)

        data = {
            "workflow_run": self._workflow_run_data(run),
            "workflow_definition": {
                "id": run.definition.id,
                "name": run.definition.name,
                "version": run.definition.version,
            },
            "workflow_nodes": [self._workflow_node_data(node) for node in nodes],
            "work_items": [self._work_item_data(work) for work in work_items],
            "events": [self._event_data(event) for event in events],
        }
        body = self._render_workflow_report(data)
        return Report(title=f"Workflow Report: {run.name}", body=body, data=data)

    def _events(
        self,
        *,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
        limit: int = 200,
    ) -> list[WorkEvent]:
        statement = select(WorkEvent).order_by(WorkEvent.created_at, WorkEvent.id).limit(limit)
        if work_item_id is not None:
            statement = statement.where(WorkEvent.work_item_id == work_item_id)
        if workflow_run_id is not None:
            statement = statement.where(WorkEvent.workflow_run_id == workflow_run_id)
        return list(self.session.scalars(statement).all())

    def _render_work_report(self, data: dict[str, Any]) -> str:
        work = data["work_item"]
        lines = [
            f"# Work Report: {work['title']}",
            "",
            f"- id: {work['id']}",
            f"- status: {work['status']}",
            f"- worker: {work['worker_kind']}",
            f"- attempts: {len(data['attempts'])}",
            f"- artifacts: {len(data['artifacts'])}",
            "",
            "## Attempts",
        ]
        for attempt in data["attempts"]:
            lines.extend(
                [
                    f"- attempt {attempt['attempt_number']}: {attempt['status']}",
                    f"  summary: {attempt.get('summary') or ''}",
                    f"  error: {attempt.get('error_message') or ''}",
                ]
            )
        lines.extend(["", "## Events"])
        for event in data["events"]:
            lines.append(f"- {event['created_at']} {event['event_type']}: {event.get('summary') or ''}")
        return "\n".join(lines)

    def _render_workflow_report(self, data: dict[str, Any]) -> str:
        run = data["workflow_run"]
        definition = data["workflow_definition"]
        lines = [
            f"# Workflow Report: {run['name']}",
            "",
            f"- id: {run['id']}",
            f"- status: {run['status']}",
            f"- definition: {definition['name']}@{definition['version']}",
            f"- started: {run.get('started_at') or ''}",
            f"- ended: {run.get('ended_at') or ''}",
            f"- nodes: {len(data['workflow_nodes'])}",
            f"- work items: {len(data['work_items'])}",
            "",
            "## Nodes",
        ]
        for node in data["workflow_nodes"]:
            details = []
            if node.get("work_item_id"):
                details.append(f"work={node['work_item_id']}")
            if node.get("failure_reason"):
                details.append(f"failure={node['failure_reason']}")
            suffix = f" ({', '.join(details)})" if details else ""
            lines.append(f"- {node['node_key']}: {node['status']} [{node['kind']}]{suffix}")
        lines.extend(["", "## Work Items"])
        for work in data["work_items"]:
            lines.append(f"- {work['title']}: {work['status']} ({work['worker_kind']})")
        lines.extend(["", "## Events"])
        for event in data["events"]:
            lines.append(f"- {event['created_at']} {event['event_type']}: {event.get('summary') or ''}")
        return "\n".join(lines)

    def _work_item_data(self, work: WorkItem) -> dict[str, Any]:
        return {
            "id": work.id,
            "title": work.title,
            "status": work.status,
            "worker_kind": work.worker_kind,
            "priority": work.priority,
            "attempt_count": work.attempt_count,
            "source_kind": work.source_kind,
            "source_id": work.source_id,
            "workflow_run_id": work.workflow_run_id,
            "created_at": work.created_at.isoformat(),
            "updated_at": work.updated_at.isoformat(),
        }

    def _attempt_data(self, attempt: WorkAttempt) -> dict[str, Any]:
        return {
            "id": attempt.id,
            "attempt_number": attempt.attempt_number,
            "status": attempt.status,
            "provider": attempt.provider,
            "provider_run_id": attempt.provider_run_id,
            "summary": attempt.summary,
            "error_type": attempt.error_type,
            "error_message": attempt.error_message,
            "produces": attempt.produces,
        }

    def _provider_run_data(self, provider_run: ProviderRun) -> dict[str, Any]:
        return {
            "id": provider_run.id,
            "provider": provider_run.provider,
            "model": provider_run.model,
            "status": provider_run.status,
            "provider_session_id": provider_run.provider_session_id,
            "stdout_artifact_id": provider_run.stdout_artifact_id,
            "stderr_artifact_id": provider_run.stderr_artifact_id,
            "raw_stream_artifact_id": provider_run.raw_stream_artifact_id,
            "usage": provider_run.usage,
        }

    def _artifact_data(self, artifact: Artifact) -> dict[str, Any]:
        return {
            "id": artifact.id,
            "kind": artifact.kind,
            "title": artifact.title,
            "local_path": artifact.local_path,
            "content_type": artifact.content_type,
            "size_bytes": artifact.size_bytes,
            "sha256": artifact.sha256,
            "tags": artifact.tags,
        }

    def _event_data(self, event: WorkEvent) -> dict[str, Any]:
        return {
            "id": event.id,
            "event_type": event.event_type,
            "entity_kind": event.entity_kind,
            "entity_id": event.entity_id,
            "source": event.source,
            "summary": event.summary,
            "payload": event.payload,
            "created_at": event.created_at.isoformat(),
        }

    def _workflow_run_data(self, run: WorkflowRun) -> dict[str, Any]:
        return {
            "id": run.id,
            "name": run.name,
            "status": run.status,
            "state": run.state,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "ended_at": run.ended_at.isoformat() if run.ended_at else None,
        }

    def _workflow_node_data(self, node: WorkflowNode) -> dict[str, Any]:
        return {
            "id": node.id,
            "node_key": node.node_key,
            "kind": node.kind,
            "status": node.status,
            "work_item_id": node.work_item_id,
            "output": node.output,
            "failure_reason": node.failure_reason,
        }


def report_to_json(report: Report) -> str:
    return json.dumps(report.data, indent=2, sort_keys=True)
