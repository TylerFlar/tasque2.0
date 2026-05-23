from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.models import Artifact, WorkEvent, WorkItem


class WorkRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_work_item(
        self,
        *,
        title: str,
        task_instruction: str,
        worker_kind: str,
        runtime_contract: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        retry_policy: dict[str, Any] | None = None,
        priority: int = 0,
        not_before: datetime | None = None,
        deadline_at: datetime | None = None,
        max_attempts: int = 1,
        idempotency_key: str | None = None,
        source_kind: str | None = None,
        source_id: str | None = None,
        schedule_id: str | None = None,
        schedule_occurrence_id: str | None = None,
        workflow_run_id: str | None = None,
        workflow_node_id: str | None = None,
        discord_thread_id: str | None = None,
    ) -> WorkItem:
        if idempotency_key:
            existing = self.session.scalar(
                select(WorkItem).where(WorkItem.idempotency_key == idempotency_key)
            )
            if existing is not None:
                return existing

        work_item = WorkItem(
            title=title,
            task_instruction=task_instruction,
            worker_kind=worker_kind,
            runtime_contract=runtime_contract or {},
            context=context or {},
            retry_policy=retry_policy or {},
            priority=priority,
            not_before=not_before,
            deadline_at=deadline_at,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            source_kind=source_kind,
            source_id=source_id,
            schedule_id=schedule_id,
            schedule_occurrence_id=schedule_occurrence_id,
            workflow_run_id=workflow_run_id,
            workflow_node_id=workflow_node_id,
            discord_thread_id=discord_thread_id,
        )
        self.session.add(work_item)
        self.session.flush()
        self.emit_event(
            event_type="work.created",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            source="repo",
            summary=f"Created work item: {title}",
            payload={
                "title": title,
                "worker_kind": worker_kind,
                "priority": priority,
                "source_kind": source_kind,
                "source_id": source_id,
            },
        )
        return work_item

    def get_work_item(self, work_item_id: str) -> WorkItem | None:
        return self.session.get(WorkItem, work_item_id)

    def emit_event(
        self,
        *,
        event_type: str,
        entity_kind: str,
        entity_id: str,
        work_item_id: str | None = None,
        attempt_id: str | None = None,
        workflow_run_id: str | None = None,
        schedule_id: str | None = None,
        source: str = "tasque",
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WorkEvent:
        event = WorkEvent(
            event_type=event_type,
            entity_kind=entity_kind,
            entity_id=entity_id,
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            workflow_run_id=workflow_run_id,
            schedule_id=schedule_id,
            source=source,
            summary=summary,
            payload=payload or {},
        )
        self.session.add(event)
        self.session.flush()
        return event

    def list_events_for_work(self, work_item_id: str) -> Sequence[WorkEvent]:
        return self.session.scalars(
            select(WorkEvent)
            .where(WorkEvent.work_item_id == work_item_id)
            .order_by(WorkEvent.created_at, WorkEvent.id)
        ).all()

    def record_artifact(
        self,
        *,
        kind: str,
        title: str,
        local_path: str,
        work_item_id: str | None = None,
        attempt_id: str | None = None,
        workflow_run_id: str | None = None,
        content_type: str | None = None,
        size_bytes: int | None = None,
        sha256: str | None = None,
        summary: str | None = None,
        tags: list[str] | None = None,
        source_kind: str | None = None,
        source_id: str | None = None,
    ) -> Artifact:
        artifact = Artifact(
            kind=kind,
            title=title,
            local_path=local_path,
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            content_type=content_type,
            size_bytes=size_bytes,
            sha256=sha256,
            summary=summary,
            tags=tags or [],
            source_kind=source_kind,
            source_id=source_id,
            workflow_run_id=workflow_run_id,
        )
        self.session.add(artifact)
        self.session.flush()
        self.emit_event(
            event_type="artifact.recorded",
            entity_kind="artifact",
            entity_id=artifact.id,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source="repo",
            summary=f"Recorded artifact: {title}",
            payload={"kind": kind, "local_path": local_path, "tags": tags or []},
        )
        return artifact
