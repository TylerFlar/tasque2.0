from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.events import record_event
from tasque2.lane_context import effective_context
from tasque2.models import WorkEvent, WorkItem
from tasque2.telemetry import current_traceparent


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
        visible: bool = True,
        lane: str | None = None,
        traceparent: str | None = None,
    ) -> WorkItem:
        """Create a ready work item; an existing idempotency key returns the original row."""
        if idempotency_key:
            existing = self.session.scalar(select(WorkItem).where(WorkItem.idempotency_key == idempotency_key))
            if existing is not None:
                return existing
        context = effective_context(context)
        work_item = WorkItem(
            title=title[:240],
            task_instruction=task_instruction,
            worker_kind=worker_kind,
            runtime_contract=runtime_contract or {},
            context=context,
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
            visible=visible,
            lane=lane or self._inherited_lane(context),
            traceparent=traceparent or current_traceparent(),
        )
        self.session.add(work_item)
        self.session.flush()
        record_event(
            self.session,
            event_type="work.created",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            workflow_run_id=workflow_run_id,
            source="repo",
            summary=f"Created work item: {work_item.title}",
            payload={
                "worker_kind": worker_kind,
                "priority": priority,
                "source_kind": source_kind,
                "source_id": source_id,
                "lane": work_item.lane,
            },
        )
        return work_item

    def get_work_item(self, work_item_id: str) -> WorkItem | None:
        return self.session.get(WorkItem, work_item_id)

    def list_events_for_work(self, work_item_id: str) -> Sequence[WorkEvent]:
        return self.session.scalars(
            select(WorkEvent).where(WorkEvent.work_item_id == work_item_id).order_by(WorkEvent.created_at, WorkEvent.id)
        ).all()

    def _inherited_lane(self, context: dict[str, Any]) -> str | None:
        explicit = context.get("lane")
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()[:120]
        parent_id = context.get("parent_work_item_id")
        if isinstance(parent_id, str) and parent_id:
            parent = self.session.get(WorkItem, parent_id)
            if parent is not None:
                return parent.lane
        return None
