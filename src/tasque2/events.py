"""The work event log: one append-only row per state change, mirrored onto the active span."""

from __future__ import annotations

from typing import Any

from opentelemetry import trace
from sqlalchemy.orm import Session

from tasque2.models import WorkEvent


def record_event(
    session: Session,
    *,
    event_type: str,
    entity_kind: str,
    entity_id: str,
    source: str,
    summary: str | None = None,
    payload: dict[str, Any] | None = None,
    work_item_id: str | None = None,
    attempt_id: str | None = None,
    workflow_run_id: str | None = None,
    schedule_id: str | None = None,
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
    session.add(event)
    session.flush()
    current = trace.get_current_span()
    if current.is_recording():
        attributes = {"tasque.entity.kind": entity_kind, "tasque.entity.id": entity_id}
        if summary:
            attributes["tasque.event.summary"] = summary[:300]
        current.add_event(event_type, attributes)
    return event
