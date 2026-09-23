from __future__ import annotations

from typing import Any

from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.mcp.tools._shared import (
    calling_work_item,
    clamp,
    event_data,
    inherit_reply_config,
    optional_string,
    required,
    resolve_instruction,
    run_json,
    string_list,
    work_data,
)
from tasque2.models import WorkEvent, WorkItem
from tasque2.work.queue import WorkQueue
from tasque2.work.repository import WorkRepository


def work_enqueue(
    title: str,
    task_instruction: str | None = None,
    task_template_path: str | None = None,
    template_base_dir: str | None = None,
    worker_kind: str = "provider.default",
    runtime_contract: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    priority: int = 0,
    max_attempts: int = 1,
    idempotency_key: str | None = None,
    discord_thread_id: str | None = None,
    lane: str | None = None,
) -> str:
    """Queue a work item from an instruction or a Markdown template file.

    Work queued from a run inherits that run's lane and reply handling unless it sets its
    own. ``discord_thread_id`` posts its result into an existing thread.
    """
    return run_json(
        lambda: _enqueue(
            title=title,
            task_instruction=task_instruction,
            task_template_path=task_template_path,
            template_base_dir=template_base_dir,
            worker_kind=worker_kind,
            runtime_contract=runtime_contract,
            context=context,
            priority=priority,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            discord_thread_id=discord_thread_id,
            lane=lane,
        )
    )


def work_list(status: str | list[str] | None = None, lane: str | None = None, limit: int = 20, intent: str = "") -> str:
    """List recent work items, optionally by status or lane."""
    return run_json(lambda: _list(status, lane, limit), intent=intent)


def work_get(work_item_id: str, intent: str = "") -> str:
    """Fetch one work item in full, including its instruction, contract and context."""
    return run_json(lambda: _get(work_item_id), intent=intent)


def work_events(work_item_id: str, limit: int = 50, intent: str = "") -> str:
    """List a work item's recent events."""
    return run_json(lambda: _events(work_item_id, limit), intent=intent)


def work_cancel(work_item_id: str) -> str:
    """Cancel a work item, or request cancellation when it is running."""
    return run_json(lambda: _transition(work_item_id, "cancel"))


def work_retry(work_item_id: str) -> str:
    """Return a dead-lettered work item to the queue."""
    return run_json(lambda: _transition(work_item_id, "retry"))


def _enqueue(**fields: Any) -> dict[str, Any]:
    instruction = resolve_instruction(
        task_instruction=fields["task_instruction"],
        task_template_path=fields["task_template_path"],
        template_base_dir=fields["template_base_dir"],
    )
    if fields["context"] is not None and not isinstance(fields["context"], dict):
        raise ValueError("context must be an object or omitted.")
    with session_scope() as session:
        caller = calling_work_item(session)
        work = WorkRepository(session).create_work_item(
            title=required(fields["title"], "title"),
            task_instruction=instruction,
            worker_kind=required(fields["worker_kind"], "worker_kind"),
            runtime_contract=dict(fields["runtime_contract"] or {}),
            context=inherit_reply_config(dict(fields["context"] or {}), caller, parent_pointer=True),
            priority=int(fields["priority"]),
            max_attempts=max(1, int(fields["max_attempts"])),
            idempotency_key=optional_string(fields["idempotency_key"]),
            source_kind="mcp",
            source_id=caller.id if caller is not None else None,
            discord_thread_id=optional_string(fields["discord_thread_id"]),
            lane=optional_string(fields["lane"]),
        )
        return {"ok": True, "work_item": work_data(work)}


def _list(status: str | list[str] | None, lane: str | None, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(WorkItem).order_by(WorkItem.created_at.desc()).limit(clamp(limit))
        statuses = string_list(status)
        if statuses:
            statement = statement.where(WorkItem.status.in_(statuses))
        if lane:
            statement = statement.where(WorkItem.lane == lane)
        return {"ok": True, "items": [work_data(work) for work in session.scalars(statement).all()]}


def _get(work_item_id: str) -> dict[str, Any]:
    with session_scope() as session:
        work = session.get(WorkItem, required(work_item_id, "work_item_id"))
        if work is None:
            raise KeyError(f"Unknown work item: {work_item_id}")
        return {"ok": True, "work_item": work_data(work, full=True)}


def _events(work_item_id: str, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        rows = session.scalars(
            select(WorkEvent)
            .where(WorkEvent.work_item_id == required(work_item_id, "work_item_id"))
            .order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc())
            .limit(clamp(limit, default=50))
        ).all()
        return {"ok": True, "items": [event_data(event) for event in rows]}


def _transition(work_item_id: str, action: str) -> dict[str, Any]:
    with session_scope() as session:
        queue = WorkQueue(session)
        work = queue.request_cancel(work_item_id) if action == "cancel" else queue.retry_dead_letter(work_item_id)
        return {"ok": True, "work_item": work_data(work)}
