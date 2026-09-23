from __future__ import annotations

from typing import Any

from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.mcp.tools._shared import (
    clamp,
    optional_string,
    required,
    run_json,
    workflow_definition_data,
    workflow_run_data,
)
from tasque2.models import WorkflowDefinition
from tasque2.workflows import WorkflowService


def workflow_list(enabled: bool | None = None, limit: int = 20, intent: str = "") -> str:
    """List workflow definitions that can be started."""
    return run_json(lambda: _list(enabled, limit), intent=intent)


def workflow_start(
    workflow_name: str | None = None,
    workflow_definition_id: str | None = None,
    version: str = "1",
    run_name: str | None = None,
    input: dict[str, Any] | None = None,
    discord_thread_id: str | None = None,
) -> str:
    """Start a workflow run by name or definition id, as its own run."""
    return run_json(lambda: _start(workflow_name, workflow_definition_id, version, run_name, input, discord_thread_id))


def _list(enabled: bool | None, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(WorkflowDefinition).order_by(WorkflowDefinition.name).limit(clamp(limit))
        if enabled is not None:
            statement = statement.where(WorkflowDefinition.enabled.is_(bool(enabled)))
        return {"ok": True, "items": [workflow_definition_data(item) for item in session.scalars(statement).all()]}


def _start(name, definition_id, version, run_name, run_input, thread_id) -> dict[str, Any]:
    if run_input is not None and not isinstance(run_input, dict):
        raise ValueError("input must be an object or omitted.")
    with session_scope() as session:
        if definition_id:
            definition = session.get(WorkflowDefinition, definition_id)
        else:
            definition = session.scalar(
                select(WorkflowDefinition).where(
                    WorkflowDefinition.name == required(name, "workflow_name"),
                    WorkflowDefinition.version == str(version or "1"),
                )
            )
        if definition is None:
            raise KeyError(f"Unknown workflow definition: {definition_id or f'{name}@{version}'}")
        run = WorkflowService(session).start_run(
            workflow_definition_id=definition.id,
            name=optional_string(run_name) or definition.name,
            input=dict(run_input or {}),
            discord_thread_id=optional_string(thread_id),
        )
        return {
            "ok": True,
            "workflow_definition": workflow_definition_data(definition),
            "workflow_run": workflow_run_data(run),
        }
