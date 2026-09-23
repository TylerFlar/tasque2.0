"""The result inbox: where a worker's ``submit_worker_result`` call lands.

The MCP server deposits the payload under the run's result token; the daemon polls for it,
stops the provider process tree once it arrives, and consumes it. Payloads carry the work
item id so a result submitted after its daemon died can still be matched to its work.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from tasque2.db import session_scope
from tasque2.models import AgentResult, utc_now

WORKER = "worker"
DEFAULT_REAP_AGE_SECONDS = 60 * 60


def mint_token() -> str:
    return uuid4().hex


def deposit(*, result_token: str, payload: dict[str, Any], agent_kind: str = WORKER) -> None:
    token = _required(result_token, "result_token")
    payload_json = json.dumps(payload, default=str)
    with session_scope() as session:
        existing = session.get(AgentResult, token)
        if existing is not None:
            existing.agent_kind = agent_kind
            existing.payload_json = payload_json
            existing.created_at = utc_now()
            return
        session.add(AgentResult(result_token=token, agent_kind=agent_kind, payload_json=payload_json))


def peek(result_token: str, *, agent_kind: str = WORKER) -> bool:
    with session_scope() as session:
        row = session.get(AgentResult, result_token)
        return row is not None and row.agent_kind == agent_kind


def read_and_consume(result_token: str, *, agent_kind: str = WORKER) -> dict[str, Any] | None:
    with session_scope() as session:
        row = session.get(AgentResult, result_token)
        if row is None:
            return None
        kind, payload_json = row.agent_kind, row.payload_json
        session.delete(row)
    if kind != agent_kind:
        return None
    return _parse(payload_json)


def consume_for_work_item(session: Session, work_item_id: str, *, agent_kind: str = WORKER) -> dict[str, Any] | None:
    """Take the payload deposited for ``work_item_id`` using the caller's session.

    Sharing the caller's session keeps consuming the payload and completing the attempt in
    one transaction, and avoids a second writer on SQLite.
    """
    target = _required(work_item_id, "work_item_id")
    rows = session.scalars(
        select(AgentResult).where(AgentResult.agent_kind == agent_kind).order_by(AgentResult.created_at.desc())
    ).all()
    for row in rows:
        parsed = _parse(row.payload_json)
        if parsed is not None and parsed.get("work_item_id") == target:
            session.delete(row)
            session.flush()
            return parsed
    return None


def reap_stale(*, max_age_seconds: int = DEFAULT_REAP_AGE_SECONDS) -> int:
    cutoff = utc_now() - timedelta(seconds=max_age_seconds)
    with session_scope() as session:
        result = session.execute(delete(AgentResult).where(AgentResult.created_at < cutoff))
        return int(result.rowcount or 0)


def _parse(payload_json: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(payload_json)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _required(value: str | None, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{field_name} is required.")
    return text
