from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select

from tasque2.db import session_scope
from tasque2.models import AgentResult, utc_now

DEFAULT_REAP_AGE_SECONDS = 60 * 60


def mint_token() -> str:
    """Generate a fresh opaque token for one provider result submission."""
    return uuid4().hex


def deposit(
    *,
    result_token: str,
    agent_kind: str,
    payload: dict[str, Any],
) -> None:
    """Persist a provider result payload for the matching runtime to consume."""
    token = _required(result_token, "result_token")
    payload_json = json.dumps(payload, default=str)
    with session_scope() as session:
        existing = session.get(AgentResult, token)
        if existing is not None:
            existing.agent_kind = _required(agent_kind, "agent_kind")
            existing.payload_json = payload_json
            existing.created_at = utc_now()
            return
        session.add(
            AgentResult(
                result_token=token,
                agent_kind=_required(agent_kind, "agent_kind"),
                payload_json=payload_json,
            )
        )


def peek(result_token: str, *, agent_kind: str = "worker") -> bool:
    """Return true when a payload exists for this token and agent kind."""
    with session_scope() as session:
        row = session.get(AgentResult, result_token)
        return row is not None and row.agent_kind == agent_kind


def read_and_consume(result_token: str, *, agent_kind: str = "worker") -> dict[str, Any] | None:
    """Fetch and delete a submitted result payload."""
    with session_scope() as session:
        row = session.get(AgentResult, result_token)
        if row is None:
            return None
        kind = row.agent_kind
        payload_json = row.payload_json
        session.delete(row)

    if kind != agent_kind:
        return None
    try:
        parsed = json.loads(payload_json)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def reap_stale(*, max_age_seconds: int = DEFAULT_REAP_AGE_SECONDS) -> int:
    """Delete unconsumed result payloads older than max_age_seconds."""
    cutoff = utc_now() - timedelta(seconds=max_age_seconds)
    with session_scope() as session:
        rows = list(
            session.scalars(select(AgentResult).where(AgentResult.created_at < cutoff)).all()
        )
        if not rows:
            return 0
        session.execute(delete(AgentResult).where(AgentResult.created_at < cutoff))
        return len(rows)


def _required(value: str, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{field_name} is required.")
    return text
