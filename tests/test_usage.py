from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.orm import Session

from tasque2.db import session_scope
from tasque2.models import ProviderRun, WorkAttempt, utc_now
from tasque2.ops.usage import UsageRow, usage_by_lane
from tasque2.work.repository import WorkRepository


def _provider_run(
    session: Session,
    *,
    title: str,
    lane: str | None,
    usage: dict[str, Any],
    model: str | None = None,
    status: str = "succeeded",
    seconds: float = 60.0,
    created_at: datetime | None = None,
) -> ProviderRun:
    work = WorkRepository(session).create_work_item(
        title=title, task_instruction="Run.", worker_kind="provider.claude", lane=lane
    )
    attempt = WorkAttempt(work_item_id=work.id, attempt_number=1, status=status, worker_kind=work.worker_kind)
    session.add(attempt)
    session.flush()
    started = (created_at or utc_now()) - timedelta(seconds=seconds)
    run = ProviderRun(
        attempt_id=attempt.id,
        provider="claude",
        model=model,
        status=status,
        usage=usage,
        started_at=started,
        ended_at=started + timedelta(seconds=seconds),
        created_at=created_at or utc_now(),
    )
    session.add(run)
    session.flush()
    return run


def _usage(model: str | None, *, cost: float, input_tokens: int = 100, output_tokens: int = 50, **extra: Any) -> dict:
    usage: dict[str, Any] = {
        "input_tokens": input_tokens,
        "cache_read_tokens": 1000,
        "cache_write_tokens": 200,
        "output_tokens": output_tokens,
        "messages": 4,
        "estimated_cost_usd": cost,
        **extra,
    }
    if model is not None:
        usage["model"] = model
    return usage


def _rows(**kwargs: Any) -> dict[tuple[str, str], UsageRow]:
    with session_scope() as session:
        return {(row.lane, row.model): row for row in usage_by_lane(session, **kwargs)}


def test_usage_sums_runs_per_lane_and_model(fresh_db: Path) -> None:
    with session_scope() as session:
        _provider_run(
            session,
            title="Finance daily",
            lane="finance",
            usage=_usage("claude-opus-5-5", cost=1.5, tool_calls={"Read": 2, "mcp__tasque__memory_get": 1}),
            seconds=120,
        )
        _provider_run(
            session,
            title="Finance daily",
            lane="finance",
            usage=_usage("claude-opus-5-5", cost=0.5, input_tokens=300, tool_calls={"Read": 3}),
            status="failed",
            seconds=60,
        )
        _provider_run(session, title="Finance reply", lane="finance", usage=_usage("claude-sonnet-5", cost=0.25))

    rows = _rows()

    assert set(rows) == {("finance", "claude-opus-5-5"), ("finance", "claude-sonnet-5")}
    opus = rows[("finance", "claude-opus-5-5")]
    assert opus.runs == 2
    assert opus.failed == 1
    assert opus.input_tokens == 400
    assert opus.cache_read_tokens == 2000
    assert opus.cache_write_tokens == 400
    assert opus.output_tokens == 100
    assert opus.prompt_tokens == 400 + 2000 + 400
    assert opus.messages == 8
    assert opus.estimated_cost_usd == pytest.approx(2.0)
    assert opus.seconds == pytest.approx(180.0)
    assert opus.tools == {"Read": 5, "mcp__tasque__memory_get": 1}
    assert rows[("finance", "claude-sonnet-5")].runs == 1


def test_usage_orders_rows_by_estimated_cost(fresh_db: Path) -> None:
    with session_scope() as session:
        _provider_run(session, title="Kitchen", lane="kitchen", usage=_usage("claude-sonnet-5", cost=0.4))
        _provider_run(session, title="Career", lane="career", usage=_usage("claude-opus-5-5", cost=3.0))
        _provider_run(session, title="Sleep", lane="sleep", usage=_usage("claude-haiku-4-5", cost=0.01))

    with session_scope() as session:
        lanes = [row.lane for row in usage_by_lane(session)]

    assert lanes == ["career", "kitchen", "sleep"]


def test_usage_falls_back_to_the_title_and_the_run_model(fresh_db: Path) -> None:
    with session_scope() as session:
        _provider_run(
            session, title="Ad hoc: check the weather", lane=None, usage=_usage(None, cost=0.1), model="claude-opus-5-5"
        )
        _provider_run(session, title="Untracked", lane=None, usage={}, model=None)

    rows = _rows()

    assert set(rows) == {("Ad hoc", "claude-opus-5-5"), ("Untracked", "unknown")}
    assert rows[("Untracked", "unknown")].estimated_cost_usd == 0.0


def test_usage_counts_only_runs_inside_the_window(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        _provider_run(
            session, title="Recent", lane="recent", usage=_usage("m", cost=1.0), created_at=now - timedelta(days=2)
        )
        _provider_run(
            session, title="Old", lane="old", usage=_usage("m", cost=1.0), created_at=now - timedelta(days=20)
        )

    assert set(_rows(days=14, now=now)) == {("recent", "m")}
    assert set(_rows(days=30, now=now)) == {("recent", "m"), ("old", "m")}
    assert set(_rows(days=1, now=now)) == set()
