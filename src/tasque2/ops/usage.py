"""Token and estimated-cost totals per lane and model, read from recorded provider runs."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.models import ProviderRun, WorkAttempt, WorkItem, utc_now


@dataclass
class UsageRow:
    lane: str
    model: str
    runs: int = 0
    failed: int = 0
    input_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    output_tokens: int = 0
    messages: int = 0
    estimated_cost_usd: float = 0.0
    seconds: float = 0.0
    tools: dict[str, int] = field(default_factory=dict)

    @property
    def prompt_tokens(self) -> int:
        return self.input_tokens + self.cache_read_tokens + self.cache_write_tokens


def usage_by_lane(session: Session, *, days: int = 14, now: datetime | None = None) -> list[UsageRow]:
    since = (now or utc_now()) - timedelta(days=max(1, days))
    rows = session.execute(
        select(ProviderRun, WorkItem.lane, WorkItem.title)
        .join(WorkAttempt, WorkAttempt.id == ProviderRun.attempt_id)
        .join(WorkItem, WorkItem.id == WorkAttempt.work_item_id)
        .where(ProviderRun.created_at >= since)
    ).all()
    totals: dict[tuple[str, str], UsageRow] = {}
    for run, lane, title in rows:
        usage = run.usage or {}
        name = lane or title.split(":")[0][:40]
        model = str(usage.get("model") or run.model or "unknown")
        row = totals.setdefault((name, model), UsageRow(lane=name, model=model))
        row.runs += 1
        if run.status not in {"succeeded"}:
            row.failed += 1
        row.input_tokens += int(usage.get("input_tokens") or 0)
        row.cache_read_tokens += int(usage.get("cache_read_tokens") or 0)
        row.cache_write_tokens += int(usage.get("cache_write_tokens") or 0)
        row.output_tokens += int(usage.get("output_tokens") or 0)
        row.messages += int(usage.get("messages") or 0)
        row.estimated_cost_usd += float(usage.get("estimated_cost_usd") or 0.0)
        if run.started_at and run.ended_at:
            row.seconds += max(0.0, (run.ended_at - run.started_at).total_seconds())
        merged: dict[str, int] = defaultdict(int, row.tools)
        for tool, count in (usage.get("tool_calls") or {}).items():
            merged[str(tool)] += int(count)
        row.tools = dict(merged)
    return sorted(totals.values(), key=lambda item: (item.estimated_cost_usd, item.prompt_tokens), reverse=True)
