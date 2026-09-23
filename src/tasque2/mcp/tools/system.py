from __future__ import annotations

import os
from typing import Any

from tasque2.db import session_scope
from tasque2.mcp.tools._shared import optional_string, required, run_json
from tasque2.ops.status import get_system_status
from tasque2.weather import fetch_local_weather
from tasque2.worker import results


def submit_worker_result(
    result_token: str,
    summary: str,
    report: str,
    produces: dict[str, Any] | None = None,
    status: str = "succeeded",
    error: str | None = None,
) -> str:
    """Submit this run's result. Call exactly once, as the last action, with the result_token
    from the prompt. ``status``: succeeded, blocked, awaiting_user, or failed (with ``error``)."""
    return run_json(lambda: _submit(result_token, summary, report, produces, status, error))


def weather_now(days: int = 3, intent: str = "") -> str:
    """Current conditions and a daily forecast (high/low, feels-like, rain chance, sunset)
    for the configured home location; ``days`` is 1-7."""
    return run_json(lambda: {"ok": True, "weather": fetch_local_weather(days=days)}, intent=intent)


def system_status(intent: str = "") -> str:
    """Queue, schedule, and workflow counts."""
    return run_json(_status, intent=intent)


def _submit(result_token, summary, report, produces, status, error) -> dict[str, Any]:
    token = required(result_token, "result_token")
    if produces is not None and not isinstance(produces, dict):
        raise ValueError("produces must be an object or omitted.")
    if not isinstance(summary, str) or not isinstance(report, str):
        raise ValueError("summary and report must be strings.")
    results.deposit(
        result_token=token,
        payload={
            "status": str(status or "succeeded").strip().lower(),
            "summary": summary,
            "report": report,
            "produces": produces or {},
            "error": optional_string(error),
            "work_item_id": optional_string(os.environ.get("TASQUE2_WORK_ITEM_ID")),
        },
    )
    return {"ok": True, "result_token": token}


def _status() -> dict[str, Any]:
    with session_scope() as session:
        status = get_system_status(session)
        return {
            "ok": True,
            "status": {
                "work_items": status.work_items,
                "failed_work_unresolved": status.failed_work_unresolved,
                "schedules_enabled": status.schedules_enabled,
                "workflow_runs": status.workflow_runs,
                "ready_work": status.ready_work,
                "running_work": status.running_work,
            },
        }
