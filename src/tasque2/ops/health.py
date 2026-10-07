"""Tasque's own health over a window: what failed, what is stuck, and whether the daemon is ticking.

``build_system_health`` reads the work, attempt, dead-letter, schedule, workflow and sticky-note
tables, the daemon's state file and its last restart, and returns one report a health-check run can
judge and word. Every count is computed here; the report names what needs attention in
``attention`` and leaves the wording to the caller.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.daemon import restart
from tasque2.daemon.control import read_state
from tasque2.models import (
    DiscordSticky,
    FailedWork,
    Schedule,
    WorkAttempt,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
    utc_now,
)
from tasque2.ops.backup_runner import backup_health
from tasque2.ops.faults import fault_summary, recurring

FAILED_ATTEMPTS = ("failed", "expired", "orphaned")
STUCK_MINUTES = 30
LONG_RUN_MINUTES = 45
STALE_SCHEDULE_MINUTES = 15
MESSAGE_CHARS = 240
GATE_WAIT_DAYS = 7
RESTART_WAIT_DAYS = 2


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=utc_now().tzinfo)


def _iso(value: datetime | None) -> str | None:
    value = _aware(value)
    return value.isoformat() if value is not None else None


def _clip(text: Any, limit: int = MESSAGE_CHARS) -> str | None:
    if text is None:
        return None
    flat = " ".join(str(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _daemon(now: datetime) -> dict[str, Any]:
    state = read_state()
    if state is None:
        return {"alive": False, "state": "no state file: the daemon has not run here or cannot write"}
    return {
        "alive": state.is_fresh(now),
        "last_tick_at": _iso(state.last_tick_at),
        "started_at": _iso(state.started_at),
        "in_flight": state.in_flight,
        "draining": state.draining,
        "version": state.version,
    }


def _failures(session: Session, since: datetime) -> dict[str, Any]:
    rows = session.execute(
        select(WorkAttempt, WorkItem)
        .join(WorkItem, WorkItem.id == WorkAttempt.work_item_id)
        .where(WorkAttempt.created_at >= since, WorkAttempt.status.in_(FAILED_ATTEMPTS))
        .order_by(WorkAttempt.created_at)
    ).all()
    by_lane: dict[str, dict[str, Any]] = {}
    for attempt, item in rows:
        lane = item.lane or "unlabelled"
        entry = by_lane.setdefault(lane, {"failed_attempts": 0, "error_types": Counter(), "items": set()})
        entry["failed_attempts"] += 1
        entry["error_types"][attempt.error_type or attempt.status] += 1
        entry["items"].add(item.id)
        entry["latest"] = {
            "at": _iso(attempt.ended_at or attempt.created_at),
            "title": _clip(item.title, 120),
            "error": _clip(attempt.error_message),
        }
    out: dict[str, Any] = {}
    for lane, entry in sorted(by_lane.items()):
        items = list(entry["items"])
        statuses = Counter(
            status for status in session.scalars(select(WorkItem.status).where(WorkItem.id.in_(items))).all()
        )
        out[lane] = {
            "failed_attempts": entry["failed_attempts"],
            "error_types": dict(entry["error_types"]),
            "items": len(items),
            "recovered": statuses.get("succeeded", 0),
            "still_failing": sum(count for status, count in statuses.items() if status in ("dead_letter", "failed")),
            "latest": entry["latest"],
        }
    return out


def _dead_letters(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(
        select(FailedWork, WorkItem)
        .join(WorkItem, WorkItem.id == FailedWork.work_item_id)
        .where(FailedWork.status == "unresolved")
        .order_by(FailedWork.created_at)
    ).all()
    return [
        {
            "work_item_id": item.id,
            "lane": item.lane,
            "title": _clip(item.title, 120),
            "since": _iso(failed.created_at),
            "error_type": failed.error_type,
            "error": _clip(failed.error_message),
        }
        for failed, item in rows
    ]


def _stuck(session: Session, now: datetime) -> list[dict[str, Any]]:
    cutoff = now - timedelta(minutes=STUCK_MINUTES)
    rows = session.execute(
        select(WorkAttempt, WorkItem)
        .join(WorkItem, WorkItem.id == WorkAttempt.work_item_id)
        .where(WorkAttempt.status == "running")
    ).all()
    stuck = []
    for attempt, item in rows:
        beat = _aware(attempt.heartbeat_at or attempt.started_at or attempt.created_at)
        if beat is not None and beat < cutoff:
            stuck.append(
                {
                    "work_item_id": item.id,
                    "lane": item.lane,
                    "title": _clip(item.title, 120),
                    "last_heartbeat": _iso(beat),
                    "minutes_silent": int((now - beat).total_seconds() // 60),
                }
            )
    return stuck


def _long_runs(session: Session, since: datetime) -> list[dict[str, Any]]:
    rows = session.execute(
        select(WorkAttempt, WorkItem)
        .join(WorkItem, WorkItem.id == WorkAttempt.work_item_id)
        .where(
            WorkAttempt.created_at >= since,
            WorkAttempt.started_at.is_not(None),
            WorkAttempt.ended_at.is_not(None),
        )
    ).all()
    runs = []
    for attempt, item in rows:
        started, ended = _aware(attempt.started_at), _aware(attempt.ended_at)
        minutes = (ended - started).total_seconds() / 60
        if minutes >= LONG_RUN_MINUTES:
            runs.append(
                {
                    "lane": item.lane,
                    "title": _clip(item.title, 120),
                    "minutes": round(minutes),
                    "status": attempt.status,
                    "at": _iso(started),
                }
            )
    runs.sort(key=lambda run: run["minutes"], reverse=True)
    return runs[:8]


def _stale_schedules(session: Session, now: datetime) -> list[dict[str, Any]]:
    cutoff = now - timedelta(minutes=STALE_SCHEDULE_MINUTES)
    out = []
    for schedule in session.scalars(select(Schedule).where(Schedule.enabled.is_(True))).all():
        evaluated = _aware(schedule.last_evaluated_at)
        if evaluated is None or evaluated < cutoff:
            out.append({"name": schedule.name, "last_evaluated_at": _iso(evaluated)})
    return out


def _unpinned_stickies(session: Session) -> int:
    rows = session.scalars(
        select(DiscordSticky).where(
            DiscordSticky.status == "on",
            DiscordSticky.discord_message_id.is_not(None),
            DiscordSticky.pinned_at.is_(None),
        )
    ).all()
    return len(rows)


def _waiting_gates(session: Session, now: datetime) -> list[dict[str, Any]]:
    """Workflow choices still waiting for the user, oldest first."""
    rows = session.execute(
        select(WorkflowNode, WorkflowRun)
        .join(WorkflowRun, WorkflowRun.id == WorkflowNode.workflow_run_id)
        .where(WorkflowNode.kind == "gate", WorkflowNode.status == "awaiting_input")
        .order_by(WorkflowNode.updated_at)
    ).all()
    waiting = []
    for node, run in rows:
        since = _aware(node.updated_at) or now
        waiting.append(
            {
                "workflow": run.name,
                "gate": node.node_key,
                "prompt": _clip((node.definition or {}).get("prompt")),
                "since": _iso(since),
                "days": (now - since).days,
            }
        )
    return waiting


def _restarts(session: Session, now: datetime, since: datetime) -> dict[str, Any]:
    """A restart still waiting for its window, and the last restart's outcome when it fell in the window."""
    report: dict[str, Any] = {"pending": None, "last": None, "attention": []}
    request = restart.read_request()
    if request is not None:
        try:
            requested = _aware(datetime.fromisoformat(str(request.get("requested_at"))))
        except ValueError:
            requested = None
        waiting = restart.waiting_reason(session, request, now=now)
        report["pending"] = {"reason": request.get("reason"), "requested_at": _iso(requested), "waiting": waiting}
        if requested is not None and now - requested > timedelta(days=RESTART_WAIT_DAYS) and waiting:
            report["attention"].append(
                f"a restart ({request.get('reason')}) has waited since {requested:%Y-%m-%d}: {waiting}"
            )
    result = restart.read_result()
    if result is not None:
        try:
            ended = _aware(datetime.fromisoformat(str(result.get("ended_at") or result.get("started_at"))))
        except ValueError:
            ended = None
        if ended is not None and ended >= since:
            report["last"] = {
                "reason": result.get("reason"),
                "at": _iso(ended),
                "ok": bool(result.get("ok")),
                "rolled_back": result.get("rolled_back") or [],
                "error": _clip(result.get("error") or result.get("switch_error")),
                "unpushed": [entry.get("repo") for entry in result.get("pushed") or [] if not entry.get("ok")],
            }
            last = report["last"]
            if last["rolled_back"]:
                report["attention"].append(
                    f"the restart on {ended:%Y-%m-%d} ({last['reason']}) did not come up and was rolled back"
                )
            elif not last["ok"]:
                detail = last["error"] or "see daemon.respawn.log"
                report["attention"].append(f"the restart on {ended:%Y-%m-%d} ({last['reason']}) failed: {detail}")
            if last["unpushed"]:
                report["attention"].append(f"merged but not pushed: {', '.join(last['unpushed'])}")
    return report


def build_system_health(session: Session, *, days: int = 7, now: datetime | None = None) -> dict[str, Any]:
    """Tasque's health over the last ``days`` days, with what needs attention named."""
    now = _aware(now) or utc_now()
    days = max(1, min(int(days), 60))
    since = now - timedelta(days=days)
    daemon = _daemon(now)
    failures = _failures(session, since)
    dead_letters = _dead_letters(session)
    stuck = _stuck(session, now)
    long_runs = _long_runs(session, since)
    stale = _stale_schedules(session, now) if daemon.get("alive") else []
    unpinned = _unpinned_stickies(session)
    faults = fault_summary(days=days, now=now)
    backups = backup_health(now=now)
    gates = _waiting_gates(session, now)
    restarts = _restarts(session, now, since)
    succeeded = session.scalars(
        select(WorkAttempt.id).where(WorkAttempt.created_at >= since, WorkAttempt.status == "succeeded")
    ).all()

    attention: list[str] = []
    if not daemon.get("alive"):
        attention.append("the daemon is not ticking")
    for item in dead_letters:
        attention.append(f"dead letter in {item['lane'] or 'an unlabelled lane'}: {item['title']}")
    for lane, entry in failures.items():
        if entry["still_failing"]:
            attention.append(f"{lane}: {entry['still_failing']} item(s) failed and did not recover")
    for item in stuck:
        where = item["lane"] or "an unlabelled lane"
        attention.append(f"stuck in {where} for {item['minutes_silent']} min: {item['title']}")
    if stale:
        attention.append(f"{len(stale)} enabled schedule(s) not evaluated in {STALE_SCHEDULE_MINUTES} min")
    if unpinned:
        attention.append(f"{unpinned} sticky note(s) posted but not pinned")
    for fault in faults:
        if recurring(fault):
            frame = fault.get("frame") or {}
            where = f"{frame.get('file')}:{frame.get('function')}"
            what = fault.get("exc_type") or "error"
            attention.append(f"code fault x{fault['count']} on {fault['days']} day(s): {what} in {where}")
    attention.extend(f"backups: {line}" for line in backups["attention"])
    for gate in gates:
        if gate["days"] >= GATE_WAIT_DAYS:
            attention.append(f"{gate['workflow']} has waited {gate['days']} days for a choice: {gate['prompt']}")
    attention.extend(restarts["attention"])

    totals: dict[str, Any] = defaultdict(int)
    totals["succeeded_attempts"] = len(succeeded)
    totals["failed_attempts"] = sum(entry["failed_attempts"] for entry in failures.values())
    totals["recovered_items"] = sum(entry["recovered"] for entry in failures.values())
    return {
        "window": {"days": days, "since": since.isoformat(), "now": now.isoformat()},
        "daemon": daemon,
        "attention": attention,
        "totals": dict(totals),
        "failures_by_lane": failures,
        "dead_letters": dead_letters,
        "stuck": stuck,
        "long_runs": long_runs,
        "stale_schedules": stale,
        "unpinned_stickies": unpinned,
        "faults": faults[:12],
        "backups": backups,
        "waiting_choices": gates,
        "restarts": {key: value for key, value in restarts.items() if key != "attention"},
    }
