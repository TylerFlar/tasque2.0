"""The monthly look: what the user is after now, taken stock of once a month, with nothing posted.

``function.workshop_look`` (its schedule: early on the 1st of each month) starts ``workshop-look`` unless a
look is still open or the last one was under 14 days ago:

1. ``look`` (a model run at ultra that reads Tasque, the threads and the web, and never writes): what the
   user is after now, from evidence rather than the written goals (the journal's weekly notes, their words
   in every thread, where their attention goes, life changes), as an inventory checked against the goal
   documents and the last look; the goals to rethink (``reviews``) and the smaller findings (``findings``:
   asks left waiting, asks no lane serves, work that should run on its own, lanes nothing consolidates or
   watches, doctrine that is long or repeats);
2. ``file`` (no model, ``function.workshop_look_file``): each goal to rethink becomes its review row
   (``tasque2.workshop.reviews`` starts the review, always as a pitch), unless its review is open or
   resting; each finding an issue row for triage. A look that failed records nothing.

Its input: ``engagement`` (each thread outside a private lane: posts, the user's messages, asks, answered
asks and pushback, the last 28 days beside the 28 before), ``prompt_sizes`` (each lane's median and largest
prompt over the same windows), ``goal_reviews`` (each goal's review row and where it stands) and
``last_look`` (the last inventory and its day).
"""

from __future__ import annotations

import json
import re
import statistics
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.lane_context import effective_context
from tasque2.localtime import local_date
from tasque2.models import (
    ProviderRun,
    WorkAttempt,
    WorkflowDefinition,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
    WorkshopIssue,
    utc_now,
)
from tasque2.workshop import ledger, pipeline
from tasque2.workshop.ledger import Signal, aware, clip

WORKFLOW_NAME = "workshop-look"
LAUNCH_WORKER = "function.workshop_look"
FILE_WORKER = "function.workshop_look_file"
LANE = "workshop-look"
TEMPLATE = "work-templates/workshop/look.template.md"
SOURCE = "look"
MIN_GAP = timedelta(days=14)
WINDOW_DAYS = 28
WINDOWS = ("last_28_days", "28_days_before")
COUNTS = ("posts", "messages", "asks", "answered", "pushback")
# The look's input stays small: the busiest threads (the rest summed in one line) and the largest prompts.
THREADS_SHOWN, LANES_SHOWN = 30, 30
MAX_REVIEWS, MAX_FINDINGS, INVENTORY_LINES, EVIDENCE_LINES = 5, 8, 15, 4
FALLBACK = """\
# Workshop: the monthly look
Take stock of what the user is after now, from evidence, following the pinned `tasque_workshop` document.
Read, never write. `produces`: {inventory: [lines], deep: {deep, why}, reviews: [{goal, title, evidence}],
findings: [{key, title, evidence}], silent: true}.
"""


def definition() -> dict[str, Any]:
    from tasque2.workshop.triage import READ_ONLY

    data = get_settings().resolved_data_dir
    return {
        "nodes": [
            {
                "key": "look",
                "kind": "work",
                "title": "Workshop: the monthly look",
                "worker_kind": "provider.default",
                "lane": LANE,
                "task_template_path": str(data / TEMPLATE),
                "task_instruction": FALLBACK,
                "runtime_contract": {
                    "model_profile": "ultra",
                    "mcp_servers": [],
                    "disallowed_tools": [*pipeline.denied_tools(), *READ_ONLY],
                },
                "max_attempts": 1,
                "tolerate_failure": True,
            },
            {
                "key": "file",
                "kind": "work",
                "title": "Workshop: keep what the look found",
                "worker_kind": FILE_WORKER,
                "task_instruction": "Keep the goals to rethink and the findings in the ledger (no model).",
                "depends_on": ["look"],
            },
        ]
    }


def ensure_definition(session: Session) -> WorkflowDefinition:
    return pipeline.ensure_bundled(session, WORKFLOW_NAME, definition())


def open_run(session: Session) -> WorkflowRun | None:
    return session.scalar(
        select(WorkflowRun).where(WorkflowRun.name == WORKFLOW_NAME, WorkflowRun.status.in_(pipeline.OPEN_RUN_STATUSES))
    )


def last_look(session: Session) -> tuple[WorkflowRun, dict[str, Any]] | None:
    """The newest look that finished its model run, with what it produced; a look that failed is none."""
    found = session.execute(
        select(WorkflowRun, WorkflowNode)
        .join(WorkflowNode, WorkflowNode.workflow_run_id == WorkflowRun.id)
        .where(WorkflowRun.name == WORKFLOW_NAME, WorkflowNode.node_key == "look", WorkflowNode.status == "succeeded")
        .order_by(WorkflowRun.created_at.desc())
        .limit(1)
    ).first()
    return (found[0], dict(found[1].output or {})) if found is not None else None


# --- what the look reads ------------------------------------------------------------------------------


def engagement(session: Session, now: datetime) -> list[dict[str, Any]]:
    """Each thread outside a private lane: what Tasque posted and what came back, the last 28 days beside the
    28 before, busiest first; past ``THREADS_SHOWN``, the quieter threads are summed in one line."""
    from tasque2.ops.effort import effort_report
    from tasque2.workshop.sweep import private_threads

    threads: dict[str, dict[str, Any]] = {}
    for window, end in zip(WINDOWS, (now, now - timedelta(days=WINDOW_DAYS)), strict=True):
        for row in effort_report(session, days=WINDOW_DAYS, now=end)["threads"]:
            if not row.get("thread_id"):
                continue
            entry = threads.setdefault(row["thread_id"], {"thread": row["label"], "thread_id": row["thread_id"]})
            entry[window] = {
                "posts": row["posts"],
                "messages": row["inbound"],
                "asks": row["asks"],
                "answered": row["answered"],
                "pushback": row["pushback"],
            }
    private = private_threads(session, set(threads))
    empty = dict.fromkeys(COUNTS, 0)
    found = [
        {"thread": entry["thread"], "thread_id": key, **{window: entry.get(window, empty) for window in WINDOWS}}
        for key, entry in threads.items()
        if key not in private
    ]
    found.sort(key=lambda entry: entry[WINDOWS[0]]["posts"] + entry[WINDOWS[0]]["messages"], reverse=True)
    if len(found) <= THREADS_SHOWN:
        return found
    rest = found[THREADS_SHOWN:]
    summed = {window: {name: sum(entry[window][name] for entry in rest) for name in COUNTS} for window in WINDOWS}
    return [*found[:THREADS_SHOWN], {"thread": f"{len(rest)} quieter thread(s), summed", "thread_id": None, **summed}]


def prompt_sizes(session: Session, now: datetime) -> list[dict[str, Any]]:
    """Each lane's prompts in characters (the median and the largest), the last 28 days beside the 28 before,
    read from the runs' usage as the sweep reads it; the ``LANES_SHOWN`` lanes with the largest prompts, the
    largest first."""
    split = now - timedelta(days=WINDOW_DAYS)
    rows = session.execute(
        select(ProviderRun.usage, ProviderRun.created_at, WorkItem.lane)
        .join(WorkAttempt, WorkAttempt.id == ProviderRun.attempt_id)
        .join(WorkItem, WorkItem.id == WorkAttempt.work_item_id)
        .where(ProviderRun.created_at >= split - timedelta(days=WINDOW_DAYS), ProviderRun.created_at <= now)
    ).all()
    lanes: dict[str, dict[str, list[int]]] = {}
    for usage, created, lane in rows:
        chars = (usage or {}).get("prompt_chars")
        if not isinstance(chars, int):
            continue
        window = WINDOWS[0] if aware(created) >= split else WINDOWS[1]
        lanes.setdefault(lane or "unlabelled", {}).setdefault(window, []).append(chars)
    found = []
    for lane, windows in lanes.items():
        entry: dict[str, Any] = {"lane": lane}
        for window in WINDOWS:
            sizes = windows.get(window) or []
            entry[window] = (
                {"runs": len(sizes), "median": int(statistics.median(sizes)), "largest": max(sizes)} if sizes else None
            )
        found.append(entry)
    found.sort(key=lambda entry: (entry[WINDOWS[0]] or {}).get("largest") or 0, reverse=True)
    return found[:LANES_SHOWN]


def goal_reviews(session: Session) -> list[dict[str, Any]]:
    """Each goal's review row and where it stands."""
    return [
        {
            "key": found.key,
            "title": found.title,
            "status": found.status,
            "rest_until": aware(found.rest_until).date().isoformat() if found.rest_until else None,
        }
        for found in ledger.review_rows(session)
    ]


def _lines(value: Any, limit: int) -> list[str]:
    items = value.splitlines() if isinstance(value, str) else value if isinstance(value, list) else []
    texts = [item if isinstance(item, str) else json.dumps(item, ensure_ascii=False) for item in items]
    return [clip(text, 300) for text in texts if text.strip()][:limit]


def _last_look_data(last: tuple[WorkflowRun, dict[str, Any]] | None) -> dict[str, Any] | None:
    if last is None:
        return None
    run, produced = last
    return {
        "on": local_date(aware(run.created_at)),
        "inventory": _lines(produced.get("inventory"), INVENTORY_LINES),
        "deep": produced.get("deep"),
    }


def launch_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_look``: this month's look, unless one is still open or the last one was under 14
    days ago. Posts nothing."""
    from tasque2.workflows import WorkflowService
    from tasque2.workshop.sweep import journal_digest

    session = pipeline._session(work_item)
    now = utc_now()
    if open_run(session) is not None:
        return {"summary": "The last look is still running.", "produces": {"silent": True}}
    last = last_look(session)
    if last is not None and now - aware(last[0].created_at) < MIN_GAP:
        day = local_date(aware(last[0].created_at))
        return {"summary": f"The last look was on {day}, under 2 weeks ago.", "produces": {"silent": True}}
    journal = journal_digest(effective_context(work_item.context))
    run = WorkflowService(session).start_run(
        workflow_definition_id=ensure_definition(session).id,
        input={
            "lane": pipeline.LANE,
            "engagement": engagement(session, now),
            "prompt_sizes": prompt_sizes(session, now),
            "goal_reviews": goal_reviews(session),
            "last_look": _last_look_data(last),
            "journal_digest": str(journal) if journal is not None else None,
            "live_data": str(get_settings().resolved_data_dir),
            "memory_namespace": "global",
            "memory_canonical_keys": pipeline.DOCTRINE_KEYS,
            "context_limits": {"memories": 2, "artifacts": 0},
        },
        discord_thread_id=work_item.discord_thread_id,
    )
    return {"summary": "Taking stock of what the user is after.", "produces": {"silent": True, "look_run_id": run.id}}


# --- what the look found --------------------------------------------------------------------------------


def _slug(value: Any) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", str(value or "").lower()))[:80]


def reviews_of(value: Any) -> list[dict[str, Any]]:
    """The goals the look would rethink, one entry per goal."""
    found: dict[str, dict[str, Any]] = {}
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict):
            continue
        goal, title = ledger.goal_name(item.get("goal")), str(item.get("title") or "").strip()
        if goal and title and goal not in found:
            evidence = _lines(item.get("evidence"), EVIDENCE_LINES)
            found[goal] = {"goal": goal, "title": clip(title, 120), "evidence": evidence}
    return list(found.values())[:MAX_REVIEWS]


def findings_of(value: Any) -> list[dict[str, Any]]:
    """The look's smaller findings, one entry per key, for triage."""
    found: dict[str, dict[str, Any]] = {}
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict) or not str(item.get("title") or "").strip():
            continue
        key = _slug(item.get("key") or item["title"])
        if key and key not in found:
            evidence = _lines(item.get("evidence"), EVIDENCE_LINES)
            found[key] = {"key": key, "title": clip(item["title"], 120), "evidence": evidence}
    return list(found.values())[:MAX_FINDINGS]


def may_review(found: WorkshopIssue | None, now: datetime) -> bool:
    """Whether the look may name this goal for a review: its review is neither open nor resting."""
    if found is None:
        return True
    if found.status == ledger.FILED:
        return False
    resting = found.status in (ledger.RESTING, ledger.DONE)
    return not (resting and found.rest_until is not None and aware(found.rest_until) > now)


def file_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_look_file``: keep what the look found, each goal to rethink as its review row and
    each finding as an issue for triage. Posts nothing; a look that failed records nothing."""
    session, run = pipeline._run(work_item)
    looked = pipeline.node_output(session, run.id, "look")
    if not looked or looked.get("tolerated_failure"):
        produced = {"silent": True, "reviews": [], "findings": 0}
        return {"summary": "The look did not finish: nothing recorded.", "produces": produced}
    now = utc_now()
    named = reviews_of(looked.get("reviews"))
    reviews = [item for item in named if may_review(ledger.row(session, ledger.review_key(item["goal"])), now)]
    skipped = [item["goal"] for item in named if item not in reviews]
    findings = findings_of(looked.get("findings"))
    signals = [
        Signal(
            key=ledger.review_key(item["goal"]),
            source=SOURCE,
            title=item["title"],
            evidence=item["evidence"],
            kind=ledger.REVIEW,
        )
        for item in reviews
    ]
    signals += [
        Signal(key=f"look:{item['key']}", source=SOURCE, title=item["title"], evidence=item["evidence"])
        for item in findings
    ]
    tally = ledger.record(session, signals, now=now)
    for item in reviews:
        ledger.mark_looked(session, ledger.review_key(item["goal"]), evidence=item["evidence"], now=now)
    goals = [item["goal"] for item in reviews]
    summary = f"The look named {len(goals)} goal(s) to rethink and {len(findings)} finding(s) for triage." + (
        f" Left alone, their review open or resting: {', '.join(skipped)}." if skipped else ""
    )
    produced = {"silent": True, "reviews": goals, "skipped": skipped, "findings": len(findings), **dict(tally)}
    return {"summary": summary, "produces": produced}
