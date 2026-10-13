"""The weekly refactor pass: one step at a time toward a product anyone could run, filed as the Workshop's own
change.

``function.workshop_refactor`` (its schedule: early each Thursday) starts ``workshop-refactor`` unless a pass is
still open or the last step is still new, waiting for a slot or filed (one step at a time):

1. ``refactor`` (a model run at ultra that reads the public repository as a newcomer would, and never writes):
   what stands in someone else's way (a domain or a method in the core that not every use needs, an assumption
   that Tasque is a personal assistant or is reached only through Discord, a personal default, a setup step or
   a document that does not match the code, a module that does two jobs), and ONE step a single change can
   build, test and ship, with the files it touches and its evidence. ``docs/architecture.md`` is its map: what
   the core holds, and where the code is going, in order;
2. ``file`` (no model, ``function.workshop_refactor_file``): the step gets its ledger row (``refactor:<slug>``,
   source ``refactor``), proposed at once so triage never sees it, and ``tasque2.workshop.triage.file_waiting``
   files it into a free slot as the Workshop's own change (``origin: workshop``), so the usual tiers decide
   whether it ships on its own or comes as a pitch. A pass that failed or found no step records nothing; a step
   whose row is open or resting (it shipped lately, or its change did not) is not proposed again.

Its input: ``history`` (the newest ``HISTORY`` steps and what became of each: shipped, in progress, waiting for
a slot, or set aside until a day), and ``open_changes`` and ``waiting`` as triage reads them.
"""

from __future__ import annotations

import re
import secrets
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import WorkflowDefinition, WorkflowRun, WorkItem, WorkshopIssue, utc_now
from tasque2.workshop import ledger, pipeline, triage
from tasque2.workshop.ledger import Signal, aware, clip
from tasque2.workshop.triage import READ_ONLY

WORKFLOW_NAME = "workshop-refactor"
LAUNCH_WORKER = "function.workshop_refactor"
FILE_WORKER = "function.workshop_refactor_file"
LANE = "workshop-refactor"
TEMPLATE = "work-templates/workshop/refactor.template.md"
SOURCE = "refactor"
HISTORY = 20
EVIDENCE_LINES = 4
# A step still on its way: one at a time, so no pass starts while one is new, waiting for a slot or filed.
OPEN = (ledger.NEW, ledger.WAITING, ledger.FILED)
OUTCOMES = {
    ledger.DONE: "shipped",
    ledger.FILED: "in progress",
    ledger.NEW: "waiting for a slot",
    ledger.WAITING: "waiting for a slot",
}
FALLBACK = """\
# Workshop: the weekly refactor pass
Read the public repository as a newcomer would and pick ONE step a single change can build, test and ship
toward a product anyone could run (a core that holds only what every use needs; Discord as one interface
among others), following the pinned `tasque_design` and `tasque_workshop` documents. Read, never write.
`produces`: {step: {title, request, files, evidence} or null, why, silent: true}.
"""


def definition() -> dict[str, Any]:
    data = get_settings().resolved_data_dir
    return {
        "nodes": [
            {
                "key": "refactor",
                "kind": "work",
                "title": "Workshop: the weekly refactor pass",
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
                "title": "Workshop: file the refactor step",
                "worker_kind": FILE_WORKER,
                "task_instruction": "Keep the step in the ledger and file it into a free slot (no model).",
                "depends_on": ["refactor"],
            },
        ]
    }


def ensure_definition(session: Session) -> WorkflowDefinition:
    return pipeline.ensure_bundled(session, WORKFLOW_NAME, definition())


def open_run(session: Session) -> WorkflowRun | None:
    return session.scalar(
        select(WorkflowRun).where(WorkflowRun.name == WORKFLOW_NAME, WorkflowRun.status.in_(pipeline.OPEN_RUN_STATUSES))
    )


def _steps(session: Session, *statuses: str, limit: int | None = None) -> list[WorkshopIssue]:
    statement = select(WorkshopIssue).where(WorkshopIssue.source == SOURCE, WorkshopIssue.kind == ledger.ISSUE)
    if statuses:
        statement = statement.where(WorkshopIssue.status.in_(statuses))
    statement = statement.order_by(WorkshopIssue.last_seen_at.desc())
    if limit is not None:
        statement = statement.limit(limit)
    return list(session.scalars(statement).all())


def open_step(session: Session) -> WorkshopIssue | None:
    """The step still new, waiting for a slot or filed, if any: one at a time."""
    return next(iter(_steps(session, *OPEN, limit=1)), None)


def outcome(found: WorkshopIssue, now: datetime) -> str:
    """What became of a step, as the next pass reads it."""
    if found.status in OUTCOMES:
        return OUTCOMES[found.status]
    if found.rest_until is not None and aware(found.rest_until) > aware(now):
        return f"set aside until {aware(found.rest_until).date().isoformat()}"
    return "set aside"


def history(session: Session, *, now: datetime) -> list[dict[str, Any]]:
    """The newest steps and what became of each, so a pass never repeats one."""
    return [
        {"title": found.title, "outcome": outcome(found, now), "on": aware(found.last_seen_at).date().isoformat()}
        for found in _steps(session, limit=HISTORY)
    ]


def launch_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_refactor``: this week's pass, unless one is still open or the last step is still new,
    waiting for a slot or filed. Posts nothing."""
    from tasque2.workflows import WorkflowService

    session = pipeline._session(work_item)
    now = utc_now()
    if open_run(session) is not None:
        return {"summary": "The last refactor pass is still running.", "produces": {"silent": True}}
    step = open_step(session)
    if step is not None:
        summary = f"The last step is still {outcome(step, now)}: {clip(step.title, 80)}."
        return {"summary": summary, "produces": {"silent": True}}
    run = WorkflowService(session).start_run(
        workflow_definition_id=ensure_definition(session).id,
        input={
            "lane": pipeline.LANE,
            "history": history(session, now=now),
            "open_changes": [triage.change_data(session, found) for found in pipeline.open_changes(session)],
            "waiting": [clip(proposal.get("title"), 80) for proposal, _rows in ledger.waiting_groups(session)],
            "live_data": str(get_settings().resolved_data_dir),
            "memory_namespace": "global",
            "memory_canonical_keys": pipeline.DOCTRINE_KEYS,
            "context_limits": {"memories": 2, "artifacts": 0},
        },
        discord_thread_id=work_item.discord_thread_id,
    )
    produced = {"silent": True, "refactor_run_id": run.id}
    return {"summary": "Reading the repository as a newcomer would.", "produces": produced}


# --- what the pass found ------------------------------------------------------------------------------


def _slug(value: Any) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", str(value or "").lower()))[:80]


def _lines(value: Any) -> list[str]:
    items = value.splitlines() if isinstance(value, str) else value if isinstance(value, list) else []
    return [clip(item, 200) for item in items if str(item or "").strip()][:EVIDENCE_LINES]


def step_of(value: Any) -> dict[str, Any] | None:
    """The pass's step as the ledger keeps it (its key from its title), or None when the pass named none."""
    if not isinstance(value, dict):
        return None
    title, request = clip(value.get("title"), 80), str(value.get("request") or "").strip()
    if not title or not request:
        return None
    return {
        "key": f"{SOURCE}:{_slug(title)}",
        "title": title,
        "request": request[:4000],
        "files": pipeline.files_of(value.get("files")),
        "evidence": _lines(value.get("evidence")),
    }


def may_propose(found: WorkshopIssue | None, now: datetime) -> bool:
    """Whether a step may be proposed: its row is neither open nor resting (it shipped lately, or its change did
    not)."""
    if found is None:
        return True
    if found.status in OPEN:
        return False
    return not (found.rest_until is not None and aware(found.rest_until) > aware(now))


def file_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_refactor_file``: keep the pass's step in the ledger, proposed at once, and file it into
    a free slot as the Workshop's own change. Posts nothing; a pass that failed or found no step records nothing,
    and a step whose row is open or resting is left alone."""
    session, run = pipeline._run(work_item)
    passed = pipeline.node_output(session, run.id, "refactor")
    produced: dict[str, Any] = {"silent": True, "step": None, "filed": []}
    if not passed or passed.get("tolerated_failure"):
        return {"summary": "The refactor pass did not finish: nothing recorded.", "produces": produced}
    step = step_of(passed.get("step"))
    if step is None:
        why = clip(passed.get("why") or "it named none", 200)
        return {"summary": f"The refactor pass found no step: {why}", "produces": produced}
    now = utc_now()
    produced.update(step=step["title"], key=step["key"])
    found = ledger.row(session, step["key"])
    if not may_propose(found, now):
        produced["left"] = outcome(found, now)
        return {"summary": f"The step {step['title']!r} is left alone: {produced['left']}.", "produces": produced}
    signal = Signal(key=step["key"], source=SOURCE, title=step["title"], evidence=step["evidence"])
    ledger.record(session, [signal], now=now)
    proposal = {"id": secrets.token_hex(4), **{name: step[name] for name in ("title", "request", "files", "evidence")}}
    ledger.propose(session, [step["key"]], proposal)
    filed = triage.file_waiting(session, thread_id=run.discord_thread_id, now=now)
    state = "filed" if ledger.row(session, step["key"]).status == ledger.FILED else "waiting for a slot"
    produced.update(state=state, filed=filed)
    summary = f"Refactor step {step['title']!r}: {state}; {len(filed)} change(s) filed."
    return {"summary": summary, "produces": produced}
