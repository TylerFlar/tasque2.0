"""Triage: the ledger's new rows grouped, a fix proposed for each group, and each fix filed as a change.

The sweep (``tasque2.workshop.sweep``) starts ``workshop-triage`` when the ledger holds new rows and no
triage is open:

1. ``triage`` (a model run that reads and never writes): groups the rows, proposes one fix per group with
   the files it will touch, and sets noise aside. The journal's weekly notes are one of its sources;
2. ``file`` (no model, ``function.workshop_file``): each group waits in the ledger with its fix, noise rests
   60 days, a row triage left out rests 30, and the fixes go into free slots.

``file_waiting`` fills the slots (the sweep calls it too): each fix becomes a change of the Workshop's own
(``origin: workshop``), so the usual tiers decide what ships on its own. At most ``MAX_OPEN`` of the
Workshop's own changes are open, cards included, and none shares a file with any open change; the user's
own asks never wait for it. A paused Workshop files nothing.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import WorkflowDefinition, WorkflowRun, WorkItem, utc_now
from tasque2.workshop import ledger, pipeline
from tasque2.workshop.ledger import aware, clip

logger = logging.getLogger(__name__)

WORKFLOW_NAME = "workshop-triage"
FILE_WORKER = "function.workshop_file"
LANE = "workshop-triage"
TEMPLATE = "work-templates/workshop/triage.template.md"
MAX_ITEMS = 25
MAX_OPEN = 3
# A triage or ideas run reads Tasque and the web and writes nothing but its result.
READ_ONLY = ["Bash", "PowerShell", "Write", "Edit", "MultiEdit", "NotebookEdit", "Agent"]
FALLBACK = """\
# Workshop: triage what the sweep found
Group the ledger rows in the task context's `items`, propose one fix per group (`request`, `files`,
`evidence`), and set noise aside, following the pinned `tasque_workshop` document. Read, never write.
`produces`: {groups: [{title, request, files, evidence, items}], noise: [keys], silent: true}.
"""


def definition() -> dict[str, Any]:
    data = get_settings().resolved_data_dir
    return {
        "nodes": [
            {
                "key": "triage",
                "kind": "work",
                "title": "Workshop: triage what the sweep found",
                "worker_kind": "provider.default",
                "lane": LANE,
                "task_template_path": str(data / TEMPLATE),
                "task_instruction": FALLBACK,
                "runtime_contract": {
                    "model_profile": "high",
                    "mcp_servers": [],
                    "disallowed_tools": [*pipeline.denied_tools(), *READ_ONLY],
                },
                "max_attempts": 1,
                "tolerate_failure": True,
            },
            {
                "key": "file",
                "kind": "work",
                "title": "Workshop: file the fixes",
                "worker_kind": FILE_WORKER,
                "task_instruction": "Keep the proposed fixes in the ledger and file them into free slots (no model).",
                "depends_on": ["triage"],
            },
        ]
    }


def ensure_definition(session: Session) -> WorkflowDefinition:
    return pipeline.ensure_bundled(session, WORKFLOW_NAME, definition())


def open_run(session: Session) -> WorkflowRun | None:
    return session.scalar(
        select(WorkflowRun).where(WorkflowRun.name == WORKFLOW_NAME, WorkflowRun.status.in_(pipeline.OPEN_RUN_STATUSES))
    )


def start(session: Session, *, thread_id: str | None, context: dict[str, Any] | None = None) -> WorkflowRun | None:
    """A triage of the ledger's new rows, unless one is open or nothing is new."""
    from tasque2.workflows import WorkflowService
    from tasque2.workshop.sweep import journal_digest

    if open_run(session) is not None:
        return None
    items = ledger.new_items(session, limit=MAX_ITEMS)
    if not items:
        return None
    journal = journal_digest(context or {})
    return WorkflowService(session).start_run(
        workflow_definition_id=ensure_definition(session).id,
        input={
            "lane": pipeline.LANE,
            "items": [ledger.item_data(found) for found in items],
            "open_changes": [_change_data(session, run) for run in pipeline.open_changes(session)],
            "waiting": [clip(proposal.get("title"), 80) for proposal, _rows in ledger.waiting_groups(session)],
            "journal_digest": str(journal) if journal is not None else None,
            "live_data": str(get_settings().resolved_data_dir),
            "memory_namespace": "global",
            "memory_canonical_keys": pipeline.DOCTRINE_KEYS,
            "context_limits": {"memories": 2, "artifacts": 0},
        },
        discord_thread_id=thread_id,
    )


def _change_data(session: Session, run: WorkflowRun) -> dict[str, Any]:
    given = run.input or {}
    title = pipeline.node_output(session, run.id, "classify").get("title") or given.get("request")
    return {
        "title": clip(title, 80),
        "origin": given.get("origin"),
        "files": sorted(pipeline.change_files(session, run)),
        "stage": pipeline.stage(session, run),
    }


# --- filing ------------------------------------------------------------------------------------------


def _keys(value: Any) -> list[str]:
    return [str(item).strip() for item in (value if isinstance(value, list) else []) if str(item).strip()]


def groups_of(value: Any, known: set[str]) -> list[dict[str, Any]]:
    """Triage's groups, each with a request and rows it was given; a row goes to the first group naming it."""
    taken: set[str] = set()
    groups = []
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict):
            continue
        keys = [key for key in _keys(item.get("items")) if key in known and key not in taken]
        request = str(item.get("request") or "").strip()
        if not keys or not request:
            continue
        taken.update(keys)
        groups.append(
            {
                "id": secrets.token_hex(4),
                "title": clip(item.get("title") or request, 80),
                "request": request[:4000],
                "files": pipeline.files_of(item.get("files")),
                "evidence": [clip(line, 200) for line in _keys(item.get("evidence"))][:4],
                "items": keys,
            }
        )
    return groups


def file_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_file``: keep triage's fixes in the ledger, rest the rest, and file what fits."""
    session, run = pipeline._run(work_item)
    given = [str(item.get("key")) for item in (run.input or {}).get("items") or [] if isinstance(item, dict)]
    triaged = pipeline.node_output(session, run.id, "triage")
    if not triaged or triaged.get("tolerated_failure"):
        why = "triage did not finish; its rows stay new for the next sweep"
        return {"summary": why, "produces": {"silent": True, "filed": []}}
    now = utc_now()
    known = set(given)
    groups = groups_of(triaged.get("groups"), known)
    grouped = {key for group in groups for key in group["items"]}
    for group in groups:
        ledger.propose(session, group["items"], group)
    noise = [key for key in _keys(triaged.get("noise")) if key in known and key not in grouped]
    ledger.set_aside(session, noise, until=now + ledger.SET_ASIDE)
    left = [key for key in given if key not in grouped and key not in noise]
    ledger.set_aside(session, left, until=now + ledger.ONE_TRY)
    filed = file_waiting(session, thread_id=run.discord_thread_id, now=now)
    summary = f"{len(groups)} fix(es) proposed, {len(noise)} row(s) set aside as noise; {len(filed)} filed."
    produced = {"silent": True, "groups": len(groups), "noise": len(noise), "left": len(left), "filed": filed}
    return {"summary": summary, "produces": produced}


def file_waiting(session: Session, *, thread_id: str | None, now: datetime | None = None) -> list[dict[str, Any]]:
    """File each waiting fix that fits: a slot free among the Workshop's own open changes, no file shared
    with any open change, and no row of it tried within 30 days."""
    now = aware(now or utc_now())
    if not thread_id or pipeline.paused():
        return []
    open_runs = pipeline.open_changes(session)
    slots = MAX_OPEN - sum(1 for run in open_runs if (run.input or {}).get("origin") == "workshop")
    busy: set[str] = set()
    for run in open_runs:
        busy |= pipeline.change_files(session, run)
    filed: list[dict[str, Any]] = []
    for proposal, found in ledger.waiting_groups(session):
        if slots <= 0:
            break
        files = set(pipeline.files_of(proposal.get("files")))
        if files & busy:
            continue
        if any(row.tried_at is not None and now - aware(row.tried_at) < ledger.ONE_TRY for row in found):
            continue
        keys = [row.key for row in found]
        try:
            run = pipeline.start_change(
                session,
                request=str(proposal.get("request") or proposal.get("title") or ""),
                origin="workshop",
                thread_id=thread_id,
                author="workshop",
                evidence=list(proposal.get("evidence") or []),
                files=sorted(files),
                issues=keys,
            )
        except pipeline.WorkshopPaused:
            break
        except Exception:  # noqa: BLE001 - a change that cannot start stays waiting for the next sweep
            logger.exception("The Workshop could not file %s", proposal.get("title"))
            break
        ledger.mark_filed(session, keys, change_id=str(run.input["change_id"]), run_id=run.id, now=now)
        busy |= files
        slots -= 1
        filed.append({"title": proposal.get("title"), "change_id": run.input["change_id"], "run_id": run.id})
    return filed
