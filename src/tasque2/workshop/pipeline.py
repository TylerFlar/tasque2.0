"""The Workshop's change pipeline: one change to Tasque, from a request to a release, with an undo.

A change starts from a request (the owner's words in the Workshop thread, an idea card's Build it, or a
fix the Workshop's triage filed: ``tasque2.workshop.triage``) and runs the ``tasque-change`` workflow in
its own worktrees (``tasque2.ops.worktree``: the core, each extension, and the config repository at
``data``), next to the live checkout:

1. ``plan`` (a model run that reads the change's own checkout; the guard refuses writes outside it):
   what to change, where and why in at most 25 lines, up to 4 questions with defaults, what it touches
   and which files. A request that needs no change (a question, a batch of ideas, evidence that no longer
   holds) ends here with its answer;
2. ``classify`` (no model): the tier from ``tasque2.workshop.policy``, never from the model;
3. ``plan_gate``: Approve, Revise or Discard, only for a change whose plan needs approving;
4. ``prepare`` (no model): Revise starts the change again with the notes typed at the gate, Discard
   ends it; otherwise the worktrees move to the live heads and a change that edits code takes the code
   lock (one code change at a time, from its build until it is live, freed while its ship card waits);
5. ``build`` (a model run in the worktrees): a failing test first for a bug, the change, commits;
6. ``verify`` (no model, ``tasque2.workshop.verify``): the suites, ruff, the privacy and secret scans, a
   rehearsal on a copy of the live database, and the tier again from the real diff (it only rises);
7. ``ship_gate``: Ship or Discard, unless the change ships on its own;
8. ``release`` (no model): a config-only change lands at once; code takes the lock again, is moved onto
   the live code and checked again when the live code moved on while the card waited, then waits for an
   idle moment (01:00-06:30 for a change that ships on its own) and a restart, announced in the thread
   once it is live;
9. ``report`` (no model): what changed, with an Undo button. A change the Workshop filed posts only its
   cards and its "Live now"; its ledger rows learn how it ended.

A card nobody answers for 14 days expires (``expire_cards``, run by the sweep): a change's card as a
Discard, an idea card as a Not now. Kill switches: ``pause`` (no change starts unless the owner asks for
it, and nothing ships on its own), and undo. Two releases rolled back within a week pause the Workshop on
its own.
"""

from __future__ import annotations

import json
import logging
import re
import secrets
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.orm import Session, object_session

from tasque2.config import get_settings
from tasque2.models import WorkflowDefinition, WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.workshop import ledger, policy

logger = logging.getLogger(__name__)

WORKFLOW_NAME = "tasque-change"
CLASSIFY_WORKER = "function.workshop_classify"
PREPARE_WORKER = "function.workshop_prepare"
VERIFY_WORKER = "function.workshop_verify"
RELEASE_WORKER = "function.workshop_release"
REPORT_WORKER = "function.workshop_report"
ANNOUNCE_WORKER = "function.workshop_announce"
REPLY_WORKER = "function.workshop_reply"
LANE, PLAN_LANE, BUILD_LANE = "workshop", "workshop-plan", "workshop-build"
APPROVE, REVISE, DISCARD, SHIP = "Approve", "Revise", "Discard", "Ship"
OPEN_RUN_STATUSES = ("pending", "active", "awaiting_input", "paused")
TOUCHES = (
    "core",
    "extension",
    "templates",
    "doctrine",
    "schedules",
    "lanes",
    "goals",
    "standing_rules",
    "migrations",
    "dependencies",
    "self",
)
CODE_TOUCHES = {"core", "extension", "migrations", "dependencies"}
WAIT_FOR_CODE = timedelta(hours=2)
ROLLBACKS_TO_PAUSE, ROLLBACK_WINDOW = 2, timedelta(days=7)
CARD_EXPIRES = timedelta(days=14)
# The answer an expired card takes: a change's card is discarded, an idea card's idea rests.
EXPIRED_ANSWERS = {WORKFLOW_NAME: DISCARD, "workshop-idea": "Not now"}
# Runs of a retired workflow whose worktrees are swept like a change's.
RETIRED_WORKFLOWS = ("tasque-repair",)
PAUSE_FILE = "workshop.paused"
LOCK_FILE = "code.lock"
# The Tasque tools a Workshop run may call: they read, or submit the run's result. The rest are denied.
READ_ONLY_TOOLS = {
    "submit_worker_result",
    "memory_recall",
    "memory_list",
    "memory_get",
    "memory_get_canonical",
    "artifact_list",
    "artifact_get",
    "artifact_read_text",
    "work_list",
    "work_get",
    "work_events",
    "schedule_list",
    "schedule_get",
    "workflow_list",
    "reminder_list",
    "sticky_get",
    "system_status",
    "system_health",
    "weather_now",
    "discord_history",
}
PLAN_TEMPLATE = "work-templates/workshop/plan.template.md"
BUILD_TEMPLATE = "work-templates/workshop/build.template.md"
DOCTRINE_KEYS = ["tasque_workshop", "tasque_design"]
PLAN_FALLBACK = """\
# Workshop: plan one change to Tasque
Plan the change the packet's task context asks for (`request`, with any `notes` and `previous_plan`),
reading your own checkout (your working directory) and changing nothing. Follow the pinned
`tasque_workshop` document. `produces`: {title, kind, no_change, answer, plan, touches, questions,
evidence, packets}.
"""
BUILD_FALLBACK = """\
# Workshop: build one planned change to Tasque
Build the change in the upstream `prepare` output's `brief`, in your working directory only, following
the pinned `tasque_workshop` document: a failing test first for a bug, then the change, the commands in
the input's `commands`, and a commit in each repository you changed. `produces`: {done, changes,
tests_added, db_script, probe, packets, why_not}.
"""


class WorkshopPaused(RuntimeError):
    """The Workshop is paused: only the owner's own asks start a change."""


# --- pause, the code lock, history ----------------------------------------------------------------


def pause_path() -> Path:
    return get_settings().resolved_data_dir / PAUSE_FILE


def paused() -> str | None:
    """Why the Workshop is paused, or None."""
    try:
        data = json.loads(pause_path().read_text(encoding="utf-8"))
    except OSError:
        return None
    except ValueError:
        return "paused"
    return str(data.get("reason") or "paused")


def pause(reason: str) -> None:
    pause_path().write_text(json.dumps({"at": utc_now().isoformat(), "reason": reason}), encoding="utf-8")


def resume() -> bool:
    existed = pause_path().exists()
    pause_path().unlink(missing_ok=True)
    return existed


def _lock_path() -> Path:
    from tasque2.ops.release import releases_dir

    return releases_dir() / LOCK_FILE


def code_holder(session: Session) -> str | None:
    """The change holding the code: from its build until its release is live, or it ends without one. A
    lock whose change is over is freed here."""
    from tasque2.ops.release import find_plan, state

    try:
        data = json.loads(_lock_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    change_id = str(data.get("change_id") or "")
    run = session.get(WorkflowRun, str(data.get("run_id") or ""))
    if run is not None and run.status in OPEN_RUN_STATUSES:
        return change_id
    plan = find_plan(change_id) if change_id else None
    if plan is not None and plan.cold and state(plan) == "queued":
        return change_id
    free_code(session)
    return None


def take_code(change_id: str, run_id: str) -> None:
    path = _lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"change_id": change_id, "run_id": run_id, "at": utc_now().isoformat()}))


def free_code(session: Session, change_id: str | None = None) -> None:
    """Free the lock (only ``change_id``'s, when given) and wake the changes waiting for it."""
    try:
        holder = json.loads(_lock_path().read_text(encoding="utf-8")).get("change_id")
    except (OSError, ValueError):
        holder = None
    if change_id is not None and holder not in (None, change_id):
        return
    _lock_path().unlink(missing_ok=True)
    session.execute(
        update(WorkItem)
        .where(
            WorkItem.worker_kind.in_((PREPARE_WORKER, RELEASE_WORKER)),
            WorkItem.status == "ready",
            WorkItem.not_before.is_not(None),
        )
        .values(not_before=None)
    )


def auto_today(now: datetime | None = None) -> int:
    """Changes that shipped on their own today (local time)."""
    from tasque2.localtime import local_date
    from tasque2.ops.release import history

    today = local_date(now or utc_now())
    count = 0
    for plan in history():
        try:
            created = datetime.fromisoformat(plan.created_at)
        except ValueError:
            continue
        if plan.tier == policy.AUTO and not plan.undo_of and local_date(created) == today:
            count += 1
    return count


# --- the workflow ------------------------------------------------------------------------------------


def denied_tools() -> list[str]:
    """Every Tasque tool that writes: a Workshop run reads live state, and changes only its own folder."""
    from tasque2.extensions import registry
    from tasque2.mcp.tools import CORE_TOOLS

    names = {tool.__name__ for tool in [*CORE_TOOLS, *registry().mcp_tools]}
    return [f"mcp__tasque2__{name}" for name in sorted(names - READ_ONLY_TOOLS)]


def definition() -> dict[str, Any]:
    data = get_settings().resolved_data_dir
    denied = denied_tools()

    def model_run(key: str, title: str, lane: str, template: str, fallback: str, mode: str) -> dict[str, Any]:
        return {
            "key": key,
            "kind": "work",
            "title": title,
            "worker_kind": "provider.default",
            "lane": lane,
            "task_template_path": str(data / template),
            "task_instruction": fallback,
            "runtime_contract": {
                "model_profile": "high",
                "mcp_servers": [],
                "disallowed_tools": denied,
                "guard": {"mode": mode},
            },
            "max_attempts": 1,
            "tolerate_failure": True,
        }

    def step(key: str, title: str, worker: str, after: str) -> dict[str, Any]:
        return {
            "key": key,
            "kind": "work",
            "title": title,
            "worker_kind": worker,
            "task_instruction": title,
            "depends_on": [after],
        }

    build = model_run("build", "Workshop: build the change", BUILD_LANE, BUILD_TEMPLATE, BUILD_FALLBACK, "build")
    build.update(
        {"depends_on": ["prepare"], "skip_when": "prepare.stop", "contract_from": {"model_profile": "classify.profile"}}
    )
    return {
        "nodes": [
            model_run("plan", "Workshop: plan the change", PLAN_LANE, PLAN_TEMPLATE, PLAN_FALLBACK, "plan"),
            step("classify", "Workshop: what the change needs", CLASSIFY_WORKER, "plan"),
            {
                "key": "plan_gate",
                "kind": "gate",
                "prompt": "Build this plan?",
                "choices": [APPROVE, REVISE, DISCARD],
                "card_from": "classify.card",
                "skip_when": "classify.skip_plan_gate",
                "depends_on": ["classify"],
            },
            step("prepare", "Workshop: prepare the build", PREPARE_WORKER, "plan_gate"),
            build,
            step("verify", "Workshop: verify the change", VERIFY_WORKER, "build"),
            {
                "key": "ship_gate",
                "kind": "gate",
                "prompt": "Ship this change?",
                "choices": [SHIP, DISCARD],
                "card_from": "verify.card",
                "skip_when": "verify.skip_ship_gate",
                "depends_on": ["verify"],
            },
            step("release", "Workshop: release the change", RELEASE_WORKER, "ship_gate"),
            step("report", "Workshop: report", REPORT_WORKER, "release"),
        ]
    }


def ensure_bundled(session: Session, name: str, wanted: dict[str, Any]) -> WorkflowDefinition:
    """A workflow the Workshop bundles, registered or brought up to date."""
    from tasque2.workflows import WorkflowService

    existing = session.scalar(select(WorkflowDefinition).where(WorkflowDefinition.name == name))
    if existing is None:
        return WorkflowService(session).create_definition(name=name, version="1", definition=wanted)
    if existing.definition != wanted:
        existing.definition = wanted
        session.flush()
    return existing


def ensure_definition(session: Session) -> WorkflowDefinition:
    """The bundled change workflow, registered or brought up to date."""
    return ensure_bundled(session, WORKFLOW_NAME, definition())


def commands(trees: list[Any]) -> dict[str, str]:
    """The commands a build runs: the suites and ruff, from the change's root."""
    python = Path(sys.executable).as_posix()
    root = next(tree for tree in trees if tree.name == "core").path
    found = {"core_suite": f'cd "{root}" && "{python}" -m pytest -q -p no:cacheprovider'}
    for tree in trees:
        if tree.name not in ("core", "data") and (Path(tree.path) / "tests").is_dir():
            found[f"{tree.name}_suite"] = (
                f'cd "{root}" && "{python}" -m pytest -q -p no:cacheprovider extensions/{tree.name}/tests'
            )
    found["ruff"] = (
        f'cd "<repository>" && "{python}" -m ruff check <changed files> && "{python}" -m ruff format <changed files>'
    )
    return found


def new_change_id() -> str:
    return f"{utc_now().strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(2)}"


def start_change(
    session: Session,
    *,
    request: str,
    origin: str = "user",
    thread_id: str | None = None,
    author: str | None = None,
    revision_of: str | None = None,
    notes: list[str] | None = None,
    previous_plan: str | None = None,
    evidence: list[str] | None = None,
    files: list[str] | None = None,
    issues: list[str] | None = None,
) -> WorkflowRun:
    """Make the change's worktrees (the live config committed first, doctrine exported) and start its run.
    A fix the Workshop filed names the ``files`` triage expects it to touch and the ledger rows (``issues``)
    it answers."""
    from tasque2.ops import datarepo
    from tasque2.ops.worktree import create_worktrees
    from tasque2.workflows import WorkflowService

    reason = paused()
    if reason and origin != "user":
        raise WorkshopPaused(reason)
    change_id = new_change_id()
    if datarepo.is_repo():
        datarepo.snapshot(f"before change {change_id}")
    trees = create_worktrees(change_id, kind="workshop", include_data=True)
    root = next(tree for tree in trees if tree.name == "core").path
    return WorkflowService(session).start_run(
        workflow_definition_id=ensure_definition(session).id,
        input={
            "change_id": change_id,
            "request": request.strip(),
            "origin": origin if origin in policy.ORIGINS else "model",
            "author": author,
            "revision_of": revision_of,
            "notes": list(notes or []),
            "previous_plan": previous_plan,
            "evidence": list(evidence or []),
            "files": list(files or []),
            "issues": list(issues or []),
            "paused": bool(reason),
            "live_data": str(get_settings().resolved_data_dir),
            "cwd": root,
            "worktrees": [tree.data() for tree in trees],
            "commands": commands(trees),
            "lane": LANE,
            "memory_namespace": "global",
            "memory_canonical_keys": DOCTRINE_KEYS,
            "context_limits": {"memories": 2, "artifacts": 0},
        },
        discord_thread_id=thread_id,
    )


# --- the workers -------------------------------------------------------------------------------------


def _session(work_item: WorkItem) -> Session:
    session = object_session(work_item)
    if session is None:
        raise RuntimeError("a Workshop worker needs the work item's session")
    return session


def _run(work_item: WorkItem) -> tuple[Session, WorkflowRun]:
    session = _session(work_item)
    run = session.get(WorkflowRun, work_item.workflow_run_id or "")
    if run is None:
        raise RuntimeError("a Workshop step runs inside its change's workflow run")
    return session, run


def node_output(session: Session, run_id: str, key: str) -> dict[str, Any]:
    node = session.scalar(
        select(WorkflowNode).where(WorkflowNode.workflow_run_id == run_id, WorkflowNode.node_key == key)
    )
    return dict(node.output or {}) if node is not None else {}


def trees_of(run: WorkflowRun) -> list[Any]:
    from tasque2.ops.worktree import RepoWorktree

    return [RepoWorktree.from_data(item) for item in (run.input or {}).get("worktrees") or []]


def files_of(value: Any) -> list[str]:
    """Paths as a change names them: from its checkout's root, with forward slashes."""
    found: list[str] = []
    for item in value if isinstance(value, list) else []:
        path = re.sub(r"^(\./)+", "", str(item).strip().replace("\\", "/")).strip("/")
        if path and path not in found:
            found.append(path)
    return found[:40]


def open_changes(session: Session) -> list[WorkflowRun]:
    return list(
        session.scalars(
            select(WorkflowRun)
            .where(WorkflowRun.name == WORKFLOW_NAME, WorkflowRun.status.in_(OPEN_RUN_STATUSES))
            .order_by(WorkflowRun.created_at)
        ).all()
    )


def change_files(session: Session, run: WorkflowRun) -> set[str]:
    """The files a change touches, as far as anyone has said: triage's guess, then its plan's own list."""
    planned = node_output(session, run.id, "classify").get("files")
    return set(files_of((run.input or {}).get("files"))) | set(files_of(planned))


def cleanup(session: Session, run: WorkflowRun, *, keep_branches: bool = False) -> None:
    """Remove the change's worktrees (and its branches, unless a release still needs them); free the code."""
    from tasque2.ops.worktree import remove_worktrees

    try:
        remove_worktrees(trees_of(run), delete_branches=not keep_branches)
    except Exception:  # noqa: BLE001 - a folder left behind is swept later; the change still ends
        logger.exception("Workshop change %s: removing its worktrees failed", (run.input or {}).get("change_id"))
    if not keep_branches:
        free_code(session, (run.input or {}).get("change_id"))


def _questions(value: Any) -> list[dict[str, str]]:
    found = []
    for item in value if isinstance(value, list) else []:
        if isinstance(item, dict) and str(item.get("question") or "").strip():
            found.append({"question": str(item["question"]).strip(), "default": str(item.get("default") or "").strip()})
        elif isinstance(item, str) and item.strip():
            found.append({"question": item.strip(), "default": ""})
    return found[:4]


def plan_card(
    title: str, kind: str, plan: dict[str, Any], questions: list[dict[str, str]], why: str, *, filed: bool = False
) -> str:
    found = "; the Workshop filed it" if filed else ""
    lines = [f"**Workshop plan: {title}** ({kind}; needs your OK: {why}{found})", ""]
    lines += [line.rstrip() for line in str(plan.get("plan") or "").strip().splitlines()[:25]]
    if questions:
        lines += ["", "**Questions** (type your answers here, then tap a button; unanswered ones take the default):"]
        for number, item in enumerate(questions, start=1):
            default = f" _(default: {item['default']})_" if item["default"] else ""
            lines.append(f"{number}. {item['question']}{default}")
    evidence = [str(item) for item in plan.get("evidence") or [] if str(item).strip()][:4]
    if evidence:
        lines += ["", "Why: " + "; ".join(evidence)]
    lines += [
        "",
        "Approve builds it (you get one more tap before it goes live). Revise plans it again with what you typed. "
        "Discard drops it.",
    ]
    return "\n".join(lines)


def classify_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_classify``: the tier the planned change needs, and the plan card."""
    session, run = _run(work_item)
    given = run.input or {}
    plan = node_output(session, run.id, "plan")

    def end(why: str, answer: str | None = None) -> dict[str, Any]:
        produced = {"silent": True, "ended": True, "why": why, "answer": answer, "skip_plan_gate": True}
        return {"summary": why, "produces": produced}

    if not plan or plan.get("tolerated_failure"):
        return end("the planning run did not finish")
    title = str(plan.get("title") or given.get("request") or "a change").strip()[:80]
    if plan.get("no_change"):
        return end("nothing to change", answer=str(plan.get("answer") or "").strip() or "Nothing needs to change.")
    kind = str(plan.get("kind") or "")
    kind = kind if kind in policy.KINDS else "feature"
    touches = [str(item) for item in plan.get("touches") or [] if str(item) in TOUCHES]
    questions = _questions(plan.get("questions"))
    origin = str(given.get("origin") or "model")
    tier, why = policy.classify(origin=origin, kind=kind, touches=touches, questions=len(questions))
    if tier == policy.AUTO and (paused() or given.get("paused")):
        tier, why = policy.TAP, "the Workshop is paused, so nothing ships on its own"
    if tier == policy.AUTO and auto_today() >= policy.AUTO_PER_DAY:
        tier, why = policy.TAP, f"{policy.AUTO_PER_DAY} changes already shipped on their own today"
    produced = {
        "silent": True,
        "ended": False,
        "title": title,
        "kind": kind,
        "tier": tier,
        "why": why,
        "touches": touches,
        "files": files_of(plan.get("files")),
        "questions": questions,
        "skip_plan_gate": tier != policy.PLAN,
        "card": plan_card(title, kind, plan, questions, why, filed=origin == "workshop") if tier == policy.PLAN else "",
        "profile": "ultra" if kind == "redesign" else "high",
        "needs_code": bool(set(touches) & CODE_TOUCHES),
    }
    return {"summary": f"{title}: {tier} ({why})", "produces": produced}


def refresh_worktrees(run: WorkflowRun) -> list[Any]:
    """Move each worktree with nothing built yet to its live checkout's HEAD (the plan may have waited),
    the live config committed and its doctrine exported first."""
    from tasque2.ops import datarepo
    from tasque2.ops.worktree import RepoWorktree, git, has_commits, status_hash

    if datarepo.is_repo():
        datarepo.snapshot(f"before building {(run.input or {}).get('change_id')}")
    fresh = []
    for tree in trees_of(run):
        live_head = git(tree.live, "rev-parse", "HEAD")
        if live_head != tree.base and not has_commits(tree):
            git(tree.path, "reset", "-q", "--hard", live_head)
            tree = RepoWorktree(tree.name, tree.live, tree.path, tree.branch, live_head, status_hash(tree.live))
        fresh.append(tree)
    run.input = {**(run.input or {}), "worktrees": [tree.data() for tree in fresh]}
    return fresh


def prepare_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_prepare``: carry out the plan gate's answer, and get the worktrees ready."""
    from tasque2.work.runner import WorkDeferred

    session, run = _run(work_item)
    given = run.input or {}
    classify = node_output(session, run.id, "classify")

    def stop(why: str, **extra: Any) -> dict[str, Any]:
        return {"summary": why, "produces": {"silent": True, "stop": True, "why": why, **extra}}

    if classify.get("ended"):
        cleanup(session, run)
        return stop(str(classify.get("why") or "ended"))
    gate = node_output(session, run.id, "plan_gate")
    answer = str(gate.get("answer") or APPROVE)
    notes = [str(note.get("text") or "") for note in gate.get("notes") or [] if str(note.get("text") or "").strip()]
    if answer == DISCARD:
        cleanup(session, run)
        return stop("expired" if gate.get("expired") else "discarded")
    if answer == REVISE:
        plan = node_output(session, run.id, "plan")
        cleanup(session, run)
        again = start_change(
            session,
            request=str(given.get("request") or ""),
            origin=str(given.get("origin") or "user"),
            thread_id=run.discord_thread_id,
            author=given.get("author"),
            revision_of=run.id,
            notes=[*(given.get("notes") or []), *notes],
            previous_plan=str(plan.get("plan") or ""),
            evidence=given.get("evidence"),
            files=given.get("files"),
            issues=given.get("issues"),
        )
        _refile(session, run, again)
        return stop("revised", revision_run_id=again.id)
    change_id = str(given.get("change_id"))
    if classify.get("needs_code"):
        holder = code_holder(session)
        if holder and holder != change_id:
            raise WorkDeferred(utc_now() + WAIT_FOR_CODE, f"waiting for change {holder} to go live first")
        take_code(change_id, run.id)
    refresh_worktrees(run)
    plan = node_output(session, run.id, "plan")
    brief = {
        "request": given.get("request"),
        "title": classify.get("title"),
        "kind": classify.get("kind"),
        "tier": classify.get("tier"),
        "plan": plan.get("plan"),
        "questions": classify.get("questions") or [],
        "answers": notes,
        "evidence": plan.get("evidence") or [],
        "packets": plan.get("packets") or [],
        "change_folder": f"changes/{change_id}",
    }
    return {"summary": "Ready to build.", "produces": {"silent": True, "stop": False, "brief": brief}}


def verify_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_verify``: the checks and the rehearsal (``tasque2.workshop.verify``)."""
    from tasque2.workshop.verify import verify

    session, run = _run(work_item)
    if node_output(session, run.id, "prepare").get("stop"):
        return {"summary": "Nothing to verify.", "produces": {"silent": True, "stop": True, "skip_ship_gate": True}}
    try:
        outcome = verify(session, run)
    except Exception as exc:  # noqa: BLE001 - a check that breaks fails the change, never the run
        logger.exception("Workshop change %s: verify failed", (run.input or {}).get("change_id"))
        outcome = {"ok": False, "reasons": [f"the checks broke: {type(exc).__name__}: {exc}"]}
    if not outcome.get("ok"):
        cleanup(session, run)
        produced = {"silent": True, "stop": True, "skip_ship_gate": True, "reasons": outcome.get("reasons") or []}
        return {"summary": "Not shipped: " + "; ".join(outcome.get("reasons") or [])[:600], "produces": produced}
    auto = outcome["tier"] == policy.AUTO and not paused() and auto_today() < policy.AUTO_PER_DAY
    if not auto:
        # A card waiting for the owner never holds up the other code changes: the lock is taken again on Ship.
        free_code(session, (run.input or {}).get("change_id"))
    produced = {"silent": True, "stop": False, **outcome, "auto_ship": auto, "skip_ship_gate": auto}
    return {"summary": f"Verified ({outcome['tier']}).", "produces": produced}


def move_onto_live(run: WorkflowRun) -> list[str]:
    """Carry each code worktree's commits onto its live checkout's HEAD where the live code moved on since
    the change was built; returns the repositories moved. Raises ``WorktreeError`` when one does not apply."""
    from tasque2.ops.worktree import RepoWorktree, WorktreeError, git, status_hash

    moved: list[str] = []
    fresh = []
    for tree in trees_of(run):
        live_head = git(tree.live, "rev-parse", "HEAD")
        if tree.name == "data" or live_head == tree.base:
            fresh.append(tree)
            continue
        try:
            git(tree.path, "-c", "core.longpaths=true", "rebase", "-q", "--onto", live_head, tree.base)
        except WorktreeError as exc:
            git(tree.path, "rebase", "--abort", check=False)
            reason = str(exc).rsplit(": ", 1)[-1].strip().splitlines()
            raise WorktreeError(f"{tree.name}: {reason[-1] if reason else 'the rebase failed'}") from exc
        moved.append(tree.name)
        fresh.append(RepoWorktree(tree.name, tree.live, tree.path, tree.branch, live_head, status_hash(tree.live)))
    if moved:
        run.input = {**(run.input or {}), "worktrees": [tree.data() for tree in fresh]}
    return moved


def _not_released(session: Session, run: WorkflowRun, problems: list[str]) -> dict[str, Any]:
    cleanup(session, run)
    return {"summary": "Not released.", "produces": {"silent": True, "released": False, "problems": problems}}


def release_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_release``: config lands now; code is queued for a restart. Never raises, except
    to wait for the code lock: a code change that waited for its tap takes the lock again, and is moved onto
    the live code and checked again when the live code moved on meanwhile."""
    from tasque2.daemon.restart import RestartBusy
    from tasque2.ops.release import ReleasePlan, preflight, queue_cold, release_hot, save_plan
    from tasque2.ops.worktree import WorktreeError, remove_worktrees
    from tasque2.work.runner import WorkDeferred
    from tasque2.workshop.verify import verify

    session, run = _run(work_item)
    verified = node_output(session, run.id, "verify")
    if verified.get("stop") or not verified.get("ok"):
        return {"summary": "Not released.", "produces": {"silent": True, "released": False}}
    gate = node_output(session, run.id, "ship_gate")
    if str(gate.get("answer") or SHIP) != SHIP:
        cleanup(session, run)
        produced = {"silent": True, "released": False, "discarded": True, "expired": bool(gate.get("expired"))}
        return {"summary": "Expired." if gate.get("expired") else "Discarded.", "produces": produced}
    plan = ReleasePlan.from_dict(verified["release_plan"])
    if plan.cold and not verified.get("auto_ship"):
        change_id = str((run.input or {}).get("change_id"))
        holder = code_holder(session)
        if holder and holder != change_id:
            raise WorkDeferred(utc_now() + WAIT_FOR_CODE, f"waiting for change {holder} to go live first")
        take_code(change_id, run.id)
        try:
            moved = move_onto_live(run)
        except WorktreeError as exc:
            return _not_released(session, run, [f"it no longer applies on the live code ({str(exc)[:200]})"])
        if moved:
            try:
                again = verify(session, run)
            except Exception as exc:  # noqa: BLE001 - a check that breaks stops the release, never the run
                logger.exception("Workshop change %s: checking it again failed", change_id)
                again = {"ok": False, "reasons": [f"the checks broke: {type(exc).__name__}: {exc}"]}
            if not again.get("ok"):
                reasons = [f"checked again on the live code: {reason}" for reason in again.get("reasons") or []]
                return _not_released(session, run, reasons)
            plan = ReleasePlan.from_dict(again["release_plan"])
    plan.run_id, plan.thread_id = run.id, run.discord_thread_id
    if not plan.cold:
        # The worktrees go first: the release merges (and may carry) the config branch, never a tree that
        # still holds it.
        remove_worktrees(trees_of(run), delete_branches=False)
        try:
            outcome = release_hot(session, plan)
        except Exception as exc:  # noqa: BLE001 - reported to the owner, recorded as a fault
            logger.exception("Workshop change %s: the hot release failed", plan.id)
            outcome = {"ok": False, "problems": [f"{type(exc).__name__}: {exc}"]}
        if outcome["ok"]:
            plan.announced = True
            save_plan(plan)
        cleanup(session, run)
        produced = {
            "silent": True,
            "released": bool(outcome["ok"]),
            "kind": "hot",
            "release_id": plan.id if outcome["ok"] else None,
            "problems": outcome.get("problems") or [],
        }
        return {"summary": "Released." if outcome["ok"] else "Not released.", "produces": produced}
    problems = preflight(session, plan)
    if not problems:
        try:
            queue_cold(plan)
        except RestartBusy as exc:
            problems = [str(exc)]
    if problems:
        return _not_released(session, run, problems)
    remove_worktrees(trees_of(run), delete_branches=False)  # the branches stay for the switch
    produced = {"silent": True, "released": True, "kind": "cold", "release_id": plan.id}
    return {"summary": "Queued for a restart.", "produces": produced}


def _lines(items: Any, limit: int = 8) -> list[str]:
    return [f"- {str(item).strip()}" for item in (items or []) if str(item).strip()][:limit]


def report_text(session: Session, run: WorkflowRun) -> tuple[str, str | None]:
    """The change's last word in the thread, and the release its Undo button reverses (if any)."""
    classify = node_output(session, run.id, "classify")
    title = str(classify.get("title") or (run.input or {}).get("request") or "the change")[:80]
    if classify.get("ended"):
        if classify.get("answer"):
            return str(classify["answer"]), None
        return f"I couldn't plan this one ({classify.get('why')}). Say it again, or put it another way.", None
    prepare = node_output(session, run.id, "prepare")
    if prepare.get("why") == "discarded":
        return f"Dropped: {title}. Nothing changed.", None
    if prepare.get("why") == "expired":
        return f"Expired: {title} waited {CARD_EXPIRES.days} days for your OK. Nothing changed; say redo for it.", None
    if prepare.get("why") == "revised":
        return f"Planning {title} again with your notes.", None
    verified = node_output(session, run.id, "verify")
    if verified.get("stop"):
        reasons = "\n".join(_lines(verified.get("reasons") or ["it did not pass its checks"], 5))
        return f"Not shipped: {title}.\n{reasons}\nNothing changed. Say redo to try again.", None
    release = node_output(session, run.id, "release")
    if release.get("expired"):
        return f"Expired: {title} waited {CARD_EXPIRES.days} days for your tap. Nothing changed; say redo for it.", None
    if release.get("discarded"):
        return f"Dropped: {title}. Nothing changed.", None
    if not release.get("released"):
        problems = release.get("problems") or ["it could not be released"]
        return f"Not shipped: {title}.\n" + "\n".join(_lines(problems, 5)) + "\nSay redo to build it again.", None
    plan = verified.get("release_plan") or {}
    changes = _lines(str(plan.get("summary") or "").splitlines())
    how = "Shipped on its own" if verified.get("auto_ship") else "Shipped"
    if release.get("kind") == "hot":
        lines = [f"**Live now: {title}** ({how.lower()}: {verified.get('why')})", *changes, "Tap Undo to reverse it."]
        return "\n".join(lines), str(release["release_id"])
    lines = [
        f"**Ready: {title}** ({verified.get('why')})",
        *changes,
        "It goes live when Tasque restarts at the next idle moment; I'll post here once it's live. "
        "Say undo to stop it.",
    ]
    return "\n".join(lines), None


def outcome(session: Session, run: WorkflowRun) -> str:
    """How a change ended, for the ledger rows it answers: shipped, discarded (or expired), or ended."""
    release = node_output(session, run.id, "release")
    if release.get("released"):
        return ledger.SHIPPED
    if node_output(session, run.id, "prepare").get("why") in ("discarded", "expired") or release.get("discarded"):
        return ledger.DISCARDED
    return ledger.ENDED


def report_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_report``: the change's final post. A change the Workshop filed posts only its
    "Live now" (its cards posted on their own), and its ledger rows learn how it ended."""
    session, run = _run(work_item)
    text, release_id = report_text(session, run)
    produced: dict[str, Any] = {"workshop_report": True}
    if release_id:
        produced["undo_release"] = release_id
    given = run.input or {}
    if given.get("origin") == "workshop":
        if node_output(session, run.id, "prepare").get("why") != "revised":
            ledger.settle(session, str(given.get("change_id")), outcome(session, run))
        if not release_id:
            produced["silent"] = True
    return {"summary": text, "produces": produced}


# --- after a restart: announce, pause on rollbacks, sweep ------------------------------------------------


def announce_text(plan: Any, status: str) -> str:
    if plan.undo_of:
        if status == "live":
            return f"**Undone: {plan.title.removeprefix('Undo: ')}** is reversed (Tasque restarted with it)."
        return f"The undo of {plan.title.removeprefix('Undo: ')} did not go live: {_why(plan)}. It is still in place."
    if status == "live":
        lines = [f"**Live now: {plan.title}** (Tasque restarted with it).", *_lines(plan.summary.splitlines())]
        return "\n".join([*lines, "Tap Undo to reverse it."])
    return f"**Not live: {plan.title}**: it was rolled back ({_why(plan)}). Nothing changed. Say redo to rebuild it."


def _why(plan: Any) -> str:
    return str((plan.outcome or {}).get("error") or "the daemon did not come up healthy")[:300]


def announce_releases(session: Session) -> int:
    """Post how each cold release went (once), record undos, pause after repeated rollbacks, free the code
    lock and sweep leftover worktrees. The daemon runs it as it starts and then once a minute, so every step
    is safe to repeat."""
    from tasque2.ops.release import history, mark_undone, save_plan, state
    from tasque2.work.repository import WorkRepository

    posted, rolled_back_now = 0, False
    for plan in history():
        if plan.announced or plan.outcome is None or not plan.cold:
            continue
        status = state(plan)
        plan.announced = True
        save_plan(plan)
        rolled_back_now = rolled_back_now or status == "failed"
        if status == "canceled":
            continue
        if plan.undo_of and status == "live":
            mark_undone(plan)
        if not plan.thread_id or (plan.origin == "workshop" and status != "live"):
            continue  # a change the Workshop filed says only "Live now"
        undo = status == "live" and not plan.undo_of
        WorkRepository(session).create_work_item(
            title=f"Workshop: {plan.title}"[:240],
            task_instruction=announce_text(plan, status),
            worker_kind=ANNOUNCE_WORKER,
            context={"release_id": plan.id, "undo": undo},
            idempotency_key=f"workshop:announce:{plan.id}",
            source_kind="workshop_release",
            source_id=plan.id,
            discord_thread_id=plan.thread_id,
            lane=LANE,
        )
        posted += 1
    if rolled_back_now:  # counted only then, so the owner's resume holds until the next rollback
        since = utc_now() - ROLLBACK_WINDOW
        rolled_back = [
            plan for plan in history() if state(plan) == "failed" and _at((plan.outcome or {}).get("at")) >= since
        ]
        if len(rolled_back) >= ROLLBACKS_TO_PAUSE and not paused():
            pause(f"{len(rolled_back)} releases rolled back within a week")
    code_holder(session)
    sweep_worktrees(session)
    return posted


def _at(value: Any) -> datetime:
    try:
        moment = datetime.fromisoformat(str(value))
    except ValueError:
        return datetime.min.replace(tzinfo=utc_now().tzinfo)
    return moment if moment.tzinfo else moment.replace(tzinfo=utc_now().tzinfo)


def announce_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_announce``: post a cold release's outcome, with Undo when it went live."""
    context = work_item.context or {}
    produced: dict[str, Any] = {"workshop_report": True}
    if context.get("undo") and context.get("release_id"):
        produced["undo_release"] = str(context["release_id"])
    return {"summary": work_item.task_instruction, "produces": produced}


def sweep_worktrees(session: Session) -> int:
    """Remove worktrees left by changes that ended (canceled from the controls, say), and by the runs of a
    retired workflow."""
    from tasque2.ops.release import find_plan, state
    from tasque2.ops.worktree import remove_worktrees

    removed = 0
    runs = session.scalars(select(WorkflowRun).where(WorkflowRun.name.in_((WORKFLOW_NAME, *RETIRED_WORKFLOWS)))).all()
    for run in runs:
        if run.status in OPEN_RUN_STATUSES:
            continue
        trees = trees_of(run)
        if not trees or not any(Path(tree.path).exists() for tree in trees):
            continue
        if run.name in RETIRED_WORKFLOWS:
            remove_worktrees(trees, delete_branches=True)
        else:
            plan = find_plan(str((run.input or {}).get("change_id")))
            cleanup(session, run, keep_branches=plan is not None and state(plan) == "queued")
        removed += 1
    return removed


def expire_cards(session: Session, *, now: datetime | None = None) -> int:
    """Answer each Workshop card nobody answered for ``CARD_EXPIRES``, marked ``expired``: a change's card
    as a Discard, an idea card as a Not now. Typing a note on a card counts as an answer to wait for."""
    from tasque2.workflows import WorkflowService

    now = now or utc_now()
    rows = session.execute(
        select(WorkflowRun, WorkflowNode)
        .join(WorkflowNode, WorkflowNode.workflow_run_id == WorkflowRun.id)
        .where(
            WorkflowRun.name.in_(list(EXPIRED_ANSWERS)),
            WorkflowRun.status == "awaiting_input",
            WorkflowNode.kind == "gate",
            WorkflowNode.status == "awaiting_input",
        )
    ).all()
    expired = 0
    for run, node in rows:
        answer = EXPIRED_ANSWERS[run.name]
        waited = now - ledger.aware(node.updated_at)
        if waited < CARD_EXPIRES or answer not in (node.definition or {}).get("choices", []):
            continue
        WorkflowService(session).answer_gate(workflow_run_id=run.id, node_key=node.node_key, answer=answer)
        node.output = {**(node.output or {}), "expired": True}
        expired += 1
    session.flush()
    return expired


# --- undo, redo, status ------------------------------------------------------------------------------------


def undo_text(plan: Any, outcome: dict[str, Any]) -> str:
    if outcome.get("kind") == "withdrawn":
        return f"Withdrawn: {plan.title} will not go live."
    if outcome.get("kind") == "cold":
        return f"Undoing {plan.title}: the reverse goes live when Tasque restarts at the next idle moment."
    blocked = outcome.get("blocked") or []
    if blocked:
        return f"Undone: {plan.title}, except what changed again since: {', '.join(blocked)}."
    return f"Undone: {plan.title} is reversed."


def undo_by_id(session: Session, release_id: str) -> str:
    """The Undo button."""
    from tasque2.ops.release import UndoError, find_plan, state, undo_release

    plan = find_plan(release_id)
    if plan is None:
        return "That release is not on record."
    if state(plan) == "undone":
        return f"{plan.title} is already undone."
    try:
        outcome = undo_release(session, plan)
    except (UndoError, RuntimeError) as exc:
        return f"Can't undo {plan.title}: {exc}."
    if outcome.get("kind") == "withdrawn":
        code_holder(session)  # a release withdrawn before it went live holds the code no longer
    return undo_text(plan, outcome)


def undo_latest(session: Session) -> str:
    """``undo`` typed in the thread: the newest release that is live or waiting to go live."""
    from tasque2.ops.release import latest_undoable

    plan = latest_undoable()
    if plan is None:
        return "Nothing to undo: no Workshop release is live or waiting."
    return undo_by_id(session, plan.id)


def latest_runs(session: Session, *, thread_id: str | None = None, limit: int = 20) -> list[WorkflowRun]:
    statement = select(WorkflowRun).where(WorkflowRun.name == WORKFLOW_NAME)
    if thread_id:
        statement = statement.where(WorkflowRun.discord_thread_id == thread_id)
    return list(session.scalars(statement.order_by(WorkflowRun.created_at.desc()).limit(limit)).all())


def redo(session: Session, *, thread_id: str | None, author: str | None = None) -> WorkflowRun | None:
    """Start the newest change that did not ship again, from the same request. A change the Workshop filed
    is not one: its rows go back to the ledger, which files them again in time."""
    for run in latest_runs(session, thread_id=thread_id):
        given = run.input or {}
        if run.status in OPEN_RUN_STATUSES or given.get("origin") == "workshop":
            continue
        release = node_output(session, run.id, "release")
        if release.get("released") or node_output(session, run.id, "classify").get("answer"):
            continue
        return start_change(
            session,
            request=str(given.get("request") or ""),
            origin=str(given.get("origin") or "user"),
            thread_id=thread_id or run.discord_thread_id,
            author=author or given.get("author"),
            revision_of=run.id,
            notes=list(given.get("notes") or []),
            evidence=given.get("evidence"),
        )
    return None


def _refile(session: Session, run: WorkflowRun, again: WorkflowRun) -> None:
    """The ledger rows a change the Workshop filed answers go with the change planned again in its place
    (a Revise)."""
    given = run.input or {}
    if given.get("origin") == "workshop" and given.get("issues"):
        ledger.refile(
            session, str(given.get("change_id")), new_change_id=str(again.input["change_id"]), run_id=again.id
        )


STAGES = (
    ("plan", "planning"),
    ("classify", "planning"),
    ("plan_gate", "waiting for your OK on the plan"),
    ("prepare", "waiting for another code change to go live"),
    ("build", "building"),
    ("verify", "checking"),
    ("ship_gate", "waiting for your tap to ship"),
    ("release", "releasing"),
    ("report", "reporting"),
)


def stage(session: Session, run: WorkflowRun) -> str:
    nodes = {
        node.node_key: node
        for node in session.scalars(select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id)).all()
    }
    for key, label in STAGES:
        node = nodes.get(key)
        if node is not None and node.status not in ("succeeded", "failed_tolerated"):
            return label
    return run.status


def status_text(session: Session) -> str:
    from tasque2.ops.release import history, state

    reason = paused()
    lines = [f"**Workshop:** {'paused (' + reason + ')' if reason else 'on'}"]
    open_runs = [run for run in latest_runs(session, limit=50) if run.status in OPEN_RUN_STATUSES]
    if open_runs:
        lines.append("In progress:")
        for run in reversed(open_runs):
            title = node_output(session, run.id, "classify").get("title") or (run.input or {}).get("request") or "?"
            lines.append(f"- {str(title)[:70]}: {stage(session, run)}")
    waiting = [plan for plan in history() if state(plan) == "queued"]
    for plan in waiting:
        lines.append(f"Waiting to go live: {plan.title} (next idle moment)")
    recent = [plan for plan in history() if state(plan) != "queued"][-5:]
    if recent:
        lines.append("Recent releases:")
        lines += [f"- {plan.created_at[:10]} {plan.title}: {state(plan)}" for plan in reversed(recent)]
    lines.append(f"Shipped on their own today: {auto_today()}/{policy.AUTO_PER_DAY}.")
    lines += ledger.status_lines(session)
    return "\n".join(lines)
