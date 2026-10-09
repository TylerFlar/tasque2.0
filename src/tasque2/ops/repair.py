"""Self-repair: a recurring code fault gets a tested fix on a branch, merged only when the user taps.

The ``tasque_repair_due`` gate opens when the fault ledger holds a recurring fault (or a dead letter
that never recovered) that is not an environmental failure (usage limits, sign-ins, the network) and
has not been tried before; at most one repair is open and two run a week. The launcher makes
throwaway worktrees (``tasque2.ops.worktree``) and starts the ``tasque-repair`` workflow:

1. ``fix`` (a model run in the worktree): reproduce the fault with a failing test, fix it, run the
   suites, commit on the repair branch;
2. ``verify`` (no model): re-run the suites, prove the live checkouts were left alone, keep the change
   to Python code and tests, scan it for personal data, and write the card. A fix that fails any of
   this ends the run quietly; the weekly health check still names the fault;
3. ``approve``: a card with "Merge and restart" and "Discard" in the run's thread;
4. ``merge`` (no model): on "Merge and restart", snapshot the database and ask the daemon to restart
   at a quiet moment, fast-forwarding the live checkouts to the repair branches and pushing the core
   one once the new daemon is healthy (``tasque2.daemon.respawn``); on "Discard", remove it all.
"""

from __future__ import annotations

import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, object_session

from tasque2.config import get_settings
from tasque2.models import FailedWork, WorkflowDefinition, WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.ops.faults import fault_summary, read_faults, recurring
from tasque2.ops.worktree import (
    RepoWorktree,
    committed_files,
    create_worktrees,
    has_commits,
    live_changes,
    remove_worktrees,
    uncommitted,
)

REPAIR_GATE = "tasque_repair_due"
WORKFLOW_NAME = "tasque-repair"
LAUNCH_WORKER = "function.tasque_repair_launch"
VERIFY_WORKER = "function.tasque_repair_verify"
MERGE_WORKER = "function.tasque_repair_merge"
MERGE_CHOICE, DISCARD_CHOICE = "Merge and restart", "Discard"
OPEN_RUN_STATUSES = ("pending", "active", "awaiting_input", "paused")
MAX_PER_WEEK = 2
TRIED_WITHIN = timedelta(days=30)
TEST_TIMEOUT_SECONDS = 30 * 60
ENVIRONMENTAL = re.compile(
    r"usage limit|rate limit|session limit|capacity|oauth|token|unauthori[sz]ed|forbidden|\b40[13]\b|log ?in|"
    r"sign[- ]?in|duo|timed? ?out|timeout|connection|connect|network|dns|ssl|certificate|"
    r"submit_worker_result|TransientProviderError",
    re.IGNORECASE,
)
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
SHELL_DENIED = ("git push", "git checkout", "git switch", "git reset", "git worktree", "git branch")
FIX_INSTRUCTION = """\
# Tasque repair: fix one recurring code fault

You work in a throwaway git worktree of Tasque: your working directory, on the branch named in the
packet input's `worktrees`. Extension repositories have their own worktrees under `extensions/` in
it. The live checkouts the daemon runs from are elsewhere: never touch them, never push, never
switch or reset branches, never run the daemon or any command against the live data.

## The fault
The packet input's `fault`: where it was raised (`frame`), the exception and message, how often,
and the latest traceback. A pinned `tasque_repair` document, when there is one, holds the owner's
rules for repairs; where it is stricter than these steps, it wins.

## Steps
1. Reproduce it: write a focused failing test in the repository the frame belongs to (the core
   `tests/`, or the extension's `tests/`), using that suite's fixtures. Run it and watch it fail for
   the reason the traceback shows.
2. Fix the cause with the smallest change that makes the test pass. Python code and tests only: no
   migrations, no model columns, no dependency or project-file changes, nothing under `data/`, no
   doctrine or templates.
3. Run the commands in the input's `commands`: the core suite, each changed extension's suite (each
   on its own), then ruff. Everything must pass.
4. Commit in each repository you changed: `git -C <its worktree> add -A`, then
   `git -C <its worktree> commit -m "<what was wrong, and the fix>"`. No personal details in code,
   tests or the message.
5. If you cannot reproduce it, or the fix needs anything outside step 2, change nothing and say why.

## Output
`produces`: `{fixed: true|false, cause: "<one sentence>", fix: "<one sentence>", files: [...],
tests_added: [...], why_not: "<when fixed is false>"}`. Keep `summary` to two sentences.
"""


# --- what needs repair ---------------------------------------------------------------------------


def environmental(text: str) -> bool:
    return bool(ENVIRONMENTAL.search(text or ""))


def _repair_runs(session: Session, since: datetime | None = None) -> list[WorkflowRun]:
    statement = select(WorkflowRun).where(WorkflowRun.name == WORKFLOW_NAME)
    if since is not None:
        statement = statement.where(WorkflowRun.created_at >= since)
    return list(session.scalars(statement.order_by(WorkflowRun.created_at)).all())


def _latest_traceback(signature: str) -> str | None:
    found = None
    for entry in read_faults():
        if entry.get("signature") == signature and entry.get("traceback"):
            found = entry["traceback"]
    return found


def candidates(session: Session, *, now: datetime | None = None) -> list[dict[str, Any]]:
    """Faults worth a repair, most frequent first: recurring code faults, then dead letters."""
    now = now or utc_now()
    tried = {
        str((run.input or {}).get("fault", {}).get("key")) for run in _repair_runs(session, since=now - TRIED_WITHIN)
    }
    found: list[dict[str, Any]] = []
    for fault in fault_summary(days=7, now=now):
        text = f"{fault.get('exc_type')} {fault.get('message')} {fault.get('logger')}"
        key = f"fault:{fault['signature']}"
        if not recurring(fault) or environmental(text) or key in tried:
            continue
        found.append(
            {
                "key": key,
                "kind": "fault",
                "logger": fault.get("logger"),
                "exc_type": fault.get("exc_type"),
                "message": fault.get("message"),
                "frame": fault.get("frame"),
                "count": fault.get("count"),
                "days": fault.get("days"),
                "traceback": _latest_traceback(fault["signature"]),
            }
        )
    rows = session.execute(
        select(FailedWork, WorkItem)
        .join(WorkItem, WorkItem.id == FailedWork.work_item_id)
        .where(FailedWork.status == "unresolved")
        .order_by(FailedWork.created_at)
    ).all()
    for failed, item in rows:
        key = f"dead_letter:{item.id}"
        if key in tried or environmental(f"{failed.error_type} {failed.error_message}"):
            continue
        found.append(
            {
                "key": key,
                "kind": "dead_letter",
                "work_item_id": item.id,
                "title": item.title,
                "lane": item.lane,
                "exc_type": failed.error_type,
                "message": (failed.error_message or "")[:1500],
                "count": 1,
            }
        )
    return found


def repair_gate(session: Session, schedule: Any = None, scheduled_for: datetime | None = None) -> str | None:
    now = scheduled_for or utc_now()
    runs = _repair_runs(session, since=now - timedelta(days=7))
    if any(run.status in OPEN_RUN_STATUSES for run in _repair_runs(session)):
        return "a repair is still open"
    if len(runs) >= MAX_PER_WEEK:
        return f"{MAX_PER_WEEK} repairs ran this week already"
    if not candidates(session, now=now):
        return "no code fault to repair"
    return None


# --- the workflow --------------------------------------------------------------------------------


def _denied_tools() -> list[str]:
    from tasque2.extensions import registry
    from tasque2.mcp.tools import CORE_TOOLS

    names = {tool.__name__ for tool in [*CORE_TOOLS, *registry().mcp_tools]}
    denied = [f"mcp__tasque2__{name}" for name in sorted(names - READ_ONLY_TOOLS)]
    for command in SHELL_DENIED:
        denied += [f"Bash({command}:*)", f"PowerShell({command}:*)"]
    return denied


def definition() -> dict[str, Any]:
    return {
        "nodes": [
            {
                "key": "fix",
                "kind": "work",
                "title": "Repair: reproduce and fix the fault",
                "worker_kind": "provider.default",
                "task_instruction": FIX_INSTRUCTION,
                "runtime_contract": {"model_profile": "high", "mcp_servers": [], "disallowed_tools": _denied_tools()},
                "max_attempts": 1,
                "tolerate_failure": True,
            },
            {
                "key": "verify",
                "kind": "work",
                "title": "Repair: verify the fix",
                "task_instruction": "Re-run the suites and check the repair's scope.",
                "worker_kind": VERIFY_WORKER,
                "depends_on": ["fix"],
            },
            {
                "key": "approve",
                "kind": "gate",
                "prompt": "Merge this repair?",
                "choices": [MERGE_CHOICE, DISCARD_CHOICE],
                "card_from": "verify.card",
                "depends_on": ["verify"],
            },
            {
                "key": "merge",
                "kind": "work",
                "title": "Repair: merge or discard",
                "task_instruction": "Apply the user's answer.",
                "worker_kind": MERGE_WORKER,
                "depends_on": ["approve"],
            },
        ]
    }


def ensure_definition(session: Session) -> WorkflowDefinition:
    """The bundled repair workflow, registered or brought up to date."""
    from tasque2.workflows import WorkflowService

    wanted = definition()
    existing = session.scalar(select(WorkflowDefinition).where(WorkflowDefinition.name == WORKFLOW_NAME))
    if existing is None:
        return WorkflowService(session).create_definition(name=WORKFLOW_NAME, version="1", definition=wanted)
    if existing.definition != wanted:
        existing.definition = wanted
        session.flush()
    return existing


def _commands(worktrees: list[RepoWorktree]) -> dict[str, str]:
    python = Path(sys.executable).as_posix()
    core = next(tree for tree in worktrees if tree.name == "core")
    commands = {"core_suite": f'cd "{core.path}" && "{python}" -m pytest -q -p no:cacheprovider'}
    for tree in worktrees:
        if tree.name != "core" and (Path(tree.path) / "tests").is_dir():
            commands[f"{tree.name}_suite"] = (
                f'cd "{core.path}" && "{python}" -m pytest -q -p no:cacheprovider extensions/{tree.name}/tests'
            )
    commands["ruff"] = (
        f'cd "{core.path}" && "{python}" -m ruff check <changed files> && "{python}" -m ruff format <changed files>'
    )
    return commands


def _session(work_item: WorkItem) -> Session:
    session = object_session(work_item)
    if session is None:
        raise RuntimeError("a repair worker needs the work item's session")
    return session


def launch_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.tasque_repair_launch``: make the worktrees and start the repair workflow."""
    from tasque2.workflows import WorkflowService

    session = _session(work_item)
    found = candidates(session)
    if not found:
        return {"summary": "Nothing to repair.", "produces": {"silent": True}}
    fault = found[0]
    repair_id = utc_now().strftime("%Y%m%d-%H%M%S")
    worktrees = create_worktrees(repair_id)
    core = next(tree for tree in worktrees if tree.name == "core")
    run = WorkflowService(session).start_run(
        workflow_definition_id=ensure_definition(session).id,
        input={
            "repair_id": repair_id,
            "fault": fault,
            "cwd": core.path,
            "worktrees": [tree.data() for tree in worktrees],
            "commands": _commands(worktrees),
            "lane": "system",
            "memory_namespace": "global",
            "memory_canonical_keys": ["tasque_repair"],
            "context_limits": {"memories": 1, "artifacts": 0},
        },
        discord_thread_id=work_item.discord_thread_id,
    )
    return {
        "summary": f"Repair {repair_id} started for {fault['key']}.",
        "produces": {"silent": True, "repair_run_id": run.id, "fault": fault["key"]},
    }


# --- verify --------------------------------------------------------------------------------------


def _node_output(session: Session, run_id: str, key: str) -> dict[str, Any]:
    node = session.scalar(
        select(WorkflowNode).where(WorkflowNode.workflow_run_id == run_id, WorkflowNode.node_key == key)
    )
    return dict(node.output or {}) if node is not None else {}


def _run_suite(command: list[str], cwd: str) -> tuple[bool, str]:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=TEST_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"{type(exc).__name__}: {exc}"
    tail = (completed.stdout.strip().splitlines() or [""])[-1]
    return completed.returncode == 0, tail[:200]


def _scope_problems(files: list[str]) -> list[str]:
    problems = []
    for name in files:
        parts = name.replace("\\", "/").split("/")
        if not name.endswith(".py"):
            problems.append(f"{name}: only Python code and tests may change")
        elif any(part in ("migrations", "alembic", "data") for part in parts) or parts[-1] == "models.py":
            problems.append(f"{name}: migrations, models and data are outside a repair")
    return problems


def verify(session: Session, run: WorkflowRun) -> dict[str, Any]:
    """The checks a repair must pass before the user sees it; returns ``{ok, card or reasons}``."""
    from tasque2.ops.privacy import scan_range

    worktrees = [RepoWorktree.from_data(item) for item in (run.input or {}).get("worktrees") or []]
    fix = _node_output(session, run.id, "fix")
    reasons: list[str] = []
    if not fix.get("fixed"):
        reasons.append(f"no fix: {fix.get('why_not') or 'the fix run did not finish'}")
    changed = [tree for tree in worktrees if has_commits(tree)]
    if not reasons and not changed:
        reasons.append("no commits on the repair branch")
    for tree in worktrees:
        if uncommitted(tree):
            reasons.append(f"{tree.name}: uncommitted changes left in the worktree")
        reasons += [f"{tree.name}: {problem}" for problem in _scope_problems(committed_files(tree))]
    moved = live_changes(worktrees)
    if moved:
        reasons.append(f"the live checkout changed during the repair: {', '.join(moved)}")
    results: list[str] = []
    if not reasons:
        python = sys.executable
        core = next(tree for tree in worktrees if tree.name == "core")
        suites = [("core", [python, "-m", "pytest", "-q", "-p", "no:cacheprovider"])]
        suites += [
            (tree.name, [python, "-m", "pytest", "-q", "-p", "no:cacheprovider", f"extensions/{tree.name}/tests"])
            for tree in changed
            if tree.name != "core"
        ]
        for name, command in suites:
            ok, tail = _run_suite(command, core.path)
            results.append(f"{name}: {tail}")
            if not ok:
                reasons.append(f"the {name} suite fails: {tail}")
        for tree in changed:
            files = [name for name in committed_files(tree) if name.endswith(".py")]
            if files:
                ok, tail = _run_suite([python, "-m", "ruff", "check", *files], tree.path)
                if not ok:
                    reasons.append(f"ruff fails in {tree.name}: {tail}")
        core_changed = next((tree for tree in changed if tree.name == "core"), None)
        if core_changed is not None:
            findings = scan_range(Path(core_changed.path), core_changed.base, "HEAD")
            if findings:
                reasons.append(f"personal data in the core change ({findings[0].kind} at {findings[0].where})")
    if reasons:
        return {"ok": False, "reasons": reasons, "silent": True}
    fault = (run.input or {}).get("fault") or {}
    files = [f"{tree.name}: {', '.join(committed_files(tree))}" for tree in changed]
    frame = fault.get("frame") or {}
    where = f"{frame.get('file')}:{frame.get('function')}" if frame else fault.get("title") or fault.get("key")
    card = (
        f"**Tasque repair** — {fault.get('exc_type') or 'fault'} in `{where}` "
        f"({fault.get('count', 1)}x over {fault.get('days', 1)} day(s))\n"
        f"Cause: {fix.get('cause') or '-'}\nFix: {fix.get('fix') or '-'}\n"
        f"Changed: {'; '.join(files)}\nTests: {'; '.join(results)}\n"
        f"Merge and restart applies it at the next quiet hour; it rolls back if the daemon does not come up."
    )
    return {"ok": True, "card": card, "silent": True}


def _hide_and_cancel(session: Session, run: WorkflowRun) -> None:
    from tasque2.workflows import WorkflowService

    for item in session.scalars(select(WorkItem).where(WorkItem.workflow_run_id == run.id)).all():
        item.visible = False
    session.flush()
    WorkflowService(session).cancel_run(run.id)


def verify_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.tasque_repair_verify``: verify, or end the run quietly and clean up."""
    session = _session(work_item)
    run = session.get(WorkflowRun, work_item.workflow_run_id)
    if run is None:
        return {"summary": "No repair run.", "produces": {"ok": False, "silent": True}}
    outcome = verify(session, run)
    if not outcome["ok"]:
        worktrees = [RepoWorktree.from_data(item) for item in (run.input or {}).get("worktrees") or []]
        remove_worktrees(worktrees, delete_branches=True)
        _hide_and_cancel(session, run)
        return {"summary": "Repair not offered: " + "; ".join(outcome["reasons"])[:600], "produces": outcome}
    return {"summary": "Repair verified.", "produces": outcome}


# --- merge or discard ----------------------------------------------------------------------------


def merge_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.tasque_repair_merge``: carry out the user's answer; never raises."""
    from tasque2.daemon.restart import RestartBusy, request_restart
    from tasque2.ops.backup import BackupService

    session = _session(work_item)
    run = session.get(WorkflowRun, work_item.workflow_run_id)
    answer = _node_output(session, run.id, "approve").get("answer") if run is not None else None
    worktrees = [RepoWorktree.from_data(item) for item in ((run.input or {}).get("worktrees") or [])] if run else []
    if answer != MERGE_CHOICE:
        remove_worktrees(worktrees, delete_branches=True)
        return {"summary": "Repair discarded.", "produces": {"silent": True, "merged": False}}
    changed = [tree for tree in worktrees if has_commits(tree)]
    remove_worktrees(worktrees, delete_branches=False)  # the branches stay for the switch
    repair_id = str((run.input or {}).get("repair_id"))
    try:
        backup = BackupService().create_backup(
            destination=get_settings().resolved_data_dir / "backups" / f"pre-repair-{repair_id}",
            include_artifacts=False,
        )
    except OSError as exc:
        return {"summary": f"Repair not merged: the database snapshot failed ({exc}).", "produces": {"silent": True}}
    try:
        request_restart(
            reason=f"repair {repair_id}",
            window="quiet",
            switch=[{"repo": tree.live, "ref": tree.branch, "push": tree.name == "core"} for tree in changed],
        )
    except RestartBusy as exc:
        branches = ", ".join(tree.branch for tree in changed)
        return {
            "summary": f"Repair {repair_id} not merged yet ({exc}); it is kept on {branches}.",
            "produces": {"silent": True, "merged": False, "waiting": True},
        }
    return {
        "summary": f"Repair {repair_id} queued: the daemon switches to it and restarts at the next quiet hour.",
        "produces": {"silent": True, "merged": True, "backup": str(backup.backup_dir)},
    }
