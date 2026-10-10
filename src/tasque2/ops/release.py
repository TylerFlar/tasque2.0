"""Releases: a Workshop change made live, hot or across a restart, and undone.

A release plan (``data/runtime/releases/<id>.json``) names what a change touched:

- the code repositories and the branches the change built on (core and extensions): code needs a
  restart, so a cold release hands the plan to ``tasque2.daemon.respawn``, which switches the code
  between the old daemon's exit and the new one's start;
- the config repository's branch (``tasque2.ops.datarepo``: templates, workflows, the lanes manifest,
  doctrine as files): merged by fast-forward after a snapshot of the live config, and refused when a
  file it changes was changed live since the change began;
- what the config branch means beyond its files: doctrine edits, applied where the live text still
  reads as the change saw it (``tasque2.ops.doctrine.apply_held``); the lanes manifest's changed keys,
  only those (a schedule the user paused stays paused unless the change is about it); changed workflow
  files, loaded; and the change's database script (``changes/<id>/db_script.py`` with ``apply(session)``
  and ``revert(session)``), applied once.

``apply_config`` does the last part and is idempotent: a rehearsal runs it twice on a database copy and
the second pass must change nothing. A hot release (no code) is merged and applied at once; a cold one
is queued for the next idle moment, and the respawn runs ``tasque2 release-apply`` with the new code.

Each plan file is also the release's record: ``outcome`` and ``released_at`` say how it went (written at
once for a hot release, by the respawn for a cold one), ``undone_at`` when it was reversed. ``undo_release``
reverses one: a hot release at once (what the change meant reversed, then its config commits reverted); a
cold one as a new release of revert commits, queued like any other, its migrations downgraded by the code
that has them before the switch.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import utc_now

LANES_FILE = "lanes.json"
LANE_SECTIONS = ("schedules", "workflows", "threads")


@dataclass
class ReleasePlan:
    id: str
    change_id: str | None
    title: str
    repos: list[dict[str, Any]] = field(default_factory=list)  # {name, live, branch, base, push}
    data: dict[str, Any] | None = None  # {branch, base, head}
    lock_changed: bool = False
    migrations: bool = False
    db_script: str | None = None  # a path in the config repository
    after_start: list[dict[str, Any]] = field(default_factory=list)
    window: str = "now"
    created_at: str = field(default_factory=lambda: utc_now().isoformat())
    # The record: who asked, how it went, whether it was reversed.
    tier: str = ""
    origin: str = ""
    summary: str = ""
    run_id: str | None = None
    thread_id: str | None = None
    undo_of: str | None = None
    db_script_ref: str | None = None  # the commit to load the script from (an undo's comes from the original)
    db_script_undo: bool = False  # run the script's revert (an undo) rather than its apply
    pre_switch: list[list[str]] = field(default_factory=list)  # tasque2 commands the old code runs first
    revision_before: list[str] = field(default_factory=list)  # the schema before release-apply upgraded it
    outcome: dict[str, Any] | None = None
    released_at: str | None = None
    undone_at: str | None = None
    announced: bool = False

    @property
    def cold(self) -> bool:
        return bool(self.repos)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ReleasePlan:
        return cls(**{name: data[name] for name in cls.__dataclass_fields__ if name in data})


def releases_dir() -> Path:
    return get_settings().resolved_data_dir / "runtime" / "releases"


def files_dir() -> Path:
    """Where a release keeps the files it loads (workflow files, database scripts), apart from the plans."""
    return releases_dir() / "files"


def plan_path(release_id: str) -> Path:
    return releases_dir() / f"{release_id}.json"


def save_plan(plan: ReleasePlan) -> Path:
    path = plan_path(plan.id)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(plan.to_dict(), indent=1), encoding="utf-8")
    temporary.replace(path)
    return path


def load_plan(path: Path | str) -> ReleasePlan:
    return ReleasePlan.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


def find_plan(release_id: str) -> ReleasePlan | None:
    try:
        return load_plan(plan_path(release_id))
    except (OSError, ValueError, TypeError):
        return None


def history() -> list[ReleasePlan]:
    """Every release plan on record, oldest first."""
    plans = []
    for path in releases_dir().glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(data, dict) and data.get("id") and "repos" in data:
            plans.append(ReleasePlan.from_dict(data))
    return sorted(plans, key=lambda plan: plan.created_at)


def state(plan: ReleasePlan) -> str:
    """queued, live, failed, canceled or undone."""
    if plan.undone_at:
        return "undone"
    if plan.outcome is None:
        return "queued"
    if plan.outcome.get("canceled"):
        return "canceled"
    return "live" if plan.released_at and plan.outcome.get("ok") else "failed"


def _git(repo: str | Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, encoding="utf-8", errors="replace"
    )


# --- what a config branch means -------------------------------------------------------------------


def config_root() -> Path:
    return get_settings().resolved_data_dir


def lanes_changes(base_text: str | None, head_text: str | None) -> dict[str, Any]:
    """The manifest's keys whose settings differ between two versions (a removed key is not undone)."""
    base = json.loads(base_text) if base_text else {}
    head = json.loads(head_text) if head_text else {}
    changed: dict[str, Any] = {}
    for section in LANE_SECTIONS:
        old, new = base.get(section) or {}, head.get(section) or {}
        keys = {key: value for key, value in new.items() if old.get(key) != value}
        if keys:
            changed[section] = keys
    return changed


def config_edits(plan: ReleasePlan, *, root: Path | None = None) -> dict[str, Any]:
    """What the plan's config branch changes: files, doctrine edits, lanes keys, workflow files."""
    from tasque2.ops.datarepo import changed_files, file_at
    from tasque2.ops.doctrine import held_edits

    if not plan.data:
        return {"files": [], "doctrine": [], "lanes": {}, "workflows": []}
    repo = root or config_root()
    base, head = plan.data["base"], plan.data.get("head") or plan.data["branch"]
    files = changed_files(base, head, root=repo)
    lanes: dict[str, Any] = {}
    if LANES_FILE in files:
        before, after = file_at(base, LANES_FILE, root=repo), file_at(head, LANES_FILE, root=repo)
        lanes = lanes_changes(before.decode() if before else None, after.decode() if after else None)
    return {
        "files": files,
        "doctrine": held_edits(repo, base, head),
        "lanes": lanes,
        "workflows": [name for name in files if name.startswith("workflows/") and file_at(head, name, root=repo)],
    }


def _load_script(repo: Path, ref: str, name: str) -> Any:
    from tasque2.ops.datarepo import file_at

    source = file_at(ref, name, root=repo)
    if source is None:
        raise ValueError(f"the database script {name} is not in {ref}")
    target = files_dir() / name.replace("/", "_")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(source)
    spec = importlib.util.spec_from_file_location(f"tasque_release_{target.stem}", target)
    if spec is None or spec.loader is None:
        raise ValueError(f"the database script {name} cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def apply_config(
    session: Session, plan: ReleasePlan, *, root: Path | None = None, undo: bool = False
) -> dict[str, Any]:
    """Doctrine, the changed lanes keys, workflows and the database script: idempotent; ``undo`` reverses."""
    from tasque2.ops.datarepo import file_at
    from tasque2.ops.doctrine import apply_held, reversed_edits
    from tasque2.ops.lanes import apply_lane_tiers
    from tasque2.workflows import WorkflowService

    repo = root or config_root()
    edits = config_edits(plan, root=repo)
    report: dict[str, Any] = {"doctrine": [], "lanes": [], "workflows": [], "db_script": None}
    doctrine = reversed_edits(edits["doctrine"]) if undo else edits["doctrine"]
    report["doctrine"] = [
        {"document": f"{c.namespace}/{c.canonical_key}", "status": c.status, "detail": c.detail}
        for c in apply_held(session, doctrine)
    ]
    if edits["lanes"] and plan.data:
        ref = plan.data["base"] if undo else plan.data.get("head") or plan.data["branch"]
        text = file_at(ref, LANES_FILE, root=repo)
        manifest = json.loads(text.decode()) if text else {}
        wanted = {
            section: {
                key: (manifest.get(section) or {}).get(key) for key in keys if (manifest.get(section) or {}).get(key)
            }
            for section, keys in edits["lanes"].items()
        }
        report["lanes"] = [
            {"target": c.target, "field": c.field, "old": c.old, "new": c.new}
            for c in apply_lane_tiers(session, wanted)
            if c.changed
        ]
    if edits["workflows"] and plan.data:
        ref = plan.data["base"] if undo else plan.data.get("head") or plan.data["branch"]
        service = WorkflowService(session)
        for name in edits["workflows"]:
            text = file_at(ref, name, root=repo)
            if text is None:
                continue
            target = files_dir() / Path(name).name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(text)
            definition = service.load_definition_file(target)
            report["workflows"].append({"file": name, "definition": definition.name})
    if plan.db_script and plan.data:
        ref = plan.db_script_ref or plan.data.get("head") or plan.data["branch"]
        module = _load_script(repo, ref, plan.db_script)
        report["db_script"] = (module.revert if undo != plan.db_script_undo else module.apply)(session)
    session.flush()
    return report


# --- checks, and a hot release ----------------------------------------------------------------------


def preflight(session: Session, plan: ReleasePlan) -> list[str]:
    """Why the plan cannot go live as it stands: live code moved past a branch's base, a config file or a
    doctrine document changed live since the change began."""
    from tasque2.ops.datarepo import conflicts
    from tasque2.ops.doctrine import held_conflicts

    problems = []
    for repo in plan.repos:
        live_head = _git(repo["live"], "rev-parse", "HEAD").stdout.strip()
        if _git(repo["live"], "merge-base", "--is-ancestor", live_head, repo["branch"]).returncode != 0:
            problems.append(f"{repo['name']}: the live code moved on since the change began; it must be rebuilt")
    if plan.data:
        edits = config_edits(plan)
        files = [name for name in edits["files"] if not name.startswith(("doctrine/", "changes/"))]
        problems += [
            f"config {name}: changed live since the change began" for name in conflicts(plan.data["base"], files)
        ]
        problems += [f"doctrine {line}" for line in held_conflicts(session, edits["doctrine"])]
    return problems


def merge_config(plan: ReleasePlan) -> str | None:
    """Commit the live config's own edits, then fast-forward it to the change's branch (carried onto the
    live edits first when there are any: preflight proved they touch none of the change's files). The
    plan's ``base`` and ``head`` then bracket the change's own commits, for an undo."""
    from tasque2.ops.datarepo import head, merge, snapshot

    if not plan.data:
        return None
    repo = config_root()
    snapshot(f"before release {plan.id}", doctrine=False)
    onto = head(repo)
    branch = plan.data["branch"]
    if _git(repo, "merge-base", "--is-ancestor", onto, branch).returncode != 0:
        rebase_branch(repo, branch, onto=onto, base=plan.data["base"])
        plan.data["base"] = onto
    plan.data["head"] = merge(branch)
    return plan.data["head"]


def rebase_branch(repo: Path, branch: str, *, onto: str, base: str) -> None:
    """Carry ``branch``'s commits since ``base`` onto ``onto`` in a temporary detached worktree, so the
    live working tree never shows the branch and the branch may still be checked out elsewhere (the
    change's own worktree); the branch then points at the carried commits."""
    import shutil
    import tempfile

    place = Path(tempfile.mkdtemp(prefix="tasque-rebase-"))
    shutil.rmtree(place, ignore_errors=True)
    added = _git(repo, "worktree", "add", "-q", "--detach", str(place), branch)
    if added.returncode != 0:
        raise RuntimeError(f"cannot check out {branch} to carry it over: {added.stderr.strip()[:300]}")
    try:
        result = _git(place, "rebase", "-q", "--onto", onto, base)
        if result.returncode != 0:
            _git(place, "rebase", "--abort")
            raise RuntimeError(f"the change no longer applies cleanly: {result.stderr.strip()[:300]}")
        carried = _git(place, "rev-parse", "HEAD").stdout.strip()
        moved = _git(repo, "update-ref", f"refs/heads/{branch}", carried)
        if moved.returncode != 0:
            raise RuntimeError(f"cannot move {branch} to the carried commits: {moved.stderr.strip()[:300]}")
    finally:
        _git(repo, "worktree", "remove", "--force", str(place))
        shutil.rmtree(place, ignore_errors=True)


def release_hot(session: Session, plan: ReleasePlan) -> dict[str, Any]:
    """A change with no code: checked, merged and applied now."""
    problems = preflight(session, plan)
    if problems:
        return {"ok": False, "problems": problems}
    head = merge_config(plan)
    report = apply_config(session, plan)
    record = {"ok": True, "kind": "hot", "config_head": head, "report": report, "at": utc_now().isoformat()}
    plan.outcome, plan.released_at = json.loads(json.dumps(record, default=str)), record["at"]
    save_plan(plan)
    return record


def queue_cold(plan: ReleasePlan) -> Path:
    """A change with code: the plan goes to the respawn at the next window (``plan.window``). Raises
    ``RestartBusy`` while another release waits to go live (it would be replaced)."""
    from tasque2.daemon.restart import request_restart

    path = save_plan(plan)
    try:
        request_restart(
            reason=f"release {plan.id}: {plan.title}"[:200],
            window=plan.window,
            switch=[
                {"repo": repo["live"], "ref": repo["branch"], "push": repo.get("push", False)} for repo in plan.repos
            ],
            release=str(path),
        )
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


# --- undo ------------------------------------------------------------------------------------------


class UndoError(RuntimeError):
    """A release cannot be reversed as asked."""


def latest_undoable() -> ReleasePlan | None:
    """The newest release that is live or waiting to go live, not itself an undo."""
    for plan in reversed(history()):
        if plan.undo_of is None and state(plan) in ("live", "queued"):
            return plan
    return None


def revert_branch(repo: Path | str, branch: str, *, before: str, after: str) -> str:
    """A branch off the repository's HEAD with revert commits for ``before..after``, made in a temporary
    worktree so the live tree never changes; returns the HEAD it starts from."""
    import shutil
    import tempfile

    base = _git(repo, "rev-parse", "HEAD").stdout.strip()
    place = Path(tempfile.mkdtemp(prefix="tasque-undo-"))
    shutil.rmtree(place, ignore_errors=True)
    added = _git(repo, "worktree", "add", "-q", "-b", branch, str(place), base)
    if added.returncode != 0:
        raise UndoError(f"cannot make the undo branch: {added.stderr.strip()[:300]}")
    try:
        done = _git(place, "revert", "--no-edit", f"{before}..{after}")
        if done.returncode != 0:
            _git(place, "revert", "--abort")
            raise UndoError("later changes touched the same lines; it cannot be reversed cleanly")
    except UndoError:
        _git(repo, "worktree", "remove", "--force", str(place))
        _git(repo, "branch", "-D", branch)
        shutil.rmtree(place, ignore_errors=True)
        raise
    _git(repo, "worktree", "remove", "--force", str(place))
    shutil.rmtree(place, ignore_errors=True)
    return base


def cancel_queued(plan: ReleasePlan) -> None:
    """A cold release that has not gone live yet: its restart request is withdrawn."""
    from tasque2.daemon.restart import clear_request, read_request

    pending = read_request() or {}
    if pending.get("release") and Path(pending["release"]).resolve() == plan_path(plan.id).resolve():
        clear_request()
    plan.outcome = {"ok": False, "canceled": True, "at": utc_now().isoformat()}
    save_plan(plan)


def undo_release(session: Session, plan: ReleasePlan) -> dict[str, Any]:
    """Reverse a release: queued, it is withdrawn; hot, its meaning is reversed and its config commits
    reverted now; cold, a release of revert commits is queued (``kind`` says which)."""
    from tasque2.ops.datarepo import snapshot

    status = state(plan)
    if status == "queued":
        cancel_queued(plan)
        return {"ok": True, "kind": "withdrawn"}
    if status != "live":
        raise UndoError(f"release {plan.id} is {status}, not live")
    if not plan.cold:
        report = apply_config(session, plan, undo=True)
        blocked = [entry["document"] for entry in report["doctrine"] if entry["status"] in ("conflict", "over_budget")]
        if plan.data and plan.data.get("head"):
            repo = config_root()
            snapshot(f"before undoing {plan.id}", doctrine=False)
            done = _git(repo, "revert", "--no-edit", f"{plan.data['base']}..{plan.data['head']}")
            if done.returncode != 0:
                _git(repo, "revert", "--abort")
                blocked.append("config files (changed again since)")
        plan.undone_at = utc_now().isoformat()
        save_plan(plan)
        return {"ok": not blocked, "kind": "hot", "blocked": blocked, "report": report}
    code = (plan.outcome or {}).get("code") or {}
    repos = []
    for repo in plan.repos:
        moved = code.get(repo["live"]) or {}
        if not moved.get("before") or not moved.get("after"):
            raise UndoError(f"the release record has no commits for {repo['name']}")
        branch = f"undo/{plan.id}"
        base = revert_branch(repo["live"], branch, before=moved["before"], after=moved["after"])
        repos.append({**repo, "branch": branch, "base": base})
    data = None
    if plan.data and plan.data.get("head"):
        repo = config_root()
        snapshot(f"before undoing {plan.id}", doctrine=False)
        base = revert_branch(repo, f"undo/{plan.id}", before=plan.data["base"], after=plan.data["head"])
        data = {"branch": f"undo/{plan.id}", "base": base}
    undo = ReleasePlan(
        id=f"undo-{plan.id}",
        change_id=plan.change_id,
        title=f"Undo: {plan.title}",
        repos=repos,
        data=data,
        lock_changed=plan.lock_changed,
        migrations=False,
        db_script=plan.db_script,
        db_script_ref=(plan.data or {}).get("head"),
        db_script_undo=True,
        pre_switch=[["migrate", "--to", *plan.revision_before[:1]]] if plan.migrations and plan.revision_before else [],
        window="now",
        tier=plan.tier,
        thread_id=plan.thread_id,
        run_id=plan.run_id,
        undo_of=plan.id,
        summary=f"Reverses {plan.title}.",
    )
    queue_cold(undo)
    return {"ok": True, "kind": "cold", "release_id": undo.id}


def mark_undone(plan: ReleasePlan) -> None:
    """An undo release went live: its original is recorded as reversed."""
    if plan.undo_of:
        original = find_plan(plan.undo_of)
        if original is not None and not original.undone_at:
            original.undone_at = plan.released_at or utc_now().isoformat()
            save_plan(original)


def release_check() -> list[str]:
    """What the new code must do before the daemon counts as healthy: extensions load, the MCP server
    builds, the schema is current."""
    problems = []
    try:
        from tasque2.extensions import registry

        registry()
    except Exception as exc:  # noqa: BLE001 - any failure here is the answer
        problems.append(f"extensions: {type(exc).__name__}: {exc}")
    try:
        from tasque2.mcp.server import build_server

        build_server()
    except Exception as exc:  # noqa: BLE001
        problems.append(f"MCP server: {type(exc).__name__}: {exc}")
    try:
        from tasque2.migrations import schema_status

        if not schema_status().is_current:
            problems.append("the database schema is not current")
    except Exception as exc:  # noqa: BLE001
        problems.append(f"schema: {type(exc).__name__}: {exc}")
    return problems
