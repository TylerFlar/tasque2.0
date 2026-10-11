"""Verify a built Workshop change: the checks no model can talk its way past, then a rehearsal.

In order, any failure ending the change:

1. the build finished, committed something, and left nothing uncommitted in its worktrees;
2. the live code checkouts were left alone while it ran;
3. a fix that changes code has a test among its files; the tier again, from the files the commits really
   touch (``policy.final_tier``: it only rises), and whether the build needs one tap after all
   (``policy.late_tap``: it touches what its plan did not name, or the Workshop was paused meanwhile);
4. with code: the core suite and each extension's suite, and ruff on every changed Python file;
5. the core commits carry nothing personal (``tasque2.ops.privacy``, the core repository is public),
   and no commit anywhere carries a secret;
6. a rehearsal (``tasque2.ops.rehearse``) on a fresh copy of the live database: migrations, the
   release applied twice, undone and applied again, the release check, the context packets the change
   touches, and its probe.

What passes becomes the release plan, and, for the rare tap, a card in plain words: what changes, and why
it waits.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from tasque2.models import WorkflowRun
from tasque2.workshop import policy

TEST_TIMEOUT_SECONDS = 30 * 60
PACKET_KINDS = ("thread", "schedule")
CARD_LINES = 4


def _run_command(command: list[str], cwd: str) -> tuple[bool, str]:
    try:
        done = subprocess.run(
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
    lines = (done.stdout.strip() or done.stderr.strip()).splitlines()
    return done.returncode == 0, (lines[-1] if lines else "")[:200]


def _is_test(name: str) -> bool:
    parts = name.replace("\\", "/").split("/")
    return "tests" in parts[:-1] or parts[-1].startswith("test_")


def _added_lines(tree: Any) -> list[str]:
    from tasque2.ops.worktree import git

    diff = git(tree.path, "diff", "--unified=0", "--no-color", f"{tree.base}..HEAD")
    return [line[1:] for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]


def _packets(value: Any) -> list[tuple[str, str]]:
    found = []
    for item in value if isinstance(value, list) else []:
        if isinstance(item, (list, tuple)) and len(item) == 2 and str(item[0]) in PACKET_KINDS and str(item[1]):
            found.append((str(item[0]), str(item[1])))
        elif isinstance(item, dict) and str(item.get("kind")) in PACKET_KINDS and item.get("name"):
            found.append((str(item["kind"]), str(item["name"])))
    return found[:6]


def release_plan(
    run: WorkflowRun,
    changed: list[Any],
    files: dict[str, list[str]],
    build: dict[str, Any],
    tier: str,
    *,
    quiet: bool = False,
):
    """The release plan for what the change committed."""
    from tasque2.ops.release import ReleasePlan

    given = run.input or {}
    change_id = str(given.get("change_id"))
    code = [tree for tree in changed if tree.name != "data"]
    data = next((tree for tree in changed if tree.name == "data"), None)
    every = [name for names in files.values() for name in names]
    data_files = set(files.get("data") or [])
    db_script = str(build.get("db_script") or "") or None
    return ReleasePlan(
        id=change_id,
        change_id=change_id,
        title=str(build.get("title") or "")[:80],
        repos=[
            {
                "name": tree.name,
                "live": tree.live,
                "branch": tree.branch,
                "base": tree.base,
                "push": tree.name == "core",
            }
            for tree in code
        ],
        data={"branch": data.branch, "base": data.base} if data is not None else None,
        lock_changed="uv.lock" in (files.get("core") or []),
        migrations=any("/migrations/" in f"/{name}" or name.startswith("alembic/") for name in every),
        db_script=db_script if db_script in data_files else None,
        tier=tier,
        origin=str(given.get("origin") or ""),
        summary="\n".join(str(line).strip() for line in build.get("changes") or [] if str(line).strip())[:1500],
        run_id=run.id,
        thread_id=run.discord_thread_id,
        quiet=quiet,
    )


def verify(session: Session, run: WorkflowRun) -> dict[str, Any]:
    """The checks a built change must pass; ``{ok, reasons}`` or ``{ok, tier, why, tap, release_plan, card, ...}``
    (``tap``: why it waits for one tap after all, or None)."""
    from tasque2.ops.datarepo import SECRET_PATTERN
    from tasque2.ops.privacy import scan_range
    from tasque2.ops.rehearse import rehearse
    from tasque2.ops.worktree import committed_files, has_commits, live_changes, uncommitted
    from tasque2.workshop.pipeline import heard, node_output, paused, trees_of

    given = run.input or {}
    classify = node_output(session, run.id, "classify")
    build = node_output(session, run.id, "build")
    trees = trees_of(run)
    reasons: list[str] = []
    if build.get("tolerated_failure"):
        reasons.append("the build run did not finish")
    elif not build.get("done"):
        reasons.append(f"the build stopped: {build.get('why_not') or 'it said it was not done'}")
    changed = [tree for tree in trees if has_commits(tree)]
    if not reasons and not changed:
        reasons.append("the build committed nothing")
    for tree in trees:
        left = uncommitted(tree)
        if left:
            reasons.append(f"{tree.name}: left uncommitted: {', '.join(left[:3])}")
    moved = live_changes([tree for tree in trees if tree.name != "data"])
    if moved:
        reasons.append(f"the live checkout changed while the change was built: {', '.join(moved)}")
    if reasons:
        return {"ok": False, "reasons": reasons}

    files = {tree.name: committed_files(tree) for tree in changed}
    origin, kind = str(given.get("origin") or "model"), str(classify.get("kind") or "feature")
    code_changed = any(tree.name != "data" for tree in changed)
    if kind == "fix" and code_changed and not any(_is_test(name) for names in files.values() for name in names):
        return {"ok": False, "reasons": ["a fix that changes code needs a test that reproduces it"]}
    tier, why = policy.final_tier(str(classify.get("tier") or policy.PLAN), origin=origin, kind=kind, files=files)
    tap = policy.late_tap(
        classify.get("touches") or [], files, paused_since_plan=bool(paused()) and not classify.get("paused")
    )

    checks: list[str] = []
    python = sys.executable
    root = next(tree for tree in trees if tree.name == "core").path
    if any(tree.name != "data" for tree in changed):
        suites = [("core", [python, "-m", "pytest", "-q", "-p", "no:cacheprovider"])]
        suites += [
            (tree.name, [python, "-m", "pytest", "-q", "-p", "no:cacheprovider", f"extensions/{tree.name}/tests"])
            for tree in trees
            if tree.name not in ("core", "data") and (Path(tree.path) / "tests").is_dir()
        ]
        for name, command in suites:
            ok, tail = _run_command(command, root)
            checks.append(f"{name} suite: {tail}")
            if not ok:
                reasons.append(f"the {name} suite fails: {tail}")
        for tree in changed:
            python_files = [
                name for name in files[tree.name] if name.endswith(".py") and (Path(tree.path) / name).exists()
            ]
            if not python_files:
                continue
            for args, label in ((["check"], "ruff"), (["format", "--check"], "ruff format")):
                ok, tail = _run_command([python, "-m", "ruff", *args, *python_files], tree.path)
                if not ok:
                    reasons.append(f"{label} fails in {tree.name}: {tail}")
        checks.append("ruff ok" if not any("ruff" in reason for reason in reasons) else "ruff fails")
    core = next((tree for tree in changed if tree.name == "core"), None)
    if core is not None:
        findings = scan_range(Path(core.path), core.base, "HEAD")
        if findings:
            reasons.append(f"personal data in the core change ({findings[0].kind} at {findings[0].where})")
        checks.append("privacy ok" if not findings else "privacy fails")
    leaked = [tree.name for tree in changed if any(SECRET_PATTERN.search(line) for line in _added_lines(tree))]
    if leaked:
        reasons.append(f"something that reads as a secret in {', '.join(leaked)}")
    checks.append("secrets ok" if not leaked else "secrets fail")
    if reasons:
        return {"ok": False, "reasons": reasons}

    quiet = not heard(session, run)
    plan = release_plan(run, changed, files, {**build, "title": classify.get("title")}, tier, quiet=quiet)
    probe = str(build.get("probe") or "") or None
    if probe and probe not in (files.get("data") or []):
        probe = None
    rehearsal = rehearse(Path(root), plan, packets=_packets(build.get("packets")), probe=probe)
    passed = sum(1 for step in rehearsal.steps if step["ok"])
    checks.append(f"rehearsal {passed}/{len(rehearsal.steps)} steps")
    if not rehearsal.ok:
        failed = next(step for step in rehearsal.steps if not step["ok"])
        return {"ok": False, "reasons": [f"the rehearsal failed at {failed['step']}: {failed['detail'][-300:]}"]}

    return {
        "ok": True,
        "tier": tier,
        "why": why,
        "tap": tap,
        "release_plan": plan.to_dict(),
        "card": tap_card(str(classify.get("title") or ""), plan, tap, filed=origin == "workshop") if tap else "",
        "checks": checks,
        "rehearsal": rehearsal.steps,
        "files": files,
    }


def tap_card(title: str, plan: Any, tap: str, *, filed: bool) -> str:
    """The card for the rare tap, in plain words: what changes, and why it waits."""
    lines = [f"**Workshop: {title}** needs your tap: {tap}."]
    if filed:
        lines.append("The Workshop filed this itself.")
    lines += [f"- {line}" for line in plan.summary.splitlines()[:CARD_LINES]]
    when = "when Tasque restarts next" if plan.cold else "at once"
    lines.append(f"Ship applies it {when}; Discard drops it. Anything you type goes with your answer.")
    return "\n".join(lines)
