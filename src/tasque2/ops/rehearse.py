"""Rehearsal: a change tried on a copy of the live database in its own staging root, before it goes live.

The root is a change's worktree folder (``tasque2.ops.worktree``): the core code at the top, the
extensions under ``extensions/``, the config worktree under ``data/``. A rehearsal puts a copy of the
live database there (the backup API, so it is consistent while the daemon writes), then runs the
change's own code against it (``PYTHONPATH`` at the root's ``src``, every ``TASQUE2_*`` path inside the
root, secrets blanked, ``TASQUE2_REHEARSAL`` set so nothing writes to the outside world):

1. ``migrate``: the change's migrations on the copy (with migrations: then back to where the copy was,
   and forward again, so an undo can step back);
2. ``release-apply`` twice: the doctrine edits, the changed lanes keys, the workflows and the database
   script land once, and the second pass must change nothing; then ``--undo`` and once more, so the
   Undo button is known to work;
3. ``release-check``: the extensions load, the MCP server builds, the schema is current;
4. the context packets the change touches, each under its budget;
5. the change's probe (``changes/<id>/probe.py``), a read-only check it wrote for itself.

The outcome lists each step with what it said; the change ships only when every step passed.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tasque2.config import get_settings
from tasque2.ops.release import ReleasePlan

PACKET_LIMIT_CHARS = 60_000
STEP_TIMEOUT_SECONDS = 15 * 60
SECRET_ENV = ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "CREDENTIAL")


@dataclass
class Rehearsal:
    ok: bool = True
    steps: list[dict[str, Any]] = field(default_factory=list)

    def add(self, name: str, ok: bool, detail: str = "") -> bool:
        self.steps.append({"step": name, "ok": ok, "detail": detail[-600:]})
        self.ok = self.ok and ok
        return ok


def copy_database(source: Path, target: Path) -> Path:
    """A consistent copy of a live SQLite database, the write-ahead log included."""
    target.parent.mkdir(parents=True, exist_ok=True)
    target.unlink(missing_ok=True)
    live = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    copy = sqlite3.connect(str(target))
    try:
        live.backup(copy)
    finally:
        copy.close()
        live.close()
    return target


def _revisions(database: Path) -> list[str]:
    """The schema revisions a database copy is at."""
    try:
        db = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        try:
            return [row[0] for row in db.execute("select version_num from alembic_version")]
        finally:
            db.close()
    except sqlite3.Error:
        return []


def staging_env(root: Path) -> dict[str, str]:
    """The environment for running the root's code against its own data: nothing points at live paths,
    and no secret is passed on."""
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("TASQUE2_") and not any(marker in name.upper() for marker in SECRET_ENV)
    }
    env.update(
        {
            "TASQUE2_DATA_DIR": str(root / "data"),
            "TASQUE2_DB_PATH": str(root / "data" / "tasque2.sqlite3"),
            "TASQUE2_EXTENSIONS_DIR": str(root / "extensions"),
            "TASQUE2_PROJECT_DIR": str(root),
            "TASQUE2_TIMEZONE": get_settings().timezone,
            "TASQUE2_REHEARSAL": "1",
            "TASQUE2_DISCORD_TOKEN": "",
            "TASQUE2_TELEMETRY": "false",
            "PYTHONPATH": str(root / "src"),
            "PYTHONIOENCODING": "utf-8",
        }
    )
    return env


def python_for(root: Path) -> str:
    """The root's own environment when it has one (a change that moved the lockfile), else this one."""
    own = root / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    return str(own) if own.is_file() else sys.executable


def run(root: Path, args: list[str], *, timeout: float = STEP_TIMEOUT_SECONDS) -> tuple[bool, str, str]:
    """``python -m tasque2 <args>`` in the root, against its own data."""
    try:
        done = subprocess.run(
            [python_for(root), "-m", "tasque2", *args],
            cwd=str(root),
            env=staging_env(root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, "", f"{type(exc).__name__}: {exc}"
    return done.returncode == 0, done.stdout, done.stderr


def _last_json(text: str) -> dict[str, Any] | None:
    """The last JSON object a command printed, on one line or indented over many (it starts a line)."""
    decoder = json.JSONDecoder()
    offset, starts = 0, []
    for line in text.splitlines(keepends=True):
        if line.startswith("{"):
            starts.append(offset)
        offset += len(line)
    for start in reversed(starts):
        try:
            value, _end = decoder.raw_decode(text[start:])
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _still_changing(report: dict[str, Any] | None) -> list[str]:
    """What a second release-apply still changed (it must change nothing)."""
    if report is None:
        return ["no report"]
    moved = [entry["document"] for entry in report.get("doctrine") or [] if entry.get("status") != "unchanged"]
    moved += [f"lanes {entry.get('target')} {entry.get('field')}" for entry in report.get("lanes") or []]
    return moved


def rehearse(
    root: Path,
    plan: ReleasePlan,
    *,
    packets: list[tuple[str, str]] | None = None,
    probe: str | None = None,
    database: Path | None = None,
) -> Rehearsal:
    """Every step on a fresh copy of the live database in ``root``; ``packets`` are ("schedule"|"thread", name)."""
    outcome = Rehearsal()
    live = database or get_settings().database_path
    try:
        copy_database(live, root / "data" / "tasque2.sqlite3")
    except sqlite3.Error as exc:
        outcome.add("copy the database", False, str(exc))
        return outcome
    before = _revisions(root / "data" / "tasque2.sqlite3")
    ok, out, err = run(root, ["migrate"])
    if not outcome.add("migrate", ok, err or out):
        return outcome
    if plan.migrations and len(before) == 1:
        ok, out, err = run(root, ["migrate", "--to", before[0]])
        if ok:
            ok, out, err = run(root, ["migrate"])
        if not outcome.add("migrations step back and forward", ok, err or out):
            return outcome
    plan_file = root / "data" / "runtime" / "releases" / f"{plan.id}.json"
    plan_file.parent.mkdir(parents=True, exist_ok=True)
    plan_file.write_text(json.dumps(plan.to_dict(), indent=1), encoding="utf-8")
    ok, out, err = run(root, ["release-apply", "--plan", str(plan_file)])
    if not outcome.add("release-apply", ok, err or out):
        return outcome
    ok, out, err = run(root, ["release-apply", "--plan", str(plan_file)])
    second = _last_json(out)
    moved = _still_changing(second) if ok else ["it failed"]
    outcome.add("release-apply again changes nothing", ok and not moved, ", ".join(moved) or (err or out))
    ok, out, err = run(root, ["release-apply", "--plan", str(plan_file), "--undo"])
    if outcome.add("release-apply --undo", ok, err or out):
        ok, out, err = run(root, ["release-apply", "--plan", str(plan_file)])
        outcome.add("release-apply after the undo", ok, err or out)
    ok, out, err = run(root, ["release-check"])
    outcome.add("release-check", ok, out or err)
    for kind, name in packets or []:
        ok, out, err = run(root, ["packet", f"--{kind}", name, "--json"])
        size = (_last_json(out) or {}).get("prompt_chars") if ok else None
        fits = size is not None and size <= PACKET_LIMIT_CHARS
        outcome.add(f"packet {kind} {name}", fits, f"{size} chars" if size is not None else (err or out))
    if probe:
        script = root / "data" / probe
        try:
            done = subprocess.run(
                [python_for(root), str(script)],
                cwd=str(root),
                env=staging_env(root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=STEP_TIMEOUT_SECONDS,
            )
            outcome.add("probe", done.returncode == 0, done.stdout or done.stderr)
        except (OSError, subprocess.TimeoutExpired) as exc:
            outcome.add("probe", False, f"{type(exc).__name__}: {exc}")
    return outcome
