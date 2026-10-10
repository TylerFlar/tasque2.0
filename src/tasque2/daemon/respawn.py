"""Start the daemon again after a requested restart: switch, start, check, roll back if needed.

Run by the daemon itself (``tasque2.daemon.restart.spawn_respawn``) just before it exits. This
module imports only the standard library: it must keep working when the code it is switching to
is broken. Steps:

1. wait for the old daemon's process to exit;
2. fast-forward each repository the request names to its ref (a failure undoes the ones done);
3. rotate the daemon logs and start the daemon hidden;
4. wait up to two minutes for a new process that ticks and has Discord up (or reports it is not
   configured);
5. if it never gets there: stop it, reset each switched repository to where it was, start the
   previous code again, and append a fault to the ledger;
6. if it is healthy: push the repositories the request asked to push, and delete the merged refs.

A request may also carry a release plan (``tasque2.ops.release``): then, before the code switch, the
database is snapshotted, the plan's ``pre_switch`` commands run with the old code (an undo's downgrade),
and the config repository is fast-forwarded to the change's branch (its own live edits committed first,
the change carried onto them when needed); after it ``uv sync`` runs when the lockfile changed and the
new code's ``tasque2 release-apply`` lands the migrations, doctrine, lanes, workflows and database
script. Any failure, then or at the health check, resets the code and the config, re-syncs, and
restores the database snapshot before the previous code starts again.

The outcome goes to ``daemon.restart.result.json``, and a release's into its plan (``outcome``, with the
commits each repository moved between, and ``released_at`` when it went live).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REQUEST_FILE = "daemon.restart.json"
RESULT_FILE = "daemon.restart.result.json"
EXIT_WAIT_SECONDS = 15 * 60
HEALTH_SECONDS = 120
FRESH_TICK_SECONDS = 60
HEALTHY_MARKERS = ("Discord connected", "Discord is not configured", "Discord disabled")


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def stop_tree(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, check=False)
    else:
        try:
            os.killpg(os.getpgid(pid), 9)
        except (ProcessLookupError, PermissionError):
            pass


def git(repo: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True, check=False)


def head_of(repo: str) -> str:
    return git(repo, "rev-parse", "HEAD").stdout.strip()


def rotate_logs(data: Path) -> None:
    for name in ("out", "err"):
        log = data / f"daemon.{name}.log"
        if log.exists():
            try:
                os.replace(log, data / f"daemon.{name}.prev.log")
            except OSError:
                pass


def default_daemon_command(project: Path) -> list[str]:
    scripts = project / ".venv" / ("Scripts" if os.name == "nt" else "bin")
    executable = scripts / ("tasque2.exe" if os.name == "nt" else "tasque2")
    return [str(executable), "daemon"]


def start_daemon(command: list[str], project: Path, data: Path) -> subprocess.Popen:
    out = (data / "daemon.out.log").open("a", encoding="utf-8")
    err = (data / "daemon.err.log").open("a", encoding="utf-8")
    options: dict[str, Any] = {"stdout": out, "stderr": err, "stdin": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        options["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options["start_new_session"] = True
    return subprocess.Popen(command, cwd=str(project), **options)


def healthy(data: Path, *, old_pid: int, timeout: float = HEALTH_SECONDS) -> bool:
    """A new daemon process that ticks, with Discord up or reported as not configured."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(2)
        try:
            state = json.loads((data / "daemon.state.json").read_text(encoding="utf-8"))
            last_tick = datetime.fromisoformat(state["last_tick_at"])
        except (OSError, ValueError, KeyError, TypeError):
            continue
        pid = int(state.get("pid") or 0)
        if pid == old_pid or not pid_alive(pid):
            continue
        if (datetime.now(UTC) - last_tick).total_seconds() > FRESH_TICK_SECONDS:
            continue
        try:
            log = (data / "daemon.err.log").read_text(encoding="utf-8", errors="replace")
        except OSError:
            log = ""
        if any(marker in log for marker in HEALTHY_MARKERS):
            return True
    return False


def record_fault(data: Path, message: str) -> None:
    entry = {
        "at": now_iso(),
        "logger": "tasque2.daemon.respawn",
        "level": "ERROR",
        "message": message[:600],
        "exc_type": "RestartFailed",
        "exc_message": message[:600],
        "frame": {"file": "tasque2/daemon/respawn.py", "line": 0, "function": "main"},
        "signature": hashlib.sha1(b"tasque2.daemon.respawn|RestartFailed").hexdigest()[:12],
        "traceback": None,
    }
    path = data / "runtime" / "faults.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry) + "\n")


def switch_repos(entries: list[dict[str, Any]], prior: dict[str, str]) -> tuple[list[str], str | None]:
    """Fast-forward each repository; on a failure undo the ones done and report it."""
    done: list[str] = []
    for entry in entries:
        result = git(entry["repo"], "merge", "--ff-only", entry["ref"])
        if result.returncode != 0:
            for repo in done:
                git(repo, "reset", "--keep", prior[repo])
            return [], f"fast-forward of {entry['repo']} to {entry['ref']} failed: {result.stderr.strip()[:300]}"
        done.append(entry["repo"])
    return done, None


def snapshot_database(database: Path, target: Path) -> Path:
    """A consistent copy of the live database (the backup API, so the write-ahead log is included)."""
    target.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    copy = sqlite3.connect(str(target))
    try:
        source.backup(copy)
    finally:
        copy.close()
        source.close()
    return target


def restore_database(snapshot: Path, database: Path) -> None:
    source = sqlite3.connect(f"file:{snapshot}?mode=ro", uri=True)
    live = sqlite3.connect(str(database))
    try:
        source.backup(live)
    finally:
        live.close()
        source.close()


def merge_config(data: Path, release: dict[str, Any]) -> tuple[str | None, str | None]:
    """Commit the live config's own edits, carry the change onto them when needed, fast-forward; returns
    (the head before, an error)."""
    branch, base = release["data"]["branch"], release["data"]["base"]
    git(str(data), "add", "-A")
    if git(str(data), "status", "--porcelain").stdout.strip():
        git(str(data), "commit", "-q", "-m", f"live: before release {release['id']}")
    prior = head_of(str(data))
    if git(str(data), "merge-base", "--is-ancestor", prior, branch).returncode != 0:
        place = Path(tempfile.mkdtemp(prefix="tasque-rebase-"))
        shutil.rmtree(place, ignore_errors=True)
        added = git(str(data), "worktree", "add", "-q", "--detach", str(place), branch)
        if added.returncode != 0:
            return prior, f"cannot check out the config branch: {added.stderr.strip()[:300]}"
        moved = git(str(place), "rebase", "-q", "--onto", prior, base)
        if moved.returncode != 0:
            git(str(place), "rebase", "--abort")
        else:
            carried = git(str(place), "rev-parse", "HEAD").stdout.strip()
            moved = git(str(data), "update-ref", f"refs/heads/{branch}", carried)
        git(str(data), "worktree", "remove", "--force", str(place))
        shutil.rmtree(place, ignore_errors=True)
        if moved.returncode != 0:
            return prior, f"the config change no longer applies cleanly: {moved.stderr.strip()[:300]}"
        release["data"]["rebased"] = True  # the change's commits now start from the live head
    merged = git(str(data), "merge", "--ff-only", "-q", branch)
    if merged.returncode != 0:
        return prior, f"the config fast-forward failed: {merged.stderr.strip()[:300]}"
    return prior, None


def tasque_command(project: Path) -> list[str]:
    scripts = project / ".venv" / ("Scripts" if os.name == "nt" else "bin")
    return [str(scripts / ("tasque2.exe" if os.name == "nt" else "tasque2"))]


def run_step(command: list[str], project: Path, timeout: float = 15 * 60) -> tuple[bool, str]:
    try:
        done = subprocess.run(command, cwd=str(project), capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"{type(exc).__name__}: {exc}"
    tail = (done.stderr.strip() or done.stdout.strip())[-400:]
    return done.returncode == 0, tail


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait-pid", type=int, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--health-seconds", type=float, default=HEALTH_SECONDS)
    parser.add_argument("--daemon-command", help="JSON list; tests replace the daemon with a stand-in")
    parser.add_argument("--database", help="the live database (default: tasque2.sqlite3 in the data directory)")
    parser.add_argument("--tasque-command", help="JSON list; tests replace the new code's CLI with a stand-in")
    args = parser.parse_args(argv)
    project, data = Path(args.project), Path(args.data)
    command = json.loads(args.daemon_command) if args.daemon_command else default_daemon_command(project)
    tasque = json.loads(args.tasque_command) if args.tasque_command else tasque_command(project)
    database = Path(args.database) if args.database else data / "tasque2.sqlite3"
    result: dict[str, Any] = {"started_at": now_iso(), "old_pid": args.wait_pid}

    deadline = time.monotonic() + EXIT_WAIT_SECONDS
    while pid_alive(args.wait_pid) and time.monotonic() < deadline:
        time.sleep(1)
    if pid_alive(args.wait_pid):
        result.update(ok=False, error="the old daemon never exited; nothing was switched or started")
        (data / RESULT_FILE).write_text(json.dumps(result, indent=1), encoding="utf-8")
        return 1

    request_file = data / REQUEST_FILE
    try:
        request = json.loads(request_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        request = {"switch": []}
    request_file.unlink(missing_ok=True)
    entries = list(request.get("switch") or [])
    prior = {entry["repo"]: head_of(entry["repo"]) for entry in entries}
    result.update(reason=request.get("reason"), prior=prior)
    release: dict[str, Any] | None = None
    error: str | None = None
    if request.get("release"):
        try:
            release = json.loads(Path(request["release"]).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            error = f"the release plan cannot be read: {exc}"
    snapshot: Path | None = None
    config_prior: str | None = None
    if release is not None:
        result["release"] = release.get("id")
        try:
            snapshot = snapshot_database(database, data / "backups" / f"pre-release-{release['id']}" / database.name)
        except sqlite3.Error as exc:
            error = f"the database snapshot failed: {exc}"
        for step in release.get("pre_switch") or []:
            if error is not None:
                break
            ok_step, tail = run_step([*tasque, *step], project)
            result.setdefault("steps", []).append({"step": " ".join(step), "ok": ok_step, "detail": tail})
            if not ok_step:
                error = f"{' '.join(step)} failed: {tail}"
        if error is None and release.get("data"):
            config_prior, error = merge_config(data, release)
            if error:
                config_prior = None if head_of(str(data)) == config_prior else config_prior
            elif release["data"].get("rebased"):
                release["data"]["base"] = config_prior  # the apply reads the change's own commits only
                Path(request["release"]).write_text(json.dumps(release, indent=1), encoding="utf-8")
        if error and snapshot is not None and release.get("pre_switch"):
            restore_database(snapshot, database)  # the old code's own steps may have moved it
    if error:
        entries = []  # nothing of a release is switched when its config or snapshot failed

    switched, switch_error = switch_repos(entries, prior)
    error = error or switch_error

    def undo(reason: str) -> None:
        """Back to where it was: code, config, dependencies, database."""
        for repo in switched:
            git(repo, "reset", "--keep", prior[repo])
        if config_prior:
            git(str(data), "reset", "--keep", config_prior)
        if release is not None and release.get("lock_changed"):
            run_step(["uv", "sync", "--frozen"], project)
        if snapshot is not None:
            restore_database(snapshot, database)
        result["rolled_back"] = [*switched, *([str(data)] if config_prior else [])]
        record_fault(data, reason)

    if release is not None and not error:
        steps = []
        if release.get("lock_changed"):
            steps.append(("uv sync", ["uv", "sync", "--frozen"]))
        steps.append(("release-apply", [*tasque, "release-apply", "--plan", str(request["release"])]))
        for name, step in steps:
            ok_step, tail = run_step(step, project)
            result.setdefault("steps", []).append({"step": name, "ok": ok_step, "detail": tail})
            if not ok_step:
                error = f"{name} failed: {tail}"
                break
        if error:
            undo(f"release {release.get('id')}: {error}; rolled back")
            switched, config_prior = [], None
            result["release_error"] = error
    elif error:
        result["switch_error"] = error
        record_fault(data, error)

    rotate_logs(data)
    process = start_daemon(command, project, data)
    ok = healthy(data, old_pid=args.wait_pid, timeout=args.health_seconds)
    if not ok and (switched or config_prior):
        stop_tree(process.pid)
        undo(f"the daemon did not come up healthy after switching {', '.join(switched) or 'the config'}; rolled back")
        rotate_logs(data)
        process = start_daemon(command, project, data)
        result["previous_code_healthy"] = healthy(data, old_pid=args.wait_pid, timeout=args.health_seconds)
        switched = []
    elif not ok:
        record_fault(data, "the daemon did not come up healthy after a restart")

    pushed = []
    if ok:
        for entry in entries:
            if entry["repo"] not in switched:
                continue
            if entry.get("push"):
                push = git(
                    entry["repo"], "push", entry.get("remote") or "origin", f"HEAD:{entry.get('branch') or 'main'}"
                )
                pushed.append({"repo": entry["repo"], "ok": push.returncode == 0, "detail": push.stderr.strip()[-300:]})
            git(entry["repo"], "branch", "-d", entry["ref"])
        if release is not None and config_prior is not None and not error:
            release["data"]["head"] = head_of(str(data))
            git(str(data), "branch", "-D", release["data"]["branch"])
    result.update(ok=ok and not error, switched=switched, pushed=pushed, new_pid=process.pid, ended_at=now_iso())
    if release is not None:
        record_release(Path(request["release"]), release, result, prior=prior, switched=switched)
    (data / RESULT_FILE).write_text(json.dumps(result, indent=1), encoding="utf-8")
    return 0 if result["ok"] else 1


def record_release(
    path: Path, release: dict[str, Any], result: dict[str, Any], *, prior: dict[str, str], switched: list[str]
) -> None:
    """How the release went, into its plan: what the new code's release-apply wrote there is kept."""
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        stored = {}
    merged = {**release, **{key: value for key, value in stored.items() if key == "revision_before"}}
    merged["outcome"] = {
        "ok": bool(result.get("ok")),
        "at": now_iso(),
        "error": result.get("release_error") or result.get("switch_error"),
        "rolled_back": result.get("rolled_back") or [],
        "code": {repo: {"before": prior[repo], "after": head_of(repo)} for repo in switched},
        "pushed": result.get("pushed") or [],
    }
    if result.get("ok"):
        merged["released_at"] = now_iso()
    try:
        path.write_text(json.dumps(merged, indent=1), encoding="utf-8")
    except OSError:
        pass


if __name__ == "__main__":
    sys.exit(main())
