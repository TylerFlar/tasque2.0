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

The outcome goes to ``daemon.restart.result.json``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait-pid", type=int, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--health-seconds", type=float, default=HEALTH_SECONDS)
    parser.add_argument("--daemon-command", help="JSON list; tests replace the daemon with a stand-in")
    args = parser.parse_args(argv)
    project, data = Path(args.project), Path(args.data)
    command = json.loads(args.daemon_command) if args.daemon_command else default_daemon_command(project)
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

    switched, error = switch_repos(entries, prior)
    if error:
        result["switch_error"] = error
        record_fault(data, error)

    rotate_logs(data)
    process = start_daemon(command, project, data)
    ok = healthy(data, old_pid=args.wait_pid, timeout=args.health_seconds)
    if not ok and switched:
        stop_tree(process.pid)
        for repo in switched:
            git(repo, "reset", "--keep", prior[repo])
        result["rolled_back"] = switched
        record_fault(data, f"the daemon did not come up healthy after switching {', '.join(switched)}; rolled back")
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
    result.update(ok=ok and not error, switched=switched, pushed=pushed, new_pid=process.pid, ended_at=now_iso())
    (data / RESULT_FILE).write_text(json.dumps(result, indent=1), encoding="utf-8")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
