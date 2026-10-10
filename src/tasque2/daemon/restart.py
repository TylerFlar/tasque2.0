"""Restart the daemon at an idle moment, optionally switching repositories to new commits first.

A restart request is ``daemon.restart.json`` in the data directory: why, and an optional
``switch``, a list of repositories to fast-forward to a ref (and whether to push the result). The
daemon checks it every tick. Once the moment is idle and nothing is in flight, it stops claiming
work and hands over to ``tasque2.daemon.respawn``, a standard-library-only process that waits for
this daemon to exit, fast-forwards each repository, starts the daemon again hidden, and checks it
comes up healthy. If it does not, the respawn resets the repositories to where they were and
starts the previous code again.

An idle moment has no work waiting (model work a usage limit holds aside), no model-backed
schedule due within 15 minutes, and no message from the user in the last 15 (the bot does not
catch up on messages sent while it is down).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import DiscordMessage, Schedule, utc_now

REQUEST_FILE = "daemon.restart.json"
RESULT_FILE = "daemon.restart.result.json"
LOOKAHEAD = timedelta(minutes=15)
USER_IDLE = timedelta(minutes=15)


class RestartBusy(RuntimeError):
    """A restart that switches code is already waiting; a second one would replace it."""


def request_path() -> Path:
    return get_settings().resolved_data_dir / REQUEST_FILE


def result_path() -> Path:
    return get_settings().resolved_data_dir / RESULT_FILE


def request_restart(*, reason: str, switch: list[dict[str, Any]] | None = None, release: str | None = None) -> Path:
    """Ask the daemon to restart; ``switch`` entries are ``{repo, ref, push?, remote?, branch?}``. ``release``
    is a release plan's path (``tasque2.ops.release``): the respawn also merges its config, runs its
    ``release-apply`` with the new code, and restores the database snapshot if anything fails."""
    pending = read_request()
    if pending and (pending.get("switch") or pending.get("release")) and (switch or release):
        raise RestartBusy(f"another restart is waiting to go live: {pending.get('reason')}")
    entries = []
    for entry in switch or []:
        if not entry.get("repo") or not entry.get("ref"):
            raise ValueError("each switch entry needs repo and ref")
        entries.append(
            {
                "repo": str(Path(entry["repo"]).resolve()),
                "ref": str(entry["ref"]),
                "push": bool(entry.get("push", False)),
                "remote": str(entry.get("remote") or "origin"),
                "branch": str(entry.get("branch") or "main"),
            }
        )
    path = request_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"requested_at": utc_now().isoformat(), "reason": reason, "switch": entries}
    if release:
        payload["release"] = str(Path(release).resolve())
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    os.replace(temporary, path)
    return path


def read_request() -> dict[str, Any] | None:
    try:
        return json.loads(request_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def clear_request() -> None:
    request_path().unlink(missing_ok=True)


def read_result() -> dict[str, Any] | None:
    """The last restart's outcome as the respawn process wrote it, or None."""
    try:
        return json.loads(result_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def waiting_reason(session: Session, request: dict[str, Any], *, now: datetime | None = None) -> str | None:
    """Why the restart must wait, or None at an idle moment. Model work a usage limit holds is not waiting
    on anything a restart would interrupt, so it does not count."""
    from tasque2.schedules import ScheduleService
    from tasque2.work.queue import WorkQueue

    now = now or utc_now()
    if WorkQueue(session).ready_count(now=now):
        return "work is waiting"
    last_inbound = session.scalar(
        select(func.max(DiscordMessage.created_at)).where(DiscordMessage.direction == "inbound")
    )
    if last_inbound is not None:
        last_inbound = last_inbound if last_inbound.tzinfo else last_inbound.replace(tzinfo=now.tzinfo)
        if now - last_inbound < USER_IDLE:
            return "the user is active"
    service = ScheduleService(session)
    for schedule in session.scalars(select(Schedule).where(Schedule.enabled.is_(True))).all():
        if schedule.worker_kind.startswith("function."):
            continue
        upcoming = service.next_fire_time(schedule, now=now)
        if upcoming is not None and upcoming <= now + LOOKAHEAD:
            return f"{schedule.name} is due"
    return None


def respawn_command(*, pid: int) -> list[str]:
    settings = get_settings()
    return [
        sys.executable,
        "-m",
        "tasque2.daemon.respawn",
        "--wait-pid",
        str(pid),
        "--project",
        str(settings.resolved_project_dir),
        "--data",
        str(settings.resolved_data_dir),
        "--database",
        str(settings.database_path),
    ]


def spawn_respawn(*, pid: int) -> subprocess.Popen:
    """Start the respawn process detached from this one, so it outlives the daemon."""
    data = get_settings().resolved_data_dir
    log = (data / "daemon.respawn.log").open("a", encoding="utf-8")
    options: dict[str, Any] = {"stdout": log, "stderr": log, "stdin": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        options["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options["start_new_session"] = True
    return subprocess.Popen(respawn_command(pid=pid), cwd=str(get_settings().resolved_project_dir), **options)
