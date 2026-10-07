"""Restart the daemon at a quiet moment, optionally switching repositories to new commits first.

A restart request is ``daemon.restart.json`` in the data directory: why, which window to wait for,
and an optional ``switch``, a list of repositories to fast-forward to a ref (and whether to push
the result). The daemon checks it every tick. Once the window opens and nothing is in flight, it
stops claiming work and hands over to ``tasque2.daemon.respawn``, a standard-library-only process
that waits for this daemon to exit, fast-forwards each repository, starts the daemon again
hidden, and checks it comes up healthy. If it does not, the respawn resets the repositories to
where they were and starts the previous code again.

Windows: ``"now"`` waits only for an idle moment; ``"quiet"`` also waits for the small hours
(01:00-06:30 local), unless the request is a day old. An idle moment has no work waiting, no
model-backed schedule due within 15 minutes, and no message from the user in the last 15 (the bot
does not catch up on messages sent while it is down).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import DiscordMessage, Schedule, WorkItem, utc_now

REQUEST_FILE = "daemon.restart.json"
RESULT_FILE = "daemon.restart.result.json"
WINDOWS = ("quiet", "now")
QUIET_FROM, QUIET_UNTIL = time(1, 0), time(6, 30)
LOOKAHEAD = timedelta(minutes=15)
USER_IDLE = timedelta(minutes=15)
STALE_REQUEST = timedelta(hours=24)


def request_path() -> Path:
    return get_settings().resolved_data_dir / REQUEST_FILE


def result_path() -> Path:
    return get_settings().resolved_data_dir / RESULT_FILE


def request_restart(*, reason: str, window: str = "quiet", switch: list[dict[str, Any]] | None = None) -> Path:
    """Ask the daemon to restart; ``switch`` entries are ``{repo, ref, push?, remote?, branch?}``."""
    if window not in WINDOWS:
        raise ValueError(f"window must be one of {', '.join(WINDOWS)}")
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
    payload = {"requested_at": utc_now().isoformat(), "reason": reason, "window": window, "switch": entries}
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


def _local_time(moment: datetime) -> time:
    from tasque2.localtime import _to_local

    return _to_local(moment).time()


def waiting_reason(session: Session, request: dict[str, Any], *, now: datetime | None = None) -> str | None:
    """Why the restart must wait, or None when the window is open."""
    from tasque2.schedules import ScheduleService

    now = now or utc_now()
    if session.scalar(select(func.count()).select_from(WorkItem).where(WorkItem.status == "ready")):
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
    if request.get("window") == "quiet":
        try:
            requested = datetime.fromisoformat(str(request.get("requested_at")))
        except ValueError:
            requested = now
        local = _local_time(now)
        if not (QUIET_FROM <= local < QUIET_UNTIL) and now - requested < STALE_REQUEST:
            return "waiting for the quiet hours"
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
