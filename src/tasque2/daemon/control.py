"""Daemon liveness and control through files in the data directory.

``daemon.state.json`` is rewritten every tick (pid, start time, last tick, in-flight work,
draining). ``daemon.drain`` asks the daemon to stop claiming new work and exit once the
work in flight has finished.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import Schedule, utc_now


@dataclass(frozen=True)
class DaemonState:
    pid: int | None
    started_at: datetime | None
    last_tick_at: datetime | None
    in_flight: int
    draining: bool
    version: str | None

    def is_fresh(self, now: datetime | None = None) -> bool:
        if self.last_tick_at is None:
            return False
        stale_after = timedelta(seconds=get_settings().daemon_stale_seconds)
        return (now or utc_now()) - self.last_tick_at <= stale_after


def state_path() -> Path:
    return get_settings().resolved_data_dir / "daemon.state.json"


def drain_path() -> Path:
    return get_settings().resolved_data_dir / "daemon.drain"


def write_state(
    *,
    started_at: datetime,
    in_flight_attempt_ids: list[str],
    draining: bool,
    version: str,
) -> None:
    path = state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "pid": os.getpid(),
        "started_at": started_at.isoformat(),
        "last_tick_at": utc_now().isoformat(),
        "in_flight_attempt_ids": in_flight_attempt_ids,
        "draining": draining,
        "version": version,
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def read_state() -> DaemonState | None:
    try:
        data = json.loads(state_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return DaemonState(
        pid=data.get("pid"),
        started_at=_parse_time(data.get("started_at")),
        last_tick_at=_parse_time(data.get("last_tick_at")),
        in_flight=len(data.get("in_flight_attempt_ids") or []),
        draining=bool(data.get("draining")),
        version=data.get("version"),
    )


def clear_state() -> None:
    state_path().unlink(missing_ok=True)


def request_drain() -> Path:
    path = drain_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(utc_now().isoformat(), encoding="utf-8")
    return path


def drain_requested() -> bool:
    return drain_path().is_file()


def clear_drain() -> None:
    drain_path().unlink(missing_ok=True)


def latest_schedule_tick(session: Session) -> datetime | None:
    """When enabled schedules were last evaluated, by the daemon or a manual tick."""
    return session.scalar(select(func.max(Schedule.last_evaluated_at)).where(Schedule.enabled.is_(True)))


def live_daemon_reason(*, now: datetime | None = None) -> str | None:
    """Why another daemon looks alive right now, or None when it is safe to start.

    A running daemon rewrites its state file every tick; a manual ``tick`` writes none, so it
    never blocks the commands that follow it.
    """
    now = now or utc_now()
    state = read_state()
    if state is not None and state.is_fresh(now) and state.pid != os.getpid():
        return f"daemon pid {state.pid} ticked at {state.last_tick_at.isoformat()}"
    return None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None
