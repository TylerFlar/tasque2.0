from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tasque2.config import get_settings, reset_settings
from tasque2.daemon import respawn
from tasque2.daemon.restart import read_request, request_restart, waiting_reason
from tasque2.daemon.service import Daemon
from tasque2.db import session_scope
from tasque2.models import DiscordMessage, WorkItem
from tasque2.schedules import ScheduleService

NIGHT = datetime(2026, 10, 7, 10, 0, tzinfo=UTC)  # 03:00 in Los Angeles
DAY = datetime(2026, 10, 7, 20, 0, tzinfo=UTC)  # 13:00 in Los Angeles


@pytest.fixture()
def la(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_TIMEZONE", "America/Los_Angeles")
    reset_settings()


def _request(window: str = "quiet", at: datetime = NIGHT) -> dict:
    return {"requested_at": at.isoformat(), "reason": "test", "window": window, "switch": []}


def test_an_idle_night_opens_the_quiet_window(fresh_db: Path, la: None) -> None:
    with session_scope() as session:
        assert waiting_reason(session, _request(), now=NIGHT) is None
        assert waiting_reason(session, _request(), now=DAY) == "waiting for the quiet hours"
        assert waiting_reason(session, _request("now"), now=DAY) is None
        # a day-old request stops waiting for the small hours
        assert waiting_reason(session, _request(at=DAY - timedelta(hours=25)), now=DAY) is None


def test_work_a_due_model_run_or_an_active_user_holds_the_restart(fresh_db: Path, la: None) -> None:
    with session_scope() as session:
        session.add(WorkItem(title="queued", task_instruction="x", worker_kind="function.echo", status="ready"))
        session.flush()
        assert waiting_reason(session, _request("now"), now=NIGHT) == "work is waiting"
        session.query(WorkItem).delete()

        session.add(
            DiscordMessage(
                discord_message_id="m1",
                discord_channel_id="c",
                direction="inbound",
                author="user",
                content_preview="hi",
                created_at=NIGHT - timedelta(minutes=5),
            )
        )
        session.flush()
        assert waiting_reason(session, _request("now"), now=NIGHT) == "the user is active"
        assert waiting_reason(session, _request("now"), now=NIGHT + timedelta(minutes=20)) is None

        ScheduleService(session).create_schedule(
            name="memory-consolidation",
            schedule_type="cron",
            expression="30 3 * * *",
            worker_kind="provider.default",
            payload={"title": "x", "task_instruction": "x"},
            timezone_name="America/Los_Angeles",
        )
        ScheduleService(session).create_schedule(
            name="cheap-watch",
            schedule_type="cron",
            expression="25 3 * * *",
            worker_kind="function.cheap_watch",
            payload={"title": "x", "task_instruction": "x"},
            timezone_name="America/Los_Angeles",
        )
        # 03:20 local: the user went quiet 25 minutes ago, the 03:30 model run is due within 15 minutes,
        # and the no-model watch at 03:25 does not count
        later = NIGHT + timedelta(minutes=20)
        assert waiting_reason(session, _request("now"), now=later) == "memory-consolidation is due"


def test_a_restart_request_validates_and_round_trips(isolated: Path) -> None:
    with pytest.raises(ValueError, match="window"):
        request_restart(reason="x", window="soon")
    with pytest.raises(ValueError, match="repo and ref"):
        request_restart(reason="x", switch=[{"repo": "."}])
    request_restart(reason="repair", switch=[{"repo": ".", "ref": "repair/a", "push": True}])
    request = read_request()
    assert request["window"] == "quiet" and request["switch"][0]["ref"] == "repair/a"
    assert request["switch"][0]["push"] is True and request["switch"][0]["branch"] == "main"


def test_the_daemon_restarts_only_when_the_window_is_open(fresh_db: Path, la: None, monkeypatch) -> None:
    daemon = Daemon(discord=False)
    assert daemon._restart_due() is False  # nothing requested
    request_restart(reason="test", window="now")
    assert daemon._restart_due() is True
    monkeypatch.setattr("tasque2.daemon.restart.waiting_reason", lambda *a, **k: "the user is active")
    assert daemon._restart_due() is False


def test_a_respawn_that_cannot_start_drops_the_request_and_the_daemon_keeps_going(
    fresh_db: Path, la: None, monkeypatch
) -> None:
    daemon = Daemon(discord=False)
    request_restart(reason="test", window="now")

    def cannot_start(**kwargs) -> None:
        raise OSError("no process for you")

    slept: list[bool] = []

    async def sleep_once(draining: bool) -> None:
        slept.append(draining)
        daemon._stop.set()

    monkeypatch.setattr("tasque2.daemon.restart.spawn_respawn", cannot_start)
    monkeypatch.setattr(daemon, "_sleep", sleep_once)
    daemon._stop = asyncio.Event()
    asyncio.run(daemon._tick_loop())
    assert read_request() is None
    assert slept == [False]  # it carried on ticking instead of handing over


# ------------------------------------------------------------------------------ the respawn process


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "user.email", "tests@example.com")
    _git(root, "config", "user.name", "Tests")
    (root / "code.py").write_text("x = 1\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "base")
    _git(root, "checkout", "-q", "-b", "repair/a")
    (root / "code.py").write_text("x = 2\n", encoding="utf-8")
    _git(root, "commit", "-q", "-am", "fix")
    _git(root, "checkout", "-q", "main")
    return root


def _dead_pid() -> int:
    process = subprocess.Popen([sys.executable, "-c", "pass"])
    process.wait()
    return process.pid


def _stand_in(data: Path, *, healthy: bool) -> str:
    if healthy:
        script = (
            "import json, os, sys, time, datetime\n"
            f"data = {str(data)!r}\n"
            "now = datetime.datetime.now(datetime.UTC).isoformat()\n"
            "open(os.path.join(data, 'daemon.state.json'), 'w').write(json.dumps({'pid': os.getpid(), "
            "'last_tick_at': now}))\n"
            "print('Discord connected as Test', file=sys.stderr, flush=True)\n"
            "time.sleep(60)\n"
        )
    else:
        script = "import sys; print('boom', file=sys.stderr); sys.exit(3)\n"
    return json.dumps([sys.executable, "-c", script])


def _kill_stand_in(data: Path) -> None:
    try:
        pid = json.loads((data / "daemon.state.json").read_text(encoding="utf-8"))["pid"]
    except (OSError, ValueError, KeyError):
        return
    respawn.stop_tree(pid)


def _write_request(data: Path, repo: Path) -> None:
    (data / "daemon.restart.json").write_text(
        json.dumps({"reason": "repair", "window": "now", "switch": [{"repo": str(repo), "ref": "repair/a"}]}),
        encoding="utf-8",
    )


def test_respawn_switches_starts_and_keeps_a_healthy_daemon(tmp_path: Path, repo: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    _write_request(data, repo)
    fix = _git(repo, "rev-parse", "repair/a")
    try:
        code = respawn.main(
            [
                "--wait-pid",
                str(_dead_pid()),
                "--project",
                str(repo),
                "--data",
                str(data),
                "--health-seconds",
                "30",
                "--daemon-command",
                _stand_in(data, healthy=True),
            ]
        )
    finally:
        _kill_stand_in(data)
    result = json.loads((data / "daemon.restart.result.json").read_text(encoding="utf-8"))
    assert code == 0 and result["ok"] is True
    assert _git(repo, "rev-parse", "HEAD") == fix
    assert "repair/a" not in _git(repo, "branch", "--list", "repair/a")  # merged ref deleted
    assert not (data / "daemon.restart.json").exists()


def test_respawn_rolls_back_when_the_new_code_does_not_come_up(tmp_path: Path, repo: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    _write_request(data, repo)
    before = _git(repo, "rev-parse", "HEAD")

    code = respawn.main(
        [
            "--wait-pid",
            str(_dead_pid()),
            "--project",
            str(repo),
            "--data",
            str(data),
            "--health-seconds",
            "5",
            "--daemon-command",
            _stand_in(data, healthy=False),
        ]
    )

    result = json.loads((data / "daemon.restart.result.json").read_text(encoding="utf-8"))
    assert code == 1 and result["ok"] is False
    assert result["rolled_back"] == [str(repo)]
    assert _git(repo, "rev-parse", "HEAD") == before
    faults = (data / "runtime" / "faults.jsonl").read_text(encoding="utf-8")
    assert "rolled back" in faults


def test_pid_alive_tells_a_live_process_from_a_finished_one() -> None:
    assert respawn.pid_alive(os.getpid())
    assert not respawn.pid_alive(_dead_pid())
    assert get_settings() is not None
