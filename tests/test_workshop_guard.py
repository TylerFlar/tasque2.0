from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from tasque2.workshop import guard


@pytest.fixture()
def places(tmp_path: Path) -> dict[str, str]:
    live = tmp_path / "tasque2.0"
    root = tmp_path / "tasque2.0-workshop" / "c1"
    for path in (live / "data", root / "data", live / ".venv" / "Scripts"):
        path.mkdir(parents=True)
    python = live / ".venv" / "Scripts" / "python.exe"
    python.write_text("", encoding="utf-8")
    return {"live": str(live), "data": str(live / "data"), "root": str(root), "python": str(python)}


def _decide(places: dict[str, str], tool: str, mode: str = "build", **tool_input) -> str | None:
    return guard.decide(
        {"tool_name": tool, "tool_input": tool_input},
        root=places["root"],
        live=places["live"],
        data=places["data"],
        mode=mode,
        allow=[places["python"]],
    )


def test_writes_stay_inside_the_change(places: dict[str, str]) -> None:
    assert _decide(places, "Write", file_path=f"{places['root']}/src/app.py") is None
    assert _decide(places, "Edit", file_path=f"{places['root']}/data/work-templates/x.md") is None
    assert "writes stay inside" in _decide(places, "Write", file_path=f"{places['live']}/src/app.py")
    assert "writes stay inside" in _decide(places, "Edit", file_path=f"{places['data']}/lanes.json")


@pytest.mark.parametrize(
    ("command", "refused"),
    [
        ('cd "{root}" && "{python}" -m pytest -q -p no:cacheprovider', None),
        ('git -C "{root}" commit -qm "fix the table"', None),
        ('"{python}" -m ruff check src', None),
        ('type "{live}\\src\\app.py"', "off limits"),
        ('cat "{data}/lanes.json"', "off limits"),
        ("git -C {root} push origin HEAD", "moves or publishes"),
        ("git checkout main", "moves or publishes"),
        ("git branch -D workshop/c1", "deleting a branch"),
        ("uv sync --frozen", "installing packages"),
        ("pip install requests", "installing packages"),
        ("Get-Content .env", ".env"),
        ("taskkill /PID 4 /F", "stopping processes"),
    ],
)
def test_commands_stay_away_from_the_live_install(places: dict[str, str], command: str, refused: str | None) -> None:
    reason = _decide(places, "Bash", command=command.format(**places))
    assert (reason is None) if refused is None else (reason is not None and refused in reason), reason


@pytest.mark.parametrize(
    ("command", "refused"),
    [
        ("tasque2 daemon-restart --now", True),
        ("tasque2.exe daemon", True),
        ('"C:/x/.venv/Scripts/tasque2.exe" daemon-stop', True),
        ("python -m tasque2 release-apply --plan p.json", True),
        ("uv run tasque2 schedule-fire-now x", True),
        ("python -m tasque2.cli daemon-stop", True),
        ("python -m tasque2.__main__ daemon", True),
        ('"{python}" -m tasque2 daemon-restart', True),
        ("ruff check src/tasque2/daemon/respawn.py", False),
        ("git add src/tasque2/daemon/restart.py", False),
        ("python -m pytest tests/test_restart.py -k daemon", False),
        ("python -m pytest src/tasque2/daemon", False),
        ('"{python}" -m ruff check src/tasque2/daemon/respawn.py', False),
        ('cd "{root}" && "{python}" -m pytest -q tests/test_restart.py -k daemon', False),
        ("tasque2 daemon-status", False),
    ],
)
def test_the_cli_is_refused_not_paths_that_name_it(places: dict[str, str], command: str, refused: bool) -> None:
    reason = _decide(places, "Bash", command=command.format(**places))
    assert (reason is not None and "the daemon's commands" in reason) if refused else (reason is None), reason


def test_a_build_works_offline_and_a_plan_may_research(places: dict[str, str]) -> None:
    assert "research belongs in the plan" in _decide(places, "WebFetch", url="https://example.com")
    assert _decide(places, "WebSearch", mode="plan", query="tasque") is None


def test_the_hook_refuses_with_status_two(places: dict[str, str], monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    call = {"tool_name": "Write", "tool_input": {"file_path": f"{places['live']}/src/app.py"}}
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(call)))
    args = ["--root", places["root"], "--live", places["live"], "--data", places["data"]]
    assert guard.main(args) == 2
    assert "Refused by the Workshop guard" in capsys.readouterr().err
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({"tool_name": "Read", "tool_input": {}})))
    assert guard.main(args) == 0
