from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from tasque2.daemon import respawn
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.models import Schedule
from tasque2.ops import datarepo
from tasque2.ops.release import ReleasePlan, apply_config, preflight, release_hot
from tasque2.schedules import ScheduleService

TEMPLATE = "work-templates/cooking/reply.template.md"
SCRIPT = """\
from tasque2.memory import MemoryService


def apply(session):
    MemoryService(session).upsert_canonical(namespace="global", canonical_key="script_note", kind="state",
                                            content="added by the change", pinned=False)
    return "noted"


def revert(session):
    note = MemoryService(session).get_canonical(namespace="global", canonical_key="script_note")
    if note is not None:
        MemoryService(session).archive_memory(note.id)
    return "removed"
"""


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


def _doc(content: str) -> None:
    with session_scope() as session:
        MemoryService(session).upsert_canonical(
            namespace="cooking", canonical_key="cooking_direction", kind="doctrine", content=content, pinned=True
        )


def _live_doc(key: str = "cooking_direction", namespace: str = "cooking") -> str | None:
    with session_scope() as session:
        memory = MemoryService(session).get_canonical(namespace=namespace, canonical_key=key)
        return memory.content if memory is not None else None


def _profile() -> str | None:
    with session_scope() as session:
        schedule = session.query(Schedule).filter(Schedule.name == "kitchen").one()
        return (schedule.runtime_contract or {}).get("model_profile")


@pytest.fixture()
def config(fresh_db: Path) -> Path:
    """A config repository with a template, the lanes manifest, one doctrine document, one schedule."""
    root = fresh_db.parent
    (root / "work-templates" / "cooking").mkdir(parents=True)
    (root / TEMPLATE).write_text("Reply.\n", encoding="utf-8")
    (root / "work-templates" / "cooking" / "week.template.md").write_text("Week.\n", encoding="utf-8")
    (root / "lanes.json").write_text(json.dumps({"schedules": {"kitchen": {"profile": "high"}}}), encoding="utf-8")
    _doc("Cook simply.\n")
    with session_scope() as session:
        ScheduleService(session).create_schedule(
            name="kitchen",
            schedule_type="cron",
            expression="0 9 * * *",
            worker_kind="provider.default",
            payload={"title": "Kitchen", "task_instruction": "Plan."},
            runtime_contract={"model_profile": "high"},
        )
    datarepo.init(root)
    return root


def _change(root: Path, tmp_path: Path) -> ReleasePlan:
    """A change on its own branch, built in a worktree: a template, a document, a lanes key, a script."""
    base = datarepo.head(root)
    place = tmp_path / "change"
    _git(root, "worktree", "add", "-q", "-b", "workshop/c1", str(place), base)
    (place / TEMPLATE).write_text("Reply, changed.\n", encoding="utf-8")
    (place / "doctrine" / "cooking" / "cooking_direction.md").write_text("Cook simply; one pan.\n", encoding="utf-8")
    (place / "lanes.json").write_text(json.dumps({"schedules": {"kitchen": {"profile": "ultra"}}}), encoding="utf-8")
    (place / "changes" / "c1").mkdir(parents=True)
    (place / "changes" / "c1" / "db_script.py").write_text(SCRIPT, encoding="utf-8")
    _git(place, "add", "-A")
    _git(place, "commit", "-q", "-m", "the change")
    _git(root, "worktree", "remove", "--force", str(place))
    return ReleasePlan(
        id="r1",
        change_id="c1",
        title="one pan",
        data={"branch": "workshop/c1", "base": base},
        db_script="changes/c1/db_script.py",
    )


def test_a_hot_release_lands_what_the_change_means_once_and_undoes(config: Path, tmp_path: Path) -> None:
    plan = _change(config, tmp_path)
    with session_scope() as session:
        assert preflight(session, plan) == []
        outcome = release_hot(session, plan)
    assert outcome["ok"] is True
    assert (config / TEMPLATE).read_text(encoding="utf-8") == "Reply, changed.\n"
    assert _live_doc() == "Cook simply; one pan.\n" and _profile() == "ultra"
    assert _live_doc("script_note", "global") == "added by the change"
    assert plan.data["head"] == datarepo.head(config)
    with session_scope() as session:  # again: nothing moves
        again = apply_config(session, plan)
    assert {entry["status"] for entry in again["doctrine"]} == {"unchanged"} and again["lanes"] == []
    with session_scope() as session:  # and the undo puts it all back
        apply_config(session, plan, undo=True)
    assert _live_doc() == "Cook simply.\n" and _profile() == "high" and _live_doc("script_note", "global") is None


def test_preflight_names_what_somebody_changed_live_since_the_change_began(config: Path, tmp_path: Path) -> None:
    plan = _change(config, tmp_path)
    (config / TEMPLATE).write_text("Reply, as a worker changed it.\n", encoding="utf-8")
    _doc("Cook simply, as the lane rewrote it.\n")
    with session_scope() as session:
        problems = preflight(session, plan)
        outcome = release_hot(session, plan)
    assert problems == [
        f"config {TEMPLATE}: changed live since the change began",
        "doctrine cooking/cooking_direction: changed live since the change began",
    ]
    assert outcome == {"ok": False, "problems": problems}
    assert (config / TEMPLATE).read_text(encoding="utf-8") == "Reply, as a worker changed it.\n"


def test_a_live_edit_to_another_file_is_carried_along(config: Path, tmp_path: Path) -> None:
    plan = _change(config, tmp_path)
    week = config / "work-templates" / "cooking" / "week.template.md"
    week.write_text("Week, as a worker changed it.\n", encoding="utf-8")
    with session_scope() as session:
        assert release_hot(session, plan)["ok"] is True
    assert week.read_text(encoding="utf-8") == "Week, as a worker changed it.\n"
    assert (config / TEMPLATE).read_text(encoding="utf-8") == "Reply, changed.\n"
    assert datarepo.changed_files(plan.data["base"], plan.data["head"], root=config) == [
        "changes/c1/db_script.py",
        "doctrine/cooking/cooking_direction.md",
        "lanes.json",
        TEMPLATE,
    ]


def test_a_change_still_checked_out_in_its_worktree_is_carried_along(config: Path, tmp_path: Path) -> None:
    plan = _change(config, tmp_path)
    held = tmp_path / "still-there"
    _git(config, "worktree", "add", "-q", str(held), "workshop/c1")  # the change's own tree, not yet removed
    week = config / "work-templates" / "cooking" / "week.template.md"
    week.write_text("Week, as a worker changed it.\n", encoding="utf-8")
    with session_scope() as session:
        assert release_hot(session, plan)["ok"] is True
    assert week.read_text(encoding="utf-8") == "Week, as a worker changed it.\n"
    assert (config / TEMPLATE).read_text(encoding="utf-8") == "Reply, changed.\n"
    assert _git(config, "rev-parse", "workshop/c1") == datarepo.head(config)


# --- a cold release, carried out by the respawn ---------------------------------------------------


def _repo(root: Path) -> Path:
    root.mkdir(parents=True)
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "user.email", "tests@example.com")
    _git(root, "config", "user.name", "Tests")
    (root / "code.py").write_text("x = 1\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "base")
    _git(root, "checkout", "-q", "-b", "workshop/c2")
    (root / "code.py").write_text("x = 2\n", encoding="utf-8")
    _git(root, "commit", "-q", "-am", "the change")
    _git(root, "checkout", "-q", "main")
    return root


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


def _apply_stand_in(database: Path, *, ok: bool) -> str:
    """The new code's release-apply: it writes to the database, then succeeds or fails."""
    script = (
        "import sqlite3, sys\n"
        f"db = sqlite3.connect({str(database)!r})\n"
        "db.execute('create table if not exists migrated (x int)'); db.commit(); db.close()\n"
        f"sys.exit({0 if ok else 1})\n"
    )
    return json.dumps([sys.executable, "-c", script])


def _dead_pid() -> int:
    process = subprocess.Popen([sys.executable, "-c", "pass"])
    process.wait()
    return process.pid


@pytest.fixture()
def cold(tmp_path: Path) -> dict[str, Path]:
    code = _repo(tmp_path / "code")
    data = tmp_path / "data"
    data.mkdir()
    _git(data, "init", "-q", "-b", "main")
    _git(data, "config", "user.email", "tests@example.com")
    _git(data, "config", "user.name", "Tests")
    (data / ".gitignore").write_text("/*\n!/.gitignore\n!/lanes.json\n!/week.md\n", encoding="utf-8")
    (data / "lanes.json").write_text("{}\n", encoding="utf-8")
    (data / "week.md").write_text("Week.\n", encoding="utf-8")
    _git(data, "add", ".")
    _git(data, "commit", "-q", "-m", "config")
    base = _git(data, "rev-parse", "HEAD")
    _git(data, "checkout", "-q", "-b", "workshop/c2")
    (data / "lanes.json").write_text('{"threads": {}}\n', encoding="utf-8")
    _git(data, "commit", "-q", "-am", "the config change")
    _git(data, "checkout", "-q", "main")
    database = data / "tasque2.sqlite3"
    db = sqlite3.connect(str(database))
    db.execute("create table work (x int)")
    db.commit()
    db.close()
    plan = {"id": "c2", "title": "x", "repos": [], "data": {"branch": "workshop/c2", "base": base}}
    plan_file = data / "runtime" / "releases" / "c2.json"
    plan_file.parent.mkdir(parents=True)
    plan_file.write_text(json.dumps(plan), encoding="utf-8")
    (data / "daemon.restart.json").write_text(
        json.dumps(
            {
                "reason": "release",
                "switch": [{"repo": str(code), "ref": "workshop/c2"}],
                "release": str(plan_file),
            }
        ),
        encoding="utf-8",
    )
    return {"code": code, "data": data, "database": database, "plan": plan_file}


def _respawn(cold: dict[str, Path], *, healthy: bool, apply_ok: bool, seconds: str = "30") -> int:
    try:
        return respawn.main(
            [
                "--wait-pid",
                str(_dead_pid()),
                "--project",
                str(cold["code"]),
                "--data",
                str(cold["data"]),
                "--database",
                str(cold["database"]),
                "--health-seconds",
                seconds,
                "--daemon-command",
                _stand_in(cold["data"], healthy=healthy),
                "--tasque-command",
                _apply_stand_in(cold["database"], ok=apply_ok),
            ]
        )
    finally:
        try:
            pid = json.loads((cold["data"] / "daemon.state.json").read_text(encoding="utf-8"))["pid"]
            respawn.stop_tree(pid)
        except (OSError, ValueError, KeyError):
            pass


def _tables(database: Path) -> set[str]:
    db = sqlite3.connect(str(database))
    try:
        return {row[0] for row in db.execute("select name from sqlite_master where type = 'table'")}
    finally:
        db.close()


def test_a_cold_release_switches_code_and_config_and_runs_the_new_code_once(cold: dict[str, Path]) -> None:
    code = _respawn(cold, healthy=True, apply_ok=True)
    result = json.loads((cold["data"] / "daemon.restart.result.json").read_text(encoding="utf-8"))
    assert code == 0 and result["ok"] is True and result["release"] == "c2"
    assert (cold["code"] / "code.py").read_text(encoding="utf-8") == "x = 2\n"
    assert (cold["data"] / "lanes.json").read_text(encoding="utf-8") == '{"threads": {}}\n'
    assert "migrated" in _tables(cold["database"])
    plan = json.loads(cold["plan"].read_text(encoding="utf-8"))
    assert plan["data"]["head"] == _git(cold["data"], "rev-parse", "HEAD") and plan["released_at"]
    assert (cold["data"] / "backups" / "pre-release-c2" / "tasque2.sqlite3").is_file()


def test_a_cold_release_carries_its_config_branch_onto_a_live_edit_wherever_it_is_checked_out(
    cold: dict[str, Path], tmp_path: Path
) -> None:
    _git(cold["data"], "worktree", "add", "-q", str(tmp_path / "still-there"), "workshop/c2")  # the change's own tree
    week = cold["data"] / "week.md"
    week.write_text("Week, as a worker changed it.\n", encoding="utf-8")  # not committed yet
    assert _respawn(cold, healthy=True, apply_ok=True) == 0
    assert week.read_text(encoding="utf-8") == "Week, as a worker changed it.\n"
    assert (cold["data"] / "lanes.json").read_text(encoding="utf-8") == '{"threads": {}}\n'
    assert _git(cold["data"], "rev-parse", "workshop/c2") == _git(cold["data"], "rev-parse", "HEAD")
    plan = json.loads(cold["plan"].read_text(encoding="utf-8"))  # its record brackets the change's own commits
    assert _git(cold["data"], "diff", "--name-only", f"{plan['data']['base']}..{plan['data']['head']}") == "lanes.json"


@pytest.mark.parametrize(("healthy", "apply_ok"), [(True, False), (False, True)])
def test_a_failed_release_puts_code_config_and_database_back(cold: dict[str, Path], healthy, apply_ok) -> None:
    code_before = _git(cold["code"], "rev-parse", "HEAD")
    config_before = _git(cold["data"], "rev-parse", "HEAD")
    code = _respawn(cold, healthy=healthy, apply_ok=apply_ok, seconds="5")
    result = json.loads((cold["data"] / "daemon.restart.result.json").read_text(encoding="utf-8"))
    assert code == 1 and result["ok"] is False and result["rolled_back"]
    assert _git(cold["code"], "rev-parse", "HEAD") == code_before
    assert _git(cold["data"], "rev-parse", "HEAD") == config_before
    assert "migrated" not in _tables(cold["database"])  # the snapshot came back
    assert "rolled back" in (cold["data"] / "runtime" / "faults.jsonl").read_text(encoding="utf-8")


def test_the_respawn_records_how_the_release_went_in_its_plan(cold: dict[str, Path]) -> None:
    before = _git(cold["code"], "rev-parse", "HEAD")
    assert _respawn(cold, healthy=True, apply_ok=True) == 0
    plan = json.loads(cold["plan"].read_text(encoding="utf-8"))
    after = _git(cold["code"], "rev-parse", "HEAD")
    assert plan["outcome"]["ok"] is True and plan["outcome"]["error"] is None
    assert plan["outcome"]["code"] == {str(cold["code"].resolve()): {"before": before, "after": after}}


def test_a_failed_step_before_the_switch_switches_nothing_and_restores_the_database(cold: dict[str, Path]) -> None:
    plan = json.loads(cold["plan"].read_text(encoding="utf-8"))
    plan["pre_switch"] = [["migrate", "--to", "abc123"]]
    cold["plan"].write_text(json.dumps(plan), encoding="utf-8")
    code_before = _git(cold["code"], "rev-parse", "HEAD")
    assert _respawn(cold, healthy=True, apply_ok=False, seconds="10") == 1
    assert _git(cold["code"], "rev-parse", "HEAD") == code_before
    assert "migrated" not in _tables(cold["database"])  # what the old code's step wrote is gone
    recorded = json.loads(cold["plan"].read_text(encoding="utf-8"))
    assert recorded["outcome"]["ok"] is False and "migrate --to abc123 failed" in recorded["outcome"]["error"]
    assert "released_at" not in recorded
