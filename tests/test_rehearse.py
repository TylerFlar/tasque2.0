from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.ops import datarepo
from tasque2.ops.rehearse import _last_json, rehearse, staging_env
from tasque2.ops.release import ReleasePlan

DOCUMENT = "doctrine/cooking/cooking_direction.md"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


def _content(database: Path) -> list[str]:
    db = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        return [
            row[0]
            for row in db.execute(
                "select content from memories where namespace = 'cooking' and canonical_key = 'cooking_direction' "
                "and superseded_by is null and archived_at is null"
            )
        ]
    finally:
        db.close()


def test_a_rehearsal_lands_the_change_on_a_copy_and_leaves_the_live_database_alone(
    fresh_db: Path, tmp_path: Path
) -> None:
    with session_scope() as session:
        MemoryService(session).upsert_canonical(
            namespace="cooking",
            canonical_key="cooking_direction",
            kind="doctrine",
            content="Cook simply.\n",
            pinned=True,
        )
    root = tmp_path / "change"
    data = root / "data"
    datarepo.init(data)
    base = datarepo.head(data)
    _git(data, "checkout", "-q", "-b", "workshop/c1")
    (data / DOCUMENT).write_text("Cook simply; one pan.\n", encoding="utf-8")
    _git(data, "commit", "-q", "-am", "one pan")
    plan = ReleasePlan(id="c1", change_id="c1", title="One pan", data={"branch": "workshop/c1", "base": base})
    outcome = rehearse(root, plan)
    assert outcome.ok, outcome.steps
    assert [step["step"] for step in outcome.steps] == [
        "migrate",
        "release-apply",
        "release-apply again changes nothing",
        "release-apply --undo",
        "release-apply after the undo",
        "release-check",
    ]
    assert _content(root / "data" / "tasque2.sqlite3") == ["Cook simply; one pan.\n"]
    assert _content(get_settings().database_path) == ["Cook simply.\n"]


def test_the_staging_environment_points_inside_the_root_and_carries_no_secret(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TASQUE2_DISCORD_TOKEN", "live-token")
    monkeypatch.setenv("SOME_API_KEY", "x")
    env = staging_env(tmp_path)
    assert env["TASQUE2_DATA_DIR"] == str(tmp_path / "data") and env["PYTHONPATH"] == str(tmp_path / "src")
    assert env["TASQUE2_DISCORD_TOKEN"] == "" and "SOME_API_KEY" not in env and env["TASQUE2_REHEARSAL"] == "1"


def test_the_last_json_a_command_printed_is_read_on_one_line_or_indented() -> None:
    assert _last_json('noise\n{"a": 1}\n') == {"a": 1}
    assert (
        _last_json('{\n  "prompt_chars": 5,\n  "packet": {\n    "x": [\n      {"y": 1}\n    ]\n  }\n}\n')[
            "prompt_chars"
        ]
        == 5
    )
    assert _last_json("nothing here") is None
