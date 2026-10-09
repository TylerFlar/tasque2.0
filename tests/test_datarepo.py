from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tasque2.config import reset_settings
from tasque2.ops import datarepo
from tasque2.ops.worktree import create_worktrees, remove_worktrees


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=check)


@pytest.fixture()
def data(fresh_db: Path) -> Path:
    """A data directory as a live one looks: config next to the database, the journal and runtime files."""
    root = fresh_db.parent
    (root / "work-templates" / "cooking").mkdir(parents=True)
    (root / "work-templates" / "cooking" / "reply.template.md").write_text("Reply.\n", encoding="utf-8")
    (root / "workflows").mkdir()
    (root / "workflows" / "daily.yaml").write_text("name: daily\n", encoding="utf-8")
    (root / "lanes.json").write_text("{}\n", encoding="utf-8")
    (root / "journal").mkdir()
    (root / "journal" / "2026-10-09.md").write_text("private\n", encoding="utf-8")
    (root / "runtime").mkdir()
    (root / "runtime" / "faults.jsonl").write_text("{}\n", encoding="utf-8")
    return root


def test_init_tracks_only_the_config(data: Path) -> None:
    assert datarepo.init(data, dry_run=True)["would_track"] == [
        "work-templates/cooking/reply.template.md",
        "workflows/daily.yaml",
        "lanes.json",
    ]
    made = datarepo.init(data)
    assert made["created"] is True
    assert set(made["tracked"]) == {
        ".gitattributes",
        ".gitignore",
        "lanes.json",
        "work-templates/cooking/reply.template.md",
        "workflows/daily.yaml",
    }
    for private in ("tasque2.sqlite3", "journal/2026-10-09.md", "runtime/faults.jsonl", "backups/x/y.db", ".env"):
        assert datarepo.ignored(private, root=data), private
    assert datarepo.init(data)["created"] is False  # once


def test_the_hook_refuses_paths_outside_the_allowlist_and_secrets(data: Path) -> None:
    datarepo.init(data)
    _git(data, "add", "-f", "runtime/faults.jsonl")
    refused = _git(data, "commit", "-q", "-m", "sneak", check=False)
    assert refused.returncode != 0 and "outside the config allowlist" in refused.stderr
    _git(data, "reset", "-q")
    (data / "work-templates" / "cooking" / "reply.template.md").write_text(
        "client_secret = abcdefghijklmnopqrstuvwxyz123456\n", encoding="utf-8"
    )
    _git(data, "add", "-A")
    refused = _git(data, "commit", "-q", "-m", "leak", check=False)
    assert refused.returncode != 0 and "reads as a secret" in refused.stderr


def test_a_snapshot_commits_live_edits_and_conflicts_name_files_changed_since_a_base(data: Path) -> None:
    datarepo.init(data)
    base = datarepo.head(data)
    assert datarepo.snapshot("nothing", root=data) is None
    template = data / "work-templates" / "cooking" / "reply.template.md"
    template.write_text("Reply, as a worker changed it.\n", encoding="utf-8")
    assert datarepo.conflicts(base, ["work-templates/cooking/reply.template.md", "lanes.json"], root=data) == [
        "work-templates/cooking/reply.template.md"
    ]
    after = datarepo.snapshot("a worker edit", root=data)
    assert after and after != base
    assert datarepo.changed_files(base, after, root=data) == ["work-templates/cooking/reply.template.md"]
    reverted = datarepo.revert(after, root=data)
    assert template.read_text(encoding="utf-8") == "Reply.\n" and reverted != after


def test_a_change_worktree_nests_the_config_and_merges_by_fast_forward(
    data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "proj"
    project.mkdir()
    _git(project, "init", "-q", "-b", "main")
    _git(project, "config", "user.email", "tests@example.com")
    _git(project, "config", "user.name", "Tests")
    (project / "app.py").write_text("x = 1\n", encoding="utf-8")
    (project / ".gitignore").write_text("data/\nextensions/\n", encoding="utf-8")
    _git(project, "add", ".")
    _git(project, "commit", "-q", "-m", "base")
    monkeypatch.setenv("TASQUE2_PROJECT_DIR", str(project))
    monkeypatch.setenv("TASQUE2_EXTENSIONS_DIR", str(project / "extensions"))
    reset_settings()
    datarepo.init(data)
    trees = create_worktrees("c1", kind="workshop", include_data=True)
    try:
        names = {tree.name: tree for tree in trees}
        assert set(names) == {"core", "data"}
        config = Path(names["data"].path)
        assert config == Path(names["core"].path) / "data" and names["data"].branch == "workshop/c1"
        (config / "lanes.json").write_text('{"threads": {}}\n', encoding="utf-8")
        _git(config, "commit", "-q", "-am", "bind the thread")
        assert datarepo.conflicts(names["data"].base, ["lanes.json"], root=data) == []
        datarepo.merge("workshop/c1", root=data)
        assert (data / "lanes.json").read_text(encoding="utf-8") == '{"threads": {}}\n'
    finally:
        remove_worktrees(trees, delete_branches=True)
    assert not Path(names["core"].path).exists()
