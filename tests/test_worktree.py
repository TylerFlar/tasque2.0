"""A change's worktrees: the core and each extension, next to the live checkout, which they notice moving."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tasque2.config import reset_settings
from tasque2.ops.worktree import create_worktrees, live_changes, remove_worktrees


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


def _init(repo: Path, files: dict[str, str]) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "tests@example.com")
    _git(repo, "config", "user.name", "Tests")
    for name, text in files.items():
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(text, encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "base")


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "proj"
    _init(root, {"src/app.py": "x = 1\n", ".gitignore": "extensions/*\n"})
    _init(
        root / "extensions" / "ext1",
        {"__init__.py": "def register(registry):\n    pass\n", "tests/test_ok.py": "def test_ok():\n    pass\n"},
    )
    monkeypatch.setenv("TASQUE2_PROJECT_DIR", str(root))
    monkeypatch.setenv("TASQUE2_EXTENSIONS_DIR", str(root / "extensions"))
    reset_settings()
    return root


def test_worktrees_cover_core_and_each_extension_and_notice_a_moving_live_checkout(project: Path) -> None:
    trees = create_worktrees("t1")
    try:
        names = {tree.name: tree for tree in trees}
        assert set(names) == {"core", "ext1"}
        assert Path(names["ext1"].path) == Path(names["core"].path) / "extensions" / "ext1"
        assert Path(names["core"].path).parent.name == "proj-workshop"
        assert (Path(names["core"].path) / "src" / "app.py").is_file()
        assert live_changes(trees) == []
        (project / "src" / "app.py").write_text("x = 99\n", encoding="utf-8")
        assert live_changes(trees) == ["core"]
    finally:
        _git(project, "checkout", "--", "src/app.py")
        remove_worktrees(trees, delete_branches=True)
    assert not Path(trees[0].path).exists()
    assert not Path(trees[0].path).parent.exists()  # no empty changes' folder left behind
    assert _git(project, "branch", "--list", "workshop/t1") == ""
