"""Throwaway git worktrees for a repair or a Workshop change: the core repository, every extension
repository in it, and (for a change) the config repository rooted at the data directory.

A repair or a change works on its own branch in its own folder, next to the live checkout, so the code
the daemon runs never changes until the user approves and the restart fast-forwards it. Each worktree
records the live HEAD it started from and a hash of the live checkout's status, so a check afterwards
can prove the live checkouts were left alone. A change's config worktree sits at ``<root>/data``, so
the root is a whole staging Tasque: code, extensions, config, and the database copy a rehearsal puts
there (ignored by git).
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from tasque2.config import get_settings


class WorktreeError(RuntimeError):
    """A git command a worktree needs failed."""


@dataclass(frozen=True)
class RepoWorktree:
    name: str
    live: str
    path: str
    branch: str
    base: str
    live_status: str

    def data(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> RepoWorktree:
        return cls(**{field: str(data[field]) for field in cls.__dataclass_fields__})


def git(repo: Path | str, *args: str, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if check and completed.returncode != 0:
        raise WorktreeError(f"git {' '.join(args)} in {repo}: {completed.stderr.strip()[:400]}")
    return completed.stdout.strip()


def status_hash(repo: Path | str) -> str:
    return hashlib.sha256(git(repo, "status", "--porcelain", "--untracked-files=no").encode("utf-8")).hexdigest()


def workspace_root(repair_id: str, kind: str = "repair") -> Path:
    project = get_settings().resolved_project_dir
    return project.parent / f"{project.name}-{kind}" / repair_id


def _extension_repos() -> list[Path]:
    root = get_settings().resolved_extensions_dir
    if not root.is_dir():
        return []
    return sorted(child for child in root.iterdir() if child.is_dir() and (child / ".git").exists())


def create_worktrees(repair_id: str, *, kind: str = "repair", include_data: bool = False) -> list[RepoWorktree]:
    """A worktree of the core repository and, nested in it, one of each extension repository (and, with
    ``include_data``, one of the config repository at ``data``)."""
    from tasque2.ops.datarepo import is_repo

    project = get_settings().resolved_project_dir
    root = workspace_root(repair_id, kind)
    branch = f"{kind}/{repair_id}"
    made: list[RepoWorktree] = []
    try:
        root.parent.mkdir(parents=True, exist_ok=True)
        base = git(project, "rev-parse", "HEAD")
        git(project, "worktree", "add", "-q", "-b", branch, str(root), base)
        made.append(RepoWorktree("core", str(project), str(root), branch, base, status_hash(project)))
        for repo in _extension_repos():
            target = root / "extensions" / repo.name
            target.parent.mkdir(parents=True, exist_ok=True)
            base = git(repo, "rev-parse", "HEAD")
            git(repo, "worktree", "add", "-q", "-b", branch, str(target), base)
            made.append(RepoWorktree(repo.name, str(repo), str(target), branch, base, status_hash(repo)))
        data = get_settings().resolved_data_dir
        if include_data and is_repo(data):
            base = git(data, "rev-parse", "HEAD")
            git(data, "worktree", "add", "-q", "-b", branch, str(root / "data"), base)
            made.append(RepoWorktree("data", str(data), str(root / "data"), branch, base, status_hash(data)))
    except WorktreeError:
        remove_worktrees(made, delete_branches=True)
        raise
    return made


def remove_worktrees(worktrees: list[RepoWorktree], *, delete_branches: bool) -> None:
    """Remove the worktree folders (nested ones first); with ``delete_branches`` also their branches."""
    for tree in sorted(worktrees, key=lambda tree: tree.name == "core"):
        git(tree.live, "worktree", "remove", "--force", tree.path, check=False)
        if delete_branches:
            git(tree.live, "branch", "-D", tree.branch, check=False)
    for tree in worktrees:
        if tree.name == "core":
            shutil.rmtree(tree.path, ignore_errors=True)
            git(tree.live, "worktree", "prune", check=False)
            try:
                Path(tree.path).parent.rmdir()  # the repairs folder, once its last repair is gone
            except OSError:
                pass


def live_changes(worktrees: list[RepoWorktree]) -> list[str]:
    """Live checkouts whose HEAD or status changed since the worktrees were made."""
    changed = []
    for tree in worktrees:
        if git(tree.live, "rev-parse", "HEAD") != tree.base or status_hash(tree.live) != tree.live_status:
            changed.append(tree.name)
    return changed


def committed_files(tree: RepoWorktree) -> list[str]:
    """Files the repair branch changed in commits since its base."""
    output = git(tree.path, "diff", "--name-only", f"{tree.base}..HEAD")
    return [line for line in output.splitlines() if line.strip()]


def uncommitted(tree: RepoWorktree) -> list[str]:
    return [line[3:] for line in git(tree.path, "status", "--porcelain").splitlines() if line.strip()]


def has_commits(tree: RepoWorktree) -> bool:
    return git(tree.path, "rev-parse", "HEAD") != tree.base
