"""The config repository: a private git repository rooted at the data directory, tracking only config.

The data directory holds the database, the journal, runtime files and backups next to the config a
change to Tasque edits: the work templates, the workflow files, the lanes manifest, the privacy
denylist, and a tracked export of the doctrine documents (``doctrine/``, compared against, never
applied from). Only those are tracked: an allowlist ``.gitignore`` ignores everything else, and a
pre-commit hook refuses any other path, any file over a megabyte, and anything that reads as a secret.
The repository has no remote; nothing in it ever leaves the machine.

With it, a change to the config is a branch: staged in a worktree, diffed, merged by fast-forward, and
reverted. ``snapshot`` first commits whatever changed in the live config since (workers and the user
edit it), so a release can tell a file it means to change from one somebody else changed meanwhile
(``conflicts``).
"""

from __future__ import annotations

import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

from tasque2.config import get_settings

TRACKED = (
    "work-templates/",
    "workflows/",
    "lanes.json",
    "privacy-denylist.txt",
    "backup.toml",
    "doctrine/",
    "changes/",
)
GITIGNORE = """\
# Tasque config: only these paths are tracked. The database, the journal, runtime files, backups,
# artifacts and secrets never are.
/*
!/.gitignore
!/.gitattributes
!/work-templates/
!/workflows/
!/lanes.json
!/privacy-denylist.txt
!/backup.toml
!/doctrine/
/doctrine/snapshot.json
!/changes/
"""
GITATTRIBUTES = "* -text\n"
MAX_FILE_BYTES = 1_000_000
SECRET_PATTERN = re.compile(
    r"(?i)(api[_-]?key|client[_-]?secret|refresh[_-]?token|access[_-]?token|password|bearer)\s*[:=]\s*['\"]?"
    r"[A-Za-z0-9_\-./+]{16,}"
)
HOOK = """\
#!/bin/sh
# Installed by Tasque: refuse config commits outside the allowlist, over a megabyte, or holding a secret.
exec "{python}" -m tasque2.ops.datarepo pre-commit
"""


class DataRepoError(RuntimeError):
    """A git command on the config repository failed."""


def data_dir() -> Path:
    return get_settings().resolved_data_dir


def git(repo: Path | str, *args: str, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if check and completed.returncode != 0:
        raise DataRepoError(f"git {' '.join(args)} in {repo}: {completed.stderr.strip()[:400]}")
    return completed.stdout.strip()


def is_repo(path: Path | None = None) -> bool:
    return ((path or data_dir()) / ".git").exists()


def _write_hook(root: Path) -> None:
    hooks = Path(git(root, "rev-parse", "--git-path", "hooks"))
    hooks = hooks if hooks.is_absolute() else root / hooks
    hooks.mkdir(parents=True, exist_ok=True)
    hook = hooks / "pre-commit"
    hook.write_text(HOOK.format(python=Path(sys.executable).as_posix()), encoding="utf-8", newline="\n")
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def allowed(name: str) -> bool:
    """True for a path the config repository may hold."""
    return name in (".gitignore", ".gitattributes") or any(
        name == entry.rstrip("/") or name.startswith(entry) for entry in TRACKED
    )


def staged_problems(root: Path | str = ".") -> list[str]:
    """What the pre-commit hook refuses among the staged files."""
    names = git(root, "diff", "--cached", "--name-only", "--diff-filter=ACM").splitlines()
    problems = []
    for name in filter(None, (line.strip() for line in names)):
        if not allowed(name):
            problems.append(f"{name}: outside the config allowlist")
            continue
        blob = subprocess.run(["git", "-C", str(root), "show", f":{name}"], capture_output=True).stdout
        if len(blob) > MAX_FILE_BYTES:
            problems.append(f"{name}: over a megabyte")
        elif SECRET_PATTERN.search(blob.decode("utf-8", "replace")):
            problems.append(f"{name}: reads as a secret")
    return problems


def refresh_doctrine(root: Path) -> int:
    """Export the live doctrine into ``doctrine/`` (secrets left out), replacing what was there."""
    from tasque2.db import session_scope
    from tasque2.ops.doctrine import SNAPSHOT_FILE, export_doctrine

    target = root / "doctrine"
    if target.exists():
        shutil.rmtree(target)
    with session_scope() as session:
        entries = export_doctrine(session, target)
    (target / SNAPSHOT_FILE).unlink(missing_ok=True)  # the hashes live in git, not in a tracked file
    return len(entries)


def would_track(root: Path) -> list[str]:
    """What an init would track: every file under the allowlisted paths."""
    found = []
    for entry in TRACKED:
        target = root / entry.rstrip("/")
        if target.is_file():
            found.append(entry.rstrip("/"))
        elif target.is_dir():
            found += sorted(p.relative_to(root).as_posix() for p in target.rglob("*") if p.is_file())
    return found


def init(path: Path | None = None, *, dry_run: bool = False, doctrine: bool = True) -> dict[str, Any]:
    """Make the data directory a config repository (once): the allowlist, the hook, a first snapshot."""
    root = path or data_dir()
    if is_repo(root):
        return {"created": False, "tracked": tracked_files(root)}
    if dry_run:
        return {"created": False, "would_track": would_track(root)}
    root.mkdir(parents=True, exist_ok=True)
    (root / ".gitignore").write_text(GITIGNORE, encoding="utf-8", newline="\n")
    (root / ".gitattributes").write_text(GITATTRIBUTES, encoding="utf-8", newline="\n")
    git(root, "init", "-q", "-b", "main")
    git(root, "config", "core.autocrlf", "false")
    git(root, "config", "user.name", "Tasque")
    git(root, "config", "user.email", "tasque@localhost")
    _write_hook(root)
    if doctrine:
        refresh_doctrine(root)
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", "Tasque config: the first snapshot")
    return {"created": True, "tracked": tracked_files(root), "head": head(root)}


def tracked_files(root: Path | None = None) -> list[str]:
    return [line for line in git(root or data_dir(), "ls-files").splitlines() if line.strip()]


def head(root: Path | None = None) -> str:
    return git(root or data_dir(), "rev-parse", "HEAD")


def snapshot(reason: str, *, root: Path | None = None, doctrine: bool = True) -> str | None:
    """Commit what changed in the live config since the last snapshot; the new head, or None if nothing."""
    repo = root or data_dir()
    if doctrine:
        refresh_doctrine(repo)
    git(repo, "add", "-A")
    if not git(repo, "status", "--porcelain"):
        return None
    git(repo, "commit", "-q", "-m", f"live: {reason}")
    return head(repo)


def changed_files(base: str, ref: str = "HEAD", *, root: Path | None = None) -> list[str]:
    output = git(root or data_dir(), "diff", "--name-only", f"{base}..{ref}")
    return [line for line in output.splitlines() if line.strip()]


def file_at(ref: str, name: str, *, root: Path | None = None) -> bytes | None:
    """A file's bytes at ``ref``, or None when it did not exist there."""
    completed = subprocess.run(["git", "-C", str(root or data_dir()), "show", f"{ref}:{name}"], capture_output=True)
    return completed.stdout if completed.returncode == 0 else None


def conflicts(base: str, files: list[str], *, root: Path | None = None) -> list[str]:
    """Files a change means to touch whose live bytes no longer match ``base``: somebody changed them."""
    repo = root or data_dir()
    found = []
    for name in files:
        path = repo / name
        live = path.read_bytes() if path.is_file() else None
        if live != file_at(base, name, root=repo):
            found.append(name)
    return found


def merge(branch: str, *, root: Path | None = None) -> str:
    """Fast-forward the live config to ``branch``; the new head."""
    repo = root or data_dir()
    git(repo, "merge", "--ff-only", "-q", branch)
    return head(repo)


def revert(sha: str, *, root: Path | None = None) -> str:
    """Undo one commit with a new one; the new head."""
    repo = root or data_dir()
    git(repo, "revert", "--no-edit", sha)
    return head(repo)


def ignored(name: str, *, root: Path | None = None) -> bool:
    """True when the allowlist keeps ``name`` out of the repository."""
    completed = subprocess.run(["git", "-C", str(root or data_dir()), "check-ignore", "-q", name], capture_output=True)
    return completed.returncode == 0


def main(argv: list[str] | None = None) -> int:
    """``python -m tasque2.ops.datarepo pre-commit``: the hook's check, run in the repository."""
    args = sys.argv[1:] if argv is None else argv
    if args[:1] != ["pre-commit"]:
        print("usage: python -m tasque2.ops.datarepo pre-commit", file=sys.stderr)
        return 2
    problems = staged_problems(".")
    if problems:
        print("config commit refused:\n  " + "\n  ".join(problems), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
