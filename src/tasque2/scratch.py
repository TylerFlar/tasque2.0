"""Per-run scratch directories for provider-backed work.

Workers used to write probe scripts, page dumps, JSON scratch and drafts
straight into the daemon's working directory -- the repository root -- because
that is where a relative path lands (390 such files by 2026-07-30, another
seventeen course-catalog dumps on 2026-09-19). Bash's ``/tmp`` made it worse:
on Windows it is Git Bash's private mapping, so a file written there could not
be read back by Python or PowerShell, and the run failed on the read.

Every provider run now gets one directory, ``data/scratch/<attempt id>``, named
in the prompt and exported as ``TASQUE2_SCRATCH_DIR`` (and as ``TMP``/``TEMP``/
``TMPDIR`` for the provider process tree, so the three shells' temp files
land in the same place). The daemon deletes directories older than the
retention window on the artifact-retention interval.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from tasque2.config import get_settings
from tasque2.models import utc_now

SCRATCH_ENV_VAR = "TASQUE2_SCRATCH_DIR"
# Temp-dir variables the provider subprocess (and everything it spawns) reads.
TEMP_ENV_VARS = ("TMP", "TEMP", "TMPDIR")


@dataclass(frozen=True)
class ScratchPruneResult:
    pruned: int
    bytes_freed: int

    @property
    def megabytes_freed(self) -> float:
        return self.bytes_freed / 1048576


def scratch_dir_for_attempt(attempt_id: str, *, root: Path | None = None) -> Path:
    """The scratch directory for one attempt, created if it does not exist."""
    base = root if root is not None else get_settings().resolved_scratch_dir
    path = base / attempt_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def scratch_environment(path: Path) -> dict[str, str]:
    """Environment variables that point a provider process tree at ``path``."""
    value = str(path)
    env = {SCRATCH_ENV_VAR: value}
    for name in TEMP_ENV_VARS:
        env[name] = value
    return env


def prune_scratch_dirs(
    *,
    root: Path | None = None,
    older_than_days: int | None = None,
) -> ScratchPruneResult:
    """Delete scratch directories older than the retention window.

    Age is the directory's own mtime: when the run created it or last added a
    top-level entry. A run lasts minutes to hours and the default window is
    days, so nothing still in flight can age out. Directories that cannot be
    removed (a file held open) are skipped and revisited on the next pass.
    """
    settings = get_settings()
    window = settings.scratch_retention_days if older_than_days is None else older_than_days
    if window <= 0:
        return ScratchPruneResult(pruned=0, bytes_freed=0)
    base = root if root is not None else settings.resolved_scratch_dir
    if not base.is_dir():
        return ScratchPruneResult(pruned=0, bytes_freed=0)

    cutoff = (utc_now() - timedelta(days=window)).timestamp()
    pruned = 0
    freed = 0
    for entry in sorted(base.iterdir()):
        if not entry.is_dir() or entry.is_symlink():
            continue
        try:
            stat = entry.stat()
        except OSError:
            continue
        if stat.st_mtime >= cutoff:
            continue
        size = _tree_size(entry)
        shutil.rmtree(entry, ignore_errors=True)
        if entry.exists():
            continue
        pruned += 1
        freed += size
    return ScratchPruneResult(pruned=pruned, bytes_freed=freed)


def _tree_size(path: Path) -> int:
    total = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for name in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, name))
            except OSError:
                continue
    return total
