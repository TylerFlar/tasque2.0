"""Per-run scratch directories.

Every provider run gets ``data/scratch/<attempt id>``: it is named in the prompt and
exported as ``TASQUE2_SCRATCH_DIR`` and as ``TMP``/``TEMP``/``TMPDIR`` for the whole
provider process tree, so temporary files never land in the project directory. The daemon
deletes directories older than the retention window.
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
TEMP_ENV_VARS = ("TMP", "TEMP", "TMPDIR")


@dataclass(frozen=True)
class ScratchPruneResult:
    pruned: int
    bytes_freed: int

    @property
    def megabytes_freed(self) -> float:
        return self.bytes_freed / 1048576


def scratch_dir_for_attempt(attempt_id: str, *, root: Path | None = None) -> Path:
    path = (root if root is not None else get_settings().resolved_scratch_dir) / attempt_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def scratch_environment(path: Path) -> dict[str, str]:
    value = str(path)
    return {SCRATCH_ENV_VAR: value, **{name: value for name in TEMP_ENV_VARS}}


def prune_scratch_dirs(*, root: Path | None = None, older_than_days: int | None = None) -> ScratchPruneResult:
    """Delete scratch directories whose own mtime is older than the retention window.

    Directories that cannot be removed yet (a file held open) are retried next pass.
    """
    settings = get_settings()
    window = settings.scratch_retention_days if older_than_days is None else older_than_days
    base = root if root is not None else settings.resolved_scratch_dir
    if window <= 0 or not base.is_dir():
        return ScratchPruneResult(pruned=0, bytes_freed=0)
    cutoff = (utc_now() - timedelta(days=window)).timestamp()
    pruned = freed = 0
    for entry in sorted(base.iterdir()):
        if not entry.is_dir() or entry.is_symlink():
            continue
        try:
            if entry.stat().st_mtime >= cutoff:
                continue
        except OSError:
            continue
        size = _tree_size(entry)
        shutil.rmtree(entry, ignore_errors=True)
        if not entry.exists():
            pruned += 1
            freed += size
    return ScratchPruneResult(pruned=pruned, bytes_freed=freed)


def _tree_size(path: Path) -> int:
    total = 0
    for dirpath, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(dirpath, name))
            except OSError:
                continue
    return total
