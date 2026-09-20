"""Per-run scratch directories.

Workers wrote probe scripts, page dumps and drafts into the daemon's working
directory (the repository root) because that is where a relative path lands,
and used Bash's ``/tmp`` which on Windows nothing else can read back. Each run
now gets its own directory, named in the prompt and exported as the process
tree's temp dir, and the daemon sweeps aged ones.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.config import get_settings, reset_settings
from tasque2.daemon import TasqueDaemon
from tasque2.db import session_scope
from tasque2.models import WorkAttempt
from tasque2.providers import FakeProvider, ProviderRegistry, ProviderRuntime
from tasque2.repo import WorkRepository
from tasque2.runtime import WorkRunner
from tasque2.scratch import prune_scratch_dirs


def _aged_dir(root: Path, name: str, *, age_days: float, files: int = 1) -> Path:
    path = root / name
    path.mkdir(parents=True)
    for index in range(files):
        (path / f"probe-{index}.txt").write_text("x" * 1024, encoding="utf-8")
    stamp = time.time() - age_days * 86400
    os.utime(path, (stamp, stamp))
    return path


def test_provider_run_gets_a_scratch_dir_in_prompt_and_environment(fresh_db: Path) -> None:
    captured = []
    registry = ProviderRegistry()
    registry.register(FakeProvider(capture_requests=captured))

    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Scratch work",
            task_instruction="Do a thing.",
            worker_kind="provider.fake",
            runtime_contract={"env": {"TEMP": r"C:\keep\mine"}},
        )
        WorkRunner(session, provider_runtime=ProviderRuntime(registry=registry)).run_next()
        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
        assert attempt is not None
        attempt_id = attempt.id

    request = captured[0]
    scratch = Path(request.env["TASQUE2_SCRATCH_DIR"])
    assert scratch == get_settings().resolved_scratch_dir / attempt_id
    assert scratch.is_dir(), "the directory must exist before the provider starts"
    # The process tree's temp dir follows, so Python/PowerShell temp files land there too...
    assert request.env["TMP"] == str(scratch)
    assert request.env["TMPDIR"] == str(scratch)
    # ...unless the work item's own contract pinned one.
    assert request.env["TEMP"] == r"C:\keep\mine"
    # The prompt names the path and forbids the two habits that made the mess.
    assert "## Scratch Space" in request.prompt
    assert str(scratch) in request.prompt
    assert "never use Bash's `/tmp`" in request.prompt
    assert request.context["tasque_context_packet"]["scratch_dir"] == str(scratch)


def test_prune_removes_only_aged_directories(tmp_path: Path) -> None:
    root = tmp_path / "scratch"
    old = _aged_dir(root, "old-run", age_days=10, files=3)
    fresh = _aged_dir(root, "fresh-run", age_days=1)
    stray = root / "notes.txt"
    stray.write_text("keep", encoding="utf-8")

    result = prune_scratch_dirs(root=root, older_than_days=7)

    assert result.pruned == 1
    assert result.bytes_freed == 3 * 1024
    assert not old.exists()
    assert fresh.is_dir()
    assert stray.is_file()  # only directories are run scratch; files are left alone


def test_prune_zero_window_and_missing_root_are_no_ops(tmp_path: Path) -> None:
    root = tmp_path / "scratch"
    old = _aged_dir(root, "old-run", age_days=400)

    assert prune_scratch_dirs(root=root, older_than_days=0).pruned == 0
    assert old.is_dir()
    assert prune_scratch_dirs(root=tmp_path / "absent", older_than_days=7).pruned == 0


def test_daemon_tick_sweeps_scratch_once_per_interval(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_ARTIFACT_RETENTION_INTERVAL_SECONDS", "3600")
    reset_settings()
    try:
        root = get_settings().resolved_scratch_dir
        first = _aged_dir(root, "first", age_days=30)

        with session_scope() as session:
            result = TasqueDaemon(session).run_once(max_work_items=0)
        assert result.scratch_dirs_pruned == 1
        assert not first.exists()

        # Another aged directory appears, but the interval has not elapsed.
        second = _aged_dir(root, "second", age_days=30)
        with session_scope() as session:
            result = TasqueDaemon(session).run_once(max_work_items=0)
        assert result.scratch_dirs_pruned == 0
        assert second.is_dir()
    finally:
        reset_settings()
