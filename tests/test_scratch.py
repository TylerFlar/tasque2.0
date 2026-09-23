from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from tasque2.config import get_settings, reset_settings
from tasque2.daemon import DaemonTick
from tasque2.db import session_scope
from tasque2.providers import FakeProvider, ProviderRegistry, ProviderRequest
from tasque2.scratch import SCRATCH_ENV_VAR, prune_scratch_dirs, scratch_dir_for_attempt, scratch_environment
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner
from tasque2.worker.runtime import ProviderRuntime


def _aged_dir(root: Path, name: str, *, age_days: float, files: int = 1) -> Path:
    path = root / name
    path.mkdir(parents=True)
    for index in range(files):
        (path / f"probe-{index}.txt").write_text("x" * 1024, encoding="utf-8")
    stamp = time.time() - age_days * 86400
    os.utime(path, (stamp, stamp))
    return path


def _pruned_scratch(metric_points) -> int:
    return sum(
        point.value
        for point in metric_points("tasque.retention.pruned")
        if point.attributes.get("tasque.retention.kind") == "scratch"
    )


def test_provider_run_gets_a_scratch_dir_in_prompt_and_environment(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    registry = ProviderRegistry()
    registry.register(FakeProvider(capture_requests=captured))

    with session_scope() as session:
        WorkRepository(session).create_work_item(
            title="Scratch work",
            task_instruction="Do a thing.",
            worker_kind="provider.fake",
            runtime_contract={"env": {"TEMP": r"C:\keep\mine"}},
        )
        outcome = WorkRunner(session, provider_runtime=ProviderRuntime(registry=registry)).run_next()

    assert outcome.status == "succeeded"
    [request] = captured
    scratch = Path(request.env[SCRATCH_ENV_VAR])
    assert scratch == get_settings().resolved_scratch_dir / outcome.attempt_id
    assert scratch.is_dir()
    assert request.env["TMP"] == str(scratch)
    assert request.env["TMPDIR"] == str(scratch)
    assert request.env["TEMP"] == r"C:\keep\mine"
    assert f"- scratch directory: {scratch}" in request.prompt
    assert SCRATCH_ENV_VAR in request.system_prompt_path.read_text(encoding="utf-8")


def test_scratch_dir_for_attempt_creates_the_directory(isolated: Path, tmp_path: Path) -> None:
    default = scratch_dir_for_attempt("attempt-1")
    custom = scratch_dir_for_attempt("attempt-2", root=tmp_path / "elsewhere")

    assert default == isolated / "data" / "scratch" / "attempt-1"
    assert default.is_dir()
    assert custom == tmp_path / "elsewhere" / "attempt-2"
    assert custom.is_dir()
    assert scratch_dir_for_attempt("attempt-1") == default


def test_scratch_environment_points_every_temp_variable_at_the_directory(tmp_path: Path) -> None:
    value = str(tmp_path)

    assert scratch_environment(tmp_path) == {SCRATCH_ENV_VAR: value, "TMP": value, "TEMP": value, "TMPDIR": value}


def test_prune_removes_only_aged_directories(tmp_path: Path) -> None:
    root = tmp_path / "scratch"
    old = _aged_dir(root, "old-run", age_days=10, files=3)
    fresh = _aged_dir(root, "fresh-run", age_days=1)
    stray = root / "notes.txt"
    stray.write_text("keep", encoding="utf-8")

    result = prune_scratch_dirs(root=root, older_than_days=7)

    assert result.pruned == 1
    assert result.bytes_freed == 3 * 1024
    assert result.megabytes_freed == pytest.approx(3 * 1024 / 1048576)
    assert not old.exists()
    assert fresh.is_dir()
    assert stray.is_file()


def test_prune_zero_window_and_missing_root_are_no_ops(tmp_path: Path) -> None:
    root = tmp_path / "scratch"
    old = _aged_dir(root, "old-run", age_days=400)

    assert prune_scratch_dirs(root=root, older_than_days=0).pruned == 0
    assert old.is_dir()
    assert prune_scratch_dirs(root=tmp_path / "absent", older_than_days=7).pruned == 0


def test_prune_uses_the_configured_retention_window(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_SCRATCH_RETENTION_DAYS", "3")
    reset_settings()
    root = get_settings().resolved_scratch_dir
    aged = _aged_dir(root, "aged", age_days=5)
    recent = _aged_dir(root, "recent", age_days=2)

    assert prune_scratch_dirs().pruned == 1
    assert not aged.exists()
    assert recent.is_dir()


def test_daemon_tick_sweeps_scratch_once_per_interval(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch, metric_points
) -> None:
    monkeypatch.setenv("TASQUE2_ARTIFACT_RETENTION_INTERVAL_SECONDS", "3600")
    reset_settings()
    root = get_settings().resolved_scratch_dir
    first = _aged_dir(root, "first", age_days=30)
    before = _pruned_scratch(metric_points)
    tick = DaemonTick()

    with session_scope() as session:
        result = tick.run(session, max_claims=0)
    assert result.scratch_pruned == 1
    assert not first.exists()
    assert _pruned_scratch(metric_points) - before == 1

    second = _aged_dir(root, "second", age_days=30)
    with session_scope() as session:
        result = tick.run(session, max_claims=0)
    assert result.scratch_pruned == 0
    assert second.is_dir()
