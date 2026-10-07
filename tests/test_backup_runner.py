from __future__ import annotations

import json
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.models import WorkItem, utc_now
from tasque2.ops import backup_runner
from tasque2.ops.backup_runner import (
    BackupConfig,
    ResticResult,
    backup_health,
    backup_worker,
    excluded_by,
    read_state,
    run_backup,
    stage_dir,
)
from tasque2.ops.health import build_system_health
from tasque2.work.runner import default_function_registry


@pytest.fixture()
def vault(monkeypatch: pytest.MonkeyPatch) -> dict[tuple[str, str], str]:
    import keyring

    store: dict[tuple[str, str], str] = {}
    monkeypatch.setattr(keyring, "get_password", lambda service, user: store.get((service, user)))
    monkeypatch.setattr(keyring, "set_password", lambda service, user, value: store.__setitem__((service, user), value))
    return store


class FakeRestic:
    """Stands in for the restic binary: records calls, answers like restic would."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.fail: set[str] = set()
        self.tamper_restore = False

    def __call__(self, config: BackupConfig, *args: str, **kwargs: Any) -> ResticResult:
        self.calls.append(args)
        command = args[0]
        if command in self.fail:
            return ResticResult(False, "", f"Fatal: {command} failed", 0.1)
        if command == "backup":
            summary = {
                "message_type": "summary",
                "snapshot_id": "abc123",
                "files_new": 3,
                "files_changed": 1,
                "data_added": 2_500_000,
                "total_bytes_processed": 9_000_000,
            }
            return ResticResult(True, '{"message_type":"status"}\n' + json.dumps(summary) + "\n", "", 1.0)
        if command == "restore":
            target = Path(args[args.index("--target") + 1])
            copy = target / "G" / "data" / "backup" / "stage"
            copy.mkdir(parents=True)
            shutil.copy2(stage_dir() / "tasque2.sqlite3", copy / "tasque2.sqlite3")
            manifest = json.loads((stage_dir() / "manifest.json").read_text(encoding="utf-8"))
            if self.tamper_restore:
                manifest["row_counts"]["work_items"] = 999
            (copy / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            return ResticResult(True, "", "", 0.5)
        return ResticResult(True, "", "", 0.1)

    def commands(self) -> list[str]:
        return [call[0] for call in self.calls]


@pytest.fixture()
def fake_restic(monkeypatch: pytest.MonkeyPatch) -> FakeRestic:
    fake = FakeRestic()
    monkeypatch.setattr(backup_runner, "restic", fake)
    return fake


@pytest.fixture()
def configured(fresh_db: Path, tmp_path: Path, vault) -> BackupConfig:
    source = tmp_path / "project"
    (source / "keep").mkdir(parents=True)
    (source / "keep" / "file.txt").write_text("x", encoding="utf-8")
    backup_runner.write_starter_config(str(tmp_path / "repo"))
    config_file = backup_runner.config_path()
    text = config_file.read_text(encoding="utf-8")
    text = text.split("sources = [")[0]
    text += f'sources = ["{source.as_posix()}", "{(tmp_path / "gone").as_posix()}"]\n'
    text += 'excludes = ["**/__pycache__"]\n'
    config_file.write_text(text, encoding="utf-8")
    backup_runner.ensure_password()
    return backup_runner.load_config()


def test_a_run_snapshots_the_database_backs_up_and_records_success(configured, fake_restic) -> None:
    with session_scope() as session:
        session.add(WorkItem(title="t", task_instruction="x", worker_kind="function.echo"))

    run = run_backup()

    assert run["ok"] is True
    assert run["database"]["check"] == "ok"
    assert run["backup"]["snapshot_id"] == "abc123"
    assert run["backup"]["data_added"] == 2_500_000
    assert run["missing_sources"] == [configured.sources[1]]
    backup_args = fake_restic.calls[0]
    assert backup_args[0] == "backup"
    assert str(stage_dir()) in backup_args
    assert "--exclude" in backup_args
    manifest = json.loads((stage_dir() / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["row_counts"]["work_items"] == 1
    state = read_state()
    assert state["last_snapshot_id"] == "abc123"
    assert "last_error" not in state


def test_prune_check_and_restore_test_run_when_due_and_not_again_soon(configured, fake_restic) -> None:
    run_backup()
    assert fake_restic.commands() == ["backup", "forget", "check", "restore"]

    fake_restic.calls.clear()
    second = run_backup(now=utc_now() + timedelta(days=1))
    assert fake_restic.commands() == ["backup"]
    assert "maintenance" not in second

    fake_restic.calls.clear()
    run_backup(now=utc_now() + timedelta(days=8))
    assert fake_restic.commands() == ["backup", "forget", "check"]
    assert read_state()["last_restore_test_ok"] is True


def test_a_restore_that_does_not_match_the_manifest_fails_the_test(configured, fake_restic) -> None:
    fake_restic.tamper_restore = True

    run = run_backup()

    assert run["ok"] is True  # the backup itself worked
    assert run["restore_test"]["ok"] is False
    assert "work_items" in run["restore_test"]["mismatched_tables"]
    assert "the last restore test failed" in backup_health()["attention"]


def test_a_failed_backup_is_recorded_not_raised_and_health_says_so(configured, fake_restic) -> None:
    fake_restic.fail.add("backup")

    run = run_backup()

    assert run["ok"] is False
    assert "Fatal: backup failed" in run["backup"]["error"]
    assert fake_restic.commands() == ["backup"]  # no maintenance on a failed run
    health = backup_health()
    assert any("failed" in line for line in health["attention"])
    assert "no backup has succeeded yet" in health["attention"]


def test_health_flags_a_backup_older_than_its_window(configured, fake_restic) -> None:
    run_backup()
    assert backup_health()["attention"] == []

    later = backup_health(now=utc_now() + timedelta(hours=72))
    assert later["attention"] == ["the last good backup is 72 hours old"]


def test_system_health_reports_backups_and_names_their_problems(configured, fake_restic) -> None:
    fake_restic.fail.add("backup")
    run_backup()

    with session_scope() as session:
        health = build_system_health(session)

    assert health["backups"]["configured"] is True
    assert any(line.startswith("backups: ") for line in health["attention"])
    assert "faults" in health


def test_unconfigured_backups_stay_out_of_health_attention(fresh_db: Path) -> None:
    assert backup_health() == {"configured": False, "attention": []}
    with session_scope() as session:
        health = build_system_health(session)
    assert not any(line.startswith("backups:") for line in health["attention"])


def test_the_password_reaches_restic_only_through_its_environment(isolated: Path, vault, monkeypatch) -> None:
    seen: dict[str, Any] = {}

    def fake_run(args, **kwargs):
        seen["args"], seen["env"] = args, kwargs["env"]
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    password, created = backup_runner.ensure_password()
    config = BackupConfig(repository="F:/repo", sources=["x"])

    result = backup_runner.restic(config, "snapshots")

    assert created and result.ok
    assert password not in " ".join(seen["args"])
    assert seen["env"]["RESTIC_PASSWORD"] == password
    assert seen["env"]["RESTIC_REPOSITORY"] == "F:/repo"
    assert backup_runner.ensure_password() == (password, False)


def test_a_missing_password_fails_cleanly_without_running_restic(isolated: Path, vault, monkeypatch) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("restic must not run"))
    result = backup_runner.restic(BackupConfig(repository="F:/repo", sources=["x"]), "snapshots")
    assert not result.ok
    assert "No repository password" in result.stderr


def test_the_backup_worker_is_a_silent_core_worker(configured, fake_restic) -> None:
    assert "function.backup" in default_function_registry()

    with session_scope() as session:
        work = WorkItem(title="Nightly backup", task_instruction="back up", worker_kind="function.backup")
        session.add(work)
        session.flush()
        result = backup_worker(work)

    assert result["produces"]["silent"] is True
    assert result["produces"]["backup"]["ok"] is True
    assert result["summary"] == "backed up (2.5 MB new)"


def test_backup_init_writes_a_starter_config_stores_a_password_and_inits(isolated: Path, vault, fake_restic) -> None:
    fake_restic.fail.add("cat")  # no repository yet

    result = CliRunner().invoke(app, ["backup-init", "F:/TasqueBackups/restic", "--restic", "C:/tools/restic.exe"])

    assert result.exit_code == 0, result.output
    assert "created and stored" in result.output
    text = backup_runner.config_path().read_text(encoding="utf-8")
    assert 'repository = "F:/TasqueBackups/restic"' in text
    assert 'restic = "C:/tools/restic.exe"' in text
    assert get_settings().resolved_project_dir.as_posix() in text
    assert fake_restic.commands() == ["cat", "init"]
    assert vault  # the password is in the store, not on screen
    assert list(vault.values())[0] not in result.output


def test_backup_status_explains_how_to_start_when_unconfigured(isolated: Path) -> None:
    result = CliRunner().invoke(app, ["backup-status"])
    assert result.exit_code == 0
    assert "backup-init" in result.output


def test_a_config_that_excludes_the_database_stage_is_refused(configured, fake_restic) -> None:
    data = get_settings().resolved_data_dir.as_posix()
    config = BackupConfig(repository=configured.repository, sources=configured.sources, excludes=[data])

    run = run_backup(config=config)

    assert run["ok"] is False
    assert "leaves the database snapshot out" in run["error"]
    assert fake_restic.calls == []  # nothing ran on a config that would back up an empty stage


def test_excluded_by_matches_parents_and_named_folders_but_not_other_globs(tmp_path: Path) -> None:
    stage = tmp_path / "data" / "backup" / "stage"
    assert excluded_by(stage, [str(tmp_path / "data")]) == str(tmp_path / "data")
    assert excluded_by(stage, [(tmp_path / "DATA").as_posix()]) is not None  # Windows paths ignore case
    assert excluded_by(stage, ["**/backup"]) == "**/backup"
    assert excluded_by(stage, [str(tmp_path / "data" / "runtime"), "**/node_modules", "*.tmp"]) is None


def test_the_starter_config_backs_up_data_but_never_its_stage(isolated: Path, vault) -> None:
    path = backup_runner.write_starter_config("F:/repo")
    config = backup_runner.load_config(path)

    assert excluded_by(stage_dir(), config.excludes) is None
    assert excluded_by(get_settings().resolved_data_dir / "memory-vault", config.excludes) is None
    assert excluded_by(get_settings().resolved_data_dir / "scratch", config.excludes) is not None
    assert excluded_by(get_settings().database_path, config.excludes) is not None
