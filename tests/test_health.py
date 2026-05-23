from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.config import reset_settings
from tasque2.db import reset_engine
from tasque2.health import run_doctor
from tasque2.migrations import schema_status

DISCORD_ENV_VARS = [
    "TASQUE2_DISCORD_TOKEN",
    "TASQUE2_DISCORD_INTAKE_CHANNEL_ID",
    "TASQUE2_DISCORD_OPS_CHANNEL_ID",
    "TASQUE2_DISCORD_JOBS_CHANNEL_ID",
    "TASQUE2_DISCORD_CHAINS_CHANNEL_ID",
    "TASQUE2_DISCORD_DLQ_CHANNEL_ID",
    "TASQUE2_DISCORD_ALLOWED_USER_IDS",
]


def _clear_discord_env(monkeypatch) -> None:
    for key in DISCORD_ENV_VARS:
        monkeypatch.setenv(key, "")
    reset_settings()


def test_doctor_runs_migrations_and_reports_core_checks(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TASQUE2_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TASQUE2_DB_PATH", str(tmp_path / "tasque2.sqlite3"))
    _clear_discord_env(monkeypatch)
    reset_engine()

    report = run_doctor(which=lambda _name: None)
    checks = {check.name: check for check in report.checks}

    assert schema_status().is_current
    assert checks["database.migrations"].status == "ok"
    assert checks["database.connection"].status == "ok"
    assert checks["artifacts.path"].status == "ok"
    assert checks["settings.timezone"].status == "ok"
    assert checks["providers.available"].status == "warn"
    assert checks["discord.config"].status == "warn"


def test_doctor_reports_discord_channel_without_token_as_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TASQUE2_DB_PATH", str(tmp_path / "tasque2.sqlite3"))
    _clear_discord_env(monkeypatch)
    monkeypatch.setenv("TASQUE2_DISCORD_TOKEN", "")
    monkeypatch.setenv("TASQUE2_DISCORD_OPS_CHANNEL_ID", "channel-1")
    reset_settings()
    reset_engine()

    report = run_doctor(which=lambda name: f"/fake/bin/{name}")
    checks = {check.name: check for check in report.checks}

    assert report.overall_status == "fail"
    assert checks["discord.config"].status == "fail"


def test_doctor_cli_json_output(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TASQUE2_DB_PATH", str(tmp_path / "tasque2.sqlite3"))
    _clear_discord_env(monkeypatch)
    reset_engine()

    result = CliRunner().invoke(app, ["doctor", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "overall_status" in payload
    assert {check["name"] for check in payload["checks"]} >= {
        "database.migrations",
        "database.connection",
        "artifacts.path",
        "providers.available",
        "discord.config",
        "system.status",
    }


def test_doctor_cli_strict_exits_nonzero_on_failures(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TASQUE2_DB_PATH", str(tmp_path / "tasque2.sqlite3"))
    _clear_discord_env(monkeypatch)
    monkeypatch.setenv("TASQUE2_DISCORD_TOKEN", "")
    monkeypatch.setenv("TASQUE2_DISCORD_INTAKE_CHANNEL_ID", "channel-1")
    reset_settings()
    reset_engine()

    result = CliRunner().invoke(app, ["doctor", "--strict"])

    assert result.exit_code == 1
    assert "discord.config" in result.output
