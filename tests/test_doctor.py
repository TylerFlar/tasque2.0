from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import inspect

from tasque2.config import reset_settings
from tasque2.daemon import control
from tasque2.db import get_engine, session_scope
from tasque2.migrations import schema_status
from tasque2.models import utc_now
from tasque2.ops.doctor import HealthCheck, HealthReport, run_doctor
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner

CHANNEL_VARIABLES = (
    "TASQUE2_DISCORD_INTAKE_CHANNEL_ID",
    "TASQUE2_DISCORD_OPS_CHANNEL_ID",
    "TASQUE2_DISCORD_JOBS_CHANNEL_ID",
    "TASQUE2_DISCORD_CHAINS_CHANNEL_ID",
    "TASQUE2_DISCORD_DLQ_CHANNEL_ID",
)


def _on_path(name: str) -> str:
    return f"/fake/bin/{name}"


def _checks(report: HealthReport) -> dict[str, HealthCheck]:
    return {check.name: check for check in report.checks}


def test_doctor_runs_migrations_and_reports_core_checks() -> None:
    report = run_doctor(which=_on_path)
    checks = _checks(report)

    assert schema_status().is_current
    assert list(checks) == [
        "database.migrations",
        "database.connection",
        "artifacts.path",
        "settings.timezone",
        "providers",
        "models.tiers",
        "discord",
        "telemetry",
        "extensions",
        "daemon",
        "queue",
    ]
    assert {name: check.status for name, check in checks.items()} == {
        "database.migrations": "ok",
        "database.connection": "ok",
        "artifacts.path": "ok",
        "settings.timezone": "ok",
        "providers": "ok",
        "models.tiers": "ok",
        "discord": "warn",
        "telemetry": "ok",
        "extensions": "ok",
        "daemon": "warn",
        "queue": "ok",
    }
    assert checks["database.connection"].details == {"foreign_keys": 1, "journal_mode": "wal"}
    assert checks["models.tiers"].details["ultra"] == "claude-fable-5-1 (effort high)"
    assert checks["models.tiers"].details["low"] == "claude-haiku-4-5 (effort default)"
    assert report.overall_status == "warn"
    assert not report.has_failures


def test_doctor_without_migrate_reports_an_unmigrated_database() -> None:
    report = run_doctor(migrate=False, which=_on_path)
    checks = _checks(report)

    assert checks["database.migrations"].status == "fail"
    assert checks["database.migrations"].details["current"] == "<none>"
    assert checks["queue"].status == "fail"
    assert report.overall_status == "fail"
    assert "alembic_version" not in inspect(get_engine()).get_table_names()


def test_doctor_fails_when_the_default_provider_cli_is_missing() -> None:
    missing = _checks(run_doctor(which=lambda _name: None))["providers"]
    only_claude = _checks(run_doctor(which=lambda name: _on_path(name) if name == "claude" else None))["providers"]

    assert missing.status == "fail"
    assert "(claude) is not on PATH" in missing.summary
    assert only_claude.status == "warn"
    assert only_claude.summary == "Provider CLI not on PATH: codex."


def test_doctor_reports_model_tiers_the_default_provider_cannot_resolve(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DEFAULT_PROVIDER", "codex")

    check = _checks(run_doctor(which=_on_path))["models.tiers"]

    assert check.status == "fail"
    assert "TASQUE2_CODEX_MODEL_LOW is required" in check.summary


def test_doctor_reports_discord_channel_without_token_as_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DISCORD_OPS_CHANNEL_ID", "channel-1")

    report = run_doctor(which=_on_path)

    assert report.overall_status == "fail"
    assert _checks(report)["discord"].status == "fail"


def test_doctor_discord_check_covers_channels_and_allowed_users(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DISCORD_TOKEN", "token")
    monkeypatch.setenv("TASQUE2_DISCORD_OPS_CHANNEL_ID", "ops")
    missing_channels = _checks(run_doctor(which=_on_path))["discord"]

    for name in CHANNEL_VARIABLES:
        monkeypatch.setenv(name, name.lower())
    reset_settings()
    open_to_everyone = _checks(run_doctor(which=_on_path))["discord"]

    monkeypatch.setenv("TASQUE2_DISCORD_ALLOWED_USER_IDS", "1234")
    reset_settings()
    configured = _checks(run_doctor(which=_on_path))["discord"]

    assert missing_channels.status == "fail"
    assert missing_channels.summary == "Missing channel ids: intake, jobs, chains, dlq."
    assert open_to_everyone.status == "warn"
    assert configured.status == "ok"


def test_doctor_rejects_an_unknown_timezone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_TIMEZONE", "Mars/Olympus_Mons")

    check = _checks(run_doctor(which=_on_path))["settings.timezone"]

    assert check.status == "fail"
    assert check.summary == "Invalid timezone: Mars/Olympus_Mons"


def test_doctor_rejects_an_unknown_telemetry_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_TELEMETRY", "carrier-pigeon")

    check = _checks(run_doctor(which=_on_path))["telemetry"]

    assert check.status == "fail"
    assert "TASQUE2_TELEMETRY must be one of" in check.summary


def test_doctor_reports_a_broken_extension(isolated: Path) -> None:
    package = isolated / "extensions" / "doctor_broken_ext"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("raise RuntimeError('boom')\n", encoding="utf-8")

    check = _checks(run_doctor(which=_on_path))["extensions"]

    assert check.status == "fail"
    assert "doctor_broken_ext" in check.summary


def test_doctor_reports_daemon_liveness_from_its_state_file() -> None:
    control.write_state(started_at=utc_now(), in_flight_attempt_ids=["attempt-1"], draining=False, version="test")
    alive = _checks(run_doctor(which=_on_path))["daemon"]

    state = json.loads(control.state_path().read_text(encoding="utf-8"))
    state["last_tick_at"] = (utc_now() - timedelta(hours=1)).isoformat()
    control.state_path().write_text(json.dumps(state), encoding="utf-8")
    stale = _checks(run_doctor(which=_on_path))["daemon"]

    assert alive.status == "ok"
    assert "is ticking; 1 run(s) in flight" in alive.summary
    assert stale.status == "warn"
    assert "it is not running" in stale.summary


def test_doctor_warns_about_unresolved_dead_letters(fresh_db: Path) -> None:
    with session_scope() as session:
        WorkRepository(session).create_work_item(
            title="Broken", task_instruction="Fail.", worker_kind="function.missing"
        )
        WorkRunner(session).run_next()

    check = _checks(run_doctor(which=_on_path))["queue"]

    assert check.status == "warn"
    assert check.summary == "1 unresolved dead letter(s)."
    assert check.details == {"ready": 0, "running": 0, "dead_letter": 1}


def test_health_report_as_dict_lists_every_check() -> None:
    report = HealthReport(
        overall_status="warn",
        checks=(HealthCheck("a", "ok", "fine"), HealthCheck("b", "warn", "hmm", {"x": 1})),
    )

    assert report.as_dict() == {
        "overall_status": "warn",
        "checks": [
            {"name": "a", "status": "ok", "summary": "fine", "details": {}},
            {"name": "b", "status": "warn", "summary": "hmm", "details": {"x": 1}},
        ],
    }
    assert not report.has_failures
