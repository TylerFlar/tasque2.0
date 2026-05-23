from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

from sqlalchemy import text

from tasque2.config import get_settings
from tasque2.db import get_engine, session_scope
from tasque2.migrations import schema_status, upgrade_database
from tasque2.status import get_system_status

HEALTH_ORDER = {"ok": 0, "warn": 1, "fail": 2}


@dataclass(frozen=True)
class HealthCheck:
    name: str
    status: str
    summary: str
    details: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class HealthReport:
    overall_status: str
    checks: tuple[HealthCheck, ...]

    @property
    def has_failures(self) -> bool:
        return any(check.status == "fail" for check in self.checks)


def run_doctor(
    *,
    migrate: bool = True,
    which: Callable[[str], str | None] = shutil.which,
) -> HealthReport:
    checks = [
        _check_migrations(migrate=migrate),
        _check_database_connection(),
        _check_artifact_path(),
        _check_timezone(),
        _check_providers(which=which),
        _check_discord_config(),
        _check_system_status(),
    ]
    overall = max((check.status for check in checks), key=lambda status: HEALTH_ORDER[status])
    return HealthReport(overall_status=overall, checks=tuple(checks))


def health_report_to_dict(report: HealthReport) -> dict[str, object]:
    return {
        "overall_status": report.overall_status,
        "checks": [
            {
                "name": check.name,
                "status": check.status,
                "summary": check.summary,
                "details": check.details,
            }
            for check in report.checks
        ],
    }


def _check_migrations(*, migrate: bool) -> HealthCheck:
    try:
        status = upgrade_database() if migrate else schema_status()
    except Exception as exc:
        return HealthCheck(
            name="database.migrations",
            status="fail",
            summary=f"Migration check failed: {exc}",
        )

    check_status = "ok" if status.is_current else "fail"
    summary = "Database schema is current." if status.is_current else "Database schema is not current."
    return HealthCheck(
        name="database.migrations",
        status=check_status,
        summary=summary,
        details={
            "database_path": str(status.database_path),
            "current": status.current_display,
            "head": status.head_display,
            "migrated": migrate,
        },
    )


def _check_database_connection() -> HealthCheck:
    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1")).scalar_one()
            foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar()
            journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar()
    except Exception as exc:
        return HealthCheck(
            name="database.connection",
            status="fail",
            summary=f"SQLite connection failed: {exc}",
        )

    if foreign_keys != 1:
        status = "fail"
        summary = "SQLite foreign key enforcement is disabled."
    elif str(journal_mode).lower() != "wal":
        status = "warn"
        summary = f"SQLite connected, but journal_mode is {journal_mode!r}."
    else:
        status = "ok"
        summary = "SQLite connection and pragmas are ready."

    return HealthCheck(
        name="database.connection",
        status=status,
        summary=summary,
        details={"foreign_keys": foreign_keys, "journal_mode": journal_mode},
    )


def _check_artifact_path() -> HealthCheck:
    artifact_dir = get_settings().resolved_data_dir / "artifacts"
    test_path = artifact_dir / ".tasque2-doctor-write-test"
    try:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        test_path.write_text("ok", encoding="utf-8")
        test_path.unlink(missing_ok=True)
    except Exception as exc:
        return HealthCheck(
            name="artifacts.path",
            status="fail",
            summary=f"Artifact directory is not writable: {exc}",
            details={"artifact_dir": str(artifact_dir)},
        )

    return HealthCheck(
        name="artifacts.path",
        status="ok",
        summary="Artifact directory is writable.",
        details={"artifact_dir": str(artifact_dir)},
    )


def _check_timezone() -> HealthCheck:
    timezone = get_settings().timezone
    try:
        ZoneInfo(timezone)
    except Exception as exc:
        return HealthCheck(
            name="settings.timezone",
            status="fail",
            summary=f"Configured timezone is invalid: {timezone}",
            details={"timezone": timezone, "error": str(exc)},
        )

    return HealthCheck(
        name="settings.timezone",
        status="ok",
        summary=f"Timezone is valid: {timezone}",
        details={"timezone": timezone},
    )


def _check_providers(*, which: Callable[[str], str | None]) -> HealthCheck:
    codex_path = which("codex")
    claude_path = which("claude")
    missing_optional = []
    if codex_path is None:
        missing_optional.append("codex")
    if claude_path is None:
        missing_optional.append("claude")

    status = "warn" if missing_optional else "ok"
    if missing_optional:
        summary = f"Optional provider CLI(s) not found on PATH: {', '.join(missing_optional)}."
    else:
        summary = "Provider CLIs are available."

    details = {
        "codex": codex_path,
        "claude": claude_path,
    }
    if get_settings().allow_test_providers:
        details.update(
            {
                "fake": "test-only built-in",
                "subprocess": "test-only built-in",
            }
        )

    return HealthCheck(
        name="providers.available",
        status=status,
        summary=summary,
        details=details,
    )


def _check_discord_config() -> HealthCheck:
    settings = get_settings()
    has_token = bool(settings.discord_token)
    required_channels = {
        "intake_channel_id": settings.discord_intake_channel_id,
        "ops_channel_id": settings.discord_ops_channel_id,
        "jobs_channel_id": settings.discord_jobs_channel_id,
        "chains_channel_id": settings.discord_chains_channel_id,
        "dlq_channel_id": settings.discord_dlq_channel_id,
    }
    details = dict(required_channels)
    has_channel = any(required_channels.values())
    allowlist_configured = bool(settings.discord_allowed_user_ids)
    missing_channels = [name for name, value in required_channels.items() if not value]

    if has_channel and not has_token:
        return HealthCheck(
            name="discord.config",
            status="fail",
            summary="Discord channel ids are configured, but TASQUE2_DISCORD_TOKEN is missing.",
            details={**details, "token_configured": False},
        )
    if has_token and missing_channels:
        return HealthCheck(
            name="discord.config",
            status="fail",
            summary="Discord bot is missing required channel ids: " + ", ".join(missing_channels) + ".",
            details={
                **details,
                "token_configured": True,
                "missing_channel_ids": missing_channels,
                "allowed_user_ids_configured": allowlist_configured,
            },
        )
    if not has_token and not has_channel:
        return HealthCheck(
            name="discord.config",
            status="warn",
            summary="Discord bot is not configured.",
            details={
                **details,
                "token_configured": False,
                "allowed_user_ids_configured": False,
            },
        )
    if not allowlist_configured:
        return HealthCheck(
            name="discord.config",
            status="warn",
            summary="Discord bot is configured without TASQUE2_DISCORD_ALLOWED_USER_IDS.",
            details={
                **details,
                "token_configured": True,
                "allowed_user_ids_configured": False,
            },
        )

    return HealthCheck(
        name="discord.config",
        status="ok",
        summary="Discord bot configuration is present.",
        details={
            **details,
            "token_configured": True,
            "allowed_user_ids_configured": True,
        },
    )


def _check_system_status() -> HealthCheck:
    try:
        with session_scope() as session:
            snapshot = get_system_status(session)
    except Exception as exc:
        return HealthCheck(
            name="system.status",
            status="fail",
            summary=f"Could not read system status: {exc}",
        )

    details = {
        "ready_work": snapshot.ready_work,
        "running_work": snapshot.running_work,
        "failed_work_unresolved": snapshot.failed_work_unresolved,
        "schedules_enabled": snapshot.schedules_enabled,
        "work_items": snapshot.work_items,
        "workflow_runs": snapshot.workflow_runs,
    }
    if snapshot.failed_work_unresolved:
        return HealthCheck(
            name="system.status",
            status="warn",
            summary=f"{snapshot.failed_work_unresolved} unresolved failed work item(s) need attention.",
            details=details,
        )
    return HealthCheck(
        name="system.status",
        status="ok",
        summary="System status is readable.",
        details=details,
    )
