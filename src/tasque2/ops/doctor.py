"""Readiness checks: database, storage, providers, Discord, telemetry, extensions, daemon."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

from sqlalchemy import text

from tasque2.config import get_settings
from tasque2.db import get_engine, session_scope
from tasque2.migrations import schema_status, upgrade_database
from tasque2.ops.status import get_system_status

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

    def as_dict(self) -> dict[str, object]:
        return {
            "overall_status": self.overall_status,
            "checks": [
                {"name": c.name, "status": c.status, "summary": c.summary, "details": c.details} for c in self.checks
            ],
        }


def run_doctor(*, migrate: bool = True, which: Callable[[str], str | None] = shutil.which) -> HealthReport:
    checks = [
        _migrations(migrate=migrate),
        _database(),
        _storage(),
        _timezone(),
        _providers(which),
        _models(),
        _discord(),
        _telemetry(),
        _extensions(),
        _daemon(),
        _queue(),
    ]
    overall = max((check.status for check in checks), key=lambda status: HEALTH_ORDER[status])
    return HealthReport(overall_status=overall, checks=tuple(checks))


def _migrations(*, migrate: bool) -> HealthCheck:
    try:
        status = upgrade_database() if migrate else schema_status()
    except Exception as exc:  # noqa: BLE001 - report, don't raise
        return HealthCheck("database.migrations", "fail", f"Migration check failed: {exc}")
    return HealthCheck(
        "database.migrations",
        "ok" if status.is_current else "fail",
        "Database schema is current." if status.is_current else "Database schema is behind the code.",
        {"database_path": str(status.database_path), "current": status.current_display, "head": status.head_display},
    )


def _database() -> HealthCheck:
    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1")).scalar_one()
            foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar()
            journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar()
    except Exception as exc:  # noqa: BLE001
        return HealthCheck("database.connection", "fail", f"SQLite connection failed: {exc}")
    details = {"foreign_keys": foreign_keys, "journal_mode": journal_mode}
    if foreign_keys != 1:
        return HealthCheck("database.connection", "fail", "SQLite foreign keys are off.", details)
    if str(journal_mode).lower() != "wal":
        return HealthCheck("database.connection", "warn", f"SQLite journal_mode is {journal_mode!r}.", details)
    return HealthCheck("database.connection", "ok", "SQLite is ready.", details)


def _storage() -> HealthCheck:
    directory = get_settings().resolved_artifact_dir
    probe = directory / ".doctor-write-test"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except OSError as exc:
        return HealthCheck(
            "artifacts.path", "fail", f"Artifact directory is not writable: {exc}", {"path": str(directory)}
        )
    return HealthCheck("artifacts.path", "ok", "Artifact directory is writable.", {"path": str(directory)})


def _timezone() -> HealthCheck:
    timezone = get_settings().timezone
    try:
        ZoneInfo(timezone)
    except Exception:  # noqa: BLE001
        return HealthCheck("settings.timezone", "fail", f"Invalid timezone: {timezone}")
    return HealthCheck("settings.timezone", "ok", f"Timezone: {timezone}")


def _providers(which: Callable[[str], str | None]) -> HealthCheck:
    settings = get_settings()
    try:
        default = settings.default_provider_name
    except ValueError as exc:
        return HealthCheck("providers", "fail", str(exc))
    paths = {name: which(name) for name in ("claude", "codex")}
    details = {**paths, "default": default}
    if default in paths and paths[default] is None:
        return HealthCheck("providers", "fail", f"The default provider CLI ({default}) is not on PATH.", details)
    missing = [name for name, path in paths.items() if path is None]
    if missing:
        return HealthCheck("providers", "warn", f"Provider CLI not on PATH: {', '.join(missing)}.", details)
    return HealthCheck("providers", "ok", "Provider CLIs are available.", details)


def _models() -> HealthCheck:
    settings = get_settings()
    details: dict[str, object] = {}
    try:
        provider = settings.default_provider_name
        for profile in ("low", "medium", "high", "ultra"):
            choice = settings.model_choice(provider, profile)
            details[profile] = f"{choice.model} (effort {choice.effort or 'default'})"
    except ValueError as exc:
        return HealthCheck("models.tiers", "fail", str(exc), details)
    return HealthCheck("models.tiers", "ok", f"Default tier: {settings.default_model_profile}.", details)


def _discord() -> HealthCheck:
    settings = get_settings()
    channels = {
        "intake": settings.discord_intake_channel_id,
        "ops": settings.discord_ops_channel_id,
        "jobs": settings.discord_jobs_channel_id,
        "chains": settings.discord_chains_channel_id,
        "dlq": settings.discord_dlq_channel_id,
    }
    missing = [name for name, value in channels.items() if not value]
    if not settings.discord_token:
        if len(missing) < len(channels):
            return HealthCheck("discord", "fail", "Channel ids are set but TASQUE2_DISCORD_TOKEN is missing.")
        return HealthCheck("discord", "warn", "Discord is not configured; the daemon runs without it.")
    if missing:
        return HealthCheck("discord", "fail", "Missing channel ids: " + ", ".join(missing) + ".")
    if not settings.allowed_discord_user_ids:
        return HealthCheck(
            "discord", "warn", "TASQUE2_DISCORD_ALLOWED_USER_IDS is empty: anyone in the server can drive it."
        )
    return HealthCheck("discord", "ok", "Discord is configured.")


def _telemetry() -> HealthCheck:
    from tasque2.telemetry import resolve_telemetry_mode

    try:
        mode = resolve_telemetry_mode()
    except ValueError as exc:
        return HealthCheck("telemetry", "fail", str(exc))
    if mode.value == "off":
        return HealthCheck("telemetry", "ok", "Telemetry is off (set OTEL_EXPORTER_OTLP_ENDPOINT to export).")
    return HealthCheck("telemetry", "ok", f"Telemetry exports over {mode.value}.")


def _extensions() -> HealthCheck:
    from tasque2.extensions import registry

    try:
        loaded = registry()
    except Exception as exc:  # noqa: BLE001
        return HealthCheck("extensions", "fail", f"An extension failed to load: {exc}")
    return HealthCheck(
        "extensions",
        "ok",
        f"{len(loaded.extension_names)} extension(s) loaded.",
        {"names": loaded.extension_names, "tools": len(loaded.mcp_tools), "digests": len(loaded.context_digests)},
    )


def _daemon() -> HealthCheck:
    from tasque2.daemon.control import read_state

    state = read_state()
    if state is None:
        return HealthCheck("daemon", "warn", "No daemon state file; the daemon has not run from this data directory.")
    if state.is_fresh():
        detail = " (draining)" if state.draining else ""
        return HealthCheck(
            "daemon", "ok", f"Daemon pid {state.pid} is ticking{detail}; {state.in_flight} run(s) in flight."
        )
    return HealthCheck("daemon", "warn", f"Daemon last ticked at {state.last_tick_at}; it is not running.")


def _queue() -> HealthCheck:
    try:
        with session_scope() as session:
            snapshot = get_system_status(session)
    except Exception as exc:  # noqa: BLE001
        return HealthCheck("queue", "fail", f"Could not read the queue: {exc}")
    details = {
        "ready": snapshot.ready_work,
        "running": snapshot.running_work,
        "dead_letter": snapshot.failed_work_unresolved,
    }
    if snapshot.failed_work_unresolved:
        return HealthCheck("queue", "warn", f"{snapshot.failed_work_unresolved} unresolved dead letter(s).", details)
    return HealthCheck("queue", "ok", "Queue is readable.", details)
