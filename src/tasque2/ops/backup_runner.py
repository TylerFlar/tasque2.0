"""Scheduled, verified backups with restic: what to keep, where, and whether the last one worked.

``data/backup.toml`` names the restic repository and the folders to back up. Each run:

1. snapshots the database with SQLite's backup API into a staging folder, checks it, and writes
   a manifest of row counts next to it (the live database file is never read directly);
2. runs ``restic backup`` over the configured sources and the staging folder;
3. when due, prunes old snapshots by the retention policy and runs ``restic check``;
4. when due, restores the latest database snapshot to a temporary folder, runs
   ``PRAGMA integrity_check`` on it and compares its row counts with the manifest.

The repository password lives in the operating system's credential store (``keyring``) and
reaches restic only through its environment. Every step's outcome goes to
``data/backup/state.json``; a run never raises, so a failed backup is reported by health checks
instead of crashing or retrying in a loop. Junctions and symlinks are stored as links by restic,
not followed: a folder reached through one must be listed as a source of its own.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import shutil
import sqlite3
import subprocess
import tempfile
import time
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from tasque2.config import get_settings
from tasque2.models import utc_now

logger = logging.getLogger(__name__)

CONFIG_FILE = "backup.toml"
KEYRING_SERVICE = "tasque2-backup"
KEYRING_USER = "restic"
BACKUP_TIMEOUT_SECONDS = 3 * 60 * 60
COMMAND_TIMEOUT_SECONDS = 60 * 60
ERROR_CHARS = 800
STARTER_CONFIG = """\
# Tasque backups (restic). Paths may use forward slashes. See tasque2.ops.backup_runner.
repository = "{repository}"
restic = "restic"            # the restic binary: a name on PATH or a full path
keep_daily = 7
keep_weekly = 5
keep_monthly = 12
stale_hours = 48             # health checks flag a backup older than this
check_every_days = 7         # forget --prune and restic check
restore_test_every_days = 30 # restore the database and verify it

# Folders to back up. The database is snapshotted separately into data/backup/stage: never list the
# live .sqlite3. restic applies excludes inside every source, so never exclude a folder that holds
# something you want, and never data/backup. restic stores junctions and symlinks as links: list the
# folder they point to as its own source.
sources = [
  "{project}",{data_source}
]

# Exact paths (or glob patterns) to leave out.
excludes = [
  "{project}/.venv",
  "{data}/scratch",
  "{data}/scratchpad",
  "{data}/tmp",
  "{data}/backups",
  "{data}/runtime",
  "{data}/tasque2.sqlite3",
  "{data}/tasque2.sqlite3-wal",
  "{data}/tasque2.sqlite3-shm",
  "**/node_modules",
  "**/__pycache__",
  "**/.pytest_cache",
  "**/.ruff_cache",
]
"""


class BackupConfigError(ValueError):
    """``data/backup.toml`` is missing or invalid."""


@dataclass(frozen=True)
class BackupConfig:
    repository: str
    restic: str = "restic"
    sources: list[str] = field(default_factory=list)
    excludes: list[str] = field(default_factory=list)
    keep_daily: int = 7
    keep_weekly: int = 5
    keep_monthly: int = 12
    stale_hours: int = 48
    check_every_days: int = 7
    restore_test_every_days: int = 30


def config_path() -> Path:
    return get_settings().resolved_data_dir / CONFIG_FILE


def state_path() -> Path:
    return get_settings().resolved_data_dir / "backup" / "state.json"


def stage_dir() -> Path:
    return get_settings().resolved_data_dir / "backup" / "stage"


def excluded_by(path: Path, patterns: list[str]) -> str | None:
    """The configured exclude that would leave ``path`` out of a backup, if any.

    restic applies excludes inside every source, so a pattern matching a parent folder empties a
    source listed under it. This checks plain paths (a parent or the path itself) and ``**/name``
    patterns (any path component named ``name``); other globs are left to restic.
    """
    target = path.resolve().as_posix().lower().rstrip("/")
    parts = target.split("/")
    for pattern in patterns:
        flat = pattern.replace("\\", "/").lower().rstrip("/")
        if flat.startswith("**/") and not any(char in flat[3:] for char in "*?["):
            if flat[3:] in parts:
                return pattern
            continue
        if any(char in flat for char in "*?["):
            continue
        if target == flat or target.startswith(flat + "/"):
            return pattern
    return None


def load_config(path: Path | None = None) -> BackupConfig:
    path = path or config_path()
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise BackupConfigError(f"No backup config at {path}; run `tasque2 backup-init` first.") from None
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise BackupConfigError(f"Cannot read {path}: {exc}") from None
    repository = str(raw.get("repository") or "").strip()
    if not repository:
        raise BackupConfigError(f"{path}: `repository` is required.")
    sources = [str(item) for item in raw.get("sources") or []]
    if not sources:
        raise BackupConfigError(f"{path}: `sources` lists nothing to back up.")
    names = {name for name in BackupConfig.__dataclass_fields__ if name not in {"repository", "sources", "excludes"}}
    extra = {name: raw[name] for name in names if name in raw}
    return BackupConfig(
        repository=repository,
        sources=sources,
        excludes=[str(item) for item in raw.get("excludes") or []],
        **extra,
    )


def write_starter_config(repository: str, path: Path | None = None) -> Path:
    """Write a starter config for this project, unless one exists."""
    path = path or config_path()
    if path.exists():
        return path
    settings = get_settings()
    project, data = settings.resolved_project_dir, settings.resolved_data_dir
    data_source = "" if data.is_relative_to(project) else f'\n  "{data.as_posix()}",'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        STARTER_CONFIG.format(
            repository=repository.replace("\\", "/"),
            project=project.as_posix(),
            data=data.as_posix(),
            data_source=data_source,
        ),
        encoding="utf-8",
    )
    return path


# --- the password -------------------------------------------------------------------------------


def stored_password() -> str | None:
    import keyring

    return keyring.get_password(KEYRING_SERVICE, KEYRING_USER)


def ensure_password() -> tuple[str, bool]:
    """The repository password, created and stored on first use; True when it is new."""
    import keyring

    existing = keyring.get_password(KEYRING_SERVICE, KEYRING_USER)
    if existing:
        return existing, False
    password = secrets.token_urlsafe(32)
    keyring.set_password(KEYRING_SERVICE, KEYRING_USER, password)
    return password, True


# --- restic ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ResticResult:
    ok: bool
    stdout: str
    stderr: str
    seconds: float


def restic(
    config: BackupConfig, *args: str, password: str | None = None, timeout: int = COMMAND_TIMEOUT_SECONDS
) -> ResticResult:
    """Run one restic command against the configured repository; never raises on failure."""
    password = password or stored_password()
    if not password:
        return ResticResult(False, "", "No repository password in the credential store; run backup-init.", 0.0)
    env = {**os.environ, "RESTIC_REPOSITORY": config.repository, "RESTIC_PASSWORD": password}
    started = time.monotonic()
    try:
        completed = subprocess.run(
            [config.restic, *args],
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return ResticResult(False, "", f"{type(exc).__name__}: {exc}", time.monotonic() - started)
    return ResticResult(completed.returncode == 0, completed.stdout, completed.stderr, time.monotonic() - started)


def repository_ready(config: BackupConfig) -> bool:
    return restic(config, "cat", "config").ok


def init_repository(config: BackupConfig) -> ResticResult:
    ensure_password()
    if repository_ready(config):
        return ResticResult(True, "repository already initialized", "", 0.0)
    return restic(config, "init")


# --- the database snapshot ---------------------------------------------------------------------


def stage_database(destination_dir: Path | None = None) -> dict[str, Any]:
    """Snapshot the live database into the staging folder and describe it in a manifest."""
    from tasque2.ops.backup import BackupService

    folder = destination_dir or stage_dir()
    folder.mkdir(parents=True, exist_ok=True)
    database = folder / "tasque2.sqlite3"
    database.unlink(missing_ok=True)
    BackupService()._backup_sqlite(database)
    manifest = describe_database(database, check="quick_check")
    manifest["created_at"] = utc_now().isoformat()
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True), encoding="utf-8")
    return manifest


def describe_database(path: Path, *, check: str = "integrity_check") -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        result = connection.execute(f"pragma {check}").fetchone()[0]
        tables = [
            row[0]
            for row in connection.execute(
                "select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name"
            )
        ]
        counts = {table: connection.execute(f'select count(*) from "{table}"').fetchone()[0] for table in tables}
    finally:
        connection.close()
    return {"check": check, "check_result": result, "row_counts": counts}


# --- state -------------------------------------------------------------------------------------


def read_state(path: Path | None = None) -> dict[str, Any]:
    try:
        return json.loads((path or state_path()).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def write_state(state: dict[str, Any], path: Path | None = None) -> None:
    path = path or state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _due(state: dict[str, Any], key: str, every_days: int, now: datetime) -> bool:
    last = state.get(key)
    if not last:
        return True
    try:
        return now - datetime.fromisoformat(last) >= timedelta(days=every_days)
    except ValueError:
        return True


def _clip(text: str) -> str:
    flat = " ".join((text or "").split())
    return flat if len(flat) <= ERROR_CHARS else flat[: ERROR_CHARS - 1] + "…"


def _summary_line(stdout: str) -> dict[str, Any]:
    """The final ``summary`` message of ``restic backup --json``."""
    for line in reversed(stdout.splitlines()):
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if isinstance(message, dict) and message.get("message_type") == "summary":
            return message
    return {}


# --- one run -----------------------------------------------------------------------------------


def run_backup(
    *,
    config: BackupConfig | None = None,
    dry_run: bool = False,
    force_maintenance: bool = False,
    force_restore_test: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Back up, then prune/check and restore-test when due. Returns (and stores) the run's state."""
    now = now or utc_now()
    state = read_state()
    run: dict[str, Any] = {"started_at": now.isoformat(), "dry_run": dry_run, "ok": False}
    try:
        config = config or load_config()
    except BackupConfigError as exc:
        run["error"] = str(exc)
        return _finish(state, run, now, dry_run=dry_run)

    blocking = excluded_by(stage_dir(), config.excludes)
    if blocking:
        run["error"] = f"data/backup.toml excludes {blocking!r}, which leaves the database snapshot out of every backup"
        return _finish(state, run, now, dry_run=dry_run)

    try:
        manifest = stage_database()
        run["database"] = {"check": manifest["check_result"], "tables": len(manifest["row_counts"])}
        if manifest["check_result"] != "ok":
            run["database"]["error"] = f"quick_check: {manifest['check_result']}"
    except Exception as exc:  # noqa: BLE001 - recorded and reported; files still get backed up
        logger.exception("Backup: database snapshot failed")
        run["database"] = {"error": _clip(f"{type(exc).__name__}: {exc}")}

    args = ["backup", "--json", "--tag", "tasque2"]
    if dry_run:
        args.append("--dry-run")
    for pattern in config.excludes:
        args += ["--exclude", pattern]
    sources = [source for source in config.sources if Path(source).exists()]
    missing = [source for source in config.sources if source not in sources]
    if missing:
        run["missing_sources"] = missing
    args += [*sources, str(stage_dir())]
    result = restic(config, *args, timeout=BACKUP_TIMEOUT_SECONDS)
    summary = _summary_line(result.stdout)
    run["backup"] = {
        "ok": result.ok,
        "seconds": round(result.seconds, 1),
        "snapshot_id": summary.get("snapshot_id"),
        "files_new": summary.get("files_new"),
        "files_changed": summary.get("files_changed"),
        "data_added": summary.get("data_added"),
        "total_bytes_processed": summary.get("total_bytes_processed"),
    }
    if not result.ok:
        run["backup"]["error"] = _clip(result.stderr or result.stdout)

    if result.ok and not dry_run:
        if force_maintenance or _due(state, "last_check_at", config.check_every_days, now):
            run["maintenance"] = maintenance(config)
            state["last_check_at"] = now.isoformat()
            state["last_check_ok"] = run["maintenance"]["ok"]
        if force_restore_test or _due(state, "last_restore_test_at", config.restore_test_every_days, now):
            run["restore_test"] = restore_test(config)
            state["last_restore_test_at"] = now.isoformat()
            state["last_restore_test_ok"] = run["restore_test"]["ok"]

    run["ok"] = result.ok and not run["database"].get("error")
    return _finish(state, run, now, dry_run=dry_run)


def _finish(state: dict[str, Any], run: dict[str, Any], now: datetime, *, dry_run: bool) -> dict[str, Any]:
    run["ended_at"] = utc_now().isoformat()
    if dry_run:
        return run
    state["last_run"] = run
    state["last_run_at"] = now.isoformat()
    if run["ok"]:
        state["last_success_at"] = now.isoformat()
        state["last_snapshot_id"] = (run.get("backup") or {}).get("snapshot_id")
        state.pop("last_error", None)
    else:
        state["last_error"] = run.get("error") or (run.get("backup") or {}).get("error") or "see last_run"
    write_state(state)
    return run


def maintenance(config: BackupConfig) -> dict[str, Any]:
    """Apply the retention policy, prune, and check the repository's structure."""
    forget = restic(
        config,
        "forget",
        "--tag",
        "tasque2",
        "--group-by",
        "host,tags",
        "--keep-daily",
        str(config.keep_daily),
        "--keep-weekly",
        str(config.keep_weekly),
        "--keep-monthly",
        str(config.keep_monthly),
        "--prune",
    )
    check = restic(config, "check")
    out: dict[str, Any] = {"ok": forget.ok and check.ok, "forget_ok": forget.ok, "check_ok": check.ok}
    if not forget.ok:
        out["forget_error"] = _clip(forget.stderr)
    if not check.ok:
        out["check_error"] = _clip(check.stderr or check.stdout)
    return out


def restore_test(config: BackupConfig) -> dict[str, Any]:
    """Restore the latest database snapshot and prove it opens, passes integrity_check and matches."""
    with tempfile.TemporaryDirectory(prefix="tasque2-restore-") as target:
        result = restic(config, "restore", "latest", "--tag", "tasque2", "--target", target, "--include", "**/stage/*")
        if not result.ok:
            return {"ok": False, "error": _clip(result.stderr or result.stdout)}
        databases = list(Path(target).rglob("backup/stage/tasque2.sqlite3"))
        manifests = list(Path(target).rglob("backup/stage/manifest.json"))
        if not databases or not manifests:
            return {"ok": False, "error": "the snapshot holds no staged database"}
        restored = describe_database(databases[0])
        expected = json.loads(manifests[0].read_text(encoding="utf-8")).get("row_counts") or {}
        mismatched = sorted(table for table, count in expected.items() if restored["row_counts"].get(table) != count)
        ok = restored["check_result"] == "ok" and not mismatched
        out: dict[str, Any] = {"ok": ok, "integrity": restored["check_result"], "tables": len(restored["row_counts"])}
        if mismatched:
            out["mismatched_tables"] = mismatched[:20]
        return out


def backup_health(state: dict[str, Any] | None = None, *, now: datetime | None = None) -> dict[str, Any]:
    """What a health check should know: configured or not, how old the last good backup is, problems."""
    now = now or utc_now()
    state = read_state() if state is None else state
    if not config_path().exists():
        return {"configured": False, "attention": []}
    try:
        stale_hours = load_config().stale_hours
    except BackupConfigError as exc:
        return {"configured": False, "attention": [f"backup config is unreadable: {exc}"]}
    attention: list[str] = []
    last_success = state.get("last_success_at")
    age_hours = None
    if last_success:
        age_hours = round((now - datetime.fromisoformat(last_success)).total_seconds() / 3600, 1)
    if age_hours is None:
        attention.append("no backup has succeeded yet")
    elif age_hours > stale_hours:
        attention.append(f"the last good backup is {age_hours:.0f} hours old")
    if state.get("last_error"):
        attention.append(f"the last backup run failed: {state['last_error']}")
    if state.get("last_check_ok") is False:
        attention.append("the last repository check failed")
    if state.get("last_restore_test_ok") is False:
        attention.append("the last restore test failed")
    return {
        "configured": True,
        "last_success_at": last_success,
        "age_hours": age_hours,
        "last_snapshot_id": state.get("last_snapshot_id"),
        "last_check_at": state.get("last_check_at"),
        "last_restore_test_at": state.get("last_restore_test_at"),
        "attention": attention,
    }


def backup_worker(work_item: Any) -> dict[str, Any]:
    """``function.backup``: one scheduled run. Quiet on Discord; health checks report problems."""
    options = (work_item.context or {}).get("backup") or {}
    run = run_backup(
        force_maintenance=bool(options.get("maintenance")),
        force_restore_test=bool(options.get("restore_test")),
    )
    status = "backed up" if run["ok"] else "backup FAILED"
    added = (run.get("backup") or {}).get("data_added")
    detail = f" ({added / 1_000_000:.1f} MB new)" if isinstance(added, int | float) else ""
    return {"summary": f"{status}{detail}", "produces": {"silent": True, "backup": run}}


def remove_stage() -> None:
    shutil.rmtree(stage_dir(), ignore_errors=True)
