from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import delete, or_, select, update
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.db import get_engine, reset_engine
from tasque2.models import (
    AgentResult,
    DiscordMessage,
    DiscordThread,
    ScheduleOccurrence,
    WorkEvent,
    WorkflowRun,
    WorkItem,
)


@dataclass(frozen=True)
class BackupResult:
    backup_dir: Path
    database_path: Path
    artifacts_dir: Path | None
    manifest_path: Path


@dataclass(frozen=True)
class RestoreResult:
    restored_database_path: Path
    restored_artifacts_dir: Path | None
    previous_database_backup: Path | None


@dataclass(frozen=True)
class JobResetResult:
    work_items_deleted: int
    workflow_runs_deleted: int
    discord_messages_deleted: int
    discord_threads_deleted: int
    workflow_events_deleted: int
    schedule_occurrences_unlinked: int
    agent_results_deleted: int


class BackupService:
    def create_backup(
        self,
        destination: Path | None = None,
        *,
        include_artifacts: bool = True,
    ) -> BackupResult:
        settings = get_settings()
        backup_dir = destination or self._default_backup_dir(settings.resolved_data_dir)
        backup_dir = backup_dir.expanduser().resolve()
        backup_dir.mkdir(parents=True, exist_ok=False)

        database_path = backup_dir / "tasque2.sqlite3"
        self._backup_sqlite(database_path)

        artifacts_dir = None
        source_artifacts = settings.resolved_data_dir / "artifacts"
        if include_artifacts and source_artifacts.exists():
            artifacts_dir = backup_dir / "artifacts"
            shutil.copytree(source_artifacts, artifacts_dir)

        manifest = {
            "created_at": datetime.now(UTC).isoformat(),
            "source_database_path": str(settings.database_path),
            "database_file": database_path.name,
            "artifacts_dir": artifacts_dir.name if artifacts_dir else None,
            "artifact_count": self._count_files(artifacts_dir) if artifacts_dir else 0,
        }
        manifest_path = backup_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        return BackupResult(
            backup_dir=backup_dir,
            database_path=database_path,
            artifacts_dir=artifacts_dir,
            manifest_path=manifest_path,
        )

    def restore_backup(self, backup_dir: Path, *, force: bool = False) -> RestoreResult:
        if not force:
            raise ValueError("Restore requires force=True.")

        settings = get_settings()
        backup_dir = backup_dir.expanduser().resolve()
        backup_database = backup_dir / "tasque2.sqlite3"
        if not backup_database.exists():
            raise FileNotFoundError(f"Backup database not found: {backup_database}")

        target_database = settings.database_path
        target_database.parent.mkdir(parents=True, exist_ok=True)
        previous_database_backup = None
        reset_engine()
        if target_database.exists():
            previous_database_backup = target_database.with_suffix(
                f".pre-restore-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}.sqlite3"
            )
            shutil.copy2(target_database, previous_database_backup)

        shutil.copy2(backup_database, target_database)

        restored_artifacts = None
        backup_artifacts = backup_dir / "artifacts"
        if backup_artifacts.exists():
            restored_artifacts = settings.resolved_data_dir / "artifacts"
            restored_artifacts.mkdir(parents=True, exist_ok=True)
            self._copytree_merge(backup_artifacts, restored_artifacts)

        reset_engine()
        return RestoreResult(
            restored_database_path=target_database,
            restored_artifacts_dir=restored_artifacts,
            previous_database_backup=previous_database_backup,
        )

    def _backup_sqlite(self, destination: Path) -> None:
        engine = get_engine()
        raw_connection = engine.raw_connection()
        try:
            source_connection = raw_connection.driver_connection
            with sqlite3.connect(destination) as target_connection:
                source_connection.backup(target_connection)
        finally:
            raw_connection.close()

    def _default_backup_dir(self, data_dir: Path) -> Path:
        stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        return data_dir / "backups" / f"tasque2-backup-{stamp}"

    def _count_files(self, path: Path | None) -> int:
        if path is None or not path.exists():
            return 0
        return sum(1 for child in path.rglob("*") if child.is_file())

    def _copytree_merge(self, source: Path, destination: Path) -> None:
        for child in source.rglob("*"):
            relative = child.relative_to(source)
            target = destination / relative
            if child.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(child, target)


def read_backup_manifest(backup_dir: Path) -> dict[str, Any]:
    manifest_path = backup_dir.expanduser().resolve() / "manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


class JobResetService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def reset_jobs(self, *, include_standalone_workflows: bool = True) -> JobResetResult:
        self.session.flush()
        work_item_ids = list(self.session.scalars(select(WorkItem.id)).all())
        workflow_run_ids = (
            list(self.session.scalars(select(WorkflowRun.id)).all())
            if include_standalone_workflows
            else []
        )

        discord_message_conditions = []
        discord_thread_conditions = []
        event_conditions = []
        if work_item_ids:
            discord_message_conditions.append(DiscordMessage.work_item_id.in_(work_item_ids))
            discord_thread_conditions.append(DiscordThread.work_item_id.in_(work_item_ids))
            event_conditions.append(WorkEvent.work_item_id.in_(work_item_ids))
            event_conditions.append(
                (WorkEvent.entity_kind == "work_item") & WorkEvent.entity_id.in_(work_item_ids)
            )
        if workflow_run_ids:
            discord_message_conditions.append(DiscordMessage.workflow_run_id.in_(workflow_run_ids))
            discord_thread_conditions.append(DiscordThread.workflow_run_id.in_(workflow_run_ids))
            event_conditions.append(WorkEvent.workflow_run_id.in_(workflow_run_ids))
            event_conditions.append(
                (WorkEvent.entity_kind == "workflow_run")
                & WorkEvent.entity_id.in_(workflow_run_ids)
            )

        discord_messages_deleted = self._delete_where(DiscordMessage, discord_message_conditions)
        discord_threads_deleted = self._delete_where(DiscordThread, discord_thread_conditions)
        workflow_events_deleted = self._delete_where(WorkEvent, event_conditions)

        schedule_occurrences_unlinked = 0
        if work_item_ids:
            schedule_occurrences_unlinked += self._rowcount(
                self.session.execute(
                    update(ScheduleOccurrence)
                    .where(ScheduleOccurrence.work_item_id.in_(work_item_ids))
                    .values(work_item_id=None)
                )
            )
        if workflow_run_ids:
            schedule_occurrences_unlinked += self._rowcount(
                self.session.execute(
                    update(ScheduleOccurrence)
                    .where(ScheduleOccurrence.workflow_run_id.in_(workflow_run_ids))
                    .values(workflow_run_id=None)
                )
            )

        workflow_runs_deleted = 0
        if workflow_run_ids:
            workflow_runs_deleted = self._rowcount(
                self.session.execute(
                    delete(WorkflowRun).where(WorkflowRun.id.in_(workflow_run_ids))
                )
            )

        work_items_deleted = 0
        if work_item_ids:
            work_items_deleted = self._rowcount(
                self.session.execute(delete(WorkItem).where(WorkItem.id.in_(work_item_ids)))
            )

        agent_results_deleted = self._rowcount(self.session.execute(delete(AgentResult)))
        self.session.flush()
        return JobResetResult(
            work_items_deleted=work_items_deleted,
            workflow_runs_deleted=workflow_runs_deleted,
            discord_messages_deleted=discord_messages_deleted,
            discord_threads_deleted=discord_threads_deleted,
            workflow_events_deleted=workflow_events_deleted,
            schedule_occurrences_unlinked=schedule_occurrences_unlinked,
            agent_results_deleted=agent_results_deleted,
        )

    def _delete_where(self, model: type, conditions: list[Any]) -> int:
        if not conditions:
            return 0
        return self._rowcount(self.session.execute(delete(model).where(or_(*conditions))))

    def _rowcount(self, result: Any) -> int:
        rowcount = getattr(result, "rowcount", 0)
        return int(rowcount if rowcount is not None and rowcount >= 0 else 0)
