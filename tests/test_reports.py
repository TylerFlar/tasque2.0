from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.artifacts import ArtifactStore
from tasque2.db import create_schema, reset_engine, session_scope
from tasque2.models import (
    AgentResult,
    Artifact,
    DiscordMessage,
    DiscordThread,
    ProviderRun,
    Schedule,
    ScheduleOccurrence,
    WorkEvent,
    WorkflowRun,
    WorkItem,
)
from tasque2.ops.backup import BackupService, JobResetService, read_backup_manifest
from tasque2.ops.reports import ReportService, report_to_json
from tasque2.ops.status import get_system_status
from tasque2.schedules import ScheduleService
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner
from tasque2.workflows import WorkflowService

ECHO_WORKFLOW = {
    "nodes": [
        {"key": "step", "kind": "work", "task_instruction": "Run.", "worker_kind": "function.echo"},
        {"key": "done", "kind": "join", "depends_on": ["step"]},
    ]
}


def test_backup_and_restore_round_trip(fresh_db: Path, tmp_path: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Back me up",
            task_instruction="Persist me.",
            worker_kind="function.echo",
        )
        ArtifactStore().write_text(
            session,
            kind="report",
            title="Report",
            content="backup artifact",
            work_item_id=work.id,
        )
        original_id = work.id

    backup_dir = tmp_path / "backup"
    result = BackupService().create_backup(backup_dir)
    manifest = read_backup_manifest(result.backup_dir)
    assert manifest["database_file"] == "tasque2.sqlite3"
    assert manifest["artifacts_dir"] == "artifacts"
    assert manifest["artifact_count"] == 1

    with session_scope() as session:
        WorkRepository(session).create_work_item(
            title="After backup",
            task_instruction="This should disappear after restore.",
            worker_kind="manual",
        )

    restored = BackupService().restore_backup(backup_dir, force=True)
    reset_engine()
    create_schema()

    assert restored.previous_database_backup is not None
    assert restored.previous_database_backup.is_file()
    with session_scope() as session:
        titles = [work.title for work in session.scalars(select(WorkItem)).all()]
        assert titles == ["Back me up"]
        assert session.get(WorkItem, original_id) is not None
        artifact = session.scalar(select(Artifact).where(Artifact.title == "Report"))
        assert artifact is not None
        assert Path(artifact.local_path).read_text(encoding="utf-8") == "backup artifact"


def test_backup_can_leave_out_artifacts(fresh_db: Path, tmp_path: Path) -> None:
    with session_scope() as session:
        ArtifactStore().write_text(session, kind="report", title="Report", content="x")

    result = BackupService().create_backup(tmp_path / "backup", include_artifacts=False)

    assert result.artifacts_dir is None
    assert read_backup_manifest(result.backup_dir)["artifact_count"] == 0
    assert not (result.backup_dir / "artifacts").exists()


def test_backup_goes_to_a_timestamped_directory_by_default(fresh_db: Path, isolated: Path) -> None:
    result = BackupService().create_backup()

    assert result.backup_dir.parent == (isolated / "data" / "backups").resolve()
    assert result.backup_dir.name.startswith("tasque2-backup-")
    assert result.database_path.is_file()


def test_restore_requires_force(fresh_db: Path, tmp_path: Path) -> None:
    backup_dir = tmp_path / "backup"
    BackupService().create_backup(backup_dir)

    with pytest.raises(ValueError, match="Restore requires force=True."):
        BackupService().restore_backup(backup_dir)


def test_restore_requires_a_backup_database(fresh_db: Path, tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()

    with pytest.raises(FileNotFoundError, match="Backup database not found"):
        BackupService().restore_backup(tmp_path / "empty", force=True)


def test_reset_jobs_clears_work_and_standalone_workflow_history(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Dead job",
            task_instruction="Fail me.",
            worker_kind="function.missing",
        )
        WorkRunner(session).run_next()
        workflows = WorkflowService(session)
        definition = workflows.create_definition(name="reset-test", version="1", definition=ECHO_WORKFLOW)
        run = workflows.start_run(workflow_definition_id=definition.id)
        schedule = ScheduleService(session).create_schedule(
            name="Every hour",
            schedule_type="interval",
            expression="hours=1",
            worker_kind="function.echo",
            payload={"task_instruction": "Tick."},
        )
        occurrence = ScheduleOccurrence(
            schedule_id=schedule.id,
            scheduled_for=work.created_at,
            status="enqueued",
            work_item_id=work.id,
            dedupe_key="occurrence-1",
        )
        session.add(occurrence)
        session.add(
            DiscordMessage(
                discord_message_id="message-1",
                discord_channel_id="jobs",
                direction="outbound",
                content_preview="old output",
                work_item_id=work.id,
            )
        )
        session.add(
            DiscordThread(
                purpose="work",
                discord_channel_id="jobs",
                discord_thread_id="thread-1",
                work_item_id=work.id,
            )
        )
        session.add(AgentResult(result_token="token-1", agent_kind="worker", payload_json="{}"))
        session.flush()
        run_id, schedule_id, occurrence_id = run.id, schedule.id, occurrence.id

        result = JobResetService(session).reset_jobs()

        assert result.work_items_deleted == 1
        assert result.workflow_runs_deleted == 1
        assert result.discord_messages_deleted == 1
        assert result.discord_threads_deleted == 1
        assert result.schedule_occurrences_unlinked == 1
        assert result.agent_results_deleted == 1
        assert result.workflow_events_deleted > 0
        assert session.scalars(select(WorkItem)).all() == []
        assert session.get(WorkflowRun, run_id) is None
        assert session.scalars(select(DiscordMessage)).all() == []
        assert session.scalars(select(DiscordThread)).all() == []
        assert session.get(Schedule, schedule_id) is not None
        assert session.get(ScheduleOccurrence, occurrence_id).work_item_id is None
        assert session.scalars(select(WorkEvent).where(WorkEvent.entity_kind == "work_item")).all() == []


def test_reset_jobs_can_keep_workflow_runs(fresh_db: Path) -> None:
    with session_scope() as session:
        workflows = WorkflowService(session)
        definition = workflows.create_definition(name="keep-runs", version="1", definition=ECHO_WORKFLOW)
        run = workflows.start_run(workflow_definition_id=definition.id)
        workflows.tick_runs()

        result = JobResetService(session).reset_jobs(include_standalone_workflows=False)

        assert result.workflow_runs_deleted == 0
        assert session.get(WorkflowRun, run.id) is not None


def test_work_report_includes_attempts_events_and_json(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Report work",
            task_instruction="Run report.",
            worker_kind="function.echo",
            lane="reports",
        )
        WorkRunner(session).run_next()

        report = ReportService(session).work_report(work.id)

        assert report.title == "Work Report: Report work"
        assert "# Work Report: Report work" in report.body
        assert "- lane: reports" in report.body
        assert "- attempt 1: succeeded" in report.body
        assert report.data["work_item"]["status"] == "succeeded"
        assert report.data["attempts"][0]["status"] == "succeeded"
        assert "work.succeeded" in {event["event_type"] for event in report.data["events"]}
        assert json.loads(report_to_json(report))["work_item"]["id"] == work.id


def test_work_report_shows_the_provider_run_and_its_stream_artifact(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Provider work", task_instruction="Answer.", worker_kind="provider.fake"
        )
        WorkRunner(session).run_next()
        run = session.scalar(select(ProviderRun).where(ProviderRun.provider == "fake"))

        report = ReportService(session).work_report(work.id)

    (run_data,) = report.data["provider_runs"]
    assert run.stdout_artifact_id is not None
    assert run_data["id"] == run.id
    assert run_data["provider"] == "fake"
    assert run_data["stream_artifact_id"] == run.stdout_artifact_id
    assert f"  stream artifact: {run.stdout_artifact_id}" in report.body
    assert "worker_report" in {artifact["kind"] for artifact in report.data["artifacts"]}


def test_workflow_report_lists_nodes_work_and_events(fresh_db: Path) -> None:
    with session_scope() as session:
        workflows = WorkflowService(session)
        definition = workflows.create_definition(name="report-flow", version="2", definition=ECHO_WORKFLOW)
        run = workflows.start_run(workflow_definition_id=definition.id, name="Report flow")
        workflows.tick_runs()
        WorkRunner(session).run_next()
        workflows.tick_runs()

        report = ReportService(session).workflow_report(run.id)

    assert report.title == "Workflow Report: Report flow"
    assert "- status: completed" in report.body
    assert "- definition: report-flow@2" in report.body
    assert "- step: succeeded [work] (work=" in report.body
    assert "- done: succeeded [join]" in report.body
    assert [node["node_key"] for node in report.data["workflow_nodes"]] == ["step", "done"]
    assert "workflow.run_completed" in {event["event_type"] for event in report.data["events"]}


def test_reports_reject_unknown_ids(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ReportService(session)
        with pytest.raises(KeyError, match="Unknown work item"):
            service.work_report("missing")
        with pytest.raises(KeyError, match="Unknown workflow run"):
            service.workflow_report("missing")


def test_system_status_counts_work_schedules_and_runs(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        repo.create_work_item(title="Waiting", task_instruction="Wait.", worker_kind="manual")
        repo.create_work_item(title="Broken", task_instruction="Fail.", worker_kind="function.missing", priority=5)
        WorkRunner(session).run_next()
        ScheduleService(session).create_schedule(
            name="Nightly", schedule_type="cron", expression="0 3 * * *", worker_kind="manual", payload={}
        )
        workflows = WorkflowService(session)
        definition = workflows.create_definition(name="status-flow", version="1", definition=ECHO_WORKFLOW)
        workflows.start_run(workflow_definition_id=definition.id)

        snapshot = get_system_status(session)

    assert snapshot.work_items == {"ready": 1, "dead_letter": 1}
    assert snapshot.ready_work == 1
    assert snapshot.running_work == 0
    assert snapshot.failed_work_unresolved == 1
    assert snapshot.schedules_enabled == 1
    assert snapshot.workflow_runs == {"active": 1}
    assert snapshot.work_attempts == {"failed": 1}
