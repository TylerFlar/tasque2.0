from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from typer.testing import CliRunner

from tasque2.artifacts import ArtifactStore
from tasque2.cli import app
from tasque2.db import create_schema, reset_engine, session_scope
from tasque2.models import Artifact, DiscordMessage, DiscordThread, WorkflowRun, WorkItem
from tasque2.ops import BackupService, JobResetService, read_backup_manifest
from tasque2.repo import WorkRepository
from tasque2.reports import ReportService, report_to_json
from tasque2.runtime import WorkRunner
from tasque2.workflows import WorkflowService


def test_backup_and_restore_round_trip(fresh_db: Path, tmp_path: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Back me up",
            task_instruction="Persist me.",
            worker_kind="function.echo",
        )
        ArtifactStore(tmp_path / "data" / "artifacts").write_text(
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
    assert manifest["artifact_count"] == 1

    with session_scope() as session:
        WorkRepository(session).create_work_item(
            title="After backup",
            task_instruction="This should disappear after restore.",
            worker_kind="manual",
        )

    BackupService().restore_backup(backup_dir, force=True)
    reset_engine()
    create_schema()

    with session_scope() as session:
        titles = [work.title for work in session.scalars(select(WorkItem)).all()]
        assert titles == ["Back me up"]
        assert session.get(WorkItem, original_id) is not None
        assert session.scalar(select(Artifact).where(Artifact.title == "Report")) is not None


def test_restore_requires_force(fresh_db: Path, tmp_path: Path) -> None:
    backup_dir = tmp_path / "backup"
    BackupService().create_backup(backup_dir)

    try:
        BackupService().restore_backup(backup_dir)
    except ValueError as exc:
        assert str(exc) == "Restore requires force=True."
    else:
        raise AssertionError("Restore without force should fail.")


def test_reset_jobs_clears_work_and_standalone_workflow_history(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Dead job",
            task_instruction="Fail me.",
            worker_kind="function.missing",
        )
        WorkRunner(session).run_next()
        definition = WorkflowService(session).create_definition(
            name="reset-test",
            version="1",
            definition={
                "nodes": [
                    {
                        "key": "step",
                        "kind": "work",
                        "task_instruction": "Run.",
                        "worker_kind": "function.echo",
                    }
                ]
            },
        )
        run = WorkflowService(session).start_run(workflow_definition_id=definition.id)
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
        run_id = run.id

        result = JobResetService(session).reset_jobs()

        assert result.work_items_deleted == 1
        assert result.workflow_runs_deleted == 1
        assert result.discord_messages_deleted == 1
        assert result.discord_threads_deleted == 1
        assert session.scalars(select(WorkItem)).all() == []
        assert session.get(WorkflowRun, run_id) is None
        assert session.scalars(select(DiscordMessage)).all() == []
        assert session.scalars(select(DiscordThread)).all() == []


def test_work_report_includes_attempts_events_and_json(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Report work",
            task_instruction="Run report.",
            worker_kind="function.echo",
        )
        WorkRunner(session).run_next()

        report = ReportService(session).work_report(work.id)

        assert "Work Report: Report work" in report.body
        assert report.data["work_item"]["status"] == "succeeded"
        assert report.data["attempts"][0]["status"] == "succeeded"
        assert "work.succeeded" in {event["event_type"] for event in report.data["events"]}
        assert '"work_item"' in report_to_json(report)


def test_backup_and_report_cli_commands(fresh_db: Path, tmp_path: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="CLI report",
            task_instruction="Run CLI report.",
            worker_kind="function.echo",
        )
        work_id = work.id

    runner = CliRunner()
    backup_result = runner.invoke(app, ["backup-create", str(tmp_path / "cli-backup"), "--no-artifacts"])
    assert backup_result.exit_code == 0
    assert "backup_dir" in backup_result.output

    report_result = runner.invoke(app, ["report-work", work_id])
    assert report_result.exit_code == 0
    assert "Work Report: CLI report" in report_result.output


def test_artifact_cli_capture_list_show_and_archive(fresh_db: Path, tmp_path: Path) -> None:
    source = tmp_path / "upload.txt"
    source.write_text("artifact body", encoding="utf-8")
    runner = CliRunner()

    captured = runner.invoke(
        app,
        [
            "artifact-capture",
            str(source),
            "--kind",
            "note",
            "--tag",
            "discord_upload",
            "--title",
            "Upload note",
        ],
    )
    assert captured.exit_code == 0
    artifact_id = captured.output.strip()

    listed = runner.invoke(app, ["artifact-list", "Upload", "--tag", "discord_upload"])
    shown = runner.invoke(app, ["artifact-show", artifact_id])
    archived = runner.invoke(app, ["artifact-archive", artifact_id])

    assert listed.exit_code == 0
    assert "Upload note" in listed.output
    assert shown.exit_code == 0
    assert str(source.name) in shown.output or "Upload note" in shown.output
    assert archived.exit_code == 0
