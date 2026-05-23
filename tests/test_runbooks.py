from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.daemon import TasqueDaemon
from tasque2.db import reset_engine, session_scope
from tasque2.models import Artifact, ProviderRun, WorkflowRun, WorkItem
from tasque2.runbooks import LOCAL_SMOKE_WORKFLOW_DEFINITION, run_local_smoke
from tasque2.workflows import WorkflowService


def test_local_smoke_runbook_exercises_core_orchestration(fresh_db: Path) -> None:
    with session_scope() as session:
        result = run_local_smoke(session, title="Test local smoke")

        workflow_run = session.get(WorkflowRun, result.workflow_run_id)
        scheduled_work = session.get(WorkItem, result.scheduled_work_item_id)
        provider_work = session.get(WorkItem, result.provider_work_item_id)
        provider_run = session.get(ProviderRun, result.provider_run_id)
        report_artifact = session.get(Artifact, result.report_artifact_id)
        provider_artifacts = session.scalars(
            select(Artifact).where(
                Artifact.work_item_id == result.provider_work_item_id,
                Artifact.source_kind == "provider_run",
            )
        ).all()

        assert workflow_run is not None
        assert workflow_run.status == "completed"
        assert scheduled_work is not None
        assert scheduled_work.status == "succeeded"
        assert provider_work is not None
        assert provider_work.status == "succeeded"
        assert provider_run is not None
        assert provider_run.status == "succeeded"
        assert len(provider_artifacts) >= 2
        assert report_artifact is not None
        assert report_artifact.workflow_run_id == workflow_run.id
        assert Path(report_artifact.local_path).read_text(encoding="utf-8").startswith(
            "# Workflow Report: Test local smoke"
        )


def test_runbook_smoke_cli_json_runs_from_empty_database(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TASQUE2_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TASQUE2_DB_PATH", str(tmp_path / "tasque2.sqlite3"))
    reset_engine()

    result = CliRunner().invoke(app, ["runbook-smoke", "--title", "CLI smoke", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["workflow_status"] == "completed"
    assert payload["scheduled_work_item_id"]
    assert payload["provider_run_id"]
    assert payload["report_artifact_id"]


def test_local_smoke_workflow_file_runs(fresh_db: Path, tmp_path: Path) -> None:
    workflow_path = tmp_path / "local-smoke.workflow.json"
    workflow_path.write_text(
        json.dumps(
            {
                "name": "tasque.local_smoke_file",
                "version": "1",
                "definition": LOCAL_SMOKE_WORKFLOW_DEFINITION,
            }
        ),
        encoding="utf-8",
    )
    with session_scope() as session:
        service = WorkflowService(session)
        definition = service.load_definition_file(workflow_path)
        run = service.start_run(workflow_definition_id=definition.id)

        for _ in range(5):
            TasqueDaemon(session).run_once(max_work_items=10)
            session.refresh(run)
            if run.status in {"completed", "failed"}:
                break

        assert session.get(WorkflowRun, run.id).status == "completed"
