from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select

from tasque2.daemon import DaemonTick
from tasque2.db import session_scope
from tasque2.models import Artifact, ProviderRun, Schedule, WorkAttempt, WorkflowRun, WorkItem
from tasque2.ops.smoke import SMOKE_WORKFLOW, run_smoke
from tasque2.workflows import WorkflowService


def test_smoke_run_exercises_core_orchestration(fresh_db: Path) -> None:
    with session_scope() as session:
        result = run_smoke(session, title="Test local smoke")

        schedule = session.get(Schedule, result.schedule_id)
        workflow_run = session.get(WorkflowRun, result.workflow_run_id)
        scheduled_work = session.get(WorkItem, result.scheduled_work_item_id)
        provider_run = session.get(ProviderRun, result.provider_run_id)
        report_artifact = session.get(Artifact, result.report_artifact_id)
        provider_work = session.get(WorkItem, session.get(WorkAttempt, provider_run.attempt_id).work_item_id)
        provider_artifacts = session.scalars(
            select(Artifact).where(Artifact.work_item_id == provider_work.id, Artifact.source_kind == "provider_run")
        ).all()

        assert schedule.schedule_type == "date"
        assert scheduled_work.status == "succeeded"
        assert scheduled_work.schedule_id == schedule.id
        assert workflow_run.status == "completed"
        assert result.workflow_status == "completed"
        assert provider_run.status == "succeeded"
        assert provider_run.provider == "fake"
        assert provider_work.status == "succeeded"
        assert provider_work.workflow_run_id == workflow_run.id
        assert {artifact.kind for artifact in provider_artifacts} >= {"provider_stream", "worker_report"}
        assert report_artifact.workflow_run_id == workflow_run.id
        assert report_artifact.tags == ["smoke", "workflow-report"]
        assert (
            Path(report_artifact.local_path)
            .read_text(encoding="utf-8")
            .startswith("# Workflow Report: Test local smoke")
        )
        assert result.ticks >= 2
        assert set(result.as_dict()) == {
            "schedule_id",
            "scheduled_work_item_id",
            "workflow_run_id",
            "workflow_status",
            "provider_run_id",
            "report_artifact_id",
            "ticks",
        }


def test_smoke_workflow_file_runs_to_completion(fresh_db: Path, tmp_path: Path) -> None:
    workflow_path = tmp_path / "local-smoke.workflow.json"
    workflow_path.write_text(
        json.dumps({"name": "tasque.local_smoke_file", "version": "1", "definition": SMOKE_WORKFLOW}),
        encoding="utf-8",
    )
    with session_scope() as session:
        service = WorkflowService(session)
        definition = service.load_definition_file(workflow_path)
        run = service.start_run(workflow_definition_id=definition.id)

        ticker = DaemonTick()
        for _ in range(5):
            ticker.run(session, max_claims=10)
            session.refresh(run)
            if run.status in {"completed", "failed"}:
                break

        assert run.status == "completed"
        assert set(run.state["outputs"]) == {"prepare", "provider", "join"}
        assert run.state["outputs"]["provider"] == {"ok": True}
