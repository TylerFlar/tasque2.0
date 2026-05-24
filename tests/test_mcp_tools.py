from __future__ import annotations

import json
from pathlib import Path

from tasque2 import result_inbox
from tasque2.db import session_scope
from tasque2.mcp import tools
from tasque2.models import WorkflowRun
from tasque2.workflows import WorkflowService


def _ok(payload: str) -> dict:
    data = json.loads(payload)
    assert data["ok"] is True
    return data


def test_mcp_memory_tools_search_and_update_canonical(fresh_db: Path) -> None:
    created = _ok(
        tools.memory_create(
            namespace="health",
            kind="note",
            content="Completed workout actual loads: bench 95x10 at RPE 8.",
            tags=["workout", "completion"],
        )
    )

    found = _ok(
        tools.memory_search(
            intent="recent workout completions",
            query="actual loads RPE",
            namespace="health",
            tags=["workout", "completion"],
        )
    )
    assert [item["id"] for item in found["items"]] == [created["memory"]["id"]]

    upserted = _ok(
        tools.memory_upsert_canonical(
            namespace="health",
            canonical_key="current_workout_state",
            kind="summary",
            content="Current workout state: last confirmed push.",
            tags=["workout", "state"],
        )
    )
    canonical = _ok(
        tools.memory_get_canonical(
            intent="current workout state",
            namespace="health",
            canonical_key="current_workout_state",
        )
    )
    assert canonical["memory"]["id"] == upserted["memory"]["id"]
    assert "last confirmed push" in canonical["memory"]["content"]


def test_mcp_memory_ingest_and_coordination_tools(fresh_db: Path) -> None:
    ingested = _ok(
        tools.memory_ingest_text(
            namespace="research",
            title="Attention paper note",
            content="Attention models should preserve source provenance.",
            source_kind="test",
            source_id="attention-note",
            tags=["paper"],
        )
    )
    assert len(ingested["memory_ids"]) == 2

    found = _ok(
        tools.memory_search(
            intent="find ingested provenance note",
            query="source provenance",
            namespace="research",
        )
    )
    assert found["items"]

    todo = _ok(
        tools.todo_write(
            scope="research-loop",
            namespace="research",
            items=[{"text": "Collect sources", "status": "done"}, "Write synthesis"],
        )
    )
    assert todo["memory"]["kind"] == "todo"
    assert "Write synthesis" in todo["memory"]["content"]

    question = _ok(
        tools.ask_user(
            question="Which account should this use?",
            context="Two accounts match the request.",
            namespace="research",
        )
    )
    assert question["memory"]["kind"] == "question"
    assert "needs_user" in question["memory"]["tags"]


def test_mcp_artifact_tools_capture_read_and_mark_for_discord(
    fresh_db: Path,
    tmp_path: Path,
) -> None:
    source = tmp_path / "result.txt"
    source.write_text("hello from worker file", encoding="utf-8")

    captured = _ok(
        tools.artifact_capture_file(
            path=str(source),
            kind="worker_file",
            title="Result",
            tags=["example"],
            discord_upload=True,
        )
    )
    artifact = captured["artifact"]
    assert "discord_upload" in artifact["tags"]
    assert Path(artifact["local_path"]).read_text(encoding="utf-8") == "hello from worker file"

    read = _ok(
        tools.artifact_read_text(
            intent="verify worker file",
            artifact_id=artifact["id"],
        )
    )
    assert read["text"] == "hello from worker file"


def test_mcp_work_tools_enqueue_and_report_status(fresh_db: Path) -> None:
    queued = _ok(
        tools.work_enqueue(
            title="Follow-up",
            task_instruction="Do the follow-up.",
            worker_kind="manual",
            context={"memory_namespace": "global"},
        )
    )

    fetched = _ok(
        tools.work_get(
            intent="inspect queued follow-up",
            work_item_id=queued["work_item"]["id"],
        )
    )
    assert fetched["work_item"]["task_instruction"] == "Do the follow-up."

    status = _ok(tools.system_status(intent="queue counts"))
    assert status["status"]["ready_work"] == 1


def test_mcp_work_enqueue_can_load_template_file(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "followup.template.md"
    template.write_text("# Follow-up Template\n\nUse the maintained template.", encoding="utf-8")

    queued = _ok(
        tools.work_enqueue(
            title="Templated follow-up",
            task_template_path="followup.template.md",
            template_base_dir=str(tmp_path),
            worker_kind="manual",
        )
    )

    fetched = _ok(
        tools.work_get(
            intent="inspect templated follow-up",
            work_item_id=queued["work_item"]["id"],
        )
    )
    assert fetched["work_item"]["task_instruction"] == (
        "# Follow-up Template\n\nUse the maintained template."
    )


def test_mcp_schedule_tools_create_and_list_work_schedule(fresh_db: Path) -> None:
    created = _ok(
        tools.schedule_create_work(
            name="Daily check",
            schedule_type="cron",
            expression="0 9 * * *",
            task_instruction="Run the daily check.",
            worker_kind="provider.default",
            context={"memory_namespace": "personal"},
        )
    )

    schedule = created["schedule"]
    assert schedule["name"] == "Daily check"
    assert schedule["payload"]["task_instruction"] == "Run the daily check."
    assert schedule["payload"]["context"]["memory_namespace"] == "personal"

    listed = _ok(tools.schedule_list(intent="inspect schedules", enabled=True))
    assert [item["id"] for item in listed["items"]] == [schedule["id"]]


def test_mcp_schedule_create_work_can_reference_template_file(
    fresh_db: Path,
    tmp_path: Path,
) -> None:
    template = tmp_path / "scheduled.template.md"
    template.write_text("# Scheduled Template\n\nRun from Markdown.", encoding="utf-8")

    created = _ok(
        tools.schedule_create_work(
            name="Templated daily check",
            schedule_type="cron",
            expression="0 9 * * *",
            task_template_path="scheduled.template.md",
            template_base_dir=str(tmp_path),
            worker_kind="provider.default",
            context={"memory_namespace": "personal"},
        )
    )

    schedule = created["schedule"]
    assert schedule["payload"]["task_template_path"] == "scheduled.template.md"
    assert schedule["payload"]["template_base_dir"] == str(tmp_path)
    assert schedule["payload"]["context"]["memory_namespace"] == "personal"


def test_mcp_workflow_tools_list_and_start_workflow(fresh_db: Path) -> None:
    with session_scope() as session:
        WorkflowService(session).create_definition(
            name="daily-gmail-cleanup",
            version="1",
            definition={
                "nodes": [
                    {
                        "key": "noop",
                        "kind": "work",
                        "task_instruction": "Run once.",
                        "worker_kind": "function.echo",
                    }
                ]
            },
        )

    listed = _ok(tools.workflow_list(intent="find email cleanup workflow", enabled=True))
    assert listed["items"][0]["name"] == "daily-gmail-cleanup"

    started = _ok(
        tools.workflow_start(
            workflow_name="daily-gmail-cleanup",
            run_name="Manual email cleanup",
            input={"source": "test"},
        )
    )
    run_id = started["workflow_run"]["id"]
    assert started["workflow_run"]["name"] == "Manual email cleanup"

    with session_scope() as session:
        run = session.get(WorkflowRun, run_id)
        assert run is not None
        assert run.input["source"] == "test"


def test_mcp_submit_worker_result_deposits_in_inbox(fresh_db: Path) -> None:
    token = result_inbox.mint_token()

    submitted = _ok(
        tools.submit_worker_result(
            result_token=token,
            report="Workout report",
            summary="Workout summary",
            produces={"focus": "push"},
        )
    )
    assert submitted["result_token"] == token

    payload = result_inbox.read_and_consume(token, agent_kind="worker")
    assert payload == {
        "status": "succeeded",
        "report": "Workout report",
        "summary": "Workout summary",
        "produces": {"focus": "push"},
        "error": None,
    }
