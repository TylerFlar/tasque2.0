from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.db import session_scope
from tasque2.models import WorkflowDefinition, WorkflowNode, WorkflowRun, WorkItem
from tasque2.runtime import WorkRunner
from tasque2.workflows import WorkflowService


def test_sequential_workflow_runs_through_work_items(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {
                "key": "first",
                "kind": "work",
                "title": "First",
                "task_instruction": "First output.",
                "worker_kind": "function.echo",
            },
            {
                "key": "second",
                "kind": "work",
                "title": "Second",
                "task_instruction": "Second output.",
                "worker_kind": "function.echo",
                "depends_on": ["first"],
            },
        ]
    }
    with session_scope() as session:
        service = WorkflowService(session)
        workflow_definition = service.create_definition(
            name="sequential",
            version="1",
            definition=definition,
        )
        run = service.start_run(workflow_definition_id=workflow_definition.id)

        assert service.tick_runs() == 1
        first_node = session.scalar(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.node_key == "first",
            )
        )
        assert first_node is not None
        assert first_node.status == "enqueued"
        assert first_node.work_item_id is not None

        WorkRunner(session).run_next()
        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "completed"
        nodes = session.scalars(
            select(WorkflowNode)
            .where(WorkflowNode.workflow_run_id == run.id)
            .order_by(WorkflowNode.node_key)
        ).all()
        assert [node.status for node in nodes] == ["succeeded", "succeeded"]
        assert nodes[0].output["task_instruction"] == "First output."


def test_workflow_gate_waits_for_answer(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {
                "key": "prepare",
                "kind": "work",
                "task_instruction": "Prepare.",
                "worker_kind": "function.echo",
            },
            {
                "key": "approval",
                "kind": "gate",
                "prompt": "Continue?",
                "depends_on": ["prepare"],
            },
        ]
    }
    with session_scope() as session:
        service = WorkflowService(session)
        workflow_definition = service.create_definition(
            name="gate",
            version="1",
            definition=definition,
        )
        run = service.start_run(workflow_definition_id=workflow_definition.id)

        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "awaiting_input"
        gate = session.scalar(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.node_key == "approval",
            )
        )
        assert gate is not None
        assert gate.status == "awaiting_input"

        service.answer_gate(workflow_run_id=run.id, node_key="approval", answer="yes")
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "completed"


def test_workflow_failure_fails_run(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {
                "key": "fail",
                "kind": "work",
                "task_instruction": "No worker.",
                "worker_kind": "missing.worker",
            }
        ]
    }
    with session_scope() as session:
        service = WorkflowService(session)
        workflow_definition = service.create_definition(
            name="fail",
            version="1",
            definition=definition,
        )
        run = service.start_run(workflow_definition_id=workflow_definition.id)

        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "failed"
        work = session.scalar(select(WorkItem).where(WorkItem.workflow_run_id == run.id))
        assert work is not None
        assert work.status == "dead_letter"


def test_workflow_run_pause_resume_and_cancel_controls_work(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {
                "key": "step",
                "kind": "work",
                "task_instruction": "Run later.",
                "worker_kind": "function.echo",
            }
        ]
    }
    with session_scope() as session:
        service = WorkflowService(session)
        workflow_definition = service.create_definition(
            name="controlled",
            version="1",
            definition=definition,
        )
        run = service.start_run(workflow_definition_id=workflow_definition.id)
        service.tick_runs()
        work = session.scalar(select(WorkItem).where(WorkItem.workflow_run_id == run.id))
        assert work is not None
        assert work.status == "ready"

        paused = service.pause_run(run.id)
        assert paused.status == "paused"
        assert session.get(WorkItem, work.id).status == "paused"

        resumed = service.resume_run(run.id)
        assert resumed.status == "active"
        assert session.get(WorkItem, work.id).status == "ready"

        canceled = service.cancel_run(run.id)
        assert canceled.status == "canceled"
        assert session.get(WorkItem, work.id).status == "canceled"
        node = session.scalar(select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id))
        assert node is not None
        assert node.status == "canceled"


def test_workflow_fan_out_and_join(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {
                "key": "fan",
                "kind": "fan_out",
                "items": ["a", "b"],
                "child_title_template": "Process {item}",
                "child_task_instruction_template": "Process item {item}",
                "child_worker_kind": "function.echo",
            },
            {"key": "join", "kind": "join", "depends_on": ["fan"]},
        ]
    }
    with session_scope() as session:
        service = WorkflowService(session)
        workflow_definition = service.create_definition(
            name="fan",
            version="1",
            definition=definition,
        )
        run = service.start_run(workflow_definition_id=workflow_definition.id)

        service.tick_runs()
        children = session.scalars(
            select(WorkflowNode)
            .where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.parent_node_id.is_not(None),
            )
            .order_by(WorkflowNode.node_key)
        ).all()
        assert [child.node_key for child in children] == ["fan.0", "fan.1"]

        service.tick_runs()
        WorkRunner(session).run_next()
        WorkRunner(session).run_next()
        service.tick_runs()

        saved = session.get(WorkflowRun, run.id)
        assert saved.status == "completed"
        join = session.scalar(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.node_key == "join",
            )
        )
        assert join is not None
        assert set(join.output["dependencies"]) >= {"fan", "fan.0", "fan.1"}


def test_workflow_fan_out_from_upstream_output(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {
                "key": "list",
                "kind": "work",
                "title": "List",
                "task_instruction": "List items.",
                "worker_kind": "provider.fake",
            },
            {
                "key": "fan",
                "kind": "fan_out",
                "items_from_output": "list.items",
                "child_title_template": "Process {item[name]}",
                "child_task_instruction_template": "Process {item[name]}",
                "child_worker_kind": "function.echo",
                "depends_on": ["list"],
            },
            {"key": "join", "kind": "join", "depends_on": ["fan"]},
        ]
    }
    with session_scope() as session:
        service = WorkflowService(session)
        workflow_definition = service.create_definition(
            name="output-fan",
            version="1",
            definition=definition,
        )
        run = service.start_run(workflow_definition_id=workflow_definition.id)

        service.tick_runs()
        work = session.scalar(select(WorkItem).where(WorkItem.workflow_run_id == run.id))
        assert work is not None
        WorkRunner(session).run_next()
        attempt = work.attempts[0]
        attempt.produces = {"items": [{"name": "alpha"}, {"name": "beta"}]}
        session.flush()
        service.tick_runs()

        children = session.scalars(
            select(WorkflowNode)
            .where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.parent_node_id.is_not(None),
            )
            .order_by(WorkflowNode.node_key)
        ).all()
        assert [child.definition["title"] for child in children] == [
            "Process alpha",
            "Process beta",
        ]


def test_workflow_file_loads_markdown_node_templates(fresh_db: Path, tmp_path: Path) -> None:
    list_template = tmp_path / "list.md"
    child_template = tmp_path / "child.md"
    list_template.write_text("# List\n\nReturn items.", encoding="utf-8")
    child_template.write_text(
        "# Child\n\nProcess {item[name]}.\n\nExample output: `{code, title}`",
        encoding="utf-8",
    )
    path = tmp_path / "workflow.json"
    path.write_text(
        json.dumps(
            {
                "name": "template-workflow",
                "version": "1",
                "definition": {
                    "nodes": [
                        {
                            "key": "list",
                            "kind": "work",
                            "task_template_path": "list.md",
                            "worker_kind": "provider.fake",
                        },
                        {
                            "key": "fan",
                            "kind": "fan_out",
                            "items_from_output": "list.items",
                            "child_task_template_path": "child.md",
                            "child_title_template": "Process {item[name]}",
                            "child_worker_kind": "function.echo",
                            "depends_on": ["list"],
                        },
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with session_scope() as session:
        service = WorkflowService(session)
        workflow_definition = service.load_definition_file(path)
        nodes = workflow_definition.definition["nodes"]
        assert nodes[0]["task_instruction"] == "# List\n\nReturn items."
        assert "Example output: `{code, title}`" in nodes[1]["child_task_instruction_template"]

        run = service.start_run(workflow_definition_id=workflow_definition.id)
        service.tick_runs()
        work = session.scalar(select(WorkItem).where(WorkItem.workflow_run_id == run.id))
        assert work is not None
        WorkRunner(session).run_next()
        work.attempts[0].produces = {"items": [{"name": "alpha"}]}
        session.flush()
        service.tick_runs()

        child = session.scalar(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.node_key == "fan.0",
            )
        )
        assert child is not None
        assert child.definition["task_instruction"] == (
            "# Child\n\nProcess alpha.\n\nExample output: `{code, title}`"
        )


def test_workflow_cli_create_validate_and_list(fresh_db: Path, tmp_path: Path) -> None:
    path = tmp_path / "workflow.json"
    path.write_text(
        json.dumps(
            {
                "name": "cli-workflow",
                "version": "1",
                "definition": {
                    "nodes": [
                        {
                            "key": "step",
                            "kind": "work",
                            "task_instruction": "Run.",
                            "worker_kind": "function.echo",
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    validated = runner.invoke(app, ["workflow-validate-file", str(path)])
    created = runner.invoke(app, ["workflow-create-file", str(path)])
    listed = runner.invoke(app, ["workflow-list"])

    assert validated.exit_code == 0
    assert "valid: cli-workflow@1" in validated.output
    assert created.exit_code == 0
    assert listed.exit_code == 0
    assert "cli-workflow" in listed.output

    with session_scope() as session:
        definition = session.scalar(
            select(WorkflowDefinition).where(WorkflowDefinition.name == "cli-workflow")
        )
        assert definition is not None
