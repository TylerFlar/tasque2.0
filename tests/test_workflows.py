from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from opentelemetry.trace import SpanKind
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.models import WorkEvent, WorkflowDefinition, WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.templates import read_template_file, resolve_template_path
from tasque2.work.queue import WorkQueue
from tasque2.work.runner import WorkRunner
from tasque2.workflows import WorkflowService, parse_definition_file, validate_definition


def _node(session: Session, run_id: str, key: str) -> WorkflowNode:
    node = session.scalar(
        select(WorkflowNode).where(WorkflowNode.workflow_run_id == run_id, WorkflowNode.node_key == key)
    )
    assert node is not None, key
    return node


def _children(session: Session, run_id: str) -> list[WorkflowNode]:
    return list(
        session.scalars(
            select(WorkflowNode)
            .where(WorkflowNode.workflow_run_id == run_id, WorkflowNode.parent_node_id.is_not(None))
            .order_by(WorkflowNode.node_key)
        ).all()
    )


def _run_work(session: Session, run_id: str) -> list[WorkItem]:
    return list(session.scalars(select(WorkItem).where(WorkItem.workflow_run_id == run_id)).all())


def _start(
    session: Session, definition_name: str, nodes: list[dict[str, Any]], **run: Any
) -> tuple[WorkflowService, WorkflowRun]:
    service = WorkflowService(session)
    definition = service.create_definition(name=definition_name, version="1", definition={"nodes": nodes})
    return service, service.start_run(workflow_definition_id=definition.id, **run)


def _write_workflow(directory: Path, data: dict[str, Any]) -> Path:
    path = directory / "workflow.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_sequential_workflow_runs_through_work_items(fresh_db: Path) -> None:
    nodes = [
        {"key": "first", "title": "First", "task_instruction": "First output.", "worker_kind": "function.echo"},
        {
            "key": "second",
            "kind": "work",
            "title": "Second",
            "task_instruction": "Second output.",
            "worker_kind": "function.echo",
            "depends_on": ["first"],
        },
    ]
    with session_scope() as session:
        service, run = _start(session, "sequential", nodes)

        assert service.tick_runs() == 1
        first_node = _node(session, run.id, "first")
        assert first_node.status == "enqueued"
        assert first_node.work_item_id is not None
        assert _node(session, run.id, "second").work_item_id is None

        WorkRunner(session).run_next()
        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()

        saved = session.get(WorkflowRun, run.id)
        assert saved.status == "completed"
        assert saved.ended_at is not None
        assert [_node(session, run.id, key).status for key in ("first", "second")] == ["succeeded", "succeeded"]
        assert _node(session, run.id, "first").output["task_instruction"] == "First output."
        assert saved.state["outputs"]["second"]["title"] == "Second"
        assert service.tick_runs() == 0


def test_workflow_node_deadline_dead_letters_stale_work(fresh_db: Path) -> None:
    past = (utc_now() - timedelta(hours=1)).isoformat()
    nodes = [
        {"key": "stale_step", "task_instruction": "Too late.", "worker_kind": "function.echo", "deadline_at": past}
    ]
    with session_scope() as session:
        service, run = _start(session, "deadline", nodes)
        service.tick_runs()

        node = _node(session, run.id, "stale_step")
        assert session.get(WorkItem, node.work_item_id).deadline_at is not None
        assert WorkQueue(session).expire_overdue_work() == 1

        service.tick_runs()
        assert session.get(WorkItem, node.work_item_id).status == "dead_letter"
        assert node.status == "failed"
        assert session.get(WorkflowRun, run.id).status == "failed"


def test_workflow_node_deadline_seconds_counts_from_enqueue(fresh_db: Path) -> None:
    nodes = [{"key": "timed", "worker_kind": "function.echo", "deadline_seconds": 90}]
    with session_scope() as session:
        service, run = _start(session, "timed", nodes)
        before = utc_now()
        service.tick_runs()

        deadline = _run_work(session, run.id)[0].deadline_at
        assert before + timedelta(seconds=89) < deadline < utc_now() + timedelta(seconds=91)


def test_workflow_gate_waits_for_answer(fresh_db: Path) -> None:
    nodes = [
        {"key": "prepare", "task_instruction": "Prepare.", "worker_kind": "function.echo"},
        {"key": "approval", "kind": "gate", "prompt": "Continue?", "depends_on": ["prepare"]},
    ]
    with session_scope() as session:
        service, run = _start(session, "gate", nodes)

        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "awaiting_input"
        assert _node(session, run.id, "approval").status == "awaiting_input"
        with pytest.raises(ValueError, match="Only gate nodes"):
            service.answer_gate(workflow_run_id=run.id, node_key="prepare", answer="yes")

        service.answer_gate(workflow_run_id=run.id, node_key="approval", answer="yes")
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "completed"
        assert _node(session, run.id, "approval").output == {"answer": "yes"}


def test_a_gate_that_is_not_waiting_cannot_be_answered(fresh_db: Path) -> None:
    nodes = [
        {"key": "prepare", "worker_kind": "function.echo"},
        {"key": "approval", "kind": "gate", "depends_on": ["prepare"]},
    ]
    with session_scope() as session:
        service, run = _start(session, "early-gate", nodes)

        with pytest.raises(ValueError, match="not waiting for an answer"):
            service.answer_gate(workflow_run_id=run.id, node_key="approval", answer="yes")
        with pytest.raises(KeyError):
            service.answer_gate(workflow_run_id=run.id, node_key="missing", answer="yes")

        assert _node(session, run.id, "approval").status == "pending"
        assert run.status == "active"


def test_answering_a_gate_of_a_canceled_run_leaves_it_canceled(fresh_db: Path) -> None:
    with session_scope() as session:
        service, run = _start(session, "canceled-gate", [{"key": "approval", "kind": "gate"}])
        service.tick_runs()
        service.cancel_run(run.id)
        ended_at = run.ended_at

        with pytest.raises(ValueError, match="gate canceled, run canceled"):
            service.answer_gate(workflow_run_id=run.id, node_key="approval", answer="yes")

        assert (run.status, run.ended_at) == ("canceled", ended_at)
        assert _node(session, run.id, "approval").output == {}
        answered = session.scalars(
            select(WorkEvent).where(
                WorkEvent.workflow_run_id == run.id, WorkEvent.event_type == "workflow.gate_answered"
            )
        ).all()
        assert answered == []


def test_answering_a_gate_twice_finalizes_the_run_once(fresh_db: Path, metric_points) -> None:
    with session_scope() as session:
        service, run = _start(session, "answer-once-probe", [{"key": "approval", "kind": "gate"}])
        service.tick_runs()
        service.answer_gate(workflow_run_id=run.id, node_key="approval", answer="yes")
        service.tick_runs()

        with pytest.raises(ValueError, match="gate succeeded, run completed"):
            service.answer_gate(workflow_run_id=run.id, node_key="approval", answer="again")
        service.tick_runs()

        assert run.status == "completed"
        assert _node(session, run.id, "approval").output == {"answer": "yes"}

    completions = [
        point.value
        for point in metric_points("tasque.workflow.runs")
        if point.attributes.get("tasque.workflow.name") == "answer-once-probe"
    ]
    assert completions == [1]


def test_a_gate_left_open_on_a_failed_run_cannot_be_answered(fresh_db: Path) -> None:
    nodes = [{"key": "approval", "kind": "gate"}, {"key": "doomed", "worker_kind": "missing.worker"}]
    with session_scope() as session:
        service, run = _start(session, "failed-gate", nodes)
        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()
        assert (run.status, _node(session, run.id, "approval").status) == ("failed", "awaiting_input")

        with pytest.raises(ValueError, match="run failed"):
            service.answer_gate(workflow_run_id=run.id, node_key="approval", answer="yes")

        assert run.status == "failed"
        assert _node(session, run.id, "approval").status == "awaiting_input"


def test_answering_a_gate_of_a_paused_run_keeps_it_paused(fresh_db: Path) -> None:
    with session_scope() as session:
        service, run = _start(session, "paused-gate", [{"key": "approval", "kind": "gate"}])
        service.tick_runs()
        service.pause_run(run.id)

        service.answer_gate(workflow_run_id=run.id, node_key="approval", answer="yes")
        assert run.status == "paused"

        assert service.resume_run(run.id).status == "active"
        service.tick_runs()
        assert run.status == "completed"


def test_workflow_failure_fails_run(fresh_db: Path) -> None:
    nodes = [{"key": "fail", "task_instruction": "No worker.", "worker_kind": "missing.worker"}]
    with session_scope() as session:
        service, run = _start(session, "fail", nodes)

        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "failed"
        assert [work.status for work in _run_work(session, run.id)] == ["dead_letter"]
        assert _node(session, run.id, "fail").failure_reason == "Work item ended with status dead_letter."


def test_a_node_that_cannot_start_fails_its_run_without_stopping_others(fresh_db: Path) -> None:
    with session_scope() as session:
        service, broken = _start(
            session, "broken", [{"key": "bad", "worker_kind": "function.echo", "priority": "high"}]
        )
        _, healthy = _start(session, "healthy", [{"key": "good", "worker_kind": "function.echo"}])

        service.tick_runs()

        bad = _node(session, broken.id, "bad")
        assert bad.status == "failed"
        assert bad.failure_reason.startswith("Node could not start:")
        assert session.get(WorkflowRun, broken.id).status == "failed"
        assert _node(session, healthy.id, "good").status == "enqueued"
        events = session.scalars(select(WorkEvent.event_type).where(WorkEvent.workflow_run_id == broken.id)).all()
        assert "workflow.node_failed" in events


def test_workflow_run_pause_resume_and_cancel_controls_work(fresh_db: Path) -> None:
    nodes = [{"key": "step", "task_instruction": "Run later.", "worker_kind": "function.echo"}]
    with session_scope() as session:
        service, run = _start(session, "controlled", nodes)
        service.tick_runs()
        work = _run_work(session, run.id)[0]
        assert work.status == "ready"

        assert service.pause_run(run.id).status == "paused"
        assert work.status == "paused"
        assert service.tick_runs() == 0

        assert service.resume_run(run.id).status == "active"
        assert work.status == "ready"

        canceled = service.cancel_run(run.id)
        assert canceled.status == "canceled"
        assert canceled.ended_at is not None
        assert work.status == "canceled"
        assert _node(session, run.id, "step").status == "canceled"
        assert service.cancel_run(run.id).status == "canceled"
        with pytest.raises(KeyError):
            service.pause_run("missing")


def test_workflow_fan_out_and_join(fresh_db: Path) -> None:
    nodes = [
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
    with session_scope() as session:
        service, run = _start(session, "fan", nodes)

        service.tick_runs()
        children = _children(session, run.id)
        assert [child.node_key for child in children] == ["fan.0", "fan.1"]
        assert [child.definition["title"] for child in children] == ["Process a", "Process b"]
        assert _node(session, run.id, "fan").output["count"] == 2

        service.tick_runs()
        WorkRunner(session).run_next()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "completed"
        join = _node(session, run.id, "join")
        assert set(join.output["dependencies"]) >= {"fan", "fan.0", "fan.1"}


def test_workflow_fan_out_from_upstream_output(fresh_db: Path) -> None:
    nodes = [
        {"key": "list", "title": "List", "task_instruction": "List items.", "worker_kind": "provider.fake"},
        {
            "key": "fan",
            "kind": "fan_out",
            "items_from_output": "list.items",
            "child_title_template": "Process {item[name]}",
            "child_task_instruction_template": "Process {item[name]} with {item[missing]}",
            "child_worker_kind": "function.echo",
            "depends_on": ["list"],
        },
        {"key": "join", "kind": "join", "depends_on": ["fan"]},
    ]
    with session_scope() as session:
        service, run = _start(session, "output-fan", nodes)

        service.tick_runs()
        work = _run_work(session, run.id)[0]
        WorkRunner(session).run_next()
        work.attempts[0].produces = {"items": [{"name": "alpha"}, {"name": "beta"}]}
        session.flush()
        service.tick_runs()

        children = _children(session, run.id)
        assert [child.definition["title"] for child in children] == ["Process alpha", "Process beta"]
        assert children[0].definition["task_instruction"] == "Process alpha with {item[missing]}"
        assert children[1].input == {"item": {"name": "beta"}, "index": 1}


def test_fan_out_items_from_a_nested_output_path(fresh_db: Path) -> None:
    nodes = [
        {"key": "list", "worker_kind": "function.echo"},
        {
            "key": "fan",
            "kind": "fan_out",
            "items_from_output": {"node": "list", "path": "result.batches.1"},
            "child_worker_kind": "function.echo",
            "depends_on": ["list"],
        },
    ]
    with session_scope() as session:
        service, run = _start(session, "nested-fan", nodes)
        service.tick_runs()
        WorkRunner(session).run_next()
        _run_work(session, run.id)[0].attempts[0].produces = {"result": {"batches": [["x"], ["y", "z"]]}}
        session.flush()
        service.tick_runs()

        assert [child.input["item"] for child in _children(session, run.id)] == ["y", "z"]
        assert _children(session, run.id)[0].definition["task_instruction"] == "Process fan-out item 0: y"


def test_fan_out_over_something_other_than_a_list_fails_the_run(fresh_db: Path) -> None:
    nodes = [{"key": "fan", "kind": "fan_out", "items_from": "targets", "child_worker_kind": "function.echo"}]
    with session_scope() as session:
        service, run = _start(session, "bad-fan", nodes, input={"targets": "not a list"})
        service.tick_runs()

        assert _node(session, run.id, "fan").failure_reason == "fan_out items must be a list."
        assert session.get(WorkflowRun, run.id).status == "failed"


def test_workflow_file_loads_markdown_node_templates(fresh_db: Path, tmp_path: Path) -> None:
    (tmp_path / "list.md").write_text("# List\n\nReturn items.", encoding="utf-8")
    (tmp_path / "child.md").write_text("# Child\n\nProcess {item[name]}.\n\nExample output: `{code, title}`", "utf-8")
    path = _write_workflow(
        tmp_path,
        {
            "name": "template-workflow",
            "version": "1",
            "definition": {
                "nodes": [
                    {"key": "list", "kind": "work", "task_template_path": "list.md", "worker_kind": "provider.fake"},
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
        },
    )

    with session_scope() as session:
        service = WorkflowService(session)
        workflow_definition = service.load_definition_file(path)
        nodes = workflow_definition.definition["nodes"]
        assert nodes[0]["task_template_path"] == str((tmp_path / "list.md").resolve())
        assert nodes[0]["task_instruction"] == "# List\n\nReturn items."
        assert "Example output: `{code, title}`" in nodes[1]["child_task_instruction_template"]

        run = service.start_run(workflow_definition_id=workflow_definition.id)
        service.tick_runs()
        work = _run_work(session, run.id)[0]
        assert work.task_instruction == "# List\n\nReturn items."
        WorkRunner(session).run_next()
        work.attempts[0].produces = {"items": [{"name": "alpha"}]}
        session.flush()
        service.tick_runs()

        child = _node(session, run.id, "fan.0")
        assert child.definition["task_instruction"] == "# Child\n\nProcess alpha.\n\nExample output: `{code, title}`"


def test_node_templates_are_read_when_the_node_is_enqueued(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "step.md"
    template.write_text("Registered wording.", encoding="utf-8")
    path = _write_workflow(
        tmp_path,
        {
            "name": "live-template",
            "definition": {"nodes": [{"key": "step", "task_template_path": "step.md", "worker_kind": "function.echo"}]},
        },
    )

    with session_scope() as session:
        service = WorkflowService(session)
        definition = service.load_definition_file(path)

        def instruction_of_a_new_run() -> str:
            run = service.start_run(workflow_definition_id=definition.id)
            service.tick_runs()
            return _run_work(session, run.id)[0].task_instruction

        assert instruction_of_a_new_run() == "Registered wording."
        template.write_text("Edited wording.\n", encoding="utf-8")
        assert instruction_of_a_new_run() == "Edited wording."
        template.unlink()
        assert instruction_of_a_new_run() == "Registered wording."


def test_fan_out_templates_are_read_when_the_fan_out_expands(fresh_db: Path, tmp_path: Path) -> None:
    child = tmp_path / "child.md"
    shared = tmp_path / "shared.md"
    child.write_text("Registered child {item}.", encoding="utf-8")
    shared.write_text("Registered shared {item}.", encoding="utf-8")
    path = _write_workflow(
        tmp_path,
        {
            "name": "live-fan",
            "definition": {
                "nodes": [
                    {
                        "key": "per_item",
                        "kind": "fan_out",
                        "items": ["a"],
                        "child_task_template_path": "child.md",
                        "child_worker_kind": "function.echo",
                    },
                    {
                        "key": "shared",
                        "kind": "fan_out",
                        "items": ["b"],
                        "task_template_path": "shared.md",
                        "child_worker_kind": "function.echo",
                    },
                ]
            },
        },
    )

    with session_scope() as session:
        service = WorkflowService(session)
        definition = service.load_definition_file(path)
        child.write_text("Edited child {item}.", encoding="utf-8")
        shared.write_text("Edited shared {item}.", encoding="utf-8")

        run = service.start_run(workflow_definition_id=definition.id)
        service.tick_runs()
        service.tick_runs()

        instructions = sorted(work.task_instruction for work in _run_work(session, run.id))
        assert instructions == ["Edited child a.", "Edited shared b."]


def test_relative_node_templates_resolve_under_the_workflows_data_dir(fresh_db: Path) -> None:
    workflows_dir = get_settings().resolved_data_dir / "workflows"
    workflows_dir.mkdir(parents=True)
    (workflows_dir / "digest.md").write_text("From the data dir.", encoding="utf-8")
    nodes = [
        {
            "key": "found",
            "task_template_path": "digest.md",
            "task_instruction": "Inline.",
            "worker_kind": "function.echo",
        },
        {
            "key": "absent",
            "task_template_path": "absent.md",
            "task_instruction": "Inline only.",
            "worker_kind": "manual",
        },
    ]
    with session_scope() as session:
        service, run = _start(session, "relative", nodes)
        service.tick_runs()

        instructions = {work.title: work.task_instruction for work in _run_work(session, run.id)}
        assert instructions == {"found": "From the data dir.", "absent": "Inline only."}


def test_read_template_file_resolves_relative_paths_and_refuses_non_files(tmp_path: Path) -> None:
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "task.md").write_text("\n  Do the task.  \n", encoding="utf-8")

    assert read_template_file("task.md", base_dir=tmp_path / "prompts") == "Do the task."
    assert read_template_file("prompts/task.md") == "Do the task."
    assert resolve_template_path("prompts/task.md") == (tmp_path / "prompts" / "task.md").resolve()
    with pytest.raises(ValueError, match="does not exist"):
        read_template_file("absent.md", base_dir=tmp_path)
    with pytest.raises(ValueError, match="is not a file"):
        read_template_file(tmp_path / "prompts")


def test_workflow_nodes_take_the_run_lane_or_the_definition_name(fresh_db: Path) -> None:
    nodes = [
        {"key": "step", "worker_kind": "function.echo"},
        {"key": "fan", "kind": "fan_out", "items": ["x"], "child_worker_kind": "function.echo"},
    ]
    with session_scope() as session:
        service, default_run = _start(session, "lanes", nodes)
        lane_run = service.start_run(
            workflow_definition_id=default_run.workflow_definition_id, input={"lane": "kitchen"}
        )
        service.tick_runs()
        service.tick_runs()

        assert {work.lane for work in _run_work(session, default_run.id)} == {"lanes"}
        assert {work.lane for work in _run_work(session, lane_run.id)} == {"kitchen"}
        assert len(_run_work(session, lane_run.id)) == 2


def test_workflow_start_records_a_producer_span_the_nodes_join(fresh_db: Path, spans) -> None:
    nodes = [
        {"key": "step", "worker_kind": "function.echo"},
        {"key": "fan", "kind": "fan_out", "items": ["x", "y"], "child_worker_kind": "function.echo"},
    ]
    with session_scope() as session:
        service, run = _start(session, "traced", nodes, name="Traced run", discord_thread_id="thread-3")
        service.tick_runs()
        service.tick_runs()
        work = _run_work(session, run.id)
        run_id, traceparent = run.id, run.traceparent

    start = next(span for span in spans.get_finished_spans() if span.name == "tasque.workflow.start")
    assert start.kind is SpanKind.PRODUCER
    assert start.parent is None
    assert dict(start.attributes) == {
        "tasque.workflow.name": "traced",
        "tasque.workflow.version": "1",
        "tasque.workflow.run.id": run_id,
    }
    _, trace_id, span_id, _ = traceparent.split("-")
    assert (trace_id, span_id) == (format(start.context.trace_id, "032x"), format(start.context.span_id, "016x"))
    assert len(work) == 3
    assert {item.traceparent for item in work} == {traceparent}
    assert {item.discord_thread_id for item in work} == {"thread-3"}


def test_workflow_node_context_merges_run_input_under_node_context(fresh_db: Path) -> None:
    nodes = [{"key": "step", "worker_kind": "function.echo", "context": {"memory_namespace": "node", "extra": 1}}]
    with session_scope() as session:
        service, run = _start(session, "context", nodes, input={"memory_namespace": "run", "source": "test"})
        service.tick_runs()

        work = _run_work(session, run.id)[0]
        assert work.context == {"memory_namespace": "node", "source": "test", "extra": 1}
        assert work.idempotency_key == f"workflow:{run.id}:step"
        assert (work.source_kind, work.source_id) == ("workflow", run.id)


def test_workflow_runs_record_their_outcome(fresh_db: Path, metric_points) -> None:
    with session_scope() as session:
        service, completed = _start(session, "outcome-probe-completed", [{"key": "gate", "kind": "gate"}])
        _, canceled = _start(session, "outcome-probe-canceled", [{"key": "only", "worker_kind": "function.echo"}])
        service.tick_runs()
        service.answer_gate(workflow_run_id=completed.id, node_key="gate", answer="go")
        service.tick_runs()
        service.cancel_run(canceled.id)

    outcomes = {
        point.attributes["tasque.workflow.name"]: (point.attributes["tasque.workflow.outcome"], point.value)
        for point in metric_points("tasque.workflow.runs")
        if str(point.attributes.get("tasque.workflow.name", "")).startswith("outcome-probe")
    }
    assert outcomes == {
        "outcome-probe-completed": ("completed", 1),
        "outcome-probe-canceled": ("canceled", 1),
    }


def test_fan_out_tolerates_child_failure_and_still_runs_report(fresh_db: Path) -> None:
    nodes = [
        {
            "key": "fan",
            "kind": "fan_out",
            "items": ["a", "b"],
            "tolerate_child_failures": True,
            "child_title_template": "Process {item}",
            "child_task_instruction_template": "Process item {item}",
            "child_worker_kind": "missing.worker",
        },
        {"key": "report", "depends_on": ["fan"], "task_instruction": "Aggregate.", "worker_kind": "function.echo"},
    ]
    with session_scope() as session:
        service, run = _start(session, "tolerant-fan", nodes)

        service.tick_runs()
        service.tick_runs()
        WorkRunner(session).run_next()
        WorkRunner(session).run_next()
        service.tick_runs()

        children = _children(session, run.id)
        assert [child.status for child in children] == ["failed_tolerated", "failed_tolerated"]
        assert children[0].output == {"tolerated_failure": True}
        report = _node(session, run.id, "report")
        assert report.work_item_id is not None

        WorkRunner(session).run_next()
        service.tick_runs()

        assert report.status == "succeeded"
        assert session.get(WorkflowRun, run.id).status == "completed"


def test_fan_out_child_failure_without_tolerance_still_fails_run(fresh_db: Path) -> None:
    nodes = [
        {
            "key": "fan",
            "kind": "fan_out",
            "items": ["a"],
            "child_title_template": "Process {item}",
            "child_task_instruction_template": "Process item {item}",
            "child_worker_kind": "missing.worker",
        },
        {"key": "report", "depends_on": ["fan"], "task_instruction": "Aggregate.", "worker_kind": "function.echo"},
    ]
    with session_scope() as session:
        service, run = _start(session, "strict-fan", nodes)

        service.tick_runs()
        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert session.get(WorkflowRun, run.id).status == "failed"
        assert _node(session, run.id, "report").work_item_id is None


def test_fan_out_lanes_feed_a_merge_that_fans_out_again(fresh_db: Path) -> None:
    lanes = [
        {"key": "remote-us", "name": "Remote US"},
        {"key": "san-diego", "name": "San Diego"},
        {"key": "ats-direct", "name": "ATS sweep"},
        {"key": "wildcard", "name": "Wildcard"},
    ]
    nodes = [
        {
            "key": "scout",
            "kind": "fan_out",
            "items": lanes,
            "tolerate_child_failures": True,
            "child_title_template": "Scout: {item[name]}",
            "child_task_instruction_template": "Sweep the {item[name]} lane.",
            "child_worker_kind": "provider.fake",
        },
        {
            "key": "merge",
            "title": "Merge lanes",
            "task_instruction": "Dedupe and pick the batch.",
            "worker_kind": "provider.fake",
            "depends_on": ["scout"],
        },
        {
            "key": "apply",
            "kind": "fan_out",
            "items_from_output": "merge.jobs",
            "tolerate_child_failures": True,
            "child_title_template": "Apply: {item[company]}",
            "child_task_instruction_template": "Apply to {item[company]}.",
            "child_worker_kind": "function.echo",
            "depends_on": ["merge"],
        },
        {"key": "report", "task_instruction": "Report.", "worker_kind": "function.echo", "depends_on": ["apply"]},
    ]
    with session_scope() as session:
        service, run = _start(session, "lane-merge-apply", nodes)
        service.tick_runs()
        service.tick_runs()

        lane_keys = [f"scout.{index}" for index in range(len(lanes))]
        assert [_node(session, run.id, key).definition["title"] for key in lane_keys] == [
            "Scout: Remote US",
            "Scout: San Diego",
            "Scout: ATS sweep",
            "Scout: Wildcard",
        ]
        assert _node(session, run.id, "scout.0").input["item"]["key"] == "remote-us"

        for finished, key in enumerate(lane_keys, start=1):
            WorkRunner(session).run_next()
            attempt = _node(session, run.id, key).work_item.attempts[0]
            attempt.produces = {"lane": lanes[finished - 1]["key"], "candidates": [{"slug": f"s{finished}"}]}
            session.flush()
            service.tick_runs()
            if finished < len(lane_keys):
                assert _node(session, run.id, "merge").work_item_id is None

        merge_node = _node(session, run.id, "merge")
        assert merge_node.work_item_id is not None
        assert [_node(session, run.id, key).output["lane"] for key in lane_keys] == [lane["key"] for lane in lanes]

        WorkRunner(session).run_next()
        merge_node.work_item.attempts[0].produces = {"jobs": [{"company": "Acme"}, {"company": "Globex"}]}
        session.flush()
        service.tick_runs()
        service.tick_runs()

        apply_children = session.scalars(
            select(WorkflowNode)
            .where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.node_key.like("apply.%"))
            .order_by(WorkflowNode.node_key)
        ).all()
        assert [child.definition["title"] for child in apply_children] == ["Apply: Acme", "Apply: Globex"]

        for _ in apply_children:
            WorkRunner(session).run_next()
        service.tick_runs()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert _node(session, run.id, "report").status == "succeeded"
        assert session.get(WorkflowRun, run.id).status == "completed"


def test_a_tolerated_fan_out_failure_does_not_block_the_merge(fresh_db: Path) -> None:
    nodes = [
        {
            "key": "scout",
            "kind": "fan_out",
            "items": [{"key": "remote-us"}, {"key": "wildcard"}],
            "tolerate_child_failures": True,
            "child_title_template": "Scout: {item[key]}",
            "child_task_instruction_template": "Sweep {item[key]}.",
            "child_worker_kind": "missing.worker",
        },
        {"key": "merge", "task_instruction": "Merge.", "worker_kind": "function.echo", "depends_on": ["scout"]},
    ]
    with session_scope() as session:
        service, run = _start(session, "tolerant-scout", nodes)

        service.tick_runs()
        service.tick_runs()
        WorkRunner(session).run_next()
        WorkRunner(session).run_next()
        service.tick_runs()

        assert _node(session, run.id, "merge").work_item_id is not None

        WorkRunner(session).run_next()
        service.tick_runs()
        assert session.get(WorkflowRun, run.id).status == "completed"


def test_create_definition_replaces_the_same_name_and_version(fresh_db: Path) -> None:
    with session_scope() as session:
        service = WorkflowService(session)
        first = service.create_definition(name="flow", version="1", definition={"nodes": [{"key": "a"}]})
        second = service.create_definition(
            name="flow", version="1", definition={"nodes": [{"key": "b"}]}, enabled=False
        )
        other = service.create_definition(name="flow", version="2", definition={"nodes": [{"key": "c"}]})

        assert second.id == first.id
        assert (second.definition["nodes"][0]["key"], second.enabled) == ("b", False)
        assert other.id != first.id
        assert session.scalar(select(func.count()).select_from(WorkflowDefinition)) == 2
        with pytest.raises(ValueError, match="disabled"):
            service.start_run(workflow_definition_id=first.id)
        with pytest.raises(KeyError):
            service.start_run(workflow_definition_id="missing")


@pytest.mark.parametrize(
    ("definition", "message"),
    [
        ({}, "non-empty nodes list"),
        ({"nodes": []}, "non-empty nodes list"),
        ({"nodes": [{"kind": "work"}]}, "requires a key"),
        ({"nodes": [{"key": "a"}, {"key": "a"}]}, "Duplicate workflow node key: a"),
        ({"nodes": [{"key": "a", "kind": "loop"}]}, "Unsupported workflow node kind: loop"),
        ({"nodes": [{"key": "a", "depends_on": ["b"]}]}, "Unknown workflow dependency: b"),
    ],
)
def test_validate_definition_rejects_malformed_definitions(definition: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_definition(definition)


@pytest.mark.parametrize(
    ("data", "message"),
    [
        (["not", "an", "object"], "must contain a JSON object"),
        ({"definition": {"nodes": [{"key": "a"}]}}, "requires name"),
        ({"name": "x", "definition": []}, "requires definition object"),
        ({"name": "x", "definition": {"nodes": [{"key": "a", "task_template_path": "gone.md"}]}}, "does not exist"),
    ],
)
def test_parse_definition_file_rejects_malformed_files(tmp_path: Path, data: Any, message: str) -> None:
    path = tmp_path / "workflow.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        parse_definition_file(path)


def test_workflow_cli_validates_registers_starts_and_cancels(tmp_path: Path) -> None:
    path = _write_workflow(
        tmp_path,
        {
            "name": "cli-workflow",
            "version": "1",
            "definition": {"nodes": [{"key": "step", "task_instruction": "Run.", "worker_kind": "function.echo"}]},
        },
    )
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps({"name": "broken", "definition": {"nodes": []}}), encoding="utf-8")
    runner = CliRunner()

    validated = runner.invoke(app, ["workflow-validate", str(path)])
    invalid = runner.invoke(app, ["workflow-validate", str(broken)])
    registered = runner.invoke(app, ["workflow-register", str(path)])
    listed = runner.invoke(app, ["workflow-list"])
    started = runner.invoke(app, ["workflow-start", "cli-workflow", "--input-json", '{"lane": "cli"}'])

    assert validated.exit_code == 0, validated.output
    assert "cli-workflow@1 ok" in validated.output.replace("\n", " ")
    assert invalid.exit_code == 1
    assert registered.exit_code == 0, registered.output
    assert listed.exit_code == 0
    assert "cli-workflow" in listed.output
    assert started.exit_code == 0, started.output
    run_id = started.stdout.strip()

    canceled = runner.invoke(app, ["workflow-cancel", run_id])
    assert canceled.exit_code == 0, canceled.output
    with session_scope() as session:
        run = session.get(WorkflowRun, run_id)
        assert run.status == "canceled"
        assert run.input == {"lane": "cli"}
        assert session.scalar(select(WorkflowDefinition).where(WorkflowDefinition.name == "cli-workflow")) is not None
