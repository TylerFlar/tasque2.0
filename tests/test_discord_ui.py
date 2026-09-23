from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.discord.gateway import FakeDiscordGateway
from tasque2.discord.output import DiscordOutputService
from tasque2.discord.ui import (
    COLOR_ALERT,
    COLOR_OK,
    DiscordUIAction,
    DiscordUIService,
    build_ops_embed,
    build_work_controls_view,
    build_workflow_controls_view,
    build_workflow_status_panel_embed,
    is_modal_action,
    make_custom_id,
    parse_custom_id,
)
from tasque2.models import WorkflowDefinition, WorkflowNode, WorkflowRun, WorkItem
from tasque2.ops.status import get_system_status
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner
from tasque2.workflows import WorkflowService


def _custom_ids(view) -> list[str]:
    return [child.custom_id for child in view.children if getattr(child, "custom_id", None)]


def _action(scope: str, action: str, entity_id: str) -> DiscordUIAction:
    parsed = parse_custom_id(make_custom_id(scope, action, entity_id))
    assert parsed is not None
    return parsed


def _manual_work(session, title: str = "UI work") -> WorkItem:
    return WorkRepository(session).create_work_item(title=title, task_instruction="Control me.", worker_kind="manual")


def test_custom_ids_round_trip_and_foreign_ids_are_ignored() -> None:
    custom_id = make_custom_id("work", "retry", "abc")

    assert custom_id == "t2:work:retry:abc"
    assert parse_custom_id(custom_id) == DiscordUIAction(scope="work", action="retry", entity_id="abc")
    assert parse_custom_id(make_custom_id("ops", "refresh")) == DiscordUIAction(scope="ops", action="refresh")
    assert parse_custom_id("other-bot:work:retry:abc") is None
    assert parse_custom_id("t2:work") is None
    with pytest.raises(ValueError):
        make_custom_id("work", "retry", "x" * 100)
    assert is_modal_action(DiscordUIAction(scope="workflow", action="answer", entity_id="run"))
    assert not is_modal_action(DiscordUIAction(scope="workflow", action="cancel", entity_id="run"))


def test_ui_controls_pause_resume_and_cancel_work(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _manual_work(session)
        service = DiscordUIService(session)

        paused = service.handle_action(_action("work", "pause", work.id))
        assert paused == f"`{work.id}` is paused."
        assert session.get(WorkItem, work.id).status == "paused"

        resumed = service.handle_action(_action("work", "resume", work.id))
        assert "ready" in resumed
        assert session.get(WorkItem, work.id).status == "ready"

        canceled = service.handle_action(_action("work", "cancel", work.id))
        assert "canceled" in canceled
        assert session.get(WorkItem, work.id).status == "canceled"


def test_ui_retry_returns_dead_letter_work_to_the_queue(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Broken work", task_instruction="Fail.", worker_kind="missing.worker"
        )
        WorkRunner(session).run_next()
        assert work.status == "dead_letter"

        message = DiscordUIService(session).handle_action(_action("work", "retry", work.id))

        assert message == f"`{work.id}` is ready."
        assert session.get(WorkItem, work.id).status == "ready"


def test_ui_show_and_report_describe_the_work(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Lane work", task_instruction="Report me.", worker_kind="manual", lane="finance-daily"
        )
        service = DiscordUIService(session)

        shown = service.handle_action(_action("work", "show", work.id))
        report = service.handle_action(_action("work", "report", work.id))

        assert f"ID: {work.id}" in shown
        assert "Lane: finance-daily" in shown
        assert "Attempts: 0/1" in shown
        assert report.startswith("# Work Report: Lane work")
        assert "- lane: finance-daily" in report


def test_ui_rejects_actions_it_cannot_route(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _manual_work(session)
        service = DiscordUIService(session)

        with pytest.raises(ValueError):
            service.handle_action(DiscordUIAction(scope="work", action="pause"))
        with pytest.raises(KeyError):
            service.handle_action(_action("work", "show", "missing"))
        assert service.handle_action(_action("work", "explode", work.id)) == "Unknown work action: explode"
        assert service.handle_action(_action("ops", "refresh", work.id)) == "Unknown Tasque action: ops:refresh"


def test_ui_reports_a_workflow_and_answers_its_gate(fresh_db: Path) -> None:
    definition = {"nodes": [{"key": "gate", "kind": "gate", "prompt": "Approve?"}]}
    with session_scope() as session:
        workflows = WorkflowService(session)
        run = workflows.start_run(
            workflow_definition_id=workflows.create_definition(
                name="ui-workflow", version="1", definition=definition
            ).id
        )
        workflows.tick_runs()
        service = DiscordUIService(session)

        report = service.handle_action(_action("workflow", "report", run.id))
        assert report.startswith("# Workflow Report: ui-workflow")
        assert "- gate: awaiting_input [gate]" in report

        answered = service.answer_gate(workflow_run_id=run.id, answer="yes")
        assert answered == "Answered workflow gate `gate`."
        workflows.tick_runs()
        assert session.get(WorkflowRun, run.id).status == "completed"
        gate = session.scalar(select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id))
        assert gate is not None
        assert gate.output == {"answer": "yes"}


def test_ui_gate_answer_without_a_key_needs_exactly_one_open_gate(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {"key": "first", "kind": "gate", "prompt": "First?"},
            {"key": "second", "kind": "gate", "prompt": "Second?"},
        ]
    }
    with session_scope() as session:
        workflows = WorkflowService(session)
        run = workflows.start_run(
            workflow_definition_id=workflows.create_definition(name="two-gates", version="1", definition=definition).id
        )
        workflows.tick_runs()
        service = DiscordUIService(session)

        with pytest.raises(ValueError, match="Name the gate"):
            service.answer_gate(workflow_run_id=run.id, answer="yes")
        assert service.answer_gate(workflow_run_id=run.id, answer="no", node_key=" second ") == (
            "Answered workflow gate `second`."
        )


def test_ui_pauses_resumes_and_cancels_a_workflow(fresh_db: Path) -> None:
    definition = {
        "nodes": [{"key": "step", "kind": "work", "task_instruction": "Control me.", "worker_kind": "manual"}]
    }
    with session_scope() as session:
        workflows = WorkflowService(session)
        run = workflows.start_run(
            workflow_definition_id=workflows.create_definition(
                name="ui-controlled-workflow", version="1", definition=definition
            ).id
        )
        workflows.tick_runs()
        service = DiscordUIService(session)

        paused = service.handle_action(_action("workflow", "pause", run.id))
        assert "paused" in paused
        assert session.get(WorkflowRun, run.id).status == "paused"

        resumed = service.handle_action(_action("workflow", "resume", run.id))
        assert "active" in resumed
        assert session.get(WorkflowRun, run.id).status == "active"

        canceled = service.handle_action(_action("workflow", "cancel", run.id))
        assert "canceled" in canceled
        assert session.get(WorkflowRun, run.id).status == "canceled"
        assert service.handle_action(_action("workflow", "show", run.id)) == "Unknown workflow action: show"


def test_work_controls_follow_the_work_status(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _manual_work(session)

        def actions() -> list[str]:
            return [custom_id.split(":")[2] for custom_id in _custom_ids(build_work_controls_view(work))]

        assert actions() == ["pause", "cancel", "show", "report"]
        work.status = "paused"
        assert actions() == ["resume", "cancel", "show", "report"]
        work.status = "dead_letter"
        assert actions() == ["retry", "show", "report"]
        work.status = "succeeded"
        assert actions() == ["show", "report"]
        assert all(custom_id.endswith(f":{work.id}") for custom_id in _custom_ids(build_work_controls_view(work)))


def test_workflow_controls_follow_the_run_status(fresh_db: Path) -> None:
    with session_scope() as session:
        definition = WorkflowDefinition(name="controls", version="1", definition={"nodes": []})
        session.add(definition)
        session.flush()
        run = WorkflowRun(workflow_definition_id=definition.id, name="controls", status="active")
        session.add(run)
        session.flush()

        def actions() -> list[str]:
            return [custom_id.split(":")[2] for custom_id in _custom_ids(build_workflow_controls_view(run))]

        assert actions() == ["pause", "cancel"]
        run.status = "awaiting_input"
        assert actions() == ["answer", "pause", "cancel"]
        run.status = "paused"
        assert actions() == ["resume", "cancel"]
        for status in ("completed", "failed", "canceled"):
            run.status = status
            assert build_workflow_controls_view(run) is None


def test_status_panel_orders_fanout_children_under_their_parent_and_uses_work_status(fresh_db: Path) -> None:
    with session_scope() as session:
        definition = WorkflowDefinition(name="panel-order", version="1", definition={"nodes": []})
        session.add(definition)
        session.flush()
        run = WorkflowRun(workflow_definition_id=definition.id, name="panel-order", status="active")
        session.add(run)
        session.flush()
        parent = WorkflowNode(
            workflow_run_id=run.id,
            node_key="cleanup",
            kind="fan_out",
            status="succeeded",
            definition={},
            input={},
            output={},
        )
        report = WorkflowNode(
            workflow_run_id=run.id, node_key="report", kind="work", status="pending", definition={}, input={}, output={}
        )
        session.add_all([parent, report])
        session.flush()
        child_work = _manual_work(session, "child done")
        child_work.status = "succeeded"
        child_0 = WorkflowNode(
            workflow_run_id=run.id,
            node_key="cleanup.0",
            kind="work",
            status="enqueued",
            definition={},
            input={},
            output={},
            parent_node_id=parent.id,
            fanout_index=0,
            work_item_id=child_work.id,
        )
        child_1 = WorkflowNode(
            workflow_run_id=run.id,
            node_key="cleanup.1",
            kind="work",
            status="enqueued",
            definition={},
            input={},
            output={},
            parent_node_id=parent.id,
            fanout_index=1,
        )
        session.add_all([child_0, child_1])
        session.flush()

        embed = build_workflow_status_panel_embed(run, [parent, report, child_0, child_1])
        description = embed["description"]

        assert embed["title"] == "Chain: panel-order - active"
        assert "step **2/4**" in description
        assert description.index("`cleanup`") < description.index("`cleanup.0`")
        assert description.index("`cleanup.1`") < description.index("`report`")
        assert "  ok `cleanup.0`" in description
        assert "  ready `cleanup.1`" in description
        assert {"name": "chain_id", "value": run.id, "inline": True} in embed["fields"]


def test_status_panel_names_the_failed_step(fresh_db: Path) -> None:
    with session_scope() as session:
        definition = WorkflowDefinition(name="failing", version="1", definition={"nodes": []})
        session.add(definition)
        session.flush()
        run = WorkflowRun(workflow_definition_id=definition.id, name="failing", status="failed")
        session.add(run)
        session.flush()
        node = WorkflowNode(
            workflow_run_id=run.id,
            node_key="fetch",
            kind="work",
            status="failed",
            definition={},
            input={},
            output={},
            failure_reason="Work item ended with status dead_letter.",
        )
        session.add(node)
        session.flush()

        embed = build_workflow_status_panel_embed(run, [node])

        assert embed["color"] == COLOR_ALERT
        assert "failed **1**" in embed["description"]
        assert "Failure on `fetch`: Work item ended with status dead_letter." in embed["description"]


def test_ops_panel_posts_once_and_updates_when_counts_change(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        service = DiscordOutputService(session)

        first = service.ensure_control_panel(channel_id="ops", gateway=gateway)
        second = service.ensure_control_panel(channel_id="ops", gateway=gateway)

        assert first is not None
        assert second is None
        assert len(gateway.sent_embeds) == 1
        embed = gateway.sent_embeds[0][1]
        assert embed["title"] == "tasque ops panel"
        assert [field["name"] for field in embed["fields"]] == ["Jobs", "In flight", "Workflows", "Schedules", "DLQ"]
        assert embed["color"] == COLOR_OK
        assert gateway.sent_views[0] is None

        assert service.refresh_control_panel(channel_id="ops", gateway=gateway) is False
        assert gateway.edited_messages == []

        _manual_work(session, "Panel-visible work")
        assert service.refresh_control_panel(channel_id="ops", gateway=gateway) is True
        channel, message_id, _content, edited, _view = gateway.edited_messages[-1]
        assert (channel, message_id) == ("ops", first.message_id)
        assert edited["title"] == "tasque ops panel"
        assert "ready **1**" in edited["fields"][0]["value"]
        assert service.refresh_control_panel(channel_id="ops", gateway=gateway) is False


def test_ops_panel_flags_unresolved_dead_letters(fresh_db: Path) -> None:
    with session_scope() as session:
        WorkRepository(session).create_work_item(title="Broken", task_instruction="Fail.", worker_kind="missing.worker")
        WorkRunner(session).run_next()

        embed = build_ops_embed(get_system_status(session))

        fields = {field["name"]: field["value"] for field in embed["fields"]}
        assert embed["color"] == COLOR_ALERT
        assert fields["DLQ"] == "unresolved **1**"
        assert fields["Jobs"] == "ready **0** - running **0** - dead letter **1**"
