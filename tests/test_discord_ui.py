from __future__ import annotations

import asyncio
from pathlib import Path

from sqlalchemy import select

from tasque2.artifacts import ArtifactStore
from tasque2.db import session_scope
from tasque2.discord_output import DiscordOutputService, FakeDiscordOutputGateway
from tasque2.discord_ui import (
    DiscordUIService,
    build_control_panel_view,
    build_workflow_status_panel_embed,
    make_custom_id,
    parse_custom_id,
)
from tasque2.models import WorkflowDefinition, WorkflowNode, WorkflowRun, WorkItem
from tasque2.repo import WorkRepository
from tasque2.workflows import WorkflowService


def _custom_ids(view) -> list[str]:
    return [child.custom_id for child in view.children if getattr(child, "custom_id", None)]


def test_control_panel_view_exposes_core_ui_actions(fresh_db: Path) -> None:
    assert build_control_panel_view() is None


def test_discord_ui_controls_work_lifecycle(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="UI work",
            task_instruction="Control me.",
            worker_kind="manual",
        )
        service = DiscordUIService(session)

        paused = service.handle_action(parse_custom_id(make_custom_id("work", "pause", work.id)))
        assert "paused" in paused.content
        assert session.get(WorkItem, work.id).status == "paused"

        resumed = service.handle_action(parse_custom_id(make_custom_id("work", "resume", work.id)))
        assert "ready" in resumed.content
        assert session.get(WorkItem, work.id).status == "ready"

        canceled = service.handle_action(parse_custom_id(make_custom_id("work", "cancel", work.id)))
        assert "canceled" in canceled.content
        assert session.get(WorkItem, work.id).status == "canceled"


def test_discord_ui_creates_work_and_schedule(fresh_db: Path) -> None:
    with session_scope() as session:
        service = DiscordUIService(session)

        work_result = service.create_work(
            title="Created from UI",
            task_instruction="Run from a modal.",
            worker_kind="function.echo",
            priority=3,
            discord_interaction_id="interaction-1",
        )
        schedule_result = service.create_schedule(
            name="UI schedule",
            schedule_type="interval",
            expression="minutes=5",
            task_instruction="Scheduled from UI.",
            worker_kind="function.echo",
        )

        assert "Queued work" in work_result.content
        assert "Created schedule" in schedule_result.content

        work = session.get(WorkItem, work_result.content.split("`")[1])
        assert work is not None
        assert work.priority == 3


def test_discord_ui_controls_workflow_lifecycle_and_gate_answer(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {"key": "gate", "kind": "gate", "prompt": "Approve?"},
        ]
    }
    with session_scope() as session:
        workflow_service = WorkflowService(session)
        workflow = workflow_service.create_definition(
            name="ui-workflow",
            version="1",
            definition=definition,
        )
        run = workflow_service.start_run(workflow_definition_id=workflow.id)
        workflow_service.tick_runs()
        service = DiscordUIService(session)

        show = service.handle_action(parse_custom_id(make_custom_id("workflow", "show", run.id)))
        assert "Workflow: ui-workflow" in show.content
        assert "Waiting: gate" in show.content

        answered = service.handle_workflow_text_action(
            workflow_run_id=run.id,
            action="answer",
            value="yes",
        )
        assert "Answered workflow gate" in answered.content
        workflow_service.tick_runs()
        assert session.get(WorkflowRun, run.id).status == "completed"
        gate = session.scalar(select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id))
        assert gate is not None
        assert gate.output["answer"] == "yes"


def test_discord_ui_pauses_resumes_and_cancels_workflow(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {
                "key": "step",
                "kind": "work",
                "task_instruction": "Control me.",
                "worker_kind": "manual",
            },
        ]
    }
    with session_scope() as session:
        workflow_service = WorkflowService(session)
        workflow = workflow_service.create_definition(
            name="ui-controlled-workflow",
            version="1",
            definition=definition,
        )
        run = workflow_service.start_run(workflow_definition_id=workflow.id)
        workflow_service.tick_runs()
        service = DiscordUIService(session)

        paused = service.handle_action(parse_custom_id(make_custom_id("workflow", "pause", run.id)))
        assert "paused" in paused.content
        assert session.get(WorkflowRun, run.id).status == "paused"

        resumed = service.handle_action(parse_custom_id(make_custom_id("workflow", "resume", run.id)))
        assert "active" in resumed.content
        assert session.get(WorkflowRun, run.id).status == "active"

        canceled = service.handle_action(parse_custom_id(make_custom_id("workflow", "cancel", run.id)))
        assert "canceled" in canceled.content
        assert session.get(WorkflowRun, run.id).status == "canceled"


def test_workflow_panel_orders_fanout_children_under_parent_and_uses_work_status(
    fresh_db: Path,
) -> None:
    with session_scope() as session:
        definition = WorkflowDefinition(name="panel-order", version="1", definition={"nodes": []})
        session.add(definition)
        session.flush()
        run = WorkflowRun(
            workflow_definition_id=definition.id,
            name="panel-order",
            status="active",
        )
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
            workflow_run_id=run.id,
            node_key="report",
            kind="work",
            status="pending",
            definition={},
            input={},
            output={},
        )
        session.add_all([parent, report])
        session.flush()
        child_work = WorkRepository(session).create_work_item(
            title="child done",
            task_instruction="done",
            worker_kind="manual",
        )
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

        assert "step **2/4**" in description
        assert description.index("`cleanup`") < description.index("`cleanup.0`")
        assert description.index("`cleanup.1`") < description.index("`report`")
        assert "ok `cleanup.0`" in description


def test_discord_output_posts_control_panel_once(fresh_db: Path) -> None:
    gateway = FakeDiscordOutputGateway()
    with session_scope() as session:
        service = DiscordOutputService(session)
        first = asyncio.run(
            service.ensure_control_panel(parent_channel_id="parent", gateway=gateway)
        )
        second = asyncio.run(
            service.ensure_control_panel(parent_channel_id="parent", gateway=gateway)
        )

        assert first is not None
        assert second is None
        assert len(gateway.sent_embeds) == 1
        assert gateway.sent_embeds[0][1]["title"] == "tasque ops panel"
        field_names = {field["name"] for field in gateway.sent_embeds[0][1]["fields"]}
        assert {"Jobs", "In flight", "Workflows", "Schedules", "DLQ"} <= field_names
        assert "Projects" not in field_names
        assert gateway.sent_views[0] is None

        unchanged = asyncio.run(
            service.refresh_control_panel(parent_channel_id="parent", gateway=gateway)
        )
        assert unchanged is None
        assert gateway.edited_messages == []

        WorkRepository(session).create_work_item(
            title="Panel-visible work",
            task_instruction="Make the ops panel count change.",
            worker_kind="manual",
        )
        refreshed = asyncio.run(
            service.refresh_control_panel(parent_channel_id="parent", gateway=gateway)
        )
        assert refreshed is not None
        assert gateway.edited_messages[-1][3]["title"] == "tasque ops panel"


def test_discord_ui_searches_artifacts(fresh_db: Path, tmp_path: Path) -> None:
    with session_scope() as session:
        ArtifactStore(tmp_path / "artifacts").write_text(
            session,
            kind="report",
            title="Needle Report",
            content="artifact",
            tags=["needle"],
        )

        result = DiscordUIService(session).search_artifacts(query="Needle", tags=["needle"])

        assert "Needle Report" in result.content
