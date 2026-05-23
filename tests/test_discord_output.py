from __future__ import annotations

import asyncio
from pathlib import Path

from sqlalchemy import select

from tasque2.artifacts import ArtifactStore
from tasque2.db import session_scope
from tasque2.discord_output import DiscordOutputService, FakeDiscordOutputGateway
from tasque2.discord_ui import make_custom_id
from tasque2.models import (
    DiscordMessage,
    DiscordThread,
    WorkAttempt,
    WorkEvent,
    WorkItem,
)
from tasque2.providers import FakeProvider, ProviderRegistry, ProviderResponse, ProviderRuntime
from tasque2.repo import WorkRepository
from tasque2.runtime import WorkRunner
from tasque2.workflows import WorkflowService


def test_discord_output_creates_work_thread_and_posts_once(fresh_db: Path) -> None:
    gateway = FakeDiscordOutputGateway()
    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Output work",
            task_instruction="Run output.",
            worker_kind="function.echo",
        )
        WorkRunner(session).run_next()

        service = DiscordOutputService(session)
        first = _post_pending(service, gateway)
        second = _post_pending(service, gateway)

        assert first == 1
        assert second == 0
        assert len(gateway.created_threads) == 1
        assert gateway.created_thread_embeds[-1]["title"] == "Work: Output work"
        assert gateway.created_thread_embeds[-1]["description"] == "Run output."
        assert len(gateway.sent_messages) == 1
        assert gateway.sent_messages[-1][1] == "Run output."
        assert make_custom_id("work", "report", work.id) in _custom_ids(gateway.sent_views[-1])

        thread = session.scalar(select(DiscordThread).where(DiscordThread.work_item_id == work.id))
        assert thread is not None
        assert thread.discord_thread_id == "fake-thread-1"

        outbound = session.scalars(
            select(DiscordMessage)
            .where(
                DiscordMessage.direction == "outbound",
                DiscordMessage.work_item_id == work.id,
            )
            .order_by(DiscordMessage.created_at)
        ).all()
        assert len(outbound) == 2
        assert outbound[-1].content_preview == "Run output."

        posted_event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.work_status_posted",
                WorkEvent.work_item_id == work.id,
            )
        )
        assert posted_event is not None
        assert posted_event.payload["status"] == "succeeded"


def test_discord_output_posts_only_final_workflow_thread_to_jobs(fresh_db: Path) -> None:
    gateway = FakeDiscordOutputGateway()
    definition = {
        "nodes": [
            {
                "key": "step",
                "kind": "work",
                "title": "Workflow Step",
                "task_instruction": "Run workflow step.",
                "worker_kind": "function.echo",
            },
        ]
    }
    with session_scope() as session:
        workflow_service = WorkflowService(session)
        workflow = workflow_service.create_definition(
            name="output-workflow",
            version="1",
            definition=definition,
        )
        run = workflow_service.start_run(workflow_definition_id=workflow.id)
        workflow_service.tick_runs()

        output = DiscordOutputService(session)
        first = _post_pending(output, gateway)

        assert first == 0
        assert len(gateway.created_threads) == 0
        ids = _custom_ids(gateway.sent_views[-1])
        assert make_custom_id("workflow", "pause", run.id) in ids
        assert make_custom_id("workflow", "cancel", run.id) in ids
        assert make_custom_id("workflow", "show", run.id) not in ids
        assert make_custom_id("workflow", "report", run.id) not in ids

        WorkRunner(session).run_next()
        workflow_service.tick_runs()
        second = _post_pending(output, gateway)

        assert second == 1
        assert len(gateway.created_threads) == 1
        assert gateway.created_threads[-1][0] == "jobs"
        assert gateway.created_threads[-1][2] == ""
        assert gateway.created_thread_embeds[-1]["title"] == "Workflow: output-workflow"
        assert gateway.created_thread_embeds[-1]["description"] == "Run workflow step."
        assert len(gateway.sent_messages) == 1
        assert gateway.sent_messages[-1][1] == "Run workflow step."

        thread = session.scalar(
            select(DiscordThread).where(
                DiscordThread.purpose == "workflow",
                DiscordThread.workflow_run_id == run.id,
            )
        )
        assert thread is not None
        assert thread.discord_thread_id == "fake-thread-1"


def test_discord_output_posts_chain_status_panel_in_chains_channel(fresh_db: Path) -> None:
    gateway = FakeDiscordOutputGateway()
    definition = {
        "nodes": [
            {
                "key": "step",
                "kind": "work",
                "title": "Workflow Step",
                "task_instruction": "Run workflow step.",
                "worker_kind": "function.echo",
            },
        ]
    }
    with session_scope() as session:
        workflow_service = WorkflowService(session)
        workflow = workflow_service.create_definition(
            name="panel-workflow",
            version="1",
            definition=definition,
        )
        workflow_service.start_run(workflow_definition_id=workflow.id)
        workflow_service.tick_runs()

        output = DiscordOutputService(session)
        first = _post_pending(output, gateway)

        assert first == 0
        panel_posts = [
            embed for channel, embed, _view in gateway.sent_embeds if channel == "chains"
        ]
        assert len(panel_posts) == 1
        assert panel_posts[0]["title"] == "Chain: panel-workflow - active"
        assert "`step`" in panel_posts[0]["description"]
        assert gateway.created_threads == []
        panel_ids = _custom_ids(gateway.sent_views[-1])
        assert all(":show:" not in custom_id for custom_id in panel_ids)
        assert all(":report:" not in custom_id for custom_id in panel_ids)

        WorkRunner(session).run_next()
        workflow_service.tick_runs()
        second = _post_pending(output, gateway)

        assert second == 1
        panel_edits = [
            embed
            for channel, _message_id, _content, embed, _view in gateway.edited_messages
            if channel == "chains" and embed is not None and embed["title"].startswith("Chain:")
        ]
        assert panel_edits[-1]["title"] == "Chain: panel-workflow - completed"
        assert gateway.created_threads[-1][0] == "jobs"


def test_discord_output_routes_work_to_jobs_and_dead_letters_to_dlq(fresh_db: Path) -> None:
    gateway = FakeDiscordOutputGateway()
    with session_scope() as session:
        ok = WorkRepository(session).create_work_item(
            title="Finished work",
            task_instruction="Run output.",
            worker_kind="function.echo",
        )
        failed = WorkRepository(session).create_work_item(
            title="Broken work",
            task_instruction="This should fail.",
            worker_kind="missing.worker",
        )
        WorkRunner(session).run_next()
        WorkRunner(session).run_next()
        assert session.get(WorkItem, ok.id).status == "succeeded"
        assert session.get(WorkItem, failed.id).status == "dead_letter"

        posted = _post_pending(DiscordOutputService(session), gateway)

        assert posted == 2
        parents = {name: parent for parent, name, _intro in gateway.created_threads}
        assert parents[next(name for name in parents if "Finished work" in name)] == "jobs"
        assert parents[next(name for name in parents if "Broken work" in name)] == "dlq"


def test_discord_output_replies_to_intake_work_without_job_thread(
    fresh_db: Path,
    tmp_path: Path,
) -> None:
    gateway = FakeDiscordOutputGateway()
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="please run email cleanup workflow once",
            task_instruction="Start the workflow and answer naturally.",
            worker_kind="function.echo",
            context={
                "discord_intake": {
                    "discord_message_id": "m-intake",
                    "discord_channel_id": "intake",
                    "author": "user",
                }
            },
            source_kind="discord",
            source_id="m-intake",
        )
        WorkRunner(session).run_next()
        report = ArtifactStore(tmp_path / "artifacts").write_text(
            session,
            kind="report",
            title="reply.md",
            content="Sure, I started the daily Gmail cleanup workflow.",
            work_item_id=work.id,
        )
        attempt = session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work.id)
            .order_by(WorkAttempt.attempt_number.desc())
        )
        assert attempt is not None
        attempt.report_artifact_id = report.id

        posted = _post_pending(DiscordOutputService(session), gateway)

        assert posted == 1
        assert gateway.created_threads == []
        assert gateway.sent_messages == [
            ("intake", "Sure, I started the daily Gmail cleanup workflow.")
        ]
        assert gateway.sent_attachments[-1] == []
        assert "Provider logs" not in gateway.sent_messages[-1][1]
        event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.work_status_posted",
                WorkEvent.entity_id == work.id,
            )
        )
        assert event is not None
        assert event.payload["mode"] == "intake_response"


def test_discord_output_posts_report_markdown_in_thread_without_upload(
    fresh_db: Path,
    tmp_path: Path,
) -> None:
    gateway = FakeDiscordOutputGateway()
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Markdown work",
            task_instruction="Write a report.",
            worker_kind="function.echo",
        )
        WorkRunner(session).run_next()
        long_report = "# Report\n\n" + "\n".join(
            f"- line {index}: " + ("detail " * 20) for index in range(80)
        )
        report = ArtifactStore(tmp_path / "artifacts").write_text(
            session,
            kind="report",
            title="report.md",
            content=long_report,
            work_item_id=work.id,
        )
        attempt = session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work.id)
            .order_by(WorkAttempt.attempt_number.desc())
        )
        assert attempt is not None
        attempt.summary = "Short report summary."
        attempt.report_artifact_id = report.id

        posted = _post_pending(DiscordOutputService(session), gateway)

        assert posted == 1
        assert gateway.created_thread_embeds[-1]["description"] == "Short report summary."
        assert len(gateway.sent_messages) > 1
        assert gateway.sent_messages[0][1].startswith("# Report")
        assert "line 79" in gateway.sent_messages[-1][1]
        assert all(len(content) <= 1900 for _channel_id, content in gateway.sent_messages)
        uploaded_ids = {
            upload.artifact_id
            for attachment_set in gateway.sent_attachments
            for upload in attachment_set
        }
        assert report.id not in uploaded_ids


def test_discord_output_posts_workflow_report_markdown_in_thread_without_upload(
    fresh_db: Path,
    tmp_path: Path,
) -> None:
    gateway = FakeDiscordOutputGateway()
    definition = {
        "nodes": [
            {
                "key": "report",
                "kind": "work",
                "title": "Workflow Report",
                "task_instruction": "Write the workflow report.",
                "worker_kind": "function.echo",
            },
        ]
    }
    with session_scope() as session:
        workflow_service = WorkflowService(session)
        workflow = workflow_service.create_definition(
            name="markdown-workflow",
            version="1",
            definition=definition,
        )
        workflow_service.start_run(workflow_definition_id=workflow.id)
        workflow_service.tick_runs()
        WorkRunner(session).run_next()
        long_report = "# Workflow Report\n\n" + "\n".join(
            f"- finding {index}: " + ("detail " * 20) for index in range(80)
        )
        attempt = session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.summary == "Write the workflow report.")
            .order_by(WorkAttempt.attempt_number.desc())
        )
        assert attempt is not None
        report = ArtifactStore(tmp_path / "artifacts").write_text(
            session,
            kind="report",
            title="workflow-report.md",
            content=long_report,
            suffix=".md",
            work_item_id=attempt.work_item_id,
            attempt_id=attempt.id,
        )
        attempt.summary = "Short workflow summary."
        attempt.report_artifact_id = report.id
        workflow_service.tick_runs()

        posted = _post_pending(DiscordOutputService(session), gateway)

        assert posted == 1
        assert gateway.created_thread_embeds[-1]["title"] == "Workflow: markdown-workflow"
        assert gateway.created_thread_embeds[-1]["description"] == "Short workflow summary."
        assert len(gateway.sent_messages) > 1
        assert gateway.sent_messages[0][1].startswith("# Workflow Report")
        assert "finding 79" in gateway.sent_messages[-1][1]
        assert all(len(content) <= 1900 for _channel_id, content in gateway.sent_messages)
        uploaded_ids = {
            upload.artifact_id
            for attachment_set in gateway.sent_attachments
            for upload in attachment_set
        }
        assert report.id not in uploaded_ids


def test_discord_output_posts_dead_letter_error_summary(fresh_db: Path) -> None:
    gateway = FakeDiscordOutputGateway()
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Missing worker",
            task_instruction="This should fail.",
            worker_kind="missing.worker",
        )
        WorkRunner(session).run_next()
        assert session.get(WorkItem, work.id).status == "dead_letter"

        posted = _post_pending(DiscordOutputService(session), gateway)

        assert posted == 1
        assert "Status: dead_letter" in gateway.sent_messages[-1][1]
        assert "No function worker registered" in gateway.sent_messages[-1][1]
        assert make_custom_id("work", "retry", work.id) in _custom_ids(gateway.sent_views[-1])


def test_discord_output_attaches_provider_logs_for_dead_letter(fresh_db: Path) -> None:
    gateway = FakeDiscordOutputGateway()
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            response=ProviderResponse(
                status="succeeded",
                summary="No submitted result.",
                output_text="plain text",
                stdout="stdout log",
                stderr="stderr log",
                raw_stream='{"type":"item.completed","item":{"type":"mcp_tool_call","tool":"workflow_start","status":"completed"}}',
            ),
            deposit_structured_result=False,
        )
    )

    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Provider log failure",
            task_instruction="Fail after writing logs.",
            worker_kind="provider.fake",
        )
        WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()
        assert session.get(WorkItem, work.id).status == "dead_letter"

        posted = _post_pending(DiscordOutputService(session), gateway)

        assert posted == 1
        assert "Provider run: fake" in gateway.sent_messages[-1][1]
        assert "Provider logs:" in gateway.sent_messages[-1][1]
        assert "stdout:" in gateway.sent_messages[-1][1]
        assert "stderr:" in gateway.sent_messages[-1][1]
        assert "raw:" in gateway.sent_messages[-1][1]
        assert "trace:" in gateway.sent_messages[-1][1]
        uploaded_names = {upload.filename for upload in gateway.sent_attachments[-1]}
        assert len(uploaded_names) == 4


def test_discord_output_uploads_explicit_work_artifacts(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordOutputGateway()
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Upload work",
            task_instruction="Upload the result.",
            worker_kind="function.echo",
        )
        WorkRunner(session).run_next()
        artifact = ArtifactStore(tmp_path / "artifacts").write_text(
            session,
            kind="worker_file",
            title="result.txt",
            content="hello from a worker file",
            work_item_id=work.id,
            tags=["discord_upload"],
        )

        posted = _post_pending(DiscordOutputService(session), gateway)

        assert posted == 1
        assert gateway.sent_attachments[-1][0].artifact_id == artifact.id
        assert gateway.sent_attachments[-1][0].path == artifact.local_path
        assert "Attached files: result.txt" in gateway.sent_messages[-1][1]
        assert artifact.local_path not in gateway.sent_messages[-1][1]


def _custom_ids(view) -> list[str]:
    return [child.custom_id for child in view.children if getattr(child, "custom_id", None)]


def _post_pending(service: DiscordOutputService, gateway: FakeDiscordOutputGateway) -> int:
    return asyncio.run(
        service.post_pending_updates(
            parent_channel_id="ops",
            gateway=gateway,
            ops_channel_id="ops",
            jobs_channel_id="jobs",
            chains_channel_id="chains",
            dlq_channel_id="dlq",
        )
    )
