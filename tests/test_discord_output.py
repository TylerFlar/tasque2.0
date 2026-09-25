from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import discord
import pytest
from PIL import Image
from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.db import session_scope
from tasque2.discord import gateway as gateway_module
from tasque2.discord.gateway import DiscordMessageGone, DiscordPyGateway, FakeDiscordGateway
from tasque2.discord.output import DiscordOutputService, OutputChannels, split_markdown
from tasque2.discord.routing import DiscordService
from tasque2.discord.ui import make_custom_id
from tasque2.discord.uploads import MAX_FILES_PER_MESSAGE, DiscordFileUpload, batch_uploads, shrink_image
from tasque2.models import DiscordMessage, DiscordThread, WorkAttempt, WorkEvent, WorkflowRun, WorkItem
from tasque2.providers import FakeProvider, ProviderRegistry, ProviderResponse
from tasque2.work.queue import WorkQueue
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner
from tasque2.worker.runtime import ProviderRuntime
from tasque2.workflows import WorkflowService

CHANNELS = OutputChannels(ops="ops", jobs="jobs", chains="chains", dlq="dlq")


def _post_pending(service: DiscordOutputService, gateway: FakeDiscordGateway) -> int:
    return service.post_pending_updates(gateway=gateway, channels=CHANNELS)


def _custom_ids(view) -> list[str]:
    return [child.custom_id for child in view.children if getattr(child, "custom_id", None)]


def _start_step_workflow(session: Session, name: str, *, discord_thread_id: str | None = None) -> WorkflowRun:
    definition = {
        "nodes": [
            {
                "key": "step",
                "kind": "work",
                "title": "Workflow Step",
                "task_instruction": "Run workflow step.",
                "worker_kind": "function.echo",
            }
        ]
    }
    workflows = WorkflowService(session)
    definition_id = workflows.create_definition(name=name, version="1", definition=definition).id
    run = workflows.start_run(workflow_definition_id=definition_id, discord_thread_id=discord_thread_id)
    workflows.tick_runs()
    return run


def _finish_workflow_step(session: Session) -> None:
    WorkRunner(session).run_next()
    WorkflowService(session).tick_runs()


def _echo_work(session: Session, title: str, instruction: str, **fields: Any) -> WorkItem:
    work = WorkRepository(session).create_work_item(
        title=title, task_instruction=instruction, worker_kind="function.echo", **fields
    )
    WorkRunner(session).run_next()
    return work


def _latest_attempt(session: Session, work_item_id: str) -> WorkAttempt:
    attempt = session.scalar(
        select(WorkAttempt).where(WorkAttempt.work_item_id == work_item_id).order_by(WorkAttempt.attempt_number.desc())
    )
    assert attempt is not None
    return attempt


def _chain_panel_edits(gateway: FakeDiscordGateway) -> list[dict[str, Any]]:
    return [
        embed
        for channel, _message_id, _content, embed, _view in gateway.edited_messages
        if channel == "chains" and embed is not None and embed["title"].startswith("Chain:")
    ]


def test_finished_work_opens_a_thread_and_posts_once(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = _echo_work(session, "Output work", "Run output.")
        service = DiscordOutputService(session)

        first = _post_pending(service, gateway)
        second = _post_pending(service, gateway)

        assert first == 1
        assert second == 0
        assert len(gateway.created_threads) == 1
        assert gateway.created_threads[0][0] == "jobs"
        assert gateway.created_thread_embeds[-1]["title"] == "Work: Output work"
        assert gateway.created_thread_embeds[-1]["description"] == "Run output."
        assert gateway.sent_messages == [("fake-thread-1", "Run output.")]
        assert make_custom_id("work", "report", work.id) in _custom_ids(gateway.sent_views[-1])

        thread = session.scalar(select(DiscordThread).where(DiscordThread.work_item_id == work.id))
        assert thread is not None
        assert thread.discord_thread_id == "fake-thread-1"
        outbound = session.scalars(
            select(DiscordMessage)
            .where(DiscordMessage.direction == "outbound", DiscordMessage.work_item_id == work.id)
            .order_by(DiscordMessage.created_at)
        ).all()
        assert len(outbound) == 2
        assert outbound[-1].content_preview == "Run output."
        assert outbound[-1].discord_thread_id == "fake-thread-1"
        posted_event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.work_status_posted", WorkEvent.work_item_id == work.id
            )
        )
        assert posted_event is not None
        assert posted_event.payload["status"] == "succeeded"


def test_work_carrying_a_bound_thread_posts_into_it_unless_it_asks_for_a_new_thread(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        service = DiscordOutputService(session)
        generator = _echo_work(session, "Calorie tracker (daily)", "Open the day thread.")
        assert _post_pending(service, gateway) == 1
        binding = session.scalar(select(DiscordThread).where(DiscordThread.work_item_id == generator.id))
        assert binding is not None
        day_thread_id = binding.discord_thread_id

        reply = _echo_work(
            session,
            "Process nutrition reply",
            "Logged: chicken burrito bowl ~1050 cal.",
            discord_thread_id=day_thread_id,
            source_kind="discord_reply_followup",
        )
        assert _post_pending(service, gateway) == 1
        assert len(gateway.created_threads) == 1
        assert gateway.sent_messages[-1] == (day_thread_id, "Logged: chicken burrito bowl ~1050 cal.")
        assert session.scalar(select(DiscordThread).where(DiscordThread.work_item_id == reply.id)) is None

        spawned = _echo_work(
            session,
            "Chef: dinner",
            "Recipe and timeline.",
            discord_thread_id=day_thread_id,
            source_kind="mcp",
        )
        assert _post_pending(service, gateway) == 1
        assert len(gateway.created_threads) == 1
        assert gateway.sent_messages[-1][0] == day_thread_id
        assert session.scalar(select(DiscordThread).where(DiscordThread.work_item_id == spawned.id)) is None

        opted_out = _echo_work(
            session,
            "Workout generator",
            "Next session prescription.",
            discord_thread_id=day_thread_id,
            source_kind="mcp",
            context={"discord_new_thread": True},
        )
        assert _post_pending(service, gateway) == 1
        assert len(gateway.created_threads) == 2
        new_thread = session.scalar(select(DiscordThread).where(DiscordThread.work_item_id == opted_out.id))
        assert new_thread is not None
        assert new_thread.discord_thread_id != day_thread_id
        assert gateway.sent_messages[-1][0] == new_thread.discord_thread_id


def test_workflow_posts_only_its_final_result_in_a_jobs_thread(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        run = _start_step_workflow(session, "output-workflow")
        output = DiscordOutputService(session)

        assert _post_pending(output, gateway) == 0
        assert gateway.created_threads == []
        ids = _custom_ids(gateway.sent_views[-1])
        assert make_custom_id("workflow", "pause", run.id) in ids
        assert make_custom_id("workflow", "cancel", run.id) in ids
        assert all(":show:" not in custom_id and ":report:" not in custom_id for custom_id in ids)

        _finish_workflow_step(session)

        assert _post_pending(output, gateway) == 1
        assert gateway.created_threads == [("jobs", gateway.created_threads[0][1], "")]
        assert gateway.created_thread_embeds[-1]["title"] == "Workflow: output-workflow"
        assert gateway.created_thread_embeds[-1]["description"] == "Run workflow step."
        assert gateway.sent_messages == [("fake-thread-1", "Run workflow step.")]
        thread = session.scalar(
            select(DiscordThread).where(DiscordThread.purpose == "workflow", DiscordThread.workflow_run_id == run.id)
        )
        assert thread is not None
        assert thread.discord_thread_id == "fake-thread-1"
        assert session.get(WorkflowRun, run.id).discord_thread_id == "fake-thread-1"
        assert _post_pending(output, gateway) == 0


def test_workflow_status_panel_posts_in_the_chains_channel_and_follows_the_run(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        _start_step_workflow(session, "panel-workflow")
        output = DiscordOutputService(session)

        assert _post_pending(output, gateway) == 0
        panel_posts = [embed for channel, embed, _view in gateway.sent_embeds if channel == "chains"]
        assert len(panel_posts) == 1
        assert panel_posts[0]["title"] == "Chain: panel-workflow - active"
        assert "`step`" in panel_posts[0]["description"]
        assert gateway.created_threads == []

        _finish_workflow_step(session)

        assert _post_pending(output, gateway) == 1
        assert _chain_panel_edits(gateway)[-1]["title"] == "Chain: panel-workflow - completed"
        assert gateway.created_threads[-1][0] == "jobs"
        edits = len(gateway.edited_messages)
        assert _post_pending(output, gateway) == 0
        assert len(gateway.edited_messages) == edits


def test_thread_started_workflow_posts_its_final_result_and_uploads_into_the_origin_thread(
    fresh_db: Path, tmp_path: Path
) -> None:
    gateway = FakeDiscordGateway()
    origin_thread_id = "origin-thread-1"
    with session_scope() as session:
        DiscordService(session).bind_thread(
            purpose="work", discord_channel_id="intake", discord_thread_id=origin_thread_id
        )
        run = _start_step_workflow(session, "stylist-build", discord_thread_id=origin_thread_id)
        output = DiscordOutputService(session)

        assert _post_pending(output, gateway) == 0
        panel_posts = [embed for channel, embed, _view in gateway.sent_embeds if channel == "chains"]
        assert [embed["title"] for embed in panel_posts] == ["Chain: stylist-build - active"]
        assert gateway.created_threads == []

        _finish_workflow_step(session)
        collage = ArtifactStore(tmp_path / "artifacts").write_text(
            session,
            kind="image_compose",
            title="lookbook.png",
            content="<collage bytes>",
            suffix=".png",
            workflow_run_id=run.id,
            tags=["discord_upload"],
        )

        assert _post_pending(output, gateway) == 1
        assert gateway.created_threads == []
        assert gateway.sent_messages[-1][0] == origin_thread_id
        assert gateway.sent_messages[-1][1].startswith("Run workflow step.")
        assert "Attached files: lookbook.png" in gateway.sent_messages[-1][1]
        assert [upload.artifact_id for upload in gateway.sent_attachments[-1]] == [collage.id]
        assert _chain_panel_edits(gateway)[-1]["title"] == "Chain: stylist-build - completed"
        assert (
            session.scalar(
                select(DiscordThread).where(
                    DiscordThread.purpose == "workflow", DiscordThread.workflow_run_id == run.id
                )
            )
            is None
        )
        final_event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.workflow_final_posted", WorkEvent.workflow_run_id == run.id
            )
        )
        assert final_event is not None
        assert final_event.payload["status"] == "completed"
        assert final_event.payload["upload_artifact_ids"] == [collage.id]


def test_scheduled_work_bound_to_a_thread_posts_into_it(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        DiscordService(session).bind_thread(
            purpose="work", discord_channel_id="intake", discord_thread_id="scout-thread"
        )
        _echo_work(
            session,
            "watch fired: cooking classes",
            "New cooking classes this week.",
            source_kind="schedule",
            discord_thread_id="scout-thread",
        )

        assert _post_pending(DiscordOutputService(session), gateway) == 1
        assert gateway.created_threads == []
        assert gateway.sent_messages == [("scout-thread", "New cooking classes this week.")]


def test_silent_run_posts_nothing_but_counts_as_handled(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = _echo_work(session, "watch fired: nothing new", "Quiet week.", source_kind="schedule")
        attempt = _latest_attempt(session, work.id)
        attempt.produces = {**(attempt.produces or {}), "silent": True}
        session.flush()
        output = DiscordOutputService(session)

        assert _post_pending(output, gateway) == 1
        assert _post_pending(output, gateway) == 0
        assert gateway.sent_messages == []
        assert gateway.created_threads == []
        event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.work_status_posted", WorkEvent.work_item_id == work.id
            )
        )
        assert event is not None
        assert event.payload["silent"] is True


def test_status_panel_posts_for_the_newest_active_run_beyond_the_window(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        for _ in range(3):
            _start_step_workflow(session, "window-workflow")
            _finish_workflow_step(session)
        active = _start_step_workflow(session, "window-workflow")

        written = DiscordOutputService(session).refresh_workflow_panels(channel_id="chains", gateway=gateway, limit=2)

        posted_run_ids = {
            event.workflow_run_id
            for event in session.scalars(
                select(WorkEvent).where(WorkEvent.event_type == "discord.workflow_status_panel_posted")
            ).all()
        }
        assert written == 1
        assert posted_run_ids == {active.id}
        assert [embed["title"] for _channel, embed, _view in gateway.sent_embeds] == ["Chain: window-workflow - active"]


def test_work_goes_to_the_jobs_channel_and_dead_letters_to_the_dlq_channel(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        ok = WorkRepository(session).create_work_item(
            title="Finished work", task_instruction="Run output.", worker_kind="function.echo"
        )
        failed = WorkRepository(session).create_work_item(
            title="Broken work", task_instruction="This should fail.", worker_kind="missing.worker"
        )
        WorkRunner(session).run_next()
        WorkRunner(session).run_next()
        assert session.get(WorkItem, ok.id).status == "succeeded"
        assert session.get(WorkItem, failed.id).status == "dead_letter"

        assert _post_pending(DiscordOutputService(session), gateway) == 2

        parents = {name: parent for parent, name, _intro in gateway.created_threads}
        assert parents[next(name for name in parents if "Finished work" in name)] == "jobs"
        assert parents[next(name for name in parents if "Broken work" in name)] == "dlq"


def test_intake_work_answers_in_the_intake_channel_without_a_thread(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = _echo_work(
            session,
            "please run email cleanup workflow once",
            "Start the workflow and answer naturally.",
            context={
                "discord_intake": {"discord_message_id": "m-intake", "discord_channel_id": "intake", "author": "user"}
            },
            source_kind="discord",
            source_id="m-intake",
        )
        report = ArtifactStore(tmp_path / "artifacts").write_text(
            session,
            kind="report",
            title="reply.md",
            content="Sure, I started the daily Gmail cleanup workflow.",
            work_item_id=work.id,
        )
        _latest_attempt(session, work.id).report_artifact_id = report.id

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        assert gateway.created_threads == []
        assert gateway.sent_messages == [("intake", "Sure, I started the daily Gmail cleanup workflow.")]
        assert gateway.sent_attachments[-1] == []
        event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.work_status_posted", WorkEvent.entity_id == work.id
            )
        )
        assert event is not None
        assert event.payload["mode"] == "intake_response"
        assert event.payload["discord_channel_id"] == "intake"


def test_failed_intake_work_answers_with_its_error(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        WorkRepository(session).create_work_item(
            title="Broken intake",
            task_instruction="This should fail.",
            worker_kind="missing.worker",
            context={"discord_intake": {"discord_channel_id": "intake"}},
            source_kind="discord",
        )
        WorkRunner(session).run_next()

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        [(channel, content)] = gateway.sent_messages
        assert channel == "intake"
        assert content.startswith("I hit an error while handling that:")
        assert "No function worker registered" in content


def test_long_report_posts_in_chunks_without_uploading_the_report(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = _echo_work(session, "Markdown work", "Write a report.")
        long_report = "# Report\n\n" + "\n".join(f"- line {index}: " + ("detail " * 20) for index in range(80))
        report = ArtifactStore(tmp_path / "artifacts").write_text(
            session, kind="report", title="report.md", content=long_report, work_item_id=work.id
        )
        attempt = _latest_attempt(session, work.id)
        attempt.summary = "Short report summary."
        attempt.report_artifact_id = report.id

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        assert gateway.created_thread_embeds[-1]["description"] == "Short report summary."
        assert len(gateway.sent_messages) > 1
        assert gateway.sent_messages[0][1].startswith("# Report")
        assert "line 79" in gateway.sent_messages[-1][1]
        assert all(len(content) <= 1900 for _channel_id, content in gateway.sent_messages)
        assert gateway.sent_views[-1] is not None
        assert all(view is None for view in gateway.sent_views[:-1])
        uploaded = {upload.artifact_id for attachments in gateway.sent_attachments for upload in attachments}
        assert report.id not in uploaded


def test_long_workflow_report_posts_in_chunks_without_uploading_the_report(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        _start_step_workflow(session, "markdown-workflow")
        WorkRunner(session).run_next()
        long_report = "# Workflow Report\n\n" + "\n".join(
            f"- finding {index}: " + ("detail " * 20) for index in range(80)
        )
        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.summary == "Run workflow step."))
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
        WorkflowService(session).tick_runs()

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        assert gateway.created_thread_embeds[-1]["title"] == "Workflow: markdown-workflow"
        assert gateway.created_thread_embeds[-1]["description"] == "Short workflow summary."
        assert len(gateway.sent_messages) > 1
        assert gateway.sent_messages[0][1].startswith("# Workflow Report")
        assert "finding 79" in gateway.sent_messages[-1][1]
        assert all(len(content) <= 1900 for _channel_id, content in gateway.sent_messages)
        uploaded = {upload.artifact_id for attachments in gateway.sent_attachments for upload in attachments}
        assert report.id not in uploaded


def test_dead_letter_posts_the_error_and_a_retry_button(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Missing worker", task_instruction="This should fail.", worker_kind="missing.worker"
        )
        WorkRunner(session).run_next()
        assert session.get(WorkItem, work.id).status == "dead_letter"

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        content = gateway.sent_messages[-1][1]
        assert gateway.created_threads[-1][0] == "dlq"
        assert "Status: dead_letter" in content
        assert "No function worker registered" in content
        assert make_custom_id("work", "retry", work.id) in _custom_ids(gateway.sent_views[-1])


def test_dead_letter_lists_the_provider_logs_and_attaches_all_but_the_raw_stream(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            response=ProviderResponse(
                status="succeeded",
                summary="Reported failure.",
                stdout='{"type":"item.completed","item":{"type":"mcp_tool_call","tool":"workflow_start","status":"completed"}}',
                stderr="stderr log",
            ),
            result_payload={
                "status": "failed",
                "summary": "Could not finish.",
                "report": "Tried and failed after writing logs.",
                "error": "Could not finish.",
            },
        )
    )
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Provider log failure", task_instruction="Fail after writing logs.", worker_kind="provider.fake"
        )
        WorkRunner(session, provider_runtime=ProviderRuntime(registry=registry)).run_next()
        assert session.get(WorkItem, work.id).status == "dead_letter"
        provider_run_id = _latest_attempt(session, work.id).provider_run_id

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        content = gateway.sent_messages[-1][1]
        assert f"Provider run: fake `{provider_run_id}`" in content
        assert "Error: Could not finish." in content
        for title in ("fake stream", "fake stderr", "fake trace"):
            assert f"- {title}: `" in content
        assert {upload.filename for upload in gateway.sent_attachments[-1]} == {"fake stderr.txt", "fake trace.md"}
        assert "Attached files: " in content


def test_artifacts_tagged_for_upload_are_attached(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = _echo_work(session, "Upload work", "Upload the result.")
        artifact = ArtifactStore(tmp_path / "artifacts").write_text(
            session,
            kind="worker_file",
            title="result.txt",
            content="hello from a worker file",
            work_item_id=work.id,
            tags=["discord_upload"],
        )

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        assert gateway.sent_attachments[-1] == [
            DiscordFileUpload(path=artifact.local_path, filename="result.txt", artifact_id=artifact.id)
        ]
        assert "Attached files: result.txt" in gateway.sent_messages[-1][1]
        assert artifact.local_path not in gateway.sent_messages[-1][1]


def test_artifacts_declared_in_produces_are_attached(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = _echo_work(session, "Declared upload", "Send the chart.")
        chart = ArtifactStore(tmp_path / "artifacts").write_bytes(
            session, kind="worker_file", title="chart.png", content=b"\x89PNG\r\n\x1a\n", work_item_id=work.id
        )
        attempt = _latest_attempt(session, work.id)
        attempt.produces = {"discord_upload_artifact_ids": [chart.id, "missing-artifact"]}
        session.flush()

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        assert [upload.artifact_id for upload in gateway.sent_attachments[-1]] == [chart.id]
        event = session.scalar(select(WorkEvent).where(WorkEvent.event_type == "discord.work_status_posted"))
        assert event is not None
        assert event.payload["upload_artifact_ids"] == [chart.id]


def test_every_upload_is_delivered_however_many_there_are(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = _echo_work(session, "Contact sheet", "Render the sheet.")
        store = ArtifactStore(tmp_path / "artifacts")
        artifacts = [
            store.write_bytes(
                session,
                kind="worker_file",
                title=f"frame-{index:02d}.png",
                content=b"\x89PNG\r\n\x1a\n",
                work_item_id=work.id,
                tags=["discord_upload"],
            )
            for index in range(MAX_FILES_PER_MESSAGE + 3)
        ]

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        assert {upload.artifact_id for upload in gateway.sent_attachments[-1]} == {
            artifact.id for artifact in artifacts
        }
        assert gateway.sent_messages[-1][1].endswith(", and 8 more")


def test_upload_filename_carries_the_file_extension(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordGateway()
    source = tmp_path / "render.png"
    source.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
    with session_scope() as session:
        work = _echo_work(session, "Render work", "Render the thing.")
        artifact = ArtifactStore(tmp_path / "artifacts").capture_file(
            session,
            path=source,
            kind="worker_file",
            title="Portrait — 1 (front view)",
            work_item_id=work.id,
            tags=["discord_upload"],
        )

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        assert artifact.content_type == "image/png"
        assert gateway.sent_attachments[-1][0].filename == "Portrait — 1 (front view).png"


def test_upload_filename_gains_an_extension_mimetypes_does_not_know(fresh_db: Path, tmp_path: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        work = _echo_work(session, "Log work", "Collect the logs.")
        store = ArtifactStore(tmp_path / "artifacts")
        for title in ("run output", "already.tq3d", "photo.jpeg"):
            suffix = ".jpg" if title == "photo.jpeg" else ".tq3d"
            store.write_bytes(
                session,
                kind="worker_file",
                title=title,
                content=b"x",
                suffix=suffix,
                work_item_id=work.id,
                tags=["discord_upload"],
            )

        assert _post_pending(DiscordOutputService(session), gateway) == 1

        assert sorted(upload.filename for upload in gateway.sent_attachments[-1]) == [
            "already.tq3d",
            "photo.jpeg",
            "run output.tq3d",
        ]


def test_default_followup_in_a_workflow_thread_posts_its_answer_in_that_thread(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        run = _start_step_workflow(session, "digest-chain")
        _finish_workflow_step(session)
        output = DiscordOutputService(session)
        assert _post_pending(output, gateway) == 1
        workflow_thread_id = session.get(WorkflowRun, run.id).discord_thread_id
        assert workflow_thread_id == "fake-thread-1"

        result = DiscordService(session).handle_thread_reply(
            discord_message_id="reply-digest",
            discord_channel_id=workflow_thread_id,
            discord_thread_id=workflow_thread_id,
            author="user",
            content="Why that one?",
        )
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test")
        assert claimed is not None
        assert claimed.work_item.id == result.entity_id
        WorkQueue(session).complete_attempt(claimed.attempt.id, summary="Because it was due first.")

        assert _post_pending(output, gateway) == 1
        assert len(gateway.created_threads) == 1
        assert gateway.sent_messages[-1] == (workflow_thread_id, "Because it was due first.")


class _PanelsFail(FakeDiscordGateway):
    def send_embed(self, *, channel_id, embed, view=None):
        raise RuntimeError("Missing Access")

    def edit_message(self, *, channel_id, message_id, content=None, embed=None, view=None):
        raise RuntimeError("Unknown Message")


def test_a_failing_panel_does_not_block_other_output_and_is_retried(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        finished = _start_step_workflow(session, "panel-failure")
        output = DiscordOutputService(session)
        assert _post_pending(output, gateway) == 0
        _finish_workflow_step(session)
        _echo_work(session, "Standalone work", "Standalone result.")
        started = _start_step_workflow(session, "panel-failure")

        failing = _PanelsFail()
        posted = _post_pending(output, failing)

        assert posted == 2
        assert ("fake-thread-2", "Standalone result.") in failing.sent_messages
        assert _post_pending(output, gateway) == 0
        assert _chain_panel_edits(gateway)[-1]["title"] == "Chain: panel-failure - completed"
        panel_runs = {
            event.workflow_run_id
            for event in session.scalars(
                select(WorkEvent).where(WorkEvent.event_type == "discord.workflow_status_panel_posted")
            ).all()
        }
        assert panel_runs == {finished.id, started.id}


def test_workflow_whose_final_work_is_silent_posts_nothing(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        run = _start_step_workflow(session, "quiet-workflow")
        output = DiscordOutputService(session)
        _post_pending(output, gateway)
        WorkRunner(session).run_next()
        [step] = session.scalars(select(WorkItem).where(WorkItem.workflow_run_id == run.id)).all()
        attempt = _latest_attempt(session, step.id)
        attempt.produces = {**(attempt.produces or {}), "silent": True}
        session.flush()
        WorkflowService(session).tick_runs()
        sent_before = list(gateway.sent_messages)

        _post_pending(output, gateway)
        _post_pending(output, gateway)

        assert session.get(WorkflowRun, run.id).status == "completed"
        assert gateway.created_threads == []
        assert gateway.sent_messages == sent_before
        event = session.scalar(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.workflow_final_posted", WorkEvent.workflow_run_id == run.id
            )
        )
        assert event is not None
        assert event.payload["silent"] is True


def test_hidden_canceled_workflow_posts_nothing(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        run = _start_step_workflow(session, "hidden-cancel-workflow")
        for work_item in session.scalars(select(WorkItem).where(WorkItem.workflow_run_id == run.id)):
            work_item.visible = False
        session.flush()
        WorkflowService(session).cancel_run(run.id)
        session.flush()

        assert _post_pending(DiscordOutputService(session), gateway) == 0
        assert gateway.created_threads == []
        assert all("Canceled." not in content for _channel, content in gateway.sent_messages)


def test_canceled_workflow_with_visible_work_posts_canceled(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        run = _start_step_workflow(session, "visible-cancel-workflow")
        WorkflowService(session).cancel_run(run.id)
        session.flush()

        assert _post_pending(DiscordOutputService(session), gateway) == 1
        assert gateway.sent_messages[-1][1] == "Canceled."


def test_split_markdown_keeps_lines_whole_and_hard_splits_overlong_lines() -> None:
    content = "intro\n" + "a" * 25 + "\nshort one\nshort two\n"

    chunks = split_markdown(content, 10)

    assert chunks == ["intro", "aaaaaaaaaa", "aaaaaaaaaa", "aaaaa", "short one", "short two"]
    assert split_markdown("  \n ", 10) == []
    assert split_markdown("one\ntwo", 100) == ["one\ntwo"]


def test_batch_uploads_notes_oversized_and_missing_files(tmp_path: Path) -> None:
    small = tmp_path / "small.bin"
    small.write_bytes(b"x" * 1_000)
    huge = tmp_path / "huge.bin"
    huge.write_bytes(b"x" * 5_000)

    batches = batch_uploads(
        [
            DiscordFileUpload(path=str(small), filename="small.bin", artifact_id="a1"),
            DiscordFileUpload(path=str(huge), filename="huge.bin", artifact_id="a2"),
            DiscordFileUpload(path=str(tmp_path / "gone.png"), filename="gone.png"),
        ],
        max_file_bytes=2_000,
        max_request_bytes=10_000,
    )

    [(sendable, notes, temps)] = batches
    assert [upload.filename for upload in sendable] == ["small.bin"]
    assert temps == []
    assert any("huge.bin" in note and "a2" in note for note in notes)
    assert any("gone.png" in note for note in notes)


def test_batch_uploads_shrinks_oversized_images_to_jpeg(tmp_path: Path) -> None:
    big = tmp_path / "render.png"
    Image.effect_noise((256, 256), 64).convert("RGB").save(big, format="PNG")
    cap = big.stat().st_size - 1

    [(sendable, notes, temps)] = batch_uploads(
        [DiscordFileUpload(path=str(big), filename="render.png", artifact_id="a3")],
        max_file_bytes=cap,
        max_request_bytes=10 * cap,
    )

    try:
        assert len(sendable) == 1
        assert sendable[0].filename == "render.jpg"
        assert sendable[0].artifact_id == "a3"
        assert Path(sendable[0].path).stat().st_size <= cap
        assert temps == [Path(sendable[0].path)]
        assert any("downscaled" in note for note in notes)
    finally:
        for temp in temps:
            temp.unlink()


def test_batch_uploads_notes_an_oversized_image_it_cannot_decode(tmp_path: Path) -> None:
    broken = tmp_path / "broken.png"
    broken.write_bytes(b"not an image" * 100)

    [(sendable, notes, temps)] = batch_uploads(
        [DiscordFileUpload(path=str(broken), filename="broken.png", artifact_id="a4")],
        max_file_bytes=100,
        max_request_bytes=1_000,
    )

    assert sendable == []
    assert temps == []
    assert any("broken.png" in note and "a4" in note for note in notes)
    assert shrink_image(broken, max_bytes=100) is None


def test_batch_uploads_moves_files_past_the_request_budget_to_continuation_messages(tmp_path: Path) -> None:
    paths = []
    for name in ("one.bin", "two.bin", "three.bin"):
        path = tmp_path / name
        path.write_bytes(b"x" * 900)
        paths.append(path)

    batches = batch_uploads(
        [DiscordFileUpload(path=str(path), filename=path.name) for path in paths],
        max_file_bytes=1_000,
        max_request_bytes=1_000,
    )

    assert [[upload.filename for upload in sendable] for sendable, _notes, _temps in batches] == [
        ["one.bin"],
        ["two.bin"],
        ["three.bin"],
    ]
    assert all(not notes for _sendable, notes, _temps in batches)


def test_batch_uploads_caps_files_per_message(tmp_path: Path) -> None:
    uploads = []
    for index in range(MAX_FILES_PER_MESSAGE + 2):
        path = tmp_path / f"file-{index:02d}.txt"
        path.write_text("x", encoding="utf-8")
        uploads.append(DiscordFileUpload(path=str(path)))

    batches = batch_uploads(uploads)

    assert [len(sendable) for sendable, _notes, _temps in batches] == [MAX_FILES_PER_MESSAGE, 2]
    assert [upload.display_name for upload in batches[1][0]] == ["file-10.txt", "file-11.txt"]
    assert batch_uploads([]) == []


class _RecordingChannel:
    def __init__(self, channel_id: int, *, reject_files: bool = False) -> None:
        self.id = channel_id
        self.reject_files = reject_files
        self.sent: list[dict[str, Any]] = []

    async def send(self, content=None, *, view=None, files=None):
        files = list(files or [])
        paths = [file.fp.name for file in files]
        for file in files:
            file.close()
        if files and self.reject_files:
            raise discord.HTTPException(SimpleNamespace(status=413, reason="Payload Too Large"), "Request too large")
        self.sent.append({"content": content, "view": view, "files": [file.filename for file in files], "paths": paths})
        return SimpleNamespace(id=1000 + len(self.sent))


class _FakeClient:
    def __init__(self, channel: _RecordingChannel) -> None:
        self.channel = channel

    def get_channel(self, channel_id: int):
        return self.channel if channel_id == self.channel.id else None


@pytest.fixture()
def event_loop_thread() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.close()


def _files(tmp_path: Path, count: int) -> list[DiscordFileUpload]:
    uploads = []
    for index in range(count):
        path = tmp_path / f"page-{index:02d}.txt"
        path.write_text(f"page {index}", encoding="utf-8")
        uploads.append(DiscordFileUpload(path=str(path), filename=path.name, artifact_id=f"art-{index}"))
    return uploads


def test_gateway_sends_attachments_past_one_message_as_continuations(
    tmp_path: Path, event_loop_thread: asyncio.AbstractEventLoop
) -> None:
    channel = _RecordingChannel(42)
    gateway = DiscordPyGateway(_FakeClient(channel), event_loop_thread)
    view = object()

    sent = gateway.send_message(
        channel_id="42", content="Here are the pages.", view=view, attachments=_files(tmp_path, 12)
    )

    assert sent.message_id == "1001"
    assert sent.channel_id == "42"
    assert [len(message["files"]) for message in channel.sent] == [10, 2]
    assert channel.sent[0]["content"] == "Here are the pages."
    assert channel.sent[0]["view"] is view
    assert channel.sent[1]["content"] == "(continued — 2 more attachments: page-10.txt, page-11.txt)"
    assert channel.sent[1]["view"] is None


def test_gateway_sends_a_shrunk_image_and_deletes_its_temp_file(
    tmp_path: Path, event_loop_thread: asyncio.AbstractEventLoop, monkeypatch: pytest.MonkeyPatch
) -> None:
    image = tmp_path / "render.png"
    Image.effect_noise((256, 256), 64).convert("RGB").save(image, format="PNG")
    cap = image.stat().st_size - 1
    monkeypatch.setattr(gateway_module, "batch_uploads", partial(batch_uploads, max_file_bytes=cap))
    channel = _RecordingChannel(7)

    DiscordPyGateway(_FakeClient(channel), event_loop_thread).send_message(
        channel_id="7", content="Render done.", attachments=[DiscordFileUpload(path=str(image), filename="render.png")]
    )

    [message] = channel.sent
    assert message["files"] == ["render.jpg"]
    assert message["content"] == "Render done.\nrender.png downscaled to fit Discord's upload cap"
    assert not Path(message["paths"][0]).exists()
    assert image.exists()


def test_gateway_falls_back_to_naming_artifacts_when_discord_rejects_the_upload(
    tmp_path: Path, event_loop_thread: asyncio.AbstractEventLoop
) -> None:
    channel = _RecordingChannel(9, reject_files=True)
    view = object()

    DiscordPyGateway(_FakeClient(channel), event_loop_thread).send_message(
        channel_id="9", content="Report attached.", view=view, attachments=_files(tmp_path, 2)
    )

    [message] = channel.sent
    assert message["files"] == []
    assert message["view"] is view
    assert message["content"] == (
        "Report attached.\n(attachments exceeded Discord's upload limit — kept as artifacts: art-0, art-1)"
    )


class _EditableMessage:
    def __init__(self, message_id: int = 0, *, pinned: bool = False) -> None:
        self.id = message_id
        self.pinned = pinned
        self.pin_calls = 0
        self.edits: list[dict[str, Any]] = []

    async def edit(self, **kwargs) -> None:
        self.edits.append(kwargs)

    async def pin(self) -> None:
        self.pin_calls += 1
        self.pinned = True


class _PinnableThread:
    """A thread that can be archived and holds the messages sent to it, to edit and pin."""

    def __init__(self, channel_id: int, *, archived: bool = False, messages: dict[int, _EditableMessage] | None = None):
        self.id = channel_id
        self.archived = archived
        self.locked = False
        self.messages = messages or {}
        self.sent: list[dict[str, Any]] = []

    async def send(self, content=None, *, embed=None, view=None, silent=False):
        self.sent.append({"embed": embed, "view": view, "silent": silent})
        message = _EditableMessage(500 + len(self.sent))
        self.messages[message.id] = message
        return message

    async def edit(self, *, archived: bool) -> None:
        self.archived = archived

    async def fetch_message(self, message_id: int) -> _EditableMessage:
        if message_id not in self.messages:
            raise discord.NotFound(SimpleNamespace(status=404, reason="Not Found"), "Unknown Message")
        return self.messages[message_id]


def test_gateway_unarchives_a_quiet_thread_before_editing_its_sticky_note(
    event_loop_thread: asyncio.AbstractEventLoop,
) -> None:
    message = _EditableMessage()
    channel = _PinnableThread(77, archived=True, messages={501: message})

    DiscordPyGateway(_FakeClient(channel), event_loop_thread).edit_message(
        channel_id="77", message_id="501", embed={"title": "Sticky note"}
    )

    assert channel.archived is False
    [edit] = message.edits
    assert edit["embed"].title == "Sticky note"
    assert edit["view"] is None


def test_gateway_reports_a_deleted_message_as_gone(event_loop_thread: asyncio.AbstractEventLoop) -> None:
    gateway = DiscordPyGateway(_FakeClient(_PinnableThread(78)), event_loop_thread)

    with pytest.raises(DiscordMessageGone):
        gateway.edit_message(channel_id="78", message_id="999", embed={"title": "Sticky note"})
    with pytest.raises(DiscordMessageGone):
        gateway.pin_message(channel_id="78", message_id="999")


def test_gateway_posts_a_sticky_note_silently_and_pins_it(event_loop_thread: asyncio.AbstractEventLoop) -> None:
    channel = _PinnableThread(79)
    gateway = DiscordPyGateway(_FakeClient(channel), event_loop_thread)

    sent = gateway.send_embed(channel_id="79", embed={"title": "Sticky note"}, silent=True)
    gateway.pin_message(channel_id="79", message_id=sent.message_id)
    gateway.pin_message(channel_id="79", message_id=sent.message_id)

    assert [message["silent"] for message in channel.sent] == [True]
    message = channel.messages[int(sent.message_id)]
    assert message.pinned is True and message.pin_calls == 1


def test_gateway_counts_a_note_the_user_pinned_as_pinned_and_unarchives_to_pin(
    event_loop_thread: asyncio.AbstractEventLoop,
) -> None:
    by_hand = _EditableMessage(601, pinned=True)
    unpinned = _EditableMessage(602)
    channel = _PinnableThread(80, archived=True, messages={601: by_hand, 602: unpinned})
    gateway = DiscordPyGateway(_FakeClient(channel), event_loop_thread)

    gateway.pin_message(channel_id="80", message_id="601")
    assert channel.archived is True and by_hand.pin_calls == 0
    gateway.pin_message(channel_id="80", message_id="602")

    assert channel.archived is False and unpinned.pin_calls == 1
