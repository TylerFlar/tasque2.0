from __future__ import annotations

from pathlib import Path

from sqlalchemy import select

from tasque2.artifacts import ArtifactStore
from tasque2.db import session_scope
from tasque2.discord_adapter import DiscordAttachmentPayload, DiscordService
from tasque2.models import (
    Artifact,
    DiscordMessage,
    DiscordThread,
    Memory,
    WorkItem,
)
from tasque2.queue import WorkQueue
from tasque2.repo import WorkRepository
from tasque2.workflows import WorkflowService


def test_discord_intake_queues_work_item_once(fresh_db: Path) -> None:
    with session_scope() as session:
        service = DiscordService(session)
        first = service.ingest_intake_message(
            discord_message_id="m1",
            discord_channel_id="c1",
            author="user",
            content="Do the thing\nwith details",
            worker_kind="function.echo",
        )
        second = service.ingest_intake_message(
            discord_message_id="m1",
            discord_channel_id="c1",
            author="user",
            content="Do the thing\nwith details",
            worker_kind="function.echo",
        )

        assert first.id == second.id
        assert first.title == "Do the thing"
        assert session.scalar(select(DiscordMessage).where(DiscordMessage.discord_message_id == "m1"))
        assert len(session.scalars(select(WorkItem)).all()) == 1


def test_discord_intake_saves_attachments_as_local_artifacts(fresh_db: Path) -> None:
    with session_scope() as session:
        work = DiscordService(session).ingest_intake_message(
            discord_message_id="m-attachment",
            discord_channel_id="c1",
            author="user",
            content="Use the attached notes.",
            worker_kind="provider.fake",
            attachments=[
                DiscordAttachmentPayload(
                    filename="notes.md",
                    content_type="text/markdown",
                    data=b"# Notes\nUse option A.",
                )
            ],
        )

        artifact = session.scalar(select(Artifact).where(Artifact.work_item_id == work.id))
        assert artifact is not None
        assert artifact.kind == "discord_attachment"
        assert Path(artifact.local_path).read_bytes() == b"# Notes\nUse option A."
        assert work.context["attachments"][0]["local_path"] == artifact.local_path
        assert "Attached files available locally" in work.task_instruction
        assert artifact.local_path in work.task_instruction


def test_discord_natural_language_intake_queues_provider_work(fresh_db: Path) -> None:
    with session_scope() as session:
        result = DiscordService(session).handle_intake_message(
            discord_message_id="m-natural",
            discord_channel_id="c1",
            author="user",
            content="Summarize the attached notes.",
        )

        work = session.get(WorkItem, result.entity_id)
        assert result.action == "work_queued"
        assert work is not None
        assert work.worker_kind == "provider.default"
        assert "Handle this Discord message naturally" in work.task_instruction
        assert "Summarize the attached notes." in work.task_instruction
        assert work.context["discord_intake"]["discord_message_id"] == "m-natural"


def test_discord_open_ended_intake_is_left_to_general_worker(fresh_db: Path) -> None:
    with session_scope() as session:
        result = DiscordService(session).handle_intake_message(
            discord_message_id="m-open-ended",
            discord_channel_id="c1",
            author="user",
            content="Please clean up the repo and verify the tests pass.",
        )

        work = session.get(WorkItem, result.entity_id)
        assert result.action == "work_queued"
        assert work is not None
        assert work.worker_kind == "provider.default"
        assert "Please clean up the repo" in work.task_instruction

        message = session.scalar(
            select(DiscordMessage).where(DiscordMessage.discord_message_id == "m-open-ended")
        )
        assert message is not None
        assert message.work_item_id == work.id


def test_discord_work_thread_reply_can_create_memory_and_followup_work(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Workout generator",
            task_instruction="Generate workout.",
            worker_kind="provider.fake",
            runtime_contract={
                "model_profile": "medium",
                "cwd": "parent-cwd-should-not-be-inherited",
            },
            context={
                "memory_namespace": "health",
                "reply_memory": {
                    "enabled": True,
                    "namespace": "health",
                    "kind": "working",
                    "tags": ["workout", "completion"],
                    "ttl_days": 90,
                    "content_template": "Workout completion from {author}:\n{content}",
                },
                "reply_followup_work": {
                    "enabled": True,
                    "title": "Parse workout completion",
                    "worker_kind": "provider.default",
                    "task_instruction": (
                        "Parse this workout completion reply into structured state and update "
                        "current_workout_state with memory_writes."
                    ),
                    "context": {
                        "memory_canonical_keys": ["current_workout_state"],
                        "memory_queries": ["completed workout actual loads"],
                    },
                },
            },
            discord_thread_id="thread-workout",
        )
        DiscordService(session).bind_thread(
            purpose="work",
            discord_channel_id="parent",
            discord_thread_id="thread-workout",
            work_item_id=work.id,
        )
        DiscordService(session).record_message(
            discord_message_id="out-workout",
            discord_channel_id="thread-workout",
            discord_thread_id="thread-workout",
            direction="outbound",
            author="tasque",
            content_preview="Posted workout prescription.",
            work_item_id=work.id,
        )
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test")
        assert claimed is not None
        report = ArtifactStore().write_text(
            session,
            kind="worker_report",
            title="Workout prescription",
            content="**Focus**: push\nBench press - 3x10 @ 95 lb",
            work_item_id=work.id,
            attempt_id=claimed.attempt.id,
            tags=["report"],
        )
        WorkQueue(session).complete_attempt(
            claimed.attempt.id,
            summary="Prescribed push.",
            produces={"focus": "push"},
            report_artifact_id=report.id,
        )

        result = DiscordService(session).handle_thread_reply(
            discord_message_id="reply-workout",
            discord_channel_id="thread-workout",
            discord_thread_id="thread-workout",
            author="user",
            content="For the workout I did bench 95x10 RPE 8.",
        )

        assert result.action == "work_reply_recorded"
        memory = session.scalar(select(Memory).where(Memory.source_id == "reply-workout"))
        assert memory is not None
        assert memory.namespace == "health"
        assert memory.kind == "working"
        assert "workout" in memory.tags
        assert "For the workout I did bench 95x10 RPE 8." in memory.content

        followup = session.scalar(select(WorkItem).where(WorkItem.source_kind == "discord_reply_followup"))
        assert followup is not None
        assert followup.worker_kind == "provider.default"
        assert followup.runtime_contract == {"model_profile": "medium"}
        assert followup.discord_thread_id == "thread-workout"
        assert "bench 95x10 RPE 8" in followup.task_instruction
        assert followup.context["memory_namespace"] == "health"
        assert followup.context["parent_work_item_id"] == work.id
        assert followup.context["parent_report_artifact_id"] == report.id
        assert followup.context["parent_work"]["latest_attempt"]["produces"] == {"focus": "push"}
        assert followup.context["input_artifacts"][0]["artifact_id"] == report.id
        assert followup.context["source_reply"]["content"] == "For the workout I did bench 95x10 RPE 8."
        assert followup.context["source_reply"]["discord_message_id"] == "reply-workout"
        conversation = followup.context["conversation"]
        assert conversation["scope"] == "thread"
        assert conversation["current_discord_message_id"] == "reply-workout"
        assert any(
            message["discord_message_id"] == "out-workout" for message in conversation["recent_messages"]
        )
        assert any(
            message["discord_message_id"] == "reply-workout" for message in conversation["recent_messages"]
        )


def test_discord_work_thread_reply_followup_uses_bound_thread_when_parent_missing_thread_id(
    fresh_db: Path,
) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Workout generator",
            task_instruction="Generate workout.",
            worker_kind="provider.fake",
            context={
                "memory_namespace": "health",
                "reply_followup_work": {
                    "enabled": True,
                    "title": "Process workout reply",
                    "worker_kind": "provider.default",
                    "task_instruction": "Update workout memory from this reply.",
                },
            },
            discord_thread_id=None,
        )
        DiscordService(session).bind_thread(
            purpose="work",
            discord_channel_id="jobs",
            discord_thread_id="thread-workout",
            work_item_id=work.id,
        )

        result = DiscordService(session).handle_thread_reply(
            discord_message_id="reply-workout-thread-fallback",
            discord_channel_id="thread-workout",
            discord_thread_id="thread-workout",
            author="user",
            content="I have done the workout as described.",
        )

        assert result.action == "work_reply_recorded"
        followup = session.scalar(
            select(WorkItem).where(WorkItem.source_id == "reply-workout-thread-fallback")
        )
        assert followup is not None
        assert result.entity_id == followup.id
        assert followup.discord_thread_id == "thread-workout"
        assert followup.context["parent_discord_thread_id"] == "thread-workout"


def test_discord_reply_followup_can_load_template_file(
    fresh_db: Path,
    tmp_path: Path,
) -> None:
    template = tmp_path / "reply.template.md"
    template.write_text("# Reply Processor\n\nParse the reply from context.", encoding="utf-8")

    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Templated reply parent",
            task_instruction="Generate something.",
            worker_kind="provider.fake",
            context={
                "reply_followup_work": {
                    "enabled": True,
                    "title": "Process templated reply",
                    "worker_kind": "provider.default",
                    "task_template_path": "reply.template.md",
                    "template_base_dir": str(tmp_path),
                }
            },
            discord_thread_id="thread-template",
        )
        DiscordService(session).bind_thread(
            purpose="work",
            discord_channel_id="parent",
            discord_thread_id="thread-template",
            work_item_id=work.id,
        )

        result = DiscordService(session).handle_thread_reply(
            discord_message_id="reply-template",
            discord_channel_id="thread-template",
            discord_thread_id="thread-template",
            author="user",
            content="Here is the update.",
        )

        assert result.action == "work_reply_recorded"
        followup = session.scalar(select(WorkItem).where(WorkItem.source_id == "reply-template"))
        assert followup is not None
        assert followup.task_instruction.startswith("# Reply Processor\n\nParse the reply")
        assert "Here is the update." in followup.task_instruction


def test_discord_channel_reply_to_referenced_work_message_routes_followup(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Workout generator",
            task_instruction="Generate workout.",
            worker_kind="provider.fake",
            runtime_contract={"model_profile": "medium"},
            context={
                "memory_namespace": "health",
                "reply_memory": {
                    "enabled": True,
                    "namespace": "health",
                    "kind": "working",
                    "tags": ["workout", "completion"],
                },
                "reply_followup_work": {
                    "enabled": True,
                    "title": "Parse channel workout reply",
                    "worker_kind": "provider.default",
                    "runtime_contract": {"model_profile": "low"},
                    "task_instruction": "Process this workout reply.",
                },
            },
        )
        DiscordService(session).record_message(
            discord_message_id="out-channel-workout",
            discord_channel_id="output-channel",
            discord_thread_id=None,
            direction="outbound",
            author="tasque",
            content_preview="Work update: Workout generator\nSummary: Prescribed push.",
            work_item_id=work.id,
        )

        result = DiscordService(session).handle_channel_message(
            discord_message_id="reply-channel-workout",
            discord_channel_id="output-channel",
            author="user",
            content="I did it: bench 95x10 RPE 8.",
            referenced_discord_message_id="out-channel-workout",
        )

        assert result.action == "work_reply_recorded"
        memory = session.scalar(select(Memory).where(Memory.source_id == "reply-channel-workout"))
        assert memory is not None
        followup = session.scalar(select(WorkItem).where(WorkItem.source_id == "reply-channel-workout"))
        assert followup is not None
        assert followup.runtime_contract == {"model_profile": "low"}
        assert followup.context["source_reply"]["referenced_discord_message_id"] == "out-channel-workout"
        conversation = followup.context["conversation"]
        assert conversation["scope"] == "channel"
        assert conversation["referenced_discord_message_id"] == "out-channel-workout"
        assert [message["discord_message_id"] for message in conversation["recent_messages"]] == [
            "out-channel-workout",
            "reply-channel-workout",
        ]


def test_discord_workflow_thread_reply_routes_to_final_work_followup(fresh_db: Path) -> None:
    definition = {
        "nodes": [
            {
                "key": "course_picker",
                "kind": "work",
                "title": "Art course picker",
                "task_instruction": "Pick a course.",
                "worker_kind": "function.echo",
                "context": {
                    "memory_namespace": "creative",
                    "reply_followup_work": {
                        "enabled": True,
                        "detach_from_workflow": True,
                        "title": "Critique art submission",
                        "worker_kind": "provider.default",
                        "task_instruction": "Critique this submitted art.",
                    },
                },
            }
        ]
    }
    with session_scope() as session:
        workflow = WorkflowService(session).create_definition(
            name="art-course-chain",
            version="1",
            definition=definition,
        )
        run = WorkflowService(session).start_run(workflow_definition_id=workflow.id)
        WorkflowService(session).tick_runs()
        work = session.scalar(select(WorkItem).where(WorkItem.workflow_run_id == run.id))
        assert work is not None
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test")
        assert claimed is not None
        WorkQueue(session).complete_attempt(
            claimed.attempt.id,
            summary="Picked course.",
            produces={"selected_course_url": "https://example.com/course"},
        )
        WorkflowService(session).tick_runs()
        DiscordService(session).bind_thread(
            purpose="workflow",
            discord_channel_id="jobs",
            discord_thread_id="thread-art-workflow",
            workflow_run_id=run.id,
        )

        result = DiscordService(session).handle_thread_reply(
            discord_message_id="reply-art-workflow",
            discord_channel_id="thread-art-workflow",
            discord_thread_id="thread-art-workflow",
            author="user",
            content="Here is my finished piece.",
        )

        assert result.action == "workflow_reply_followup_recorded"
        followup = session.scalar(select(WorkItem).where(WorkItem.source_id == "reply-art-workflow"))
        assert followup is not None
        assert followup.workflow_run_id is None
        assert followup.context["parent_work_item_id"] == work.id
        assert followup.context["parent_work"]["latest_attempt"]["produces"] == {
            "selected_course_url": "https://example.com/course"
        }
        assert "Here is my finished piece." in followup.task_instruction


def test_discord_work_thread_reply_memory_is_idempotent(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Workout generator",
            task_instruction="Generate workout.",
            worker_kind="provider.fake",
            context={
                "memory_namespace": "health",
                "reply_memory": {"enabled": True, "namespace": "health"},
            },
        )
        DiscordService(session).bind_thread(
            purpose="work",
            discord_channel_id="parent",
            discord_thread_id="thread-once",
            work_item_id=work.id,
        )

        for _ in range(2):
            DiscordService(session).handle_thread_reply(
                discord_message_id="reply-once",
                discord_channel_id="thread-once",
                discord_thread_id="thread-once",
                author="user",
                content="completed workout",
            )

        memories = session.scalars(
            select(Memory).where(
                Memory.source_kind == "discord_reply",
                Memory.source_id == "reply-once",
            )
        ).all()
        assert len(memories) == 1


def test_discord_thread_binding_reuses_existing_thread(fresh_db: Path) -> None:
    with session_scope() as session:
        service = DiscordService(session)
        first = service.bind_thread(
            purpose="work",
            discord_channel_id="parent",
            discord_thread_id="thread-3",
            work_item_id="work-1",
        )
        second = service.bind_thread(
            purpose="work",
            discord_channel_id="parent",
            discord_thread_id="thread-3",
            work_item_id="work-1",
        )

        assert first.id == second.id
        assert len(session.scalars(select(DiscordThread)).all()) == 1
