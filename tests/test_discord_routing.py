from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.db import session_scope
from tasque2.discord.conversation import collapse_conversation
from tasque2.discord.routing import (
    DEFAULT_REPLY_INSTRUCTION,
    INTAKE_LANE,
    DiscordAttachmentPayload,
    DiscordService,
    followup_contract,
)
from tasque2.models import Artifact, DiscordMessage, DiscordThread, Memory, WorkflowNode, WorkflowRun, WorkItem
from tasque2.work.queue import WorkQueue
from tasque2.work.repository import WorkRepository
from tasque2.worker.context import WorkerContextBuilder
from tasque2.workflows import WorkflowService


def _thread_work(
    session: Session,
    *,
    thread_id: str,
    title: str = "Parent work",
    context: dict[str, Any] | None = None,
    runtime_contract: dict[str, Any] | None = None,
    lane: str | None = None,
) -> WorkItem:
    work = WorkRepository(session).create_work_item(
        title=title,
        task_instruction=f"Do: {title}",
        worker_kind="provider.fake",
        runtime_contract=runtime_contract,
        context=context,
        discord_thread_id=thread_id,
        lane=lane,
    )
    DiscordService(session).bind_thread(
        purpose="work", discord_channel_id="jobs", discord_thread_id=thread_id, work_item_id=work.id
    )
    return work


def _reply(session: Session, *, thread_id: str, message_id: str, content: str):
    return DiscordService(session).handle_thread_reply(
        discord_message_id=message_id,
        discord_channel_id=thread_id,
        discord_thread_id=thread_id,
        author="user",
        content=content,
    )


def _followup(session: Session, message_id: str) -> WorkItem:
    followup = session.scalar(select(WorkItem).where(WorkItem.source_id == message_id))
    assert followup is not None
    return followup


def _single_step_workflow(session: Session, *, name: str, context: dict[str, Any] | None = None) -> WorkflowRun:
    definition = {
        "nodes": [
            {
                "key": "step",
                "kind": "work",
                "title": "Workflow step",
                "task_instruction": "Run the step.",
                "worker_kind": "function.echo",
                "context": context or {},
            }
        ]
    }
    workflows = WorkflowService(session)
    return workflows.start_run(
        workflow_definition_id=workflows.create_definition(name=name, version="1", definition=definition).id
    )


def test_intake_message_queues_one_work_item_per_discord_message(fresh_db: Path) -> None:
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


def test_intake_attachments_become_local_artifacts_listed_in_the_instruction(fresh_db: Path) -> None:
    with session_scope() as session:
        work = DiscordService(session).ingest_intake_message(
            discord_message_id="m-attachment",
            discord_channel_id="c1",
            author="user",
            content="Use the attached notes.",
            worker_kind="provider.fake",
            attachments=[
                DiscordAttachmentPayload(
                    filename="notes.md", content_type="text/markdown", data=b"# Notes\nUse option A."
                )
            ],
        )

        artifact = session.scalar(select(Artifact).where(Artifact.work_item_id == work.id))
        assert artifact is not None
        assert artifact.kind == "discord_attachment"
        assert Path(artifact.local_path).read_bytes() == b"# Notes\nUse option A."
        assert work.context["attachments"][0]["local_path"] == artifact.local_path
        assert work.context["input_artifacts"][0]["artifact_id"] == artifact.id
        assert "Attached files available locally" in work.task_instruction
        assert artifact.local_path in work.task_instruction


def test_intake_message_queues_provider_work_with_the_intake_instruction(fresh_db: Path) -> None:
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
        assert work.task_instruction.startswith("# Discord intake")
        assert "Summarize the attached notes." in work.task_instruction
        assert work.context["discord_intake"] == {
            "discord_message_id": "m-natural",
            "discord_channel_id": "c1",
            "author": "user",
        }


def test_intake_message_is_linked_to_its_work_item(fresh_db: Path) -> None:
    with session_scope() as session:
        result = DiscordService(session).handle_intake_message(
            discord_message_id="m-open-ended",
            discord_channel_id="c1",
            author="user",
            content="Please clean up the repo and verify the tests pass.",
        )

        work = session.get(WorkItem, result.entity_id)
        assert work is not None
        assert "Please clean up the repo" in work.task_instruction
        message = session.scalar(select(DiscordMessage).where(DiscordMessage.discord_message_id == "m-open-ended"))
        assert message is not None
        assert message.direction == "inbound"
        assert message.work_item_id == work.id


def test_intake_work_runs_in_the_discord_intake_lane(fresh_db: Path) -> None:
    with session_scope() as session:
        service = DiscordService(session)
        queued = service.handle_intake_message(
            discord_message_id="m-lane", discord_channel_id="c1", author="user", content="What is on today?"
        )
        ingested = service.ingest_intake_message(
            discord_message_id="m-lane-2", discord_channel_id="c1", author="user", content="And tomorrow?"
        )

        assert INTAKE_LANE == "discord-intake"
        assert session.get(WorkItem, queued.entity_id).lane == "discord-intake"
        assert ingested.lane == "discord-intake"


def test_repeated_intake_message_is_reported_as_already_recorded(fresh_db: Path) -> None:
    with session_scope() as session:
        service = DiscordService(session)
        first = service.handle_intake_message(
            discord_message_id="m-twice", discord_channel_id="c1", author="user", content="Book the dentist."
        )
        second = service.handle_intake_message(
            discord_message_id="m-twice", discord_channel_id="c1", author="user", content="Book the dentist."
        )

        assert first.action == "work_queued"
        assert second.action == "intake_already_recorded"
        assert second.entity_id == first.entity_id
        assert len(session.scalars(select(WorkItem)).all()) == 1


def test_intake_message_recorded_without_work_is_still_queued(fresh_db: Path) -> None:
    with session_scope() as session:
        service = DiscordService(session)
        unbound = service.handle_channel_message(
            discord_message_id="m-reply",
            discord_channel_id="intake",
            author="user",
            content="Also add eggs to the list.",
            referenced_discord_message_id="not-a-tasque-message",
        )
        queued = service.handle_intake_message(
            discord_message_id="m-reply",
            discord_channel_id="intake",
            author="user",
            content="Also add eggs to the list.",
        )

        assert unbound.action == "unbound_channel"
        assert queued.action == "work_queued"
        message = session.scalar(select(DiscordMessage).where(DiscordMessage.discord_message_id == "m-reply"))
        assert message is not None
        assert message.work_item_id == queued.entity_id


def test_thread_reply_records_memory_and_queues_a_followup_with_the_parent_context(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _thread_work(
            session,
            thread_id="thread-workout",
            title="Workout generator",
            runtime_contract={"model_profile": "medium", "cwd": "parent-only"},
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
                    "task_instruction": "Parse this workout completion reply and update current_workout_state.",
                    "context": {
                        "memory_canonical_keys": ["current_workout_state"],
                        "memory_queries": ["completed workout actual loads"],
                    },
                },
            },
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
            claimed.attempt.id, summary="Prescribed push.", produces={"focus": "push"}, report_artifact_id=report.id
        )

        result = _reply(
            session,
            thread_id="thread-workout",
            message_id="reply-workout",
            content="For the workout I did bench 95x10 RPE 8.",
        )

        assert result.action == "work_reply_recorded"
        memory = session.scalar(select(Memory).where(Memory.source_id == "reply-workout"))
        assert memory is not None
        assert memory.namespace == "health"
        assert memory.kind == "working"
        assert memory.ttl_days == 90
        assert {"workout", "completion", "discord", "reply"} <= set(memory.tags)
        assert memory.content == "Workout completion from user:\nFor the workout I did bench 95x10 RPE 8."

        followup = _followup(session, "reply-workout")
        assert result.entity_id == followup.id
        assert followup.source_kind == "discord_reply_followup"
        assert followup.title == "Parse workout completion"
        assert followup.worker_kind == "provider.default"
        assert followup.runtime_contract == {"model_profile": "medium"}
        assert followup.discord_thread_id == "thread-workout"
        assert followup.task_instruction.startswith("Parse this workout completion reply")
        assert "bench 95x10 RPE 8" in followup.task_instruction
        assert followup.context["memory_namespace"] == "health"
        assert followup.context["memory_canonical_keys"] == ["current_workout_state"]
        assert followup.context["parent_work_item_id"] == work.id
        assert followup.context["parent_report_artifact_id"] == report.id
        assert followup.context["input_artifacts"][0]["artifact_id"] == report.id
        assert followup.context["source_reply"]["content"] == "For the workout I did bench 95x10 RPE 8."
        assert followup.context["source_reply"]["discord_message_id"] == "reply-workout"
        assert "reply_default_processor" not in followup.context
        conversation = followup.context["conversation"]
        assert conversation["scope"] == "thread"
        assert conversation["current_discord_message_id"] == "reply-workout"
        assert [message["discord_message_id"] for message in conversation["recent_messages"]] == [
            "out-workout",
            "reply-workout",
        ]

        packet = WorkerContextBuilder(session).build_for_work(followup)
        assert packet["parent_work"]["latest_attempt"]["produces"] == {"focus": "push"}
        assert packet["parent_work"]["report_artifact"]["id"] == report.id


def test_thread_reply_followup_posts_into_the_bound_thread_when_the_parent_has_no_thread_id(fresh_db: Path) -> None:
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
                    "task_instruction": "Update workout memory from this reply.",
                },
            },
        )
        DiscordService(session).bind_thread(
            purpose="work", discord_channel_id="jobs", discord_thread_id="thread-workout", work_item_id=work.id
        )

        result = _reply(
            session, thread_id="thread-workout", message_id="reply-fallback", content="I did the workout as described."
        )

        assert result.action == "work_reply_recorded"
        followup = _followup(session, "reply-fallback")
        assert result.entity_id == followup.id
        assert followup.discord_thread_id == "thread-workout"
        assert followup.context["parent_discord_thread_id"] == "thread-workout"


def test_reply_followup_reads_its_instruction_from_a_template_file(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "reply.template.md"
    template.write_text("# Reply Processor\n\nParse the reply from context.", encoding="utf-8")
    with session_scope() as session:
        _thread_work(
            session,
            thread_id="thread-template",
            context={
                "reply_followup_work": {
                    "enabled": True,
                    "title": "Process templated reply",
                    "task_template_path": "reply.template.md",
                    "template_base_dir": str(tmp_path),
                }
            },
        )

        result = _reply(
            session, thread_id="thread-template", message_id="reply-template", content="Here is the update."
        )

        assert result.action == "work_reply_recorded"
        followup = _followup(session, "reply-template")
        assert followup.task_instruction.startswith("# Reply Processor\n\nParse the reply")
        assert "Here is the update." in followup.task_instruction


def test_reply_with_a_missing_processor_template_falls_back_to_the_default_processor(
    fresh_db: Path, tmp_path: Path
) -> None:
    with session_scope() as session:
        _thread_work(
            session,
            thread_id="thread-missing-template",
            lane="vetting",
            context={
                "reply_followup_work": {
                    "enabled": True,
                    "title": "Process vetting reply",
                    "task_template_path": "reply.template.md",
                    "template_base_dir": str(tmp_path / "gone"),
                    "runtime_contract": {"model_profile": "high"},
                }
            },
        )

        result = _reply(
            session, thread_id="thread-missing-template", message_id="reply-missing", content="Is this one worth it?"
        )

        assert result.action == "work_reply_recorded"
        followup = _followup(session, "reply-missing")
        assert followup.task_instruction.startswith(DEFAULT_REPLY_INSTRUCTION.strip()[:40])
        assert "Is this one worth it?" in followup.task_instruction
        assert followup.context["reply_default_processor"] is True
        assert followup.runtime_contract["model_profile"] == "high"
        assert followup.lane == "vetting"


def test_channel_reply_to_a_tasque_message_queues_a_followup(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Workout generator",
            task_instruction="Generate workout.",
            worker_kind="provider.fake",
            runtime_contract={"model_profile": "medium"},
            context={
                "memory_namespace": "health",
                "reply_memory": {"enabled": True, "namespace": "health", "kind": "working", "tags": ["workout"]},
                "reply_followup_work": {
                    "enabled": True,
                    "title": "Parse channel workout reply",
                    "runtime_contract": {"model_profile": "low"},
                    "task_instruction": "Process this workout reply.",
                },
            },
        )
        service = DiscordService(session)
        service.record_message(
            discord_message_id="out-channel-workout",
            discord_channel_id="output-channel",
            discord_thread_id=None,
            direction="outbound",
            author="tasque",
            content_preview="Work update: Workout generator\nSummary: Prescribed push.",
            work_item_id=work.id,
        )

        result = service.handle_channel_message(
            discord_message_id="reply-channel-workout",
            discord_channel_id="output-channel",
            author="user",
            content="I did it: bench 95x10 RPE 8.",
            referenced_discord_message_id="out-channel-workout",
        )

        assert result.action == "work_reply_recorded"
        assert session.scalar(select(Memory).where(Memory.source_id == "reply-channel-workout")) is not None
        followup = _followup(session, "reply-channel-workout")
        assert followup.runtime_contract == {"model_profile": "low"}
        assert followup.context["source_reply"]["referenced_discord_message_id"] == "out-channel-workout"
        conversation = followup.context["conversation"]
        assert conversation["scope"] == "channel"
        assert conversation["referenced_discord_message_id"] == "out-channel-workout"
        assert [message["discord_message_id"] for message in conversation["recent_messages"]] == [
            "out-channel-workout",
            "reply-channel-workout",
        ]


def test_workflow_thread_reply_goes_to_the_reply_processor_of_the_final_work(fresh_db: Path) -> None:
    with session_scope() as session:
        run = _single_step_workflow(
            session,
            name="art-course-chain",
            context={
                "memory_namespace": "creative",
                "reply_followup_work": {
                    "enabled": True,
                    "detach_from_workflow": True,
                    "title": "Critique art submission",
                    "task_instruction": "Critique this submitted art.",
                },
            },
        )
        workflows = WorkflowService(session)
        workflows.tick_runs()
        work = session.scalar(select(WorkItem).where(WorkItem.workflow_run_id == run.id))
        assert work is not None
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test")
        assert claimed is not None
        WorkQueue(session).complete_attempt(
            claimed.attempt.id, summary="Picked course.", produces={"selected_course_url": "https://example.com/course"}
        )
        workflows.tick_runs()
        DiscordService(session).bind_thread(
            purpose="workflow", discord_channel_id="jobs", discord_thread_id="thread-art", workflow_run_id=run.id
        )

        result = _reply(session, thread_id="thread-art", message_id="reply-art", content="Here is my finished piece.")

        assert result.action == "workflow_reply_followup_recorded"
        followup = _followup(session, "reply-art")
        assert result.entity_id == followup.id
        assert followup.title == "Critique art submission"
        assert followup.workflow_run_id is None
        assert followup.discord_thread_id == "thread-art"
        assert followup.context["parent_work_item_id"] == work.id
        assert "Here is my finished piece." in followup.task_instruction
        packet = WorkerContextBuilder(session).build_for_work(followup)
        assert packet["parent_work"]["latest_attempt"]["produces"] == {
            "selected_course_url": "https://example.com/course"
        }


def test_default_followup_to_a_workflow_thread_reply_stays_out_of_the_run(fresh_db: Path) -> None:
    with session_scope() as session:
        run = _single_step_workflow(session, name="digest-chain")
        workflows = WorkflowService(session)
        workflows.tick_runs()
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test")
        assert claimed is not None
        WorkQueue(session).complete_attempt(claimed.attempt.id, summary="Digest sent.")
        workflows.tick_runs()
        DiscordService(session).bind_thread(
            purpose="workflow", discord_channel_id="jobs", discord_thread_id="thread-digest", workflow_run_id=run.id
        )

        result = _reply(session, thread_id="thread-digest", message_id="reply-digest", content="Why skip the news?")

        assert result.action == "workflow_reply_followup_recorded"
        followup = _followup(session, "reply-digest")
        assert followup.context["reply_default_processor"] is True
        assert followup.context["parent_work_item_id"] == claimed.work_item.id
        assert followup.workflow_run_id is None
        assert followup.discord_thread_id == "thread-digest"
        assert followup.lane == "digest-chain"


def test_workflow_thread_reply_answers_the_single_open_gate(fresh_db: Path) -> None:
    definition = {"nodes": [{"key": "approve", "kind": "gate", "prompt": "Approve?"}]}
    with session_scope() as session:
        workflows = WorkflowService(session)
        run = workflows.start_run(
            workflow_definition_id=workflows.create_definition(name="gated", version="1", definition=definition).id
        )
        workflows.tick_runs()
        assert run.status == "awaiting_input"
        DiscordService(session).bind_thread(
            purpose="workflow", discord_channel_id="jobs", discord_thread_id="thread-gate", workflow_run_id=run.id
        )

        result = _reply(session, thread_id="thread-gate", message_id="reply-gate", content="yes")
        workflows.tick_runs()

        gate = session.scalar(select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id))
        assert gate is not None
        assert result.action == "workflow_gate_answered"
        assert result.entity_id == gate.id
        assert gate.output == {"answer": "yes"}
        assert session.get(WorkflowRun, run.id).status == "completed"
        assert session.scalar(select(WorkItem)) is None


def test_reply_memory_is_recorded_once_per_discord_message(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread_work(
            session,
            thread_id="thread-once",
            context={"memory_namespace": "health", "reply_memory": {"enabled": True, "namespace": "health"}},
        )

        for _ in range(2):
            _reply(session, thread_id="thread-once", message_id="reply-once", content="completed workout")

        memories = session.scalars(
            select(Memory).where(Memory.source_kind == "discord_reply", Memory.source_id == "reply-once")
        ).all()
        assert len(memories) == 1
        assert len(session.scalars(select(WorkItem).where(WorkItem.source_id == "reply-once")).all()) == 1


def test_binding_a_thread_twice_reuses_the_binding(fresh_db: Path) -> None:
    with session_scope() as session:
        service = DiscordService(session)
        first = service.bind_thread(
            purpose="work", discord_channel_id="parent", discord_thread_id="thread-3", work_item_id="work-1"
        )
        second = service.bind_thread(
            purpose="work", discord_channel_id="parent", discord_thread_id="thread-3", work_item_id="work-1"
        )

        assert first.id == second.id
        assert len(session.scalars(select(DiscordThread)).all()) == 1


def test_thread_reply_without_reply_config_queues_the_default_processor(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _thread_work(
            session,
            thread_id="thread-paycheck",
            title="Route the paycheck",
            runtime_contract={"model_profile": "medium"},
            context={
                "memory_namespace": "finance",
                "memory_canonical_keys": ["finance_plan", "finance_direction"],
                "memory_tags": ["finance"],
                "unrelated_key": "stays with the parent",
            },
        )

        result = _reply(
            session,
            thread_id="thread-paycheck",
            message_id="reply-paycheck",
            content="No, you should still invest part of the money.",
        )

        assert result.action == "work_reply_recorded"
        followup = _followup(session, "reply-paycheck")
        assert result.entity_id == followup.id
        assert followup.title == "Reply: Route the paycheck"
        assert followup.worker_kind == "provider.default"
        assert followup.task_instruction.startswith(DEFAULT_REPLY_INSTRUCTION.strip())
        assert "No, you should still invest part of the money." in followup.task_instruction
        assert followup.runtime_contract == {"model_profile": "medium"}
        assert followup.discord_thread_id == "thread-paycheck"
        assert followup.context["memory_namespace"] == "finance"
        assert followup.context["memory_canonical_keys"] == ["finance_plan", "finance_direction"]
        assert followup.context["memory_tags"] == ["finance"]
        assert "unrelated_key" not in followup.context
        assert followup.context["reply_default_processor"] is True
        assert followup.context["parent_work_item_id"] == work.id
        assert followup.context["source_reply"]["discord_message_id"] == "reply-paycheck"


def test_disabled_reply_config_queues_no_followup(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _thread_work(session, thread_id="thread-silent", context={"reply_followup_work": {"enabled": False}})

        result = _reply(session, thread_id="thread-silent", message_id="reply-silent", content="Any update?")

        assert result.action == "work_reply_recorded"
        assert result.entity_id == work.id
        assert session.scalar(select(WorkItem).where(WorkItem.source_kind == "discord_reply_followup")) is None


def test_reply_followup_carries_the_parents_reply_config_forward(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread_work(
            session,
            thread_id="thread-finance",
            context={
                "memory_namespace": "finance",
                "reply_memory": {"enabled": True, "namespace": "finance", "kind": "working"},
                "reply_followup_work": {
                    "enabled": True,
                    "title": "Finance manager reply",
                    "task_instruction": "Process the finance reply.",
                },
            },
        )

        _reply(session, thread_id="thread-finance", message_id="reply-finance", content="Please re-route the money.")

        followup = _followup(session, "reply-finance")
        assert followup.context["reply_followup_work"]["title"] == "Finance manager reply"
        assert followup.context["reply_memory"]["namespace"] == "finance"


def test_reply_followup_inherits_the_owning_work_items_lane(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread_work(session, thread_id="thread-default", lane="finance-daily")
        _thread_work(
            session,
            thread_id="thread-processor",
            lane="kitchen",
            context={"reply_followup_work": {"enabled": True, "title": "Kitchen reply", "task_instruction": "Cook."}},
        )

        _reply(session, thread_id="thread-default", message_id="reply-default", content="Skip the transfer.")
        _reply(session, thread_id="thread-processor", message_id="reply-processor", content="No mushrooms.")

        assert _followup(session, "reply-default").lane == "finance-daily"
        assert _followup(session, "reply-processor").lane == "kitchen"


def test_reply_followup_inherits_the_parents_model_and_tool_limits(fresh_db: Path) -> None:
    inherited = {
        "model_profile": "high",
        "model": "claude-opus-5-5",
        "effort": "max",
        "mcp_servers": ["google-workspace", "autopilot"],
        "disallowed_tools": ["mcp__autopilot__fill_login"],
        "max_turns": 40,
        "max_budget_usd": 2.5,
    }
    parent_only = {"cwd": "parent-only", "env": {"PARENT_ONLY": "1"}, "argv": ["parent-only"]}
    with session_scope() as session:
        _thread_work(session, thread_id="thread-contract", runtime_contract={**inherited, **parent_only})

        _reply(session, thread_id="thread-contract", message_id="reply-contract", content="Try again.")

        assert _followup(session, "reply-contract").runtime_contract == inherited


def test_reply_config_model_keys_replace_all_of_the_parents_model_keys() -> None:
    parent = {"model_profile": "high", "model": "claude-opus-5-5", "effort": "max", "max_turns": 10}

    assert followup_contract(parent, {"effort": "low"}) == {"effort": "low", "max_turns": 10}
    assert followup_contract(parent, {"model_profile": "low"}) == {"model_profile": "low", "max_turns": 10}


def test_followup_contract_passes_on_tool_limits_and_the_reply_config_overrides_them() -> None:
    parent = {
        "model_profile": "medium",
        "mcp_servers": ["google-workspace", "autopilot"],
        "disallowed_tools": ["mcp__autopilot__fill_login"],
        "cwd": "parent-only",
    }

    assert followup_contract(parent, {}) == {
        "model_profile": "medium",
        "mcp_servers": ["google-workspace", "autopilot"],
        "disallowed_tools": ["mcp__autopilot__fill_login"],
    }
    assert followup_contract(parent, {"disallowed_tools": []})["disallowed_tools"] == []


def test_unbound_thread_reply_is_recorded_without_queueing_work(fresh_db: Path) -> None:
    with session_scope() as session:
        result = _reply(session, thread_id="thread-unknown", message_id="reply-unknown", content="Hello?")

        message = session.scalar(select(DiscordMessage).where(DiscordMessage.discord_message_id == "reply-unknown"))
        assert result.action == "unbound_thread"
        assert message is not None
        assert message.discord_thread_id == "thread-unknown"
        assert message.work_item_id is None
        assert session.scalar(select(WorkItem)) is None


def test_a_reply_answers_the_newest_post_in_the_thread_but_runs_as_the_owner_says(fresh_db: Path) -> None:
    with session_scope() as session:
        opener = _thread_work(
            session,
            thread_id="thread-daybook",
            title="Daybook opener",
            lane="daybook",
            context={"reply_followup_work": {"title": "Daybook reply", "runtime_contract": {"model_profile": "low"}}},
        )
        discord = DiscordService(session)
        posts = {}
        for title in ("Monday brief", "Tuesday brief"):
            post = WorkRepository(session).create_work_item(
                title=title, task_instruction="Brief.", worker_kind="manual", discord_thread_id="thread-daybook"
            )
            discord.record_message(
                discord_message_id=f"post-{title}",
                discord_channel_id="jobs",
                discord_thread_id="thread-daybook",
                direction="outbound",
                author="tasque",
                content_preview=title,
                work_item_id=post.id,
            )
            posts[title] = post

        _reply(session, thread_id="thread-daybook", message_id="reply-latest", content="Move lunch to 1.")
        quoted = discord.handle_thread_reply(
            discord_message_id="reply-quoted",
            discord_channel_id="thread-daybook",
            discord_thread_id="thread-daybook",
            author="user",
            content="That was wrong on Monday.",
            referenced_discord_message_id="post-Monday brief",
        )

        latest = _followup(session, "reply-latest")
        assert latest.context["parent_work_item_id"] == posts["Tuesday brief"].id
        assert latest.title == "Daybook reply"
        assert latest.runtime_contract["model_profile"] == "low"
        assert latest.lane == opener.lane
        assert session.get(WorkItem, quoted.entity_id).context["parent_work_item_id"] == posts["Monday brief"].id


def test_reply_in_an_archived_lane_thread_is_recorded_without_queueing_work(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread_work(session, thread_id="thread-shelved", context={"reply_followup_work": {"title": "Old lane"}})
        binding = session.scalar(select(DiscordThread).where(DiscordThread.discord_thread_id == "thread-shelved"))
        binding.status = "archived"
        session.flush()

        result = _reply(session, thread_id="thread-shelved", message_id="reply-shelved", content="Still there?")

        assert result.action == "archived_thread"
        assert session.scalar(select(WorkItem).where(WorkItem.source_id == "reply-shelved")) is None
        assert session.scalar(select(DiscordMessage).where(DiscordMessage.discord_message_id == "reply-shelved"))


def test_thread_reply_conversation_window_counts_logical_messages(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _thread_work(
            session,
            thread_id="thread-cooking",
            context={
                "memory_namespace": "cooking",
                "reply_followup_work": {"enabled": True, "title": "Cooking chef reply"},
            },
        )
        service = DiscordService(session)
        service.record_message(
            discord_message_id="in-old",
            discord_channel_id="thread-cooking",
            discord_thread_id="thread-cooking",
            direction="inbound",
            author="user",
            content_preview="what should I cook",
        )
        for chunk in range(30):
            service.record_message(
                discord_message_id=f"out-{chunk}",
                discord_channel_id="thread-cooking",
                discord_thread_id="thread-cooking",
                direction="outbound",
                author="tasque",
                content_preview=f"chunk {chunk}",
                work_item_id=work.id,
            )

        _reply(session, thread_id="thread-cooking", message_id="reply-cooking", content="shorter please")

        recent = _followup(session, "reply-cooking").context["conversation"]["recent_messages"]
        assert [message["discord_message_id"] for message in recent] == ["in-old", "out-0", "reply-cooking"]
        assert recent[1]["parts"] == 30
        assert recent[1]["last_discord_message_id"] == "out-29"


def _conversation_row(message_id: str, *, direction: str, content: str, work_item_id: str | None = None) -> dict:
    return {
        "discord_message_id": message_id,
        "direction": direction,
        "author": "tasque" if direction == "outbound" else "user",
        "content": content,
        "work_item_id": work_item_id,
        "created_at": f"2026-07-25T00:00:{int(message_id.split('-')[-1]):02d}",
    }


def test_collapse_conversation_folds_a_chunked_reply_into_one_message() -> None:
    rows = [
        _conversation_row("in-1", direction="inbound", content="what should I cook"),
        *[
            _conversation_row(f"out-{index}", direction="outbound", content=f"part {index}", work_item_id="w1")
            for index in range(2, 8)
        ],
        _conversation_row("in-8", direction="inbound", content="just the recipe please"),
        _conversation_row("out-9", direction="outbound", content="here it is", work_item_id="w2"),
    ]

    collapsed = collapse_conversation(rows, limit=20)

    assert [message["discord_message_id"] for message in collapsed] == ["in-1", "out-2", "in-8", "out-9"]
    assert collapsed[1]["parts"] == 6
    assert collapsed[1]["last_discord_message_id"] == "out-7"
    assert collapsed[1]["created_at"] == rows[6]["created_at"]
    assert collapsed[1]["content"] == "\n\n".join(f"part {index}" for index in range(2, 8))
    assert collapsed[0]["parts"] == 1


def test_collapse_conversation_does_not_join_outbound_rows_of_different_work() -> None:
    rows = [
        _conversation_row("out-1", direction="outbound", content="first answer", work_item_id="w1"),
        _conversation_row("out-2", direction="outbound", content="second answer", work_item_id="w2"),
        _conversation_row("out-3", direction="outbound", content="status post"),
        _conversation_row("out-4", direction="outbound", content="another status post"),
    ]

    collapsed = collapse_conversation(rows)

    assert [message["discord_message_id"] for message in collapsed] == ["out-1", "out-2", "out-3", "out-4"]
    assert all(message["parts"] == 1 for message in collapsed)


def test_collapse_conversation_keeps_user_turns_when_the_worker_is_verbose() -> None:
    rows = []
    for turn in range(1, 4):
        rows.append(_conversation_row(f"in-{turn}0", direction="inbound", content=f"ask {turn}"))
        rows.extend(
            _conversation_row(f"out-{turn}{chunk}", direction="outbound", content="x" * 1900, work_item_id=f"w{turn}")
            for chunk in range(1, 9)
        )

    collapsed = collapse_conversation(rows, limit=4)

    assert [message["direction"] for message in collapsed] == ["inbound", "outbound", "inbound", "outbound"]
    assert [message["content"] for message in collapsed if message["direction"] == "inbound"] == ["ask 2", "ask 3"]


def test_collapse_conversation_truncates_long_messages_and_holds_a_budget() -> None:
    rows = [
        _conversation_row(f"out-{index}", direction="outbound", content="y" * 5_000, work_item_id=f"w{index}")
        for index in range(1, 6)
    ]

    collapsed = collapse_conversation(rows, limit=5, max_chars_per_message=1_000, total_max_chars=2_500)

    assert [message["discord_message_id"] for message in collapsed] == ["out-4", "out-5"]
    assert all(message["content"].endswith("[trimmed 4000 chars]") for message in collapsed)
    assert sum(len(message["content"]) for message in collapsed) <= 2_500


def test_collapse_conversation_always_keeps_the_newest_message() -> None:
    rows = [
        _conversation_row("in-1", direction="inbound", content="short"),
        _conversation_row("in-2", direction="inbound", content="z" * 500),
    ]

    assert [message["discord_message_id"] for message in collapse_conversation(rows, total_max_chars=100)] == ["in-2"]
    assert collapse_conversation(rows, limit=0) == []
