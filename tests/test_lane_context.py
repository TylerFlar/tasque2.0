from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from tasque2.db import session_scope
from tasque2.discord.routing import DiscordService
from tasque2.lane_context import effective_context
from tasque2.models import WorkItem
from tasque2.work.repository import WorkRepository


def _lane_file(directory: Path, data: dict) -> Path:
    path = directory / "data" / "work-templates" / "finance" / "context.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_context_without_a_lane_file_is_returned_as_stored() -> None:
    stored = {"memory_namespace": "finance", "account": "checking"}

    assert effective_context(stored) == stored
    assert effective_context(None) == {}


def test_lane_file_keys_override_the_stored_copy_and_stored_keys_fill_the_rest(isolated: Path) -> None:
    _lane_file(isolated, {"memory_canonical_keys": ["finance_direction"], "finance_timezone": "UTC"})
    stored = {
        "context_file": "data/work-templates/finance/context.json",
        "memory_canonical_keys": ["finance_direction", "finance_log", "finance_access_playbook"],
        "schedule_occurrence_id": "occurrence-1",
    }

    context = effective_context(stored)

    assert context == {
        "context_file": "data/work-templates/finance/context.json",
        "memory_canonical_keys": ["finance_direction"],
        "finance_timezone": "UTC",
        "schedule_occurrence_id": "occurrence-1",
    }


def test_an_unreadable_lane_file_leaves_the_stored_context_in_force(
    isolated: Path, caplog: pytest.LogCaptureFixture
) -> None:
    stored = {"context_file": "data/work-templates/missing/context.json", "memory_namespace": "finance"}

    with caplog.at_level(logging.WARNING, logger="tasque2.lane_context"):
        context = effective_context(stored)

    assert context == stored
    assert "is unavailable" in caplog.text


def test_new_work_items_take_their_lane_configuration_from_the_file(fresh_db: Path, isolated: Path) -> None:
    _lane_file(isolated, {"memory_namespace": "finance", "lane": "finance"})
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Finance daily",
            task_instruction="Run the daily pass.",
            worker_kind="manual",
            context={"context_file": "data/work-templates/finance/context.json", "memory_namespace": "stale"},
        )

        assert work.context["memory_namespace"] == "finance"
        assert work.lane == "finance"


def test_replies_follow_the_thread_owners_current_lane_file(fresh_db: Path, isolated: Path) -> None:
    reply_template = isolated / "data" / "work-templates" / "finance" / "reply.template.md"
    reply_template.parent.mkdir(parents=True, exist_ok=True)
    reply_template.write_text("# Finance reply\n\nAnswer from the board.", encoding="utf-8")
    with session_scope() as session:
        owner = WorkRepository(session).create_work_item(
            title="Finance opener",
            task_instruction="Open the finance thread.",
            worker_kind="manual",
            context={
                "memory_namespace": "finance",
                "reply_followup_work": {"title": "Old reply", "task_instruction": "Old processor."},
            },
            discord_thread_id="thread-finance",
        )
        DiscordService(session).bind_thread(
            purpose="work", discord_channel_id="jobs", discord_thread_id="thread-finance", work_item_id=owner.id
        )
        owner.context = {**owner.context, "context_file": "data/work-templates/finance/context.json"}
        _lane_file(
            isolated,
            {
                "memory_namespace": "finance",
                "reply_followup_work": {
                    "title": "Finance reply",
                    "task_template_path": "reply.template.md",
                    "template_base_dir": str(reply_template.parent),
                    "runtime_contract": {"model_profile": "high"},
                },
            },
        )

        result = DiscordService(session).handle_thread_reply(
            discord_message_id="reply-1",
            discord_channel_id="thread-finance",
            discord_thread_id="thread-finance",
            author="user",
            content="Did the rent go out?",
        )
        followup = session.get(WorkItem, result.entity_id)

        assert followup.title == "Finance reply"
        assert followup.task_instruction.startswith("# Finance reply")
        assert followup.runtime_contract["model_profile"] == "high"
