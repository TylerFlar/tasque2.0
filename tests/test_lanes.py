from __future__ import annotations

import json
from pathlib import Path

import pytest

from tasque2.db import session_scope
from tasque2.discord.routing import DiscordService
from tasque2.models import Schedule, WorkflowDefinition, WorkItem
from tasque2.ops.lanes import LaneManifestError, apply_lane_tiers
from tasque2.schedules import ScheduleService
from tasque2.work.repository import WorkRepository
from tasque2.workflows import WorkflowService

LANE_FILE = "data/work-templates/finance/context.json"


def _lane_file(root: Path, data: dict) -> None:
    path = root / LANE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _schedule(session, **payload_context) -> Schedule:
    return ScheduleService(session).create_schedule(
        name="finance-daily",
        schedule_type="cron",
        expression="30 7 * * *",
        worker_kind="provider.default",
        payload={"title": "finance-daily", "task_instruction": "Run.", "context": payload_context},
        runtime_contract={"model_profile": "medium"},
    )


def _thread_owner(session, thread_id: str, context: dict) -> WorkItem:
    owner = WorkRepository(session).create_work_item(
        title="Finance opener", task_instruction="Open.", worker_kind="manual", context=context
    )
    DiscordService(session).bind_thread(
        purpose="work", discord_channel_id="jobs", discord_thread_id=thread_id, work_item_id=owner.id
    )
    return owner


def test_manifest_sets_schedule_workflow_and_thread_tiers(fresh_db: Path) -> None:
    with session_scope() as session:
        schedule = _schedule(session, reply_followup_work={"title": "Reply"})
        definition = WorkflowService(session).create_definition(
            name="gmail",
            version="1",
            definition={
                "nodes": [{"key": "report", "title": "Report", "task_instruction": "Sum.", "worker_kind": "manual"}]
            },
        )
        owner = _thread_owner(session, "thread-1", {"reply_followup_work": {"title": "Reply"}})
        manifest = {
            "schedules": {"finance-daily": {"profile": "high", "reply_profile": "high"}},
            "workflows": {"gmail": {"report": "low"}},
            "threads": {"thread-1": {"reply_profile": "medium"}},
        }

        preview = apply_lane_tiers(session, manifest, dry_run=True)
        assert schedule.runtime_contract == {"model_profile": "medium"}
        changes = apply_lane_tiers(session, manifest)
        again = apply_lane_tiers(session, manifest)

        assert schedule.runtime_contract["model_profile"] == "high"
        assert schedule.payload["context"]["reply_followup_work"]["runtime_contract"]["model_profile"] == "high"
        assert definition.definition["nodes"][0]["runtime_contract"]["model_profile"] == "low"
        assert owner.context["reply_followup_work"]["runtime_contract"]["model_profile"] == "medium"

    assert [change.changed for change in preview] == [change.changed for change in changes] == [True] * 4
    assert not any(change.changed for change in again)


def test_manifest_binds_schedules_and_threads_to_their_lane_file(fresh_db: Path, isolated: Path) -> None:
    _lane_file(isolated, {"memory_namespace": "finance", "reply_followup_work": {"title": "Finance reply"}})
    with session_scope() as session:
        schedule = _schedule(session, memory_namespace="stale", reply_followup_work={"title": "Old"}, keep="me")
        owner = _thread_owner(
            session, "thread-1", {"memory_namespace": "stale", "reply_followup_work": {"title": "Old"}}
        )
        manifest = {
            "schedules": {"finance-daily": {"context_file": LANE_FILE}},
            "threads": {"thread-1": {"context_file": LANE_FILE}},
        }

        changes = apply_lane_tiers(session, manifest)

        assert schedule.payload["context"] == {"context_file": LANE_FILE, "keep": "me"}
        assert owner.context == {"context_file": LANE_FILE}
        with pytest.raises(LaneManifestError, match="lane file owns the reply tier"):
            apply_lane_tiers(session, {"threads": {"thread-1": {"reply_profile": "high"}}})

    assert [(change.field, change.new) for change in changes] == [("context", LANE_FILE), ("context", LANE_FILE)]


def test_manifest_binds_a_workflow_thread_through_the_work_its_replies_go_to(fresh_db: Path, isolated: Path) -> None:
    _lane_file(isolated, {"reply_followup_work": {"title": "Coach reply"}})
    with session_scope() as session:
        workflows = WorkflowService(session)
        definition = workflows.create_definition(
            name="coach",
            version="1",
            definition={"nodes": [{"key": "coach", "task_instruction": "Coach.", "worker_kind": "manual"}]},
        )
        run = workflows.start_run(workflow_definition_id=definition.id)
        workflows.tick_runs()
        DiscordService(session).bind_thread(
            purpose="workflow", discord_channel_id="jobs", discord_thread_id="thread-coach", workflow_run_id=run.id
        )
        [coach] = session.query(WorkItem).filter(WorkItem.workflow_run_id == run.id).all()

        apply_lane_tiers(session, {"threads": {"thread-coach": {"context_file": LANE_FILE}}})

        assert coach.context == {"context_file": LANE_FILE}
        assert DiscordService(session).workflow_reply_owner(run.id).id == coach.id


def test_manifest_switches_schedules_on_and_off(fresh_db: Path) -> None:
    with session_scope() as session:
        schedule = _schedule(session)

        apply_lane_tiers(session, {"schedules": {"finance-daily": {"enabled": False}}})
        assert schedule.enabled is False
        apply_lane_tiers(session, {"schedules": {"finance-daily": {"enabled": True}}})
        assert schedule.enabled is True


def test_manifest_sets_the_servers_and_tool_bans_a_schedule_runs_with(fresh_db: Path) -> None:
    with session_scope() as session:
        schedule = _schedule(session)
        manifest = {
            "schedules": {
                "finance-daily": {
                    "profile": "high",
                    "mcp_servers": ["autopilot", "google-workspace"],
                    "disallowed_tools": ["mcp__tasque2__memory_delete"],
                }
            }
        }

        changes = apply_lane_tiers(session, manifest)
        again = apply_lane_tiers(session, manifest)
        emptied = apply_lane_tiers(session, {"schedules": {"finance-daily": {"mcp_servers": []}}})

        assert schedule.runtime_contract == {
            "model_profile": "high",
            "mcp_servers": [],
            "disallowed_tools": ["mcp__tasque2__memory_delete"],
        }
        with pytest.raises(LaneManifestError, match="mcp_servers must be a list"):
            apply_lane_tiers(session, {"schedules": {"finance-daily": {"mcp_servers": "autopilot"}}})

    assert [(change.field, change.new) for change in changes] == [
        ("run", "high"),
        ("mcp_servers", "autopilot, google-workspace"),
        ("disallowed_tools", "mcp__tasque2__memory_delete"),
    ]
    assert not any(change.changed for change in again)
    # an empty list means no servers, which reads differently from the configured default
    assert [(change.old, change.new) for change in emptied if change.field == "mcp_servers"] == [
        ("autopilot, google-workspace", "none")
    ]


def test_manifest_errors_name_what_is_wrong(fresh_db: Path) -> None:
    with session_scope() as session:
        _schedule(session)
        WorkflowService(session).create_definition(
            name="gmail",
            version="1",
            definition={"nodes": [{"key": "report", "task_instruction": "x", "worker_kind": "manual"}]},
        )

        with pytest.raises(LaneManifestError, match="Unknown schedule"):
            apply_lane_tiers(session, {"schedules": {"nope": {"profile": "low"}}})
        with pytest.raises(LaneManifestError, match="has no node 'merge'"):
            apply_lane_tiers(session, {"workflows": {"gmail": {"merge": "low"}}})
        with pytest.raises(LaneManifestError, match="profile must be one of"):
            apply_lane_tiers(session, {"schedules": {"finance-daily": {"profile": "huge"}}})
        with pytest.raises(LaneManifestError, match="lane file .* is unusable"):
            apply_lane_tiers(session, {"schedules": {"finance-daily": {"context_file": "data/missing.json"}}})
        assert session.query(WorkflowDefinition).count() == 1


def test_keep_stored_leaves_the_lane_file_as_the_whole_context(fresh_db: Path, isolated: Path) -> None:
    _lane_file(isolated, {"memory_namespace": "finance", "reply_followup_work": {"title": "Finance reply"}})
    with session_scope() as session:
        schedule = _schedule(session, memory_namespace="stale", finance_template_path="old.md", lanes=["finance"])
        owner = _thread_owner(session, "thread-1", {"stale_flag": True, "reply_followup_work": {"title": "Old"}})

        apply_lane_tiers(
            session,
            {
                "schedules": {"finance-daily": {"context_file": LANE_FILE, "keep_stored": ["lanes"]}},
                "threads": {"thread-1": {"context_file": LANE_FILE, "keep_stored": []}},
            },
        )

        assert schedule.payload["context"] == {"context_file": LANE_FILE, "lanes": ["finance"]}
        assert owner.context == {"context_file": LANE_FILE}
        with pytest.raises(LaneManifestError, match="keep_stored must be a list"):
            apply_lane_tiers(session, {"threads": {"thread-1": {"context_file": LANE_FILE, "keep_stored": "all"}}})
