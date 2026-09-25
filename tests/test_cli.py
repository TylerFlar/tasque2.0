from __future__ import annotations

import json
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from click.testing import Result
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import func, select
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.config import reset_settings
from tasque2.daemon import control
from tasque2.db import session_scope
from tasque2.discord.routing import DiscordService
from tasque2.memory import MemoryService
from tasque2.models import (
    Artifact,
    Base,
    Memory,
    ProviderRun,
    Schedule,
    WorkAttempt,
    WorkDependency,
    WorkflowDefinition,
    WorkflowRun,
    WorkItem,
    utc_now,
)
from tasque2.sticky import StickyService
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner

runner = CliRunner(env={"COLUMNS": "300"})

ECHO_NODES = [
    {"key": "step", "title": "Echo step", "task_instruction": "Echo.", "worker_kind": "function.echo"},
    {"key": "done", "kind": "join", "depends_on": ["step"]},
]


def cli(*args: object) -> Result:
    result = runner.invoke(app, [str(arg) for arg in args])
    assert result.exit_code == 0, f"{result.output}\n{result.exception!r}"
    return result


def cli_error(*args: object, code: int = 1) -> Result:
    result = runner.invoke(app, [str(arg) for arg in args])
    assert result.exit_code == code, f"{result.output}\n{result.exception!r}"
    assert not isinstance(result.exception, (KeyError, ValueError, TypeError)), result.exception
    return result


def created_id(result: Result) -> str:
    return result.stdout.strip().splitlines()[-1]


def workflow_file(directory: Path, name: str = "cli-flow", nodes: list[dict[str, Any]] | None = None) -> Path:
    path = directory / f"{name}.workflow.json"
    path.write_text(
        json.dumps({"name": name, "version": "1", "definition": {"nodes": nodes or ECHO_NODES}}), encoding="utf-8"
    )
    return path


def work_item(work_id: str) -> WorkItem:
    with session_scope() as session:
        work = session.get(WorkItem, work_id)
        assert work is not None
        return work


def schedule_row(schedule_id: str) -> Schedule:
    with session_scope() as session:
        schedule = session.get(Schedule, schedule_id)
        assert schedule is not None
        return schedule


def database_rows() -> dict[str, list[tuple[Any, ...]]]:
    with session_scope() as session:
        return {
            table.name: sorted((tuple(row) for row in session.execute(select(table)).all()), key=repr)
            for table in Base.metadata.sorted_tables
        }


def write_foreign_daemon_state() -> None:
    now = utc_now().isoformat()
    state = {
        "pid": os.getpid() + 1,
        "started_at": now,
        "last_tick_at": now,
        "in_flight_attempt_ids": [],
        "draining": False,
        "version": "test",
    }
    control.state_path().parent.mkdir(parents=True, exist_ok=True)
    control.state_path().write_text(json.dumps(state), encoding="utf-8")


def add_provider_run(*, lane: str, title: str, usage: dict[str, Any], status: str = "succeeded") -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title=title, task_instruction="Run.", worker_kind="provider.claude", lane=lane
        )
        attempt = WorkAttempt(work_item_id=work.id, attempt_number=1, status=status, worker_kind=work.worker_kind)
        session.add(attempt)
        session.flush()
        ended = utc_now()
        session.add(
            ProviderRun(
                attempt_id=attempt.id,
                provider="claude",
                status=status,
                usage=usage,
                started_at=ended - timedelta(minutes=3),
                ended_at=ended,
            )
        )


@pytest.fixture(autouse=True)
def hashed_embeddings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_EMBEDDING_PROVIDER", "hash")


def test_queue_records_lane_profile_context_and_thread(fresh_db: Path, spans: InMemorySpanExporter) -> None:
    result = cli(
        "queue",
        "Write report",
        "Summarize the week.",
        "--lane",
        "reports",
        "--profile",
        "HIGH",
        "--context-json",
        '{"topic": "week"}',
        "--thread",
        "thread-9",
        "--priority",
        "3",
        "--max-attempts",
        "2",
        "--idempotency-key",
        "report-week-1",
    )

    work = work_item(created_id(result))
    assert work.title == "Write report"
    assert work.task_instruction == "Summarize the week."
    assert work.worker_kind == "provider.default"
    assert work.lane == "reports"
    assert work.runtime_contract == {"model_profile": "high"}
    assert work.context == {"topic": "week"}
    assert work.discord_thread_id == "thread-9"
    assert work.priority == 3
    assert work.max_attempts == 2
    assert (work.source_kind, work.source_id) == ("cli", "report-week-1")
    (command_span,) = [item for item in spans.get_finished_spans() if item.name == "tasque.cli queue"]
    assert work.traceparent.split("-")[1] == format(command_span.context.trace_id, "032x")


def test_queue_reads_the_instruction_from_a_template(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "digest.md"
    template.write_text("# Digest\n\nCollect the week's notes.\n", encoding="utf-8")

    work = work_item(created_id(cli("queue", "Digest", "--template", template, "--worker-kind", "function.echo")))

    assert work.task_instruction == "# Digest\n\nCollect the week's notes."
    assert work.worker_kind == "function.echo"


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["queue", "Both", "Do it.", "--template", "missing.md"], "exactly one of"),
        (["queue", "Neither"], "exactly one of"),
        (["queue", "Missing", "--template", "missing.md"], "file does not exist"),
        (["queue", "Bad context", "Do it.", "--context-json", "[1, 2]"], "must be a JSON object"),
        (["queue", "Bad profile", "Do it.", "--profile", "turbo"], "model_profile must be one of"),
    ],
)
def test_queue_rejects_bad_input(fresh_db: Path, args: list[str], message: str) -> None:
    result = cli_error(*args, code=2)

    assert message in result.output
    with session_scope() as session:
        assert session.scalar(select(func.count()).select_from(WorkItem)) == 0


def test_list_filters_by_lane_and_status_and_shows_titles_verbatim(fresh_db: Path) -> None:
    finance = created_id(cli("queue", "[finance] daily", "Check.", "--lane", "finance"))
    kitchen = created_id(cli("queue", "[/x] odd title", "Cook.", "--lane", "kitchen"))

    everything = cli("list").output
    finance_only = cli("list", "--lane", "finance").output
    succeeded = cli("list", "--status", "succeeded").output

    assert finance in everything and kitchen in everything
    assert "[finance] daily" in everything
    assert "[/x] odd title" in everything
    assert finance in finance_only and kitchen not in finance_only
    assert finance not in succeeded and kitchen not in succeeded


def test_show_explains_state_dependencies_last_run_and_events(fresh_db: Path) -> None:
    with session_scope() as session:
        repo = WorkRepository(session)
        upstream = repo.create_work_item(title="Upstream", task_instruction="First.", worker_kind="manual")
        work = repo.create_work_item(
            title="[finance] Summary",
            task_instruction="Second.",
            worker_kind="provider.claude",
            lane="finance",
            runtime_contract={"model_profile": "high"},
        )
        session.add(WorkDependency(blocked_work_item_id=work.id, dependency_work_item_id=upstream.id))
        attempt = WorkAttempt(
            work_item_id=work.id,
            attempt_number=1,
            status="failed",
            worker_kind=work.worker_kind,
            error_message="Tool failed with [errno 2] missing file",
        )
        session.add(attempt)
        session.flush()
        session.add(
            ProviderRun(
                attempt_id=attempt.id,
                provider="claude",
                model="claude-opus-5-5",
                status="failed",
                usage={"input_tokens": 12, "cache_read_tokens": 300, "output_tokens": 45, "estimated_cost_usd": 1.5},
            )
        )
        work_id, upstream_id = work.id, upstream.id

    output = cli("show", work_id).output

    assert "[finance] Summary" in output
    assert f"id: {work_id}" in output
    assert "status: ready" in output
    assert "lane: finance" in output
    assert "contract: {'model_profile': 'high'}" in output
    assert f"waits for {upstream_id} to be succeeded (now ready)" in output
    assert "last attempt: #1 failed" in output
    assert "last error: Tool failed with [errno 2] missing file" in output
    assert "provider: claude claude-opus-5-5 input=12 cache_read=300 output=45 cost=$1.50" in output
    assert "work.created" in output


def test_show_rejects_an_unknown_work_item(fresh_db: Path) -> None:
    assert "Unknown work item: nope" in cli_error("show", "nope").output


def test_events_filter_by_work_item_and_type(fresh_db: Path) -> None:
    first = created_id(cli("queue", "First", "One."))
    second = created_id(cli("queue", "Second", "Two."))
    cli("cancel", second)

    for_first = cli("events", "--work-item-id", first).output
    canceled = cli("events", "--type", "work.canceled").output

    assert f"work_item:{first}" in for_first
    assert second not in for_first
    assert f"work_item:{second}" in canceled
    assert first not in canceled


def test_cancel_and_retry_move_work_between_states(fresh_db: Path) -> None:
    waiting = created_id(cli("queue", "Waiting", "Wait."))
    with session_scope() as session:
        broken = WorkRepository(session).create_work_item(
            title="Broken", task_instruction="Fail.", worker_kind="function.missing", priority=10
        )
        WorkRunner(session).run_next()
        broken_id = broken.id

    assert f"{waiting}: canceled" in cli("cancel", waiting).output
    assert f"{broken_id}: ready" in cli("retry", broken_id).output
    assert work_item(waiting).status == "canceled"
    assert work_item(broken_id).status == "ready"
    assert "Unknown work item: nope" in cli_error("cancel", "nope").output


def test_run_next_runs_one_ready_item(fresh_db: Path) -> None:
    work_id = created_id(cli("queue", "Echo", "[echo] hello", "--worker-kind", "function.echo"))

    output = cli("run-next").output

    assert f"succeeded: {work_id}" in output
    assert "[echo] hello" in output
    assert "No ready work." in cli("run-next").output

    waiting = created_id(cli("queue", "Echo again", "Again.", "--worker-kind", "function.echo"))
    write_foreign_daemon_state()
    assert "A daemon is alive" in cli_error("run-next").output
    assert work_item(waiting).status == "ready"
    assert f"succeeded: {waiting}" in cli("run-next", "--force").output


def test_report_work_renders_text_and_complete_json(fresh_db: Path) -> None:
    long_instruction = "Report on everything that happened this week. " * 6
    work_id = created_id(cli("queue", "CLI report", long_instruction, "--worker-kind", "function.echo"))
    cli("run-next")

    text = cli("report-work", work_id).output
    narrow = CliRunner(env={"COLUMNS": "40"}).invoke(app, ["report-work", work_id, "--json"])

    assert "# Work Report: CLI report" in text
    assert "- attempt 1: succeeded" in text
    assert narrow.exit_code == 0, narrow.output
    data = json.loads(narrow.stdout)
    assert data["work_item"]["id"] == work_id
    assert data["attempts"][0]["summary"] == long_instruction
    assert "Unknown work item: nope" in cli_error("report-work", "nope").output


def test_schedule_create_with_a_task(fresh_db: Path) -> None:
    schedule_id = created_id(
        cli(
            "schedule-create",
            "Nightly digest",
            "--type",
            "cron",
            "--expr",
            "0 9 * * *",
            "--task",
            "Digest the day.",
            "--lane",
            "digest",
            "--profile",
            "low",
            "--context-json",
            '{"depth": 2}',
            "--thread",
            "thread-1",
            "--timezone",
            "UTC",
        )
    )

    schedule = schedule_row(schedule_id)
    assert schedule.name == "Nightly digest"
    assert schedule.enabled
    assert (schedule.schedule_type, schedule.expression, schedule.timezone) == ("cron", "0 9 * * *", "UTC")
    assert schedule.worker_kind == "provider.default"
    assert schedule.runtime_contract == {"model_profile": "low"}
    assert schedule.payload == {
        "title": "Nightly digest",
        "task_instruction": "Digest the day.",
        "lane": "digest",
        "discord_thread_id": "thread-1",
        "context": {"depth": 2},
    }


def test_schedule_create_with_a_template(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "weekly.md"
    template.write_text("Plan the week.\n", encoding="utf-8")

    schedule_id = created_id(
        cli("schedule-create", "Weekly plan", "--type", "interval", "--expr", "days=7", "-t", template, "--disabled")
    )

    schedule = schedule_row(schedule_id)
    assert not schedule.enabled
    assert schedule.payload == {"title": "Weekly plan", "task_template_path": str(template.resolve())}


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--type", "weekly", "--expr", "0 9 * * *"], "schedule_type must be one of"),
        (["--type", "cron", "--expr", "not a cron"], "Invalid cron expression: 'not a cron'"),
        (["--type", "interval", "--expr", "6h"], "Interval expression must look like"),
        (["--type", "date", "--expr", "tomorrow"], "Invalid isoformat string"),
        (["--type", "cron", "--expr", "0 9 * * *", "--timezone", "Mars/Olympus"], "No time zone found"),
        (["--type", "cron", "--expr", "0 9 * * *", "--catchup-policy", "maybe"], "catchup_policy must be one of"),
    ],
)
def test_schedule_create_rejects_invalid_timing(fresh_db: Path, args: list[str], message: str) -> None:
    result = cli_error("schedule-create", "Broken", "--task", "Run.", *args)

    assert message in result.output
    with session_scope() as session:
        assert session.scalar(select(func.count()).select_from(Schedule)) == 0


def test_schedule_list_shows_the_next_fire_time(fresh_db: Path) -> None:
    cli("schedule-create", "Morning", "--type", "cron", "--expr", "0 9 * * *", "--task", "Wake.", "--timezone", "UTC")
    cli(
        "schedule-create",
        "Paused",
        "--type",
        "cron",
        "--expr",
        "0 10 * * *",
        "--task",
        "Wait.",
        "--profile",
        "high",
        "--disabled",
    )

    everything = cli("schedule-list").output
    enabled = cli("schedule-list", "--enabled").output

    morning = next(line for line in everything.splitlines() if "Morning" in line)
    paused = next(line for line in everything.splitlines() if "Paused" in line)
    assert re.search(r"\d\d-\d\d 09:00 UTC", morning)
    assert "cron 0 9 * * *" in morning
    assert "high" in paused
    assert not re.search(r"\d\d-\d\d \d\d:\d\d", paused)
    assert "Paused" not in enabled


def test_schedule_show_resolves_ids_prefixes_and_names(fresh_db: Path) -> None:
    schedule_id = created_id(
        cli("schedule-create", "Hourly", "--type", "interval", "--expr", "hours=1", "--task", "Tick.")
    )

    by_id = json.loads(cli("schedule-show", schedule_id, "--json").stdout)
    by_prefix = json.loads(cli("schedule-show", schedule_id[:8], "--json").stdout)
    by_name = json.loads(cli("schedule-show", "Hourly", "--json").stdout)
    table = cli("schedule-show", "Hourly").output

    assert by_id == by_prefix == by_name
    assert by_id["id"] == schedule_id
    assert by_id["schedule_type"] == "interval"
    assert by_id["expression"] == "hours=1"
    assert by_id["payload"] == {"title": "Hourly", "task_instruction": "Tick."}
    assert by_id["runtime_contract"] == {}
    assert "hours=1" in table
    assert "Unknown schedule: nope" in cli_error("schedule-show", "nope").output

    cli("schedule-create", "Hourly", "--type", "interval", "--expr", "hours=2", "--task", "Tock.")
    assert "matches 2 schedules" in cli_error("schedule-show", "Hourly").output


def test_schedule_edit_changes_profile_task_and_timing(fresh_db: Path, tmp_path: Path) -> None:
    schedule_id = created_id(
        cli(
            "schedule-create", "Editable", "--type", "cron", "--expr", "0 9 * * *", "--task", "Old.", "--profile", "low"
        )
    )
    template = tmp_path / "new.md"
    template.write_text("New instruction.", encoding="utf-8")

    assert f"{schedule_id}: updated" in cli("schedule-edit", schedule_id, "--profile", "high").output
    cli("schedule-edit", schedule_id, "--template", template, "--expr", "30 7 * * *", "--name", "Edited")

    shown = json.loads(cli("schedule-show", schedule_id, "--json").stdout)
    assert shown["runtime_contract"] == {"model_profile": "high"}
    assert shown["payload"] == {"title": "Edited", "task_template_path": str(template.resolve())}
    assert shown["expression"] == "30 7 * * *"
    assert shown["name"] == "Edited"

    cli("schedule-edit", schedule_id, "--contract-json", '{"model_profile": "ultra", "mcp_servers": ["tasque"]}')
    assert schedule_row(schedule_id).runtime_contract == {"model_profile": "ultra", "mcp_servers": ["tasque"]}
    cli_error("schedule-edit", schedule_id, "--profile", "extreme", code=2)
    assert "Invalid cron expression: 'never'" in cli_error("schedule-edit", schedule_id, "--expr", "never").output
    assert schedule_row(schedule_id).expression == "30 7 * * *"


def test_schedule_edit_name_retitles_the_work_it_queues(fresh_db: Path) -> None:
    schedule_id = created_id(
        cli("schedule-create", "Old name", "--type", "cron", "--expr", "0 9 * * *", "--task", "Report.")
    )

    cli("schedule-edit", schedule_id, "--name", "New name")
    fired = cli("schedule-fire-now", schedule_id).output

    assert schedule_row(schedule_id).payload == {"title": "New name", "task_instruction": "Report."}
    assert work_item(re.search(r"work item: (\S+)", fired).group(1)).title == "New name"

    cli(
        "schedule-edit",
        schedule_id,
        "--name",
        "Replaced",
        "--payload-json",
        '{"title": "Other", "task_instruction": "Go."}',
    )
    assert schedule_row(schedule_id).payload == {"title": "Replaced", "task_instruction": "Go."}
    assert schedule_row(schedule_id).name == "Replaced"


def test_schedule_disable_enable_and_delete(fresh_db: Path) -> None:
    schedule_id = created_id(
        cli("schedule-create", "Toggle", "--type", "interval", "--expr", "minutes=30", "--task", "Toggle.")
    )

    assert "Toggle: disabled" in cli("schedule-disable", schedule_id).output
    assert not schedule_row(schedule_id).enabled
    assert "Toggle: enabled" in cli("schedule-enable", schedule_id).output
    assert schedule_row(schedule_id).enabled

    refused = cli_error("schedule-delete", schedule_id)
    assert "pass --yes to confirm" in refused.output
    assert schedule_row(schedule_id) is not None

    assert "Toggle: deleted" in cli("schedule-delete", schedule_id, "--yes").output
    with session_scope() as session:
        assert session.get(Schedule, schedule_id) is None


def test_schedule_fire_now_queues_work(fresh_db: Path) -> None:
    schedule_id = created_id(
        cli("schedule-create", "On demand", "--type", "cron", "--expr", "0 0 1 1 *", "--task", "Go.", "--lane", "demo")
    )

    output = cli("schedule-fire-now", schedule_id).output

    work_id = re.search(r"work item: (\S+)", output).group(1)
    work = work_item(work_id)
    assert work.schedule_id == schedule_id
    assert work.lane == "demo"
    assert work.task_instruction == "Go."


def test_schedule_workflow_create_targets_a_registered_workflow(fresh_db: Path, tmp_path: Path) -> None:
    cli("workflow-register", workflow_file(tmp_path, "scheduled-flow"))

    schedule_id = created_id(
        cli(
            "schedule-workflow-create",
            "Flow every 6h",
            "--type",
            "interval",
            "--expr",
            "hours=6",
            "--workflow",
            "scheduled-flow",
            "--run-name",
            "Scheduled flow",
            "--input-json",
            '{"lane": "flows"}',
        )
    )

    schedule = schedule_row(schedule_id)
    with session_scope() as session:
        definition = session.scalar(select(WorkflowDefinition).where(WorkflowDefinition.name == "scheduled-flow"))
    assert schedule.worker_kind == "workflow"
    assert schedule.payload == {
        "workflow_definition_id": definition.id,
        "run_name": "Scheduled flow",
        "input": {"lane": "flows"},
    }
    fired = cli("schedule-fire-now", schedule_id).output
    assert "workflow run:" in fired
    missing = cli_error(
        "schedule-workflow-create", "X", "--type", "interval", "--expr", "hours=1", "--workflow", "nope"
    )
    assert "Unknown workflow: nope" in missing.output


def test_workflow_register_validate_and_list(fresh_db: Path, tmp_path: Path) -> None:
    good = workflow_file(tmp_path, "good-flow")
    bad = tmp_path / "bad.workflow.json"
    bad.write_text(
        json.dumps({"name": "bad", "definition": {"nodes": [{"key": "a", "depends_on": ["missing"]}]}}),
        encoding="utf-8",
    )

    registered = cli("workflow-register", good).output
    validated = cli_error("workflow-validate", good, bad)
    listed = cli("workflow-list").output

    assert "good-flow@1 (new)" in registered
    assert re.search(r"registered [0-9a-f-]{36}", registered)
    assert "good-flow@1 ok" in validated.output
    assert "Unknown workflow dependency: missing" in validated.output
    assert "good-flow" in listed
    assert "Unknown workflow dependency: missing" in cli_error("workflow-register", bad).output
    not_json = tmp_path / "broken.workflow.json"
    not_json.write_text("{not json", encoding="utf-8")
    assert "Expecting property name" in cli_error("workflow-register", not_json).output


def test_workflow_register_dry_run_shows_node_changes_without_writing(fresh_db: Path, tmp_path: Path) -> None:
    path = workflow_file(tmp_path, "tiered-flow")
    cli("workflow-register", path)
    workflow_file(
        tmp_path,
        "tiered-flow",
        nodes=[
            {**ECHO_NODES[0], "runtime_contract": {"model_profile": "low"}},
            {"key": "extra", "title": "Extra", "task_instruction": "More.", "worker_kind": "function.echo"},
        ],
    )

    dry = cli("workflow-register", path, "--dry-run").output

    assert 'step: runtime_contract: null -> {"model_profile": "low"}' in dry
    assert "+ extra" in dry
    assert "- done" in dry
    assert not re.search(r"^\s+registered ", dry, re.MULTILINE)
    with session_scope() as session:
        stored = session.scalar(select(WorkflowDefinition).where(WorkflowDefinition.name == "tiered-flow"))
        assert [node["key"] for node in stored.definition["nodes"]] == ["step", "done"]

    applied = cli("workflow-register", path).output

    assert "+ extra" in applied
    with session_scope() as session:
        stored = session.scalar(select(WorkflowDefinition).where(WorkflowDefinition.name == "tiered-flow"))
        assert [node["key"] for node in stored.definition["nodes"]] == ["step", "extra"]


def test_workflow_start_by_name_and_by_file_then_runs_show_and_cancel(fresh_db: Path, tmp_path: Path) -> None:
    path = workflow_file(tmp_path, "start-flow")
    cli("workflow-register", path)

    by_name = created_id(
        cli("workflow-start", "start-flow", "--input-json", '{"lane": "flows"}', "--run-name", "Named")
    )
    by_file = created_id(cli("workflow-start", path, "--thread", "thread-7"))

    runs = cli("workflow-runs").output
    assert by_name in runs and by_file in runs
    assert "Named" in runs
    with session_scope() as session:
        named = session.get(WorkflowRun, by_name)
        from_file = session.get(WorkflowRun, by_file)
        assert named.input == {"lane": "flows"}
        assert from_file.discord_thread_id == "thread-7"
        assert from_file.name == "start-flow"

    cli("tick", "--no-claim")
    waiting = cli("workflow-show", by_name).output
    assert "Named" in waiting
    assert "status: active" in waiting
    assert re.search(r"step\s+.\s+work\s+.\s+enqueued\s+.\s+[0-9a-f-]{36}", waiting)
    assert re.search(r"done\s+.\s+join\s+.\s+pending", waiting)

    assert f"{by_file}: canceled" in cli("workflow-cancel", by_file).output
    cli("tick")
    finished = cli("workflow-show", by_name).output
    assert "status: completed" in finished
    assert re.search(r"done\s+.\s+join\s+.\s+succeeded", finished)
    assert by_file in cli("workflow-runs", "--status", "canceled").output
    assert by_name not in cli("workflow-runs", "--status", "canceled").output
    assert "Unknown workflow: nope" in cli_error("workflow-start", "nope").output
    assert "Unknown workflow run: nope" in cli_error("workflow-show", "nope").output
    assert "Unknown workflow run: nope" in cli_error("workflow-cancel", "nope").output


def test_workflow_answer_releases_a_gate(fresh_db: Path, tmp_path: Path) -> None:
    nodes = [
        {"key": "approve", "kind": "gate", "prompt": "Go?"},
        {"key": "after", "kind": "join", "depends_on": ["approve"]},
    ]
    run_id = created_id(cli("workflow-start", workflow_file(tmp_path, "gated", nodes)))
    cli("tick", "--no-claim")

    assert "approve: succeeded" in cli("workflow-answer", run_id, "approve", "yes").output
    cli("tick", "--no-claim")
    with session_scope() as session:
        assert session.get(WorkflowRun, run_id).status == "completed"


def test_memory_add_search_show_archive_and_delete(fresh_db: Path) -> None:
    first = created_id(
        cli(
            "memory-add",
            "The kitchen prep day is Saturday.",
            "--namespace",
            "kitchen",
            "--kind",
            "fact",
            "--tag",
            "prep",
            "--importance",
            "4",
            "--ttl-days",
            "30",
            "--pinned",
        )
    )
    second = created_id(cli("memory-add", "Saturday prep uses the big pot.", "--namespace", "kitchen"))
    third = created_id(cli("memory-add", "Unrelated note about bikes."))

    with session_scope() as session:
        memory = session.get(Memory, first)
        assert (memory.namespace, memory.kind, memory.tags) == ("kitchen", "fact", ["prep"])
        assert (memory.importance, memory.ttl_days, memory.pinned, memory.source_kind) == (4, 30, True, "cli")

    found = cli("memory-search", "saturday prep", "--namespace", "kitchen").output
    assert first in found and second in found and third not in found
    assert "The kitchen prep day is Saturday." in found

    shown = cli("memory-show", first).output
    assert f"kitchen/fact {first}" in shown
    assert "The kitchen prep day is Saturday." in shown

    assert "archived 2 memories" in cli("memory-archive", first, second).output
    assert first not in cli("memory-search", "saturday prep").output
    with session_scope() as session:
        assert session.get(Memory, first).archived_at is not None

    assert "pass --yes to confirm" in cli_error("memory-delete", third).output
    assert "deleted 1" in cli("memory-delete", third, "--yes").output
    with session_scope() as session:
        assert session.get(Memory, third) is None
    assert "Unknown memory: nope" in cli_error("memory-archive", "nope").output
    assert "Unknown memory: nope" in cli_error("memory-delete", "nope", "--yes").output


def test_memory_prune_deletes_only_long_archived_memories(fresh_db: Path) -> None:
    old = created_id(cli("memory-add", "Archived long ago."))
    recent = created_id(cli("memory-add", "Archived yesterday."))
    active = created_id(cli("memory-add", "Still in use."))
    cli("memory-archive", old, recent)
    with session_scope() as session:
        session.get(Memory, old).archived_at = utc_now() - timedelta(days=120)
        session.get(Memory, recent).archived_at = utc_now() - timedelta(days=1)

    assert "pass --yes to confirm" in cli_error("memory-prune").output
    assert "deleted 1" in cli("memory-prune", "--older-than-days", "90", "--yes").output
    with session_scope() as session:
        assert session.get(Memory, old) is None
        assert session.get(Memory, recent) is not None
        assert session.get(Memory, active) is not None


def test_memory_show_prints_a_canonical_document_exactly(fresh_db: Path) -> None:
    content = "# Kitchen rules [v2]\n\nCook on :thumbs_up: Sundays. " + "Keep every line intact. " * 8
    with session_scope() as session:
        MemoryService(session).upsert_canonical(
            namespace="doctrine", canonical_key="kitchen_rules", kind="doctrine", content=content
        )

    narrow = CliRunner(env={"COLUMNS": "40"}).invoke(
        app, ["memory-show", "--namespace", "doctrine", "--key", "kitchen_rules"]
    )

    assert narrow.exit_code == 0, narrow.output
    header, body = narrow.stdout.split("\n", 1)
    assert header.startswith("doctrine/kitchen_rules ")
    assert body == content + "\n"
    assert "No such memory." in cli_error("memory-show", "--namespace", "doctrine", "--key", "missing").output
    assert "Give a memory id" in cli_error("memory-show", code=2).output


def test_memory_ingest_text_makes_a_file_searchable(fresh_db: Path, tmp_path: Path) -> None:
    source = tmp_path / "notes.txt"
    source.write_text("Bread rises overnight in the fridge.\n\nBake at 250C.", encoding="utf-8")

    assert re.search(r"ingested \d+ memories", cli("memory-ingest-text", source, "--namespace", "baking").output)
    assert "Bread rises overnight" in cli("memory-search", "bread fridge", "--namespace", "baking").output


def test_artifact_capture_list_and_archive(fresh_db: Path, tmp_path: Path) -> None:
    source = tmp_path / "upload.txt"
    source.write_text("artifact body", encoding="utf-8")

    artifact_id = created_id(
        cli("artifact-capture", source, "--kind", "note", "--tag", "discord_upload", "--title", "Upload note")
    )

    with session_scope() as session:
        artifact = session.get(Artifact, artifact_id)
        assert Path(artifact.local_path).read_text(encoding="utf-8") == "artifact body"
        assert (artifact.kind, artifact.tags, artifact.source_kind) == ("note", ["discord_upload"], "cli")
    assert "Upload note" in cli("artifact-list", "Upload", "--tag", "discord_upload").output
    assert "Upload note" not in cli("artifact-list", "--kind", "report").output
    assert f"{artifact_id}: archived" in cli("artifact-archive", artifact_id).output
    assert "Upload note" not in cli("artifact-list").output
    assert "Upload note" in cli("artifact-list", "--include-archived").output
    assert "file does not exist" in cli_error("artifact-capture", tmp_path / "missing.txt", code=2).output
    assert "Unknown artifact: nope" in cli_error("artifact-archive", "nope").output


def test_status_counts_work_schedules_and_dead_letters(fresh_db: Path) -> None:
    cli("queue", "One", "Do.")
    cli("queue", "Two", "Do.")
    cli("schedule-create", "Hourly", "--type", "interval", "--expr", "hours=1", "--task", "Tick.")

    output = cli("status").output

    assert re.search(r"work\s+.\s+ready\s+.\s+2", output)
    assert re.search(r"schedules\s+.\s+enabled\s+.\s+1", output)
    assert re.search(r"dead letters\s+.\s+unresolved\s+.\s+0", output)


def test_usage_reports_tokens_and_cost_per_lane_and_model(fresh_db: Path) -> None:
    opus = {
        "model": "claude-opus-5-5",
        "input_tokens": 100,
        "cache_read_tokens": 900,
        "cache_write_tokens": 0,
        "output_tokens": 50,
        "messages": 3,
        "estimated_cost_usd": 1.25,
        "tool_calls": {"Read": 2},
    }
    add_provider_run(lane="finance", title="Finance daily", usage=opus)
    add_provider_run(lane="finance", title="Finance daily", usage={**opus, "estimated_cost_usd": 0.75}, status="failed")
    add_provider_run(
        lane="kitchen",
        title="Kitchen",
        usage={"model": "claude-sonnet-5", "input_tokens": 10, "estimated_cost_usd": 0.1},
    )

    table = cli("usage").output
    rows = json.loads(cli("usage", "--json").stdout)

    finance = next(line for line in table.splitlines() if "finance" in line)
    assert "claude-opus-5-5" in finance
    assert re.search(r"2\s+.\s+1\s+.\s+2,000\s+.\s+90\s+.\s+100\s+.\s+2\.00\s+.\s+6", finance)
    assert "Estimated cost over 14 days: $2.10" in table
    assert [(row["lane"], row["model"]) for row in rows] == [
        ("finance", "claude-opus-5-5"),
        ("kitchen", "claude-sonnet-5"),
    ]
    assert rows[0]["runs"] == 2
    assert rows[0]["failed"] == 1
    assert rows[0]["prompt_tokens"] == 2000
    assert rows[0]["tools"] == {"Read": 4}
    assert rows[0]["estimated_cost_usd"] == pytest.approx(2.0)


def test_lanes_lists_schedule_and_workflow_node_tiers(fresh_db: Path, tmp_path: Path) -> None:
    cli(
        "schedule-create",
        "Finance daily",
        "--type",
        "cron",
        "--expr",
        "30 7 * * *",
        "--task",
        "Money.",
        "--profile",
        "high",
    )
    cli("schedule-create", "Kitchen", "--type", "cron", "--expr", "0 21 * * *", "--task", "Cook.")
    nodes = [
        {
            "key": "scan",
            "task_instruction": "Scan.",
            "worker_kind": "provider.default",
            "runtime_contract": {"model_profile": "low", "mcp_servers": ["tasque", "google-workspace"]},
        },
        {
            "key": "fan",
            "kind": "fan_out",
            "items": [1, 2],
            "child_worker_kind": "provider.claude",
            "runtime_contract": {"model_profile": "ultra"},
            "depends_on": ["scan"],
        },
        {"key": "note", "task_instruction": "Acknowledge.", "depends_on": ["fan"]},
        {"key": "merge", "kind": "join", "depends_on": ["note"]},
    ]
    cli("workflow-register", workflow_file(tmp_path, "gmail-cleanup", nodes))
    cli(
        "schedule-workflow-create",
        "Cleanup daily",
        "--type",
        "cron",
        "--expr",
        "0 6 * * *",
        "--workflow",
        "gmail-cleanup",
    )

    lines = cli("lanes").output.splitlines()

    def row(name: str) -> str:
        return next(line for line in lines if name in line)

    assert "claude-opus-5-5 / high" in row("Finance daily")
    assert "(default)" in row("Finance daily")
    assert "claude-sonnet-5 / medium" in row("Kitchen")
    assert "0 6 * * *" in row("gmail-cleanup/scan")
    assert "claude-haiku-4-5" in row("gmail-cleanup/scan")
    assert "tasque, google-workspace" in row("gmail-cleanup/scan")
    assert "claude-fable-5-1 / high" in row("gmail-cleanup/fan")
    assert "(manual)" in row("gmail-cleanup/note")
    assert not any("gmail-cleanup/merge" in line for line in lines)
    assert not any("Cleanup daily" in line for line in lines)


def test_stickies_prints_each_sticky_note_as_discord_shows_it(fresh_db: Path) -> None:
    with session_scope() as session:
        opener = WorkRepository(session).create_work_item(
            title="Career opener", task_instruction="Open.", worker_kind="manual", lane="career"
        )
        DiscordService(session).bind_thread(
            purpose="work", discord_channel_id="jobs", discord_thread_id="t-career", work_item_id=opener.id
        )
        StickyService(session).set_notes("t-career", "- Reply to Anthony (Leidos) on LinkedIn")
    cli(
        "schedule-create",
        "career-review",
        "--type",
        "cron",
        "--expr",
        "0 12 * * SUN",
        "--task",
        "Review.",
        "--thread",
        "t-career",
    )

    output = cli("stickies").output
    data = json.loads(cli("stickies", "--json").output)

    assert "== t-career (career)\n- Reply to Anthony (Leidos) on LinkedIn\n\nComing up\n" in output
    assert "· career-review" in output
    assert [sticky["notes"] for sticky in data["stickies"]] == ["- Reply to Anthony (Leidos) on LinkedIn"]
    assert data["off"] == []


def test_lanes_shows_a_bad_contract_in_its_row(fresh_db: Path) -> None:
    with session_scope() as session:
        session.add(
            Schedule(
                name="Broken tier",
                schedule_type="cron",
                expression="0 9 * * *",
                timezone="UTC",
                payload={},
                worker_kind="provider.default",
                runtime_contract={"model_profile": "turbo"},
            )
        )

    assert "invalid: model_profile must be one of" in cli("lanes").output


def test_doctor_json_without_migrating(fresh_db: Path) -> None:
    cli("migrate")

    payload = json.loads(cli("doctor", "--json", "--no-migrate").stdout)

    checks = {check["name"]: check for check in payload["checks"]}
    assert payload["overall_status"] in {"ok", "warn", "fail"}
    assert set(checks) == {
        "database.migrations",
        "database.connection",
        "artifacts.path",
        "settings.timezone",
        "providers",
        "models.tiers",
        "discord",
        "telemetry",
        "extensions",
        "daemon",
        "queue",
    }
    assert checks["database.migrations"]["status"] == "ok"
    assert checks["database.migrations"]["details"]["current"] == "core_0003"


def test_doctor_strict_exits_nonzero_only_on_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DEFAULT_PROVIDER", "fake")
    passing = cli("doctor", "--strict").output

    monkeypatch.setenv("TASQUE2_DISCORD_INTAKE_CHANNEL_ID", "channel-1")
    reset_settings()
    failing = cli_error("doctor", "--strict")

    assert "database.migrations" in passing
    assert re.search(r"discord\s+.\s+fail", failing.output)


def test_daemon_status_reads_the_state_file(fresh_db: Path) -> None:
    idle = cli("daemon-status").output
    control.write_state(started_at=utc_now(), in_flight_attempt_ids=["attempt-1"], draining=True, version="9.9.9")
    alive = cli("daemon-status").output

    assert re.search(r"state file\s+.\s+none", idle)
    assert re.search(r"schedules evaluated\s+.\s+never", idle)
    assert re.search(r"drain requested\s+.\s+False", idle)
    assert re.search(rf"pid\s+.\s+{os.getpid()}", alive)
    assert re.search(r"version\s+.\s+9\.9\.9", alive)
    assert re.search(r"alive\s+.\s+True", alive)
    assert re.search(r"in flight\s+.\s+1", alive)
    assert re.search(r"draining\s+.\s+True", alive)


def test_daemon_stop_requests_a_drain(fresh_db: Path) -> None:
    assert "Drain requested." in cli("daemon-stop", "--no-wait").output
    assert control.drain_requested()
    assert re.search(r"drain requested\s+.\s+True", cli("daemon-status").output)
    assert "Daemon stopped." in cli("daemon-stop").output


def test_tick_runs_ready_work_and_defers_to_a_live_daemon(fresh_db: Path) -> None:
    first = created_id(cli("queue", "Echo", "Hello.", "--worker-kind", "function.echo"))

    assert "finished=1" in cli("tick").output
    assert work_item(first).status == "succeeded"

    second = created_id(cli("queue", "Echo again", "Hello again.", "--worker-kind", "function.echo"))
    assert "Nothing to do." in cli("tick", "--no-claim").output
    assert work_item(second).status == "ready"

    write_foreign_daemon_state()
    refused = cli_error("tick")
    assert "A daemon is alive" in refused.output
    assert work_item(second).status == "ready"

    assert "finished=1" in cli("tick", "--force").output
    assert work_item(second).status == "succeeded"


def test_smoke_json_runs_from_an_empty_database() -> None:
    payload = json.loads(cli("smoke", "--json").stdout)

    assert payload["workflow_status"] == "completed"
    assert payload["scheduled_work_item_id"]
    assert payload["provider_run_id"]
    assert payload["report_artifact_id"]


def test_smoke_refuses_without_test_providers_or_with_ready_work(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli("queue", "Real work", "Do not run me.", "--worker-kind", "function.echo")
    busy = cli_error("smoke")

    monkeypatch.setenv("TASQUE2_ALLOW_TEST_PROVIDERS", "false")
    reset_settings()
    no_test_providers = cli_error("smoke")

    assert "ready work item" in busy.output
    assert "TASQUE2_ALLOW_TEST_PROVIDERS=true" in no_test_providers.output
    with session_scope() as session:
        assert session.scalar(select(WorkItem).where(WorkItem.title == "Real work")).status == "ready"


def test_provider_smoke_runs_the_subprocess_provider(fresh_db: Path) -> None:
    output = cli("provider-smoke", "subprocess").output

    assert "succeeded: provider smoke passed" in output
    assert re.search(r"status\s+.\s+succeeded", output)
    with session_scope() as session:
        work = session.scalar(select(WorkItem).where(WorkItem.source_kind == "provider_smoke"))
        assert (work.status, work.lane, work.visible) == ("succeeded", "provider-smoke", False)
    assert "provider must be one of" in cli_error("provider-smoke", "gemini", code=2).output


def test_provider_smoke_shows_the_stream_artifact(fresh_db: Path) -> None:
    output = cli("provider-smoke", "fake").output

    with session_scope() as session:
        run = session.scalar(select(ProviderRun).where(ProviderRun.provider == "fake"))
        stream = session.get(Artifact, run.stdout_artifact_id)
    assert "succeeded: Fake provider completed." in output
    assert re.search(rf"stream artifact\s+.\s+{run.stdout_artifact_id}", output)
    assert Path(stream.local_path).read_text(encoding="utf-8") == "Fake provider completed."


def test_provider_smoke_runs_its_own_item_first(fresh_db: Path) -> None:
    other = created_id(cli("queue", "Urgent", "Echo.", "--worker-kind", "function.echo", "--priority", "50"))

    cli("provider-smoke", "subprocess")

    assert work_item(other).status == "ready"


def test_discord_output_simulate_leaves_the_database_unchanged(fresh_db: Path) -> None:
    cli("queue", "Visible result", "Hello Discord.", "--worker-kind", "function.echo")
    cli("tick")
    before = database_rows()

    first = cli("discord-output-simulate").output
    second = cli("discord-output-simulate").output

    assert "would post 1 update(s): 1 thread(s), 1 message(s)" in first
    assert first == second
    assert database_rows() == before


def test_migrate_and_db_status() -> None:
    migrated = cli("migrate").output
    status = cli("db-status").output

    assert "tasque2.sqlite3: core_0003" in migrated
    assert re.search(r"current\s+.\s+core_0003", status)
    assert re.search(r"head\s+.\s+core_0003", status)
    assert re.search(r"up to date\s+.\s+True", status)


def test_backup_create_and_restore(fresh_db: Path, tmp_path: Path) -> None:
    cli("queue", "Kept", "Stay.")
    backup_dir = tmp_path / "cli-backup"

    created = cli("backup-create", backup_dir, "--no-artifacts").output
    cli("queue", "Dropped", "Go away.")
    refused = cli_error("backup-restore", backup_dir)
    restored = cli("backup-restore", backup_dir, "--force").output

    assert "backup:" in created and (backup_dir / "tasque2.sqlite3").is_file()
    assert "pass --force to confirm" in refused.output
    assert "restored:" in restored and "(core_0003)" in restored
    assert "previous database kept at:" in restored
    listing = cli("list").output
    assert "Kept" in listing
    assert "Dropped" not in listing


def test_reset_jobs_deletes_work_history(fresh_db: Path, isolated: Path) -> None:
    cli("queue", "Old job", "Done.")

    refused = cli_error("reset-jobs")
    assert "pass --yes to confirm" in refused.output

    output = cli("reset-jobs", "--yes", "--no-backup").output
    assert "work_items_deleted: 1" in output
    assert not (isolated / "data" / "backups").exists()
    with session_scope() as session:
        assert session.scalar(select(func.count()).select_from(WorkItem)) == 0

    cli("queue", "Another", "Done.")
    backed_up = cli("reset-jobs", "--yes").output
    assert "backup:" in backed_up
    assert any((isolated / "data" / "backups").iterdir())
