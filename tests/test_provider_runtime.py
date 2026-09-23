from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest
from opentelemetry.trace import SpanKind, StatusCode
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.config import get_settings, reset_settings
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.models import Artifact, FailedWork, ProviderRun, WorkAttempt, WorkItem, utc_now
from tasque2.providers import (
    ClaudeCodeProvider,
    CodexCliProvider,
    FakeProvider,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
    SubprocessProvider,
    mcp_config,
    provider_name_for_worker_kind,
)
from tasque2.providers.claude import response_from_stream
from tasque2.providers.process import run_process
from tasque2.telemetry import TelemetryMode
from tasque2.telemetry import setup as telemetry_setup
from tasque2.work.repository import WorkRepository
from tasque2.work.retry import capacity_gate, limit_retry_delay_seconds
from tasque2.work.runner import RunOutcome, WorkRunner
from tasque2.worker import results
from tasque2.worker.prompt import WORKER_CONTRACT
from tasque2.worker.runtime import ProviderRuntime, worker_telemetry_env

SUBMITTED = {"summary": "Done.", "report": "Report.", "produces": {}}
LIMIT_MESSAGE = "You've hit your session limit · resets 3pm (America/Los_Angeles)"


def _stream(*events: dict) -> str:
    return "\n".join(json.dumps(event) for event in events)


def _init(*servers: tuple[str, str], model: str = "claude-sonnet-5-20260801") -> dict:
    return {
        "type": "system",
        "subtype": "init",
        "session_id": "sess-7",
        "model": model,
        "mcp_servers": [{"name": name, "status": status} for name, status in servers],
    }


def _tool_call(message_id: str, tool_id: str, name: str, usage: tuple[int, int, int, int]) -> dict:
    input_tokens, output_tokens, cache_read, cache_write = usage
    return {
        "type": "assistant",
        "message": {
            "id": message_id,
            "model": "claude-sonnet-5-20260801",
            "content": [{"type": "tool_use", "id": tool_id, "name": name, "input": {}}],
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_input_tokens": cache_read,
                "cache_creation_input_tokens": cache_write,
            },
        },
    }


def _result(text: str, *, is_error: bool = False) -> dict:
    return {"type": "result", "subtype": "success", "is_error": is_error, "result": text, "session_id": "sess-7"}


CLAUDE_STREAM = _stream(
    _init(("tasque2", "connected")),
    _tool_call("msg_1", "toolu_1", "mcp__tasque2__memory_recall", (100, 40, 2000, 500)),
    _tool_call("msg_2", "toolu_2", "Bash", (20, 60, 2500, 0)),
    _result("Done."),
)


class ScriptedClaude(ClaudeCodeProvider):
    """The real Claude command line with a canned stream in place of the CLI."""

    def __init__(self, stdout: str, *, returncode: int = 0, submit: dict | None = SUBMITTED) -> None:
        super().__init__()
        self.stdout = stdout
        self.returncode = returncode
        self.submit = submit
        self.requests: list[ProviderRequest] = []

    def run(self, request: ProviderRequest) -> ProviderResponse:
        self.requests.append(request)
        if self.submit is not None:
            results.deposit(result_token=request.result_token, payload=self.submit)
        return response_from_stream(
            provider=self.name, stdout=self.stdout, stderr="", returncode=self.returncode, terminated_after_result=False
        )


class ScriptedCodex(CodexCliProvider):
    def run(self, request: ProviderRequest) -> ProviderResponse:
        results.deposit(result_token=request.result_token, payload=SUBMITTED)
        return ProviderResponse(status="succeeded", summary="Done.")


class ExplodingProvider:
    name = "exploding"

    def run(self, request: ProviderRequest) -> ProviderResponse:
        raise RuntimeError("socket closed")


def _run(session: Session, adapter, **fields) -> tuple[RunOutcome, WorkItem]:
    work = WorkRepository(session).create_work_item(
        **{
            "title": "Provider work",
            "task_instruction": "Do the work.",
            "worker_kind": f"provider.{adapter.name}",
            **fields,
        }
    )
    registry = ProviderRegistry()
    registry.register(adapter)
    outcome = WorkRunner(session, provider_runtime=ProviderRuntime(registry=registry)).run_next()
    assert outcome is not None
    return outcome, work


def _attempt(session: Session, work: WorkItem) -> WorkAttempt:
    return session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))


def _failed(session: Session, work: WorkItem) -> FailedWork:
    return session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))


def _span(spans, name: str):
    return next(item for item in spans.get_finished_spans() if item.name == name)


def _packet(prompt: str) -> dict:
    return json.loads(prompt.split("# Context packet\n\n", 1)[1])


@pytest.fixture()
def telemetry_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry_setup, "_state", telemetry_setup._TelemetryState(mode=TelemetryMode.OTLP))


def test_provider_runtime_runs_only_provider_worker_kinds() -> None:
    runtime = ProviderRuntime(registry=ProviderRegistry())

    assert runtime.can_run("provider.claude") is True
    assert runtime.can_run("function.echo") is False


def test_fake_provider_success_records_the_run_and_its_artifacts(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    with session_scope() as session:
        outcome, work = _run(session, FakeProvider(capture_requests=captured), runtime_contract={"model": "fake-model"})
        attempt = _attempt(session, work)
        run = session.get(ProviderRun, attempt.provider_run_id)
        stream = session.get(Artifact, run.stdout_artifact_id)
        report = session.get(Artifact, attempt.report_artifact_id)
        artifacts = session.scalars(select(Artifact).where(Artifact.attempt_id == attempt.id)).all()

        assert outcome.status == "succeeded"
        assert outcome.summary == "Fake provider completed."
        assert captured[0].provider == "fake"
        assert captured[0].model == "fake-model"
        assert attempt.provider == "fake"
        assert attempt.produces == {"ok": True}
        assert run.attempt_id == attempt.id
        assert run.status == "succeeded"
        assert run.model == "fake-model"
        assert run.cwd == str(get_settings().resolved_project_dir)
        assert run.started_at is not None
        assert run.ended_at is not None
        assert {"TASQUE2_RESULT_TOKEN", "TASQUE2_WORK_ITEM_ID", "TASQUE2_SCRATCH_DIR", "TMP"} <= set(run.env_keys)
        assert run.usage["messages"] == 0
        assert run.stderr_artifact_id is None
        assert stream.kind == "provider_stream"
        assert stream.tags == ["provider", "fake", "stream"]
        assert (stream.source_kind, stream.source_id) == ("provider_run", run.id)
        assert Path(stream.local_path).read_text(encoding="utf-8") == "Fake provider completed."
        assert report.kind == "worker_report"
        assert report.title == "Provider work report"
        assert Path(report.local_path).read_text(encoding="utf-8") == "Fake provider completed."
        assert {artifact.id for artifact in artifacts} == {stream.id, report.id}


def test_claude_stream_run_records_usage_cost_and_a_trace(fresh_db: Path) -> None:
    with session_scope() as session:
        _, work = _run(session, ScriptedClaude(CLAUDE_STREAM), lane="usage-lane")
        attempt = _attempt(session, work)
        run = session.get(ProviderRun, attempt.provider_run_id)
        traces = [
            artifact
            for artifact in session.scalars(select(Artifact).where(Artifact.source_id == run.id)).all()
            if "trace" in artifact.tags
        ]

        assert run.provider == "claude"
        assert run.model == "claude-sonnet-5"
        assert run.provider_session_id == "sess-7"
        assert run.usage["input_tokens"] == 120
        assert run.usage["output_tokens"] == 100
        assert run.usage["cache_read_tokens"] == 4500
        assert run.usage["cache_write_tokens"] == 500
        assert run.usage["messages"] == 2
        assert run.usage["model"] == "claude-sonnet-5-20260801"
        assert run.usage["tool_calls"] == {"mcp__tasque2__memory_recall": 1, "Bash": 1}
        assert run.usage["estimated_cost_usd"] == 0.0041
        assert attempt.exit_code == 0
        assert len(traces) == 1
        assert traces[0].local_path.endswith(".md")
        assert Path(traces[0].local_path).read_text(encoding="utf-8").startswith("# Provider trace")


def test_prompt_carries_the_run_header_template_and_packet(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    with session_scope() as session:
        MemoryService(session).create_memory(
            namespace="global", kind="preference", content="Prefer concise status reports."
        )
        _, work = _run(
            session,
            FakeProvider(capture_requests=captured),
            title="Context work",
            task_instruction="Use concise status reports.",
        )
        attempt = _attempt(session, work)

    request = captured[0]
    scratch = get_settings().resolved_scratch_dir / attempt.id
    packet = _packet(request.prompt)
    assert request.prompt.startswith("# Run\n")
    assert f"- result_token: {request.result_token}" in request.prompt
    assert f"- work_item_id: {work.id}" in request.prompt
    assert f"- scratch directory: {scratch}" in request.prompt
    assert "# Work template\n\nUse concise status reports." in request.prompt
    assert packet["work_item"]["id"] == work.id
    assert "Prefer concise status reports." in [memory["content"] for memory in packet["memories"]]
    assert request.system_prompt_path.read_text(encoding="utf-8") == WORKER_CONTRACT
    assert request.work_item_id == work.id
    assert request.env["TASQUE2_RESULT_TOKEN"] == request.result_token
    assert request.env["TASQUE2_WORK_ITEM_ID"] == work.id
    for name in ("TASQUE2_SCRATCH_DIR", "TMP", "TEMP", "TMPDIR"):
        assert request.env[name] == str(scratch)
    assert scratch.is_dir()


def test_packet_uses_the_default_memory_budget(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    with session_scope() as session:
        memory = MemoryService(session)
        for index in range(40):
            memory.create_memory(namespace="global", kind="note", content=f"Budgeted memory {index} " + "detail " * 500)
        outcome, _ = _run(
            session,
            FakeProvider(capture_requests=captured),
            title="Budgeted memory",
            task_instruction="Use budgeted memory details.",
        )

    packet = _packet(captured[0].prompt)
    assert outcome.status == "succeeded"
    assert len(packet["memories"]) == 24
    assert all(memory["content_compacted"] for memory in packet["memories"])
    assert len(captured[0].prompt) < 120_000


def test_contract_env_cwd_argv_and_limits_reach_the_request(fresh_db: Path, tmp_path: Path) -> None:
    captured: list[ProviderRequest] = []
    contract = {
        "env": {"EXTRA": "1", "COUNT": 2},
        "cwd": str(tmp_path / "work"),
        "argv": ["tool", 3],
        "disallowed_tools": ["mcp__autopilot__fill_login", " ", " x "],
        "max_turns": "12",
        "max_budget_usd": "0.5",
    }
    with session_scope() as session:
        _run(session, FakeProvider(capture_requests=captured), runtime_contract=contract)
        _run(session, FakeProvider(capture_requests=captured), context={"cwd": str(tmp_path / "from-context")})

    request, fallback = captured
    assert request.env["EXTRA"] == "1"
    assert request.env["COUNT"] == "2"
    assert request.cwd == str(tmp_path / "work")
    assert request.argv == ["tool", "3"]
    assert request.disallowed_tools == ["mcp__autopilot__fill_login", "x"]
    assert (request.max_turns, request.max_budget_usd) == (12, 0.5)
    assert request.mcp_servers is None
    assert fallback.cwd == str(tmp_path / "from-context")
    assert (fallback.max_turns, fallback.max_budget_usd, fallback.disallowed_tools) == (None, None, [])


@pytest.mark.parametrize(
    ("contract", "message"),
    [
        ({"disallowed_tools": "mcp__autopilot__fill_login"}, "runtime_contract.disallowed_tools must be a list."),
        ({"disallowed_tools": ["ok", 3]}, "runtime_contract.disallowed_tools must be a list of strings."),
        ({"argv": "python -c pass"}, "runtime_contract.argv must be a list."),
        ({"mcp_servers": "autopilot"}, "runtime_contract.mcp_servers must be a list of strings."),
        ({"env": ["EXTRA=1"]}, "runtime_contract.env must be an object."),
    ],
)
def test_malformed_contract_dead_letters_the_work(fresh_db: Path, contract: dict, message: str) -> None:
    captured: list[ProviderRequest] = []
    with session_scope() as session:
        outcome, work = _run(session, FakeProvider(capture_requests=captured), runtime_contract=contract)
        failed = _failed(session, work)

    assert outcome.status == "dead_letter"
    assert (failed.error_type, failed.error_message) == ("ProviderExecutionError", message)
    assert captured == []
    with session_scope() as session:
        runs = session.scalars(select(ProviderRun)).all()
        assert [run.status for run in runs] == ([] if "env" in contract else ["failed"])
        assert all(run.ended_at is not None for run in runs)


def test_a_provider_that_raises_closes_its_run_as_failed(fresh_db: Path, spans) -> None:
    with session_scope() as session:
        outcome, work = _run(session, ExplodingProvider())
        attempt = _attempt(session, work)
        run_id = attempt.provider_run_id

    assert outcome.status == "dead_letter"
    assert attempt.error_type == "RuntimeError"
    with session_scope() as session:
        run = session.get(ProviderRun, run_id)
        assert run.status == "failed"
        assert run.ended_at is not None
    invoke = _span(spans, "invoke_agent Provider work")
    assert invoke.status.status_code is StatusCode.ERROR
    assert invoke.attributes["error.type"] == "RuntimeError"


def test_default_worker_kind_runs_the_configured_provider(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DEFAULT_PROVIDER", "fake")
    reset_settings()
    captured: list[ProviderRequest] = []
    with session_scope() as session:
        outcome, work = _run(session, FakeProvider(capture_requests=captured), worker_kind="provider.default")
        attempt = _attempt(session, work)

    assert outcome.status == "succeeded"
    assert captured[0].provider == "fake"
    assert attempt.provider == "fake"
    assert provider_name_for_worker_kind("provider.codex") == "codex"


def test_default_provider_must_be_a_model_provider_without_the_test_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DEFAULT_PROVIDER", "fake")
    monkeypatch.delenv("TASQUE2_ALLOW_TEST_PROVIDERS")
    reset_settings()

    with pytest.raises(ValueError, match="TASQUE2_DEFAULT_PROVIDER must be one of: claude, codex."):
        provider_name_for_worker_kind("provider.default")


def test_unregistered_provider_dead_letters_the_work(fresh_db: Path) -> None:
    with session_scope() as session:
        outcome, work = _run(session, FakeProvider(), worker_kind="provider.nope")
        failed = _failed(session, work)
        runs = session.scalars(select(ProviderRun)).all()

    assert outcome.status == "dead_letter"
    assert (failed.error_type, failed.error_message) == (
        "ProviderExecutionError",
        "No provider adapter registered for 'nope'.",
    )
    assert runs == []


def test_invalid_model_profile_fails_before_the_provider_runs(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    with session_scope() as session:
        outcome, work = _run(
            session, FakeProvider(capture_requests=captured), runtime_contract={"model_profile": "hint:fast"}
        )
        failed = _failed(session, work)

    assert outcome.status == "dead_letter"
    assert captured == []
    assert (failed.error_type, failed.error_message) == (
        "ValueError",
        "model_profile must be one of: low, medium, high, ultra.",
    )


def test_failed_run_without_a_result_is_retried_as_transient(fresh_db: Path) -> None:
    adapter = FakeProvider(
        deposit_result=False,
        response=ProviderResponse(status="failed", summary="Fake failure.", stderr="bad things happened", exit_code=9),
    )
    with session_scope() as session:
        outcome, work = _run(session, adapter, max_attempts=1)
        attempt = _attempt(session, work)
        run = session.get(ProviderRun, attempt.provider_run_id)
        stderr = session.get(Artifact, run.stderr_artifact_id)

        assert outcome.status == "ready"
        assert session.get(WorkItem, work.id).not_before is not None
        assert (attempt.error_type, attempt.error_message) == ("TransientProviderError", "Fake failure.")
        assert attempt.exit_code == 9
        assert run.status == "failed"
        assert stderr.tags == ["provider", "fake", "stderr"]
        assert Path(stderr.local_path).read_text(encoding="utf-8") == "bad things happened"


def test_missing_result_is_retried_as_transient(fresh_db: Path) -> None:
    adapter = FakeProvider(
        deposit_result=False,
        response=ProviderResponse(
            status="succeeded", summary="No result.", output_text="plain text", stdout="plain text"
        ),
    )
    with session_scope() as session:
        outcome, work = _run(session, adapter, max_attempts=1)
        attempt = _attempt(session, work)

    assert outcome.status == "ready"
    assert attempt.error_type == "TransientProviderError"
    assert attempt.error_message == (
        "The worker did not call submit_worker_result; no result was submitted. Final assistant text: 'plain text'"
    )


def test_missing_result_names_a_failed_tasque_mcp_server(fresh_db: Path) -> None:
    stdout = _stream(
        _init(("tasque2", "failed")),
        _result("The tasque2 MCP server is down this session; nothing I can submit."),
    )
    adapter = FakeProvider(
        deposit_result=False, response=ProviderResponse(status="succeeded", summary="", stdout=stdout)
    )
    with session_scope() as session:
        _, work = _run(session, adapter)
        attempt = _attempt(session, work)

    assert attempt.error_type == "TransientProviderError"
    assert "Tasque MCP server ('tasque2') failed to connect" in attempt.error_message
    assert "did not call submit_worker_result" not in attempt.error_message


def test_missing_result_quotes_the_agents_closing_words(fresh_db: Path) -> None:
    stdout = _stream(
        _init(("tasque2", "connected"), ("autopilot", "failed")),
        _result("Both research agents are still running in the background; I'll resume once they report back."),
    )
    adapter = FakeProvider(
        deposit_result=False, response=ProviderResponse(status="succeeded", summary="", stdout=stdout)
    )
    with session_scope() as session:
        _, work = _run(session, adapter)
        message = _attempt(session, work).error_message

    assert message.startswith("The worker did not call submit_worker_result")
    assert "MCP servers that failed to connect: autopilot." in message
    assert 'Final assistant text: "Both research agents are still running' in message


def test_blocked_result_completes_the_work_with_its_blocker(fresh_db: Path) -> None:
    payload = {
        "status": "blocked",
        "summary": "Needs auth.",
        "report": "Needs auth.",
        "error": "login_required",
        "produces": {"outcome": "blocked"},
    }
    with session_scope() as session:
        outcome, work = _run(session, FakeProvider(result_payload=payload))
        attempt = _attempt(session, work)
        failed = _failed(session, work)

    assert outcome.status == "succeeded"
    assert attempt.summary == "Needs auth."
    assert attempt.produces == {"outcome": "blocked", "completion_signal": "blocked", "blocker": "login_required"}
    assert failed is None


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            {"status": "failed", "summary": "Could not log in.", "report": "", "error": "login_required"},
            "login_required",
        ),
        ({"status": "failed", "summary": "Could not log in.", "report": ""}, "Could not log in."),
        ({"summary": "Done?", "report": "", "error": "half the rows were rejected"}, "half the rows were rejected"),
        ({"summary": "Done."}, "submit_worker_result payload is missing string field 'report'."),
        ({"report": "Done."}, "submit_worker_result payload is missing string field 'summary'."),
        (
            {"summary": "Done.", "report": "", "produces": ["x"]},
            "submit_worker_result payload field 'produces' must be an object.",
        ),
    ],
)
def test_failed_or_malformed_result_dead_letters_the_work(fresh_db: Path, payload: dict, message: str) -> None:
    with session_scope() as session:
        outcome, work = _run(session, FakeProvider(result_payload=payload))
        failed = _failed(session, work)

    assert outcome.status == "dead_letter"
    assert (failed.error_type, failed.error_message) == ("ProviderExecutionError", message)


def test_empty_report_writes_no_report_artifact(fresh_db: Path) -> None:
    payload = {"summary": "Nothing new.", "report": "  ", "produces": {"silent": True}}
    with session_scope() as session:
        outcome, work = _run(session, FakeProvider(result_payload=payload))
        attempt = _attempt(session, work)

    assert outcome.status == "succeeded"
    assert attempt.summary == "Nothing new."
    assert attempt.produces == {"silent": True}
    assert attempt.report_artifact_id is None


def test_limit_stop_in_the_stream_holds_provider_work_until_the_reset(fresh_db: Path, metric_points) -> None:
    stream = _stream(_init(("tasque2", "connected")), _result(LIMIT_MESSAGE, is_error=True))
    before = utc_now()
    with session_scope() as session:
        outcome, work = _run(session, ScriptedClaude(stream, returncode=1, submit=None), lane="limit-lane")
        attempt = _attempt(session, work)
        run = session.get(ProviderRun, attempt.provider_run_id)
        not_before = session.get(WorkItem, work.id).not_before

    expected = before + timedelta(seconds=limit_retry_delay_seconds(LIMIT_MESSAGE, now=before))
    assert outcome.status == "ready"
    assert (attempt.error_type, attempt.error_message) == ("TransientProviderError", LIMIT_MESSAGE)
    assert attempt.exit_code == 1
    assert run.status == "failed"
    assert abs(not_before - expected) < timedelta(seconds=5)
    assert capacity_gate.is_closed()
    assert [
        point.value
        for point in metric_points("tasque.provider.limit_stops")
        if point.attributes.get("tasque.work.lane") == "limit-lane"
    ] == [1]


def test_invoke_agent_span_describes_the_provider_run(fresh_db: Path, spans) -> None:
    with session_scope() as session:
        _, work = _run(session, ScriptedClaude(CLAUDE_STREAM), lane="finance-daily")
        run_id = _attempt(session, work).provider_run_id

    invoke = _span(spans, "invoke_agent finance-daily")
    work_run = _span(spans, "tasque.work.run")
    assert invoke.kind is SpanKind.CLIENT
    assert invoke.parent.span_id == work_run.context.span_id
    assert invoke.context.trace_id == work_run.context.trace_id
    assert dict(invoke.attributes) == {
        "gen_ai.operation.name": "invoke_agent",
        "gen_ai.provider.name": "anthropic",
        "gen_ai.agent.name": "finance-daily",
        "gen_ai.request.model": "claude-sonnet-5",
        "gen_ai.response.model": "claude-sonnet-5-20260801",
        "gen_ai.conversation.id": "sess-7",
        "gen_ai.usage.input_tokens": 5120,
        "gen_ai.usage.output_tokens": 100,
        "gen_ai.usage.cache_read.input_tokens": 4500,
        "gen_ai.usage.cache_creation.input_tokens": 500,
        "tasque.model.profile": "medium",
        "tasque.model.effort": "medium",
        "tasque.work.id": work.id,
        "tasque.provider.run.id": run_id,
        "tasque.provider.messages": 2,
        "tasque.provider.subagent_messages": 0,
        "tasque.provider.estimated_cost_usd": 0.0041,
        "tasque.provider.status": "succeeded",
        "tasque.provider.exit_code": 0,
    }


def test_invoke_agent_span_falls_back_to_the_title_without_a_lane(fresh_db: Path, spans) -> None:
    with session_scope() as session:
        _run(session, FakeProvider(), title="Unlaned work")

    invoke = _span(spans, "invoke_agent Unlaned work")
    assert invoke.attributes["gen_ai.agent.name"] == "Unlaned work"
    assert invoke.attributes["gen_ai.provider.name"] == "fake"
    assert "gen_ai.request.model" not in invoke.attributes
    assert invoke.attributes["gen_ai.response.model"] == "unknown"


def test_provider_run_metrics_are_recorded_for_the_lane(fresh_db: Path, metric_points) -> None:
    lane = "metrics-lane"
    with session_scope() as session:
        _run(session, ScriptedClaude(CLAUDE_STREAM), lane=lane)

    def points(name: str) -> list:
        return [point for point in metric_points(name) if point.attributes.get("tasque.work.lane") == lane]

    usage = {point.attributes["gen_ai.token.type"]: point.sum for point in points("gen_ai.client.token.usage")}
    tokens = {point.attributes["tasque.token.type"]: point.value for point in points("tasque.provider.tokens")}
    tools = {
        point.attributes["gen_ai.tool.name"]: (point.attributes["tasque.tool.server"], point.value)
        for point in points("tasque.worker.tool_calls")
    }
    assert usage == {"input": 5120, "output": 100}
    assert all(
        point.attributes["gen_ai.provider.name"] == "anthropic"
        and point.attributes["gen_ai.request.model"] == "claude-sonnet-5"
        for point in points("gen_ai.client.token.usage")
    )
    assert tokens == {"input": 120, "output": 100, "cache_read": 4500, "cache_write": 500}
    assert [point.value for point in points("tasque.provider.cost")] == [pytest.approx(0.00414)]
    assert [(point.count, point.sum) for point in points("tasque.provider.turns")] == [(1, 2)]
    assert tools == {"mcp__tasque2__memory_recall": ("tasque2", 1), "Bash": ("builtin", 1)}


def test_provider_process_joins_the_invoke_agent_trace(fresh_db: Path, spans) -> None:
    script = (
        "import os\n"
        "from tasque2.worker import results\n"
        "results.deposit(result_token=os.environ['TASQUE2_RESULT_TOKEN'], payload={'summary': 'Traced.', "
        "'report': '', 'produces': {'traceparent': os.environ.get('TRACEPARENT')}})\n"
    )
    argv = [sys.executable, "-c", script]
    with session_scope() as session:
        outcome, work = _run(session, SubprocessProvider(), lane="subprocess-lane", runtime_contract={"argv": argv})
        attempt = _attempt(session, work)
        recorded_argv = session.get(ProviderRun, attempt.provider_run_id).argv

    invoke = _span(spans, "invoke_agent subprocess-lane")
    assert outcome.status == "succeeded"
    assert recorded_argv == argv
    assert attempt.produces["traceparent"].split("-")[1:3] == [
        f"{invoke.context.trace_id:032x}",
        f"{invoke.context.span_id:016x}",
    ]


def test_recorded_claude_argv_names_mcp_servers_without_their_config(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        mcp_config, "user_scope_mcp_servers", lambda: {"autopilot": {"command": "ap", "env": {"TOKEN": "secret"}}}
    )
    with session_scope() as session:
        _, work = _run(
            session,
            ScriptedClaude(CLAUDE_STREAM),
            runtime_contract={"mcp_servers": ["autopilot"], "model_profile": "low"},
        )
        argv = session.get(ProviderRun, _attempt(session, work).provider_run_id).argv

    assert argv[argv.index("--mcp-config") + 1] == "<servers: autopilot, tasque2>"
    assert "--strict-mcp-config" in argv
    assert argv[argv.index("--model") + 1] == "claude-haiku-4-5"
    assert not any("secret" in part or "TASQUE2_DATA_DIR" in part for part in argv)


def test_recorded_codex_argv_redacts_the_mcp_server_env(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_MEDIUM", "codex-medium-test")
    reset_settings()
    with session_scope() as session:
        _, work = _run(session, ScriptedCodex())
        argv = session.get(ProviderRun, _attempt(session, work).provider_run_id).argv

    assert "mcp_servers.tasque2.env.TASQUE2_DATA_DIR=<redacted>" in argv
    assert "mcp_servers.tasque2.env.TRACEPARENT=<redacted>" in argv
    assert f"mcp_servers.tasque2.command={json.dumps(sys.executable)}" in argv
    assert argv[argv.index("--model") + 1] == "codex-medium-test"


def _work_and_attempt(lane: str | None = "finance daily") -> tuple[WorkItem, WorkAttempt]:
    work = WorkItem(id="work-1", title="Finance", task_instruction="x", worker_kind="provider.claude", lane=lane)
    return work, WorkAttempt(id="attempt-1", attempt_number=2, worker_kind="provider.claude")


def test_worker_telemetry_env_is_empty_while_telemetry_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry_setup, "_state", None)
    assert worker_telemetry_env("claude", *_work_and_attempt()) == {}

    monkeypatch.setattr(telemetry_setup, "_state", telemetry_setup._TelemetryState(mode=TelemetryMode.OFF))
    assert worker_telemetry_env("claude", *_work_and_attempt()) == {}


def test_worker_telemetry_env_tags_claude_exports_with_the_run(telemetry_on: None) -> None:
    assert worker_telemetry_env("claude", *_work_and_attempt()) == {
        "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
        "OTEL_METRICS_EXPORTER": "otlp",
        "OTEL_LOGS_EXPORTER": "otlp",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        "OTEL_METRIC_EXPORT_INTERVAL": "10000",
        "OTEL_LOGS_EXPORT_INTERVAL": "2000",
        "OTEL_RESOURCE_ATTRIBUTES": (
            "service.name=claude-code,tasque.work.id=work-1,tasque.work.lane=finance_daily,tasque.work.attempt=2"
        ),
    }


def test_worker_telemetry_env_keeps_a_protocol_already_chosen(
    telemetry_on: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")

    assert worker_telemetry_env("claude", *_work_and_attempt())["OTEL_EXPORTER_OTLP_PROTOCOL"] == "grpc"


def test_worker_telemetry_env_marks_work_without_a_lane(telemetry_on: None) -> None:
    attributes = worker_telemetry_env("claude", *_work_and_attempt(lane=None))["OTEL_RESOURCE_ATTRIBUTES"]

    assert "tasque.work.lane=unassigned" in attributes.split(",")


def test_worker_telemetry_env_is_only_for_claude(telemetry_on: None) -> None:
    for provider in ("codex", "fake", "subprocess"):
        assert worker_telemetry_env(provider, *_work_and_attempt()) == {}


def test_worker_telemetry_env_follows_the_export_and_trace_settings(
    telemetry_on: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_TELEMETRY_WORKER_TRACES", "true")
    reset_settings()
    env = worker_telemetry_env("claude", *_work_and_attempt())
    assert env["CLAUDE_CODE_ENHANCED_TELEMETRY_BETA"] == "1"
    assert env["OTEL_TRACES_EXPORTER"] == "otlp"

    monkeypatch.setenv("TASQUE2_TELEMETRY_WORKER_EXPORT", "false")
    reset_settings()
    assert worker_telemetry_env("claude", *_work_and_attempt()) == {}


def test_active_telemetry_reaches_the_claude_process_env(fresh_db: Path, telemetry_on: None) -> None:
    adapter = ScriptedClaude(CLAUDE_STREAM)
    with session_scope() as session:
        _run(session, adapter, lane="telemetry-lane")

    env = adapter.requests[0].env
    assert env["CLAUDE_CODE_ENABLE_TELEMETRY"] == "1"
    assert "tasque.work.lane=telemetry-lane" in env["OTEL_RESOURCE_ATTRIBUTES"].split(",")
    assert env["TRACEPARENT"].startswith("00-")


def _subprocess_request(*argv: str, prompt: str = "", result_token: str | None = None) -> ProviderRequest:
    return ProviderRequest(provider="subprocess", prompt=prompt, argv=list(argv), result_token=result_token)


def test_subprocess_provider_sends_the_prompt_on_stdin_and_decodes_utf8() -> None:
    script = "import sys; sys.stdout.buffer.write(b'got:' + sys.stdin.buffer.read() + b'\\xff')"

    response = SubprocessProvider().run(_subprocess_request(sys.executable, "-c", script, prompt="héllo ✓"))

    assert response.status == "succeeded"
    assert response.output_text == "got:héllo ✓�"
    assert response.summary == "got:héllo ✓�"
    assert response.exit_code == 0


def test_subprocess_provider_reports_a_nonzero_exit() -> None:
    script = "import sys; print('partial'); sys.stderr.write('boom'); sys.exit(3)"

    response = SubprocessProvider().run(_subprocess_request(sys.executable, "-c", script))

    assert response.status == "failed"
    assert response.summary == "partial"
    assert response.stderr == "boom"
    assert response.exit_code == 3


def test_subprocess_provider_without_output_reports_its_exit_code() -> None:
    response = SubprocessProvider().run(_subprocess_request(sys.executable, "-c", "pass"))

    assert response.status == "succeeded"
    assert response.summary == "Subprocess exited 0."
    assert (response.stdout, response.stderr) == ("", "")


def test_subprocess_provider_needs_an_argv() -> None:
    response = SubprocessProvider().run(_subprocess_request())

    assert response.status == "failed"
    assert response.summary == "No subprocess argv was provided."
    assert response.exit_code == 2


def test_missing_command_is_a_launch_failure(tmp_path: Path) -> None:
    missing = str(tmp_path / "no-such-provider.exe")

    response = SubprocessProvider().run(_subprocess_request(missing))

    assert response.status == "failed"
    assert response.summary == f"Provider command not found: {missing}"
    assert response.exit_code == 127


def test_windows_command_line_limit_is_named(monkeypatch: pytest.MonkeyPatch) -> None:
    class CommandLineTooLong(OSError):
        winerror = 206

    def refuse(*_args, **_kwargs):
        raise CommandLineTooLong("[WinError 206] The filename or extension is too long")

    monkeypatch.setattr(subprocess, "Popen", refuse)

    response = SubprocessProvider().run(_subprocess_request("codex", "exec", "long-prompt"))

    assert response.status == "failed"
    assert response.summary == "Provider command line too long: codex"


def test_subprocess_provider_stops_the_process_tree_once_the_result_arrives(fresh_db: Path) -> None:
    token = results.mint_token()
    script = (
        "import subprocess, sys, time\n"
        "from tasque2.worker import results\n"
        f"results.deposit(result_token={token!r}, payload={{'summary': 'done', 'report': 'done'}})\n"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        "time.sleep(60)\n"
    )
    started = time.monotonic()

    response = SubprocessProvider().run(_subprocess_request(sys.executable, "-c", script, result_token=token))

    assert response.status == "succeeded"
    assert response.summary == "Worker submitted its result."
    assert response.terminated_after_result is True
    assert results.peek(token) is True
    assert time.monotonic() - started < 30


def test_a_failing_result_poll_does_not_orphan_the_process(caplog: pytest.LogCaptureFixture) -> None:
    polls = iter([RuntimeError("database is locked"), True])

    def probe() -> bool:
        outcome = next(polls)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    started = time.monotonic()
    result = run_process(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdin_text="",
        cwd=None,
        env=dict(os.environ),
        result_ready=probe,
    )

    assert result.terminated_after_result is True
    assert time.monotonic() - started < 30
    assert "Result inbox poll failed" in caplog.text


def test_provider_smoke_cli_runs_the_subprocess_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry_setup, "_state", None)

    result = CliRunner().invoke(app, ["provider-smoke", "subprocess"])

    assert result.exit_code == 0, result.output
    assert "succeeded: provider smoke passed" in result.output
