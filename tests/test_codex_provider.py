from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from tasque2.config import reset_settings
from tasque2.db import session_scope
from tasque2.providers import (
    CodexCliProvider,
    FakeProvider,
    ProviderExecutionError,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
    default_provider_registry,
)
from tasque2.providers import codex as codex_provider
from tasque2.providers.claude import missing_result_message
from tasque2.providers.process import ProcessResult
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner
from tasque2.worker.runtime import ProviderRuntime, model_choice_for

TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
CONTEXT_WINDOW_ERROR = "Codex ran out of room in the model's context window. Start a new thread."


class FakeCodexProcess:
    """Stands in for ``run_process``: records each launch and answers with a canned stream."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.stdout = _stream({"type": "item.completed", "item": {"type": "agent_message", "text": "Done."}})
        self.stderr = ""
        self.returncode: int | None = 0
        self.launch_error: str | None = None

    def __call__(self, argv, *, stdin_text, cwd, env, result_ready=None, exit_grace_seconds=0.0) -> ProcessResult:
        self.calls.append(
            {
                "argv": list(argv),
                "stdin": stdin_text,
                "cwd": cwd,
                "env": dict(env),
                "result_ready": result_ready,
                "exit_grace_seconds": exit_grace_seconds,
            }
        )
        return ProcessResult(self.returncode, self.stdout, self.stderr, False, launch_error=self.launch_error)


@pytest.fixture()
def codex_process(monkeypatch: pytest.MonkeyPatch) -> FakeCodexProcess:
    fake = FakeCodexProcess()
    monkeypatch.setattr(codex_provider, "run_process", fake)
    return fake


def _request(**fields) -> ProviderRequest:
    return ProviderRequest(provider="codex", prompt="Do the work.", **fields)


def _argv(**fields) -> list[str]:
    return CodexCliProvider().build_argv(_request(**fields))


def _config(argv: list[str]) -> dict[str, str]:
    pairs = [argv[index + 1] for index, part in enumerate(argv) if part == "-c"]
    return dict(pair.split("=", 1) for pair in pairs)


def _stream(*events: dict) -> str:
    return "\n".join(json.dumps(event) for event in events)


def _run_as_codex(runtime_contract: dict) -> list[ProviderRequest]:
    captured: list[ProviderRequest] = []
    adapter = FakeProvider(capture_requests=captured)
    adapter.name = "codex"
    registry = ProviderRegistry()
    registry.register(adapter)
    with session_scope() as session:
        WorkRepository(session).create_work_item(
            title="Codex work",
            task_instruction="Do the work.",
            worker_kind="provider.codex",
            runtime_contract=runtime_contract,
        )
        outcome = WorkRunner(session, provider_runtime=ProviderRuntime(registry=registry)).run_next()
    assert outcome is not None
    assert outcome.status == "succeeded"
    return captured


def test_argv_runs_codex_exec_with_the_prompt_on_stdin() -> None:
    argv = _argv()

    assert argv[:4] == ["codex", "exec", "--json", "--dangerously-bypass-approvals-and-sandbox"]
    assert argv[-1] == "-"
    assert "Do the work." not in argv
    assert "--model" not in argv
    assert "--cd" not in argv


def test_argv_configures_the_tasque_mcp_server() -> None:
    config = _config(_argv(work_item_id="work-1"))

    assert config["mcp_servers.tasque2.command"] == json.dumps(sys.executable)
    assert config["mcp_servers.tasque2.args"] == '["-m", "tasque2.mcp"]'
    assert config["mcp_servers.tasque2.tool_timeout_sec"] == "86400"
    assert config["mcp_servers.tasque2.env.TASQUE2_WORK_ITEM_ID"] == '"work-1"'
    assert json.loads(config["mcp_servers.tasque2.env.TASQUE2_DATA_DIR"]) == os.environ["TASQUE2_DATA_DIR"]
    env_keys = [key for key in config if key.startswith("mcp_servers.tasque2.env.")]
    assert env_keys == sorted(env_keys)


def test_argv_carries_cwd_model_and_effort(tmp_path: Path) -> None:
    argv = _argv(cwd=str(tmp_path), model="gpt-codex-5", effort="high")

    assert argv[argv.index("--cd") + 1] == str(tmp_path)
    assert argv[argv.index("--model") + 1] == "gpt-codex-5"
    assert _config(argv)["model_reasoning_effort"] == '"high"'


def test_trace_context_reaches_the_tasque_mcp_server_env() -> None:
    env = {"TRACEPARENT": TRACEPARENT, "TRACESTATE": "vendor=1", "BAGGAGE": "k=v", "TASQUE2_RESULT_TOKEN": "tok"}

    config = _config(_argv(env=env))

    assert config["mcp_servers.tasque2.env.TRACEPARENT"] == json.dumps(TRACEPARENT)
    assert config["mcp_servers.tasque2.env.TRACESTATE"] == '"vendor=1"'
    assert config["mcp_servers.tasque2.env.BAGGAGE"] == '"k=v"'
    assert "mcp_servers.tasque2.env.TASQUE2_RESULT_TOKEN" not in config


def test_run_prepends_the_worker_contract_to_the_prompt(codex_process: FakeCodexProcess, tmp_path: Path) -> None:
    contract = tmp_path / "contract.md"
    contract.write_text("# Contract", encoding="utf-8")

    CodexCliProvider().run(_request(cwd=str(tmp_path), system_prompt_path=contract, env={"EXTRA": "1"}))

    call = codex_process.calls[0]
    assert call["stdin"] == "# Contract\n\nDo the work."
    assert call["cwd"] == str(tmp_path)
    assert call["env"]["EXTRA"] == "1"
    assert call["env"]["TASQUE2_DATA_DIR"] == os.environ["TASQUE2_DATA_DIR"]
    assert call["result_ready"] is None
    assert call["exit_grace_seconds"] == 20.0


def test_run_without_a_contract_file_sends_the_prompt_alone(codex_process: FakeCodexProcess) -> None:
    CodexCliProvider().run(_request())

    assert codex_process.calls[0]["stdin"] == "Do the work."


def test_run_parses_the_codex_stream(codex_process: FakeCodexProcess) -> None:
    codex_process.stdout = _stream(
        {"type": "thread.started", "thread_id": "codex-session"},
        {"type": "item.completed", "item": {"type": "mcp_tool_call", "server": "tasque2", "tool": "memory_recall"}},
        {"type": "item.completed", "item": {"type": "agent_message", "text": "Filed the report.\nMore."}},
        {"type": "turn.completed", "usage": {"input_tokens": 900, "cached_input_tokens": 600, "output_tokens": 40}},
    )

    response = CodexCliProvider().run(_request())

    assert response.status == "succeeded"
    assert response.summary == "Filed the report."
    assert response.provider_session_id == "codex-session"
    assert response.usage["input_tokens"] == 300
    assert response.usage["cache_read_tokens"] == 600
    assert response.usage["output_tokens"] == 40
    assert response.usage["tool_calls"] == {"mcp__tasque2__memory_recall": 1}


def test_failure_summary_uses_the_stream_error_message(codex_process: FakeCodexProcess) -> None:
    codex_process.returncode = 1
    codex_process.stdout = _stream(
        {"type": "thread.started", "thread_id": "t-1"},
        {"type": "error", "message": CONTEXT_WINDOW_ERROR},
        {"type": "turn.failed", "error": {"message": CONTEXT_WINDOW_ERROR}},
    )

    response = CodexCliProvider().run(_request())

    assert response.status == "failed"
    assert response.summary == CONTEXT_WINDOW_ERROR
    assert response.provider_session_id == "t-1"


def test_launch_failure_is_a_failed_response(codex_process: FakeCodexProcess) -> None:
    codex_process.returncode = None
    codex_process.launch_error = "Provider command line too long: codex"

    response = CodexCliProvider().run(_request())

    assert response.status == "failed"
    assert response.summary == "Provider command line too long: codex"
    assert response.exit_code == 127


def test_codex_rejects_a_deny_list_it_cannot_enforce(codex_process: FakeCodexProcess) -> None:
    with pytest.raises(ProviderExecutionError, match="only by the claude provider"):
        CodexCliProvider().run(_request(disallowed_tools=["mcp__autopilot__fill_login"]))

    assert codex_process.calls == []


def test_missing_result_names_the_failed_submit_call() -> None:
    stdout = _stream(
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {
                "type": "mcp_tool_call",
                "tool": "submit_worker_result",
                "status": "failed",
                "error": {"message": "user cancelled MCP tool call"},
            },
        },
    )

    message = missing_result_message(ProviderResponse(status="succeeded", summary="", stdout=stdout))

    assert message == "The worker called submit_worker_result, but the tool call failed: user cancelled MCP tool call."


def test_default_registry_runs_codex_with_the_cli_adapter() -> None:
    assert isinstance(default_provider_registry().get("codex"), CodexCliProvider)


def test_model_profile_picks_the_configured_codex_tier(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_HIGH", "codex-high-test")
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_MEDIUM", "codex-medium-test")
    reset_settings()

    captured = _run_as_codex({"model_profile": "medium"})

    assert captured[0].model == "codex-medium-test"
    assert captured[0].effort is None


def test_explicit_model_overrides_the_codex_tier(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_HIGH", "codex-high-test")
    monkeypatch.setenv("TASQUE2_CODEX_EFFORT_HIGH", "xhigh")
    reset_settings()

    captured = _run_as_codex({"model": "codex-explicit-test", "model_profile": "high"})

    assert captured[0].model == "codex-explicit-test"
    assert captured[0].effort == "xhigh"


def test_codex_tier_without_a_configured_model_is_an_error() -> None:
    with pytest.raises(ValueError, match="TASQUE2_CODEX_MODEL_MEDIUM is required for model_profile=medium."):
        model_choice_for("codex", {})
