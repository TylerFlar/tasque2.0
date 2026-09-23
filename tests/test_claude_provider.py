from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tasque2.config import WORKER_BUILTIN_TOOLS, reset_settings
from tasque2.db import session_scope
from tasque2.providers import ClaudeCodeProvider, ProviderExecutionError, ProviderRequest, ProviderResponse, mcp_config
from tasque2.providers import claude as claude_provider
from tasque2.providers.claude import missing_result_message
from tasque2.providers.process import ProcessResult
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import RunOutcome, WorkRunner
from tasque2.worker import results
from tasque2.worker.prompt import WORKER_CONTRACT
from tasque2.worker.runtime import ProviderRuntime, model_choice_for

TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


class FakeClaudeProcess:
    """Stands in for ``run_process``: records each launch and answers with a canned stream."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.stdout = _stream(_result("Done."))
        self.stderr = ""
        self.returncode: int | None = 0
        self.terminated_after_result = False
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
        if env.get("TASQUE2_RESULT_TOKEN"):
            results.deposit(
                result_token=env["TASQUE2_RESULT_TOKEN"],
                payload={"summary": "Done.", "report": "Report.", "produces": {}},
            )
        return ProcessResult(
            self.returncode, self.stdout, self.stderr, self.terminated_after_result, launch_error=self.launch_error
        )

    @property
    def argv(self) -> list[str]:
        return self.calls[-1]["argv"]


@pytest.fixture()
def claude_process(monkeypatch: pytest.MonkeyPatch) -> FakeClaudeProcess:
    fake = FakeClaudeProcess()
    monkeypatch.setattr(claude_provider, "run_process", fake)
    return fake


@pytest.fixture()
def user_servers(monkeypatch: pytest.MonkeyPatch) -> dict:
    servers = {"autopilot": {"command": "ap"}, "blender": {"command": "bl"}}
    monkeypatch.setattr(mcp_config, "user_scope_mcp_servers", lambda: servers)
    return servers


def _request(**fields) -> ProviderRequest:
    return ProviderRequest(provider="claude", prompt="Do the work.", **fields)


def _argv(**fields) -> list[str]:
    return ClaudeCodeProvider().build_argv(_request(**fields))


def _flag(argv: list[str], name: str) -> str:
    return argv[argv.index(name) + 1]


def _mcp_servers(argv: list[str]) -> dict:
    return json.loads(_flag(argv, "--mcp-config"))["mcpServers"]


def _stream(*events: dict) -> str:
    return "\n".join(json.dumps(event) for event in events)


def _init(*servers: tuple[str, str]) -> dict:
    return {
        "type": "system",
        "subtype": "init",
        "session_id": "sess-1",
        "model": "claude-sonnet-5",
        "mcp_servers": [{"name": name, "status": status} for name, status in servers],
    }


def _assistant(message_id: str, *blocks: dict) -> dict:
    return {"type": "assistant", "message": {"id": message_id, "model": "claude-sonnet-5", "content": list(blocks)}}


def _tool_error(tool_id: str, content: str) -> dict:
    block = {"type": "tool_result", "tool_use_id": tool_id, "is_error": True, "content": content}
    return {"type": "user", "message": {"content": [block]}}


def _result(text: str, **fields) -> dict:
    return {"type": "result", "subtype": "success", "is_error": False, "result": text, "session_id": "sess-1", **fields}


def _missing(stdout: str) -> str:
    return missing_result_message(ProviderResponse(status="succeeded", summary="", stdout=stdout))


def _run_claude_work(*, runtime_contract: dict | None = None, lane: str = "claude-lane") -> RunOutcome:
    with session_scope() as session:
        WorkRepository(session).create_work_item(
            title="Claude work",
            task_instruction="Do the work.",
            worker_kind="provider.claude",
            runtime_contract=runtime_contract or {},
            lane=lane,
        )
        outcome = WorkRunner(session, provider_runtime=ProviderRuntime()).run_next()
    assert outcome is not None
    return outcome


def test_argv_runs_a_headless_stream_json_session() -> None:
    argv = _argv()

    assert argv[:8] == [
        "claude",
        "--print",
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-mode",
        "bypassPermissions",
        "--mcp-config",
    ]
    assert json.loads(_flag(argv, "--settings")) == {"autoMemoryEnabled": False}
    assert "Do the work." not in argv
    for flag in (
        "--strict-mcp-config",
        "--disallowedTools",
        "--model",
        "--effort",
        "--append-system-prompt-file",
        "--max-turns",
        "--max-budget-usd",
    ):
        assert flag not in argv


def test_argv_uses_the_configured_executable() -> None:
    assert ClaudeCodeProvider(executable="claude.cmd").build_argv(_request())[0] == "claude.cmd"


def test_argv_limits_builtin_tools_to_the_worker_tool_list() -> None:
    expected = [*WORKER_BUILTIN_TOOLS, *(["PowerShell"] if os.name == "nt" else [])]

    assert _flag(_argv(), "--tools") == ",".join(expected)


def test_worker_tools_setting_replaces_the_tool_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_WORKER_TOOLS", "Read, Grep")
    reset_settings()

    assert _flag(_argv(), "--tools") == "Read,Grep"


def test_argv_carries_model_effort_contract_file_and_limits(tmp_path: Path) -> None:
    contract = tmp_path / "contract.md"

    argv = _argv(model="claude-opus-5-5", effort="high", system_prompt_path=contract, max_turns=40, max_budget_usd=2.5)

    assert _flag(argv, "--model") == "claude-opus-5-5"
    assert _flag(argv, "--effort") == "high"
    assert _flag(argv, "--append-system-prompt-file") == str(contract)
    assert _flag(argv, "--max-turns") == "40"
    assert _flag(argv, "--max-budget-usd") == "2.5"
    assert _flag(_argv(max_budget_usd=3.0), "--max-budget-usd") == "3"


def test_auto_memory_setting_drops_the_settings_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_WORKER_AUTO_MEMORY", "true")
    reset_settings()

    assert "--settings" not in _argv()


def test_deny_list_reaches_the_cli_as_one_deduplicated_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_WORKER_DISALLOWED_TOOLS", "WebSearch, mcp__autopilot__fill_login")
    reset_settings()

    argv = _argv(disallowed_tools=["mcp__autopilot__fill_login", " mcp__autopilot__reveal_credentials ", ""])

    assert _flag(argv, "--disallowedTools") == (
        "WebSearch,mcp__autopilot__fill_login,mcp__autopilot__reveal_credentials"
    )


def test_no_deny_list_means_no_disallowed_tools_flag() -> None:
    assert "--disallowedTools" not in _argv()
    assert "--disallowedTools" not in _argv(disallowed_tools=[" "])


def test_without_an_allowlist_only_tasque_is_configured_and_user_servers_are_inherited() -> None:
    argv = _argv()

    assert "--strict-mcp-config" not in argv
    assert list(_mcp_servers(argv)) == ["tasque2"]
    assert _mcp_servers(argv)["tasque2"]["args"] == ["-m", "tasque2.mcp"]


def test_mcp_allowlist_inlines_the_named_servers_and_sets_strict(user_servers: dict) -> None:
    argv = _argv(mcp_servers=["autopilot"])

    assert "--strict-mcp-config" in argv
    servers = _mcp_servers(argv)
    assert set(servers) == {"tasque2", "autopilot"}
    assert servers["autopilot"] == {"command": "ap"}


def test_empty_mcp_allowlist_loads_only_tasque(user_servers: dict) -> None:
    argv = _argv(mcp_servers=[])

    assert "--strict-mcp-config" in argv
    assert set(_mcp_servers(argv)) == {"tasque2"}


def test_unknown_mcp_server_is_rejected(user_servers: dict) -> None:
    with pytest.raises(
        ProviderExecutionError, match="'autopilto', which is not a user-scope MCP server. Known: autopilot"
    ):
        _argv(mcp_servers=["autopilto"])


def test_trace_context_reaches_the_tasque_mcp_server_env() -> None:
    env = {"TRACEPARENT": TRACEPARENT, "TRACESTATE": "vendor=1", "BAGGAGE": "k=v", "TASQUE2_RESULT_TOKEN": "tok"}

    server_env = _mcp_servers(_argv(env=env, work_item_id="work-1"))["tasque2"]["env"]

    assert server_env["TRACEPARENT"] == TRACEPARENT
    assert server_env["TRACESTATE"] == "vendor=1"
    assert server_env["BAGGAGE"] == "k=v"
    assert server_env["TASQUE2_WORK_ITEM_ID"] == "work-1"
    assert "TASQUE2_RESULT_TOKEN" not in server_env


def test_run_sends_the_prompt_on_stdin_with_long_mcp_timeouts(
    claude_process: FakeClaudeProcess, tmp_path: Path
) -> None:
    claude_process.stdout = _stream(
        _init(("tasque2", "connected")), _result("Finished.\nDetails.", total_cost_usd=0.01)
    )

    response = ClaudeCodeProvider().run(_request(cwd=str(tmp_path), env={"EXTRA": "1"}))

    call = claude_process.calls[0]
    assert call["stdin"] == "Do the work."
    assert call["cwd"] == str(tmp_path)
    assert call["env"]["EXTRA"] == "1"
    assert call["env"]["TASQUE2_DATA_DIR"] == os.environ["TASQUE2_DATA_DIR"]
    assert call["env"]["MCP_TIMEOUT"] == "86400000"
    assert call["env"]["MCP_TOOL_TIMEOUT"] == "100000000"
    assert call["result_ready"] is None
    assert call["exit_grace_seconds"] == 20.0
    assert response.status == "succeeded"
    assert response.summary == "Finished."
    assert response.output_text == "Finished.\nDetails."
    assert response.provider_session_id == "sess-1"
    assert response.usage["total_cost_usd"] == 0.01
    assert response.usage["model"] == "claude-sonnet-5"
    assert response.exit_code == 0


def test_request_env_can_set_the_mcp_timeout(claude_process: FakeClaudeProcess) -> None:
    ClaudeCodeProvider().run(_request(env={"MCP_TIMEOUT": "5000"}))

    assert claude_process.calls[0]["env"]["MCP_TIMEOUT"] == "5000"


def test_run_watches_the_result_inbox_for_its_token(fresh_db: Path, claude_process: FakeClaudeProcess) -> None:
    token = results.mint_token()

    ClaudeCodeProvider().run(_request(result_token=token))

    probe = claude_process.calls[0]["result_ready"]
    assert probe() is False
    results.deposit(result_token=token, payload={"summary": "s", "report": "r"})
    assert probe() is True


def test_error_result_event_fails_the_run(claude_process: FakeClaudeProcess) -> None:
    claude_process.returncode = 1
    claude_process.stdout = _stream(
        _init(("tasque2", "connected")),
        {"type": "result", "subtype": "success", "is_error": True, "result": "Claude AI usage limit reached"},
    )

    response = ClaudeCodeProvider().run(_request())

    assert response.status == "failed"
    assert response.summary == "Claude AI usage limit reached"
    assert response.exit_code == 1


def test_run_killed_before_a_result_event_has_a_readable_summary(claude_process: FakeClaudeProcess) -> None:
    claude_process.returncode = 4294967295
    claude_process.stdout = _stream(
        _init(("tasque2", "connected")), _assistant("msg_1", {"type": "text", "text": "Hi"})
    )

    quiet = ClaudeCodeProvider().run(_request())
    claude_process.stderr = "warning: slow\nfatal: connection reset\n"
    noisy = ClaudeCodeProvider().run(_request())

    assert quiet.status == "failed"
    assert quiet.summary == "claude exited with code 4294967295 before emitting a result event"
    assert noisy.summary == "claude exited with code 4294967295 before emitting a result event: fatal: connection reset"


def test_run_without_a_stream_reports_the_last_stderr_line(claude_process: FakeClaudeProcess) -> None:
    claude_process.returncode = 2
    claude_process.stdout = ""

    silent = ClaudeCodeProvider().run(_request())
    claude_process.stderr = "Usage: claude\nerror: unknown option '--bogus'\n"
    explained = ClaudeCodeProvider().run(_request())

    assert silent.summary == "claude exited 2."
    assert explained.summary == "error: unknown option '--bogus'"
    assert explained.status == "failed"


def test_launch_failure_is_a_failed_response(claude_process: FakeClaudeProcess) -> None:
    claude_process.returncode = None
    claude_process.launch_error = "Provider command not found: claude"

    response = ClaudeCodeProvider().run(_request())

    assert response.status == "failed"
    assert response.summary == "Provider command not found: claude"
    assert response.exit_code == 127


def test_run_stopped_after_submitting_counts_as_success(claude_process: FakeClaudeProcess) -> None:
    claude_process.returncode = 1
    claude_process.terminated_after_result = True
    claude_process.stdout = _stream(
        _init(("tasque2", "connected")), _assistant("msg_1", {"type": "text", "text": "Hi"})
    )

    response = ClaudeCodeProvider().run(_request())

    assert response.status == "succeeded"
    assert response.summary == "Worker submitted its result."
    assert response.terminated_after_result is True
    assert response.usage["terminated_after_result"] is True
    assert response.exit_code == 1


def test_missing_result_says_the_worker_never_submitted() -> None:
    assert _missing("") == "The worker did not call submit_worker_result; no result was submitted."


def test_missing_result_names_a_failed_tasque_mcp_server() -> None:
    message = _missing(
        _stream(
            _init(("tasque2", "failed")),
            _result("The tasque2 MCP server is down this session; nothing I can submit."),
        )
    )

    assert message == (
        "The Tasque MCP server ('tasque2') failed to connect at startup, "
        "so submit_worker_result was never available to the worker."
    )


def test_missing_result_reports_a_failed_submit_call() -> None:
    call = {"type": "tool_use", "id": "toolu_9", "name": "mcp__tasque2__submit_worker_result", "input": {}}

    message = _missing(
        _stream(_assistant("msg_1", call), _tool_error("toolu_9", "Error executing tool: user cancelled MCP tool call"))
    )

    assert message == (
        "The worker called submit_worker_result, but the tool call failed: "
        "Error executing tool: user cancelled MCP tool call."
    )


def test_missing_result_quotes_the_closing_words_and_failed_servers() -> None:
    message = _missing(
        _stream(
            _init(("tasque2", "connected"), ("autopilot", "failed")),
            _result("Both research agents are still running in the background; I'll resume once they report back."),
        )
    )

    assert message.startswith("The worker did not call submit_worker_result; no result was submitted.")
    assert "MCP servers that failed to connect: autopilot." in message
    assert 'Final assistant text: "Both research agents are still running in the background;' in message


def test_missing_result_lists_recent_tool_failures_and_trims_long_text() -> None:
    calls = [
        {"type": "tool_use", "id": f"toolu_{index}", "name": f"mcp__autopilot__step{index}", "input": {}}
        for index in range(4)
    ]
    errors = [_tool_error(f"toolu_{index}", f"failure {index}") for index in range(4)]

    message = _missing(_stream(_assistant("msg_1", *calls), *errors, _result("word " * 100)))

    assert "Recent tool failures: mcp__autopilot__step1: failure 1; mcp__autopilot__step2: failure 2; " in message
    assert "step0" not in message
    assert message.endswith("...'")
    assert len(message.split("Final assistant text: ", 1)[1]) == 300 + len("...") + 2


def test_contract_keys_reach_the_claude_command_line(
    fresh_db: Path, claude_process: FakeClaudeProcess, user_servers: dict
) -> None:
    outcome = _run_claude_work(
        runtime_contract={
            "model_profile": "high",
            "mcp_servers": ["autopilot"],
            "disallowed_tools": ["mcp__autopilot__fill_login"],
            "max_turns": 30,
            "max_budget_usd": 1.5,
        }
    )

    argv = claude_process.argv
    assert outcome.status == "succeeded"
    assert _flag(argv, "--model") == "claude-opus-5-5"
    assert _flag(argv, "--effort") == "high"
    assert "--strict-mcp-config" in argv
    assert set(_mcp_servers(argv)) == {"tasque2", "autopilot"}
    assert _flag(argv, "--disallowedTools") == "mcp__autopilot__fill_login"
    assert _flag(argv, "--max-turns") == "30"
    assert _flag(argv, "--max-budget-usd") == "1.5"
    assert Path(_flag(argv, "--append-system-prompt-file")).read_text(encoding="utf-8") == WORKER_CONTRACT
    assert json.loads(_flag(argv, "--settings")) == {"autoMemoryEnabled": False}


def test_default_profile_runs_sonnet_at_medium_effort(fresh_db: Path, claude_process: FakeClaudeProcess) -> None:
    _run_claude_work()

    assert _flag(claude_process.argv, "--model") == "claude-sonnet-5"
    assert _flag(claude_process.argv, "--effort") == "medium"
    assert "--strict-mcp-config" not in claude_process.argv


def test_mcp_server_joins_the_invoke_agent_trace(fresh_db: Path, claude_process: FakeClaudeProcess, spans) -> None:
    _run_claude_work(lane="trace-lane")

    call = claude_process.calls[0]
    traceparent = call["env"]["TRACEPARENT"]
    invoke = next(item for item in spans.get_finished_spans() if item.name == "invoke_agent trace-lane")
    assert traceparent.split("-")[1:3] == [f"{invoke.context.trace_id:032x}", f"{invoke.context.span_id:016x}"]
    assert _mcp_servers(call["argv"])["tasque2"]["env"]["TRACEPARENT"] == traceparent


@pytest.mark.parametrize(
    ("profile", "model", "effort"),
    [
        ("low", "claude-haiku-4-5", None),
        ("medium", "claude-sonnet-5", "medium"),
        ("high", "claude-opus-5-5", "high"),
        ("ultra", "claude-fable-5-1", "high"),
    ],
)
def test_model_profile_picks_the_claude_tier(profile: str, model: str, effort: str | None) -> None:
    choice = model_choice_for("claude", {"model_profile": profile})

    assert (choice.profile, choice.model, choice.effort) == (profile, model, effort)


def test_explicit_model_and_effort_override_the_tier() -> None:
    choice = model_choice_for("claude", {"model_profile": "high", "model": "claude-custom", "effort": "MAX"})

    assert (choice.model, choice.effort) == ("claude-custom", "max")


def test_tier_settings_override_the_claude_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_CLAUDE_MODEL_HIGH", "claude-opus-next")
    monkeypatch.setenv("TASQUE2_CLAUDE_EFFORT_HIGH", "xhigh")
    monkeypatch.setenv("TASQUE2_DEFAULT_MODEL_PROFILE", "high")
    reset_settings()

    choice = model_choice_for("claude", {})

    assert (choice.profile, choice.model, choice.effort) == ("high", "claude-opus-next", "xhigh")


def test_unknown_model_profile_or_effort_is_rejected() -> None:
    with pytest.raises(ValueError, match="model_profile must be one of: low, medium, high, ultra."):
        model_choice_for("claude", {"model_profile": "hint:fast"})
    with pytest.raises(ValueError, match="effort must be one of"):
        model_choice_for("claude", {"effort": "extreme"})
