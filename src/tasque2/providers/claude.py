"""The Claude Code CLI adapter (``claude --print --output-format stream-json``)."""

from __future__ import annotations

import json
import os

from tasque2.config import get_settings
from tasque2.providers.base import ProviderRequest, ProviderResponse
from tasque2.providers.mcp_config import TASQUE_MCP_SERVER_NAME, claude_mcp_config
from tasque2.providers.process import run_process
from tasque2.providers.stream import parse_stream

MCP_STARTUP_TIMEOUT_MS = 24 * 60 * 60 * 1000
MCP_TOOL_TIMEOUT_MS = 100_000_000
MISSING_RESULT_EXCERPT_CHARS = 300


class ClaudeCodeProvider:
    name = "claude"

    def __init__(self, *, executable: str = "claude") -> None:
        self.executable = executable

    def build_argv(self, request: ProviderRequest) -> list[str]:
        settings = get_settings()
        argv = [
            self.executable,
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-mode",
            "bypassPermissions",
            "--mcp-config",
            claude_mcp_config(
                work_item_id=request.work_item_id,
                servers=request.mcp_servers,
                extra_env=_trace_env(request.env),
            ),
        ]
        if request.mcp_servers is not None:
            argv.append("--strict-mcp-config")
        tools = settings.worker_tool_list
        if tools:
            argv.extend(["--tools", ",".join(tools)])
        denied = _dedupe([*settings.worker_disallowed_tool_list, *request.disallowed_tools])
        if denied:
            argv.extend(["--disallowedTools", ",".join(denied)])
        if request.model:
            argv.extend(["--model", request.model])
        if request.effort:
            argv.extend(["--effort", request.effort])
        if request.system_prompt_path is not None:
            argv.extend(["--append-system-prompt-file", str(request.system_prompt_path)])
        if request.max_turns:
            argv.extend(["--max-turns", str(request.max_turns)])
        if request.max_budget_usd:
            argv.extend(["--max-budget-usd", f"{request.max_budget_usd:g}"])
        if not settings.worker_auto_memory:
            argv.extend(["--settings", json.dumps({"autoMemoryEnabled": False})])
        return argv

    def run(self, request: ProviderRequest) -> ProviderResponse:
        env = {**os.environ, **request.env}
        env.setdefault("MCP_TIMEOUT", str(MCP_STARTUP_TIMEOUT_MS))
        env.setdefault("MCP_TOOL_TIMEOUT", str(MCP_TOOL_TIMEOUT_MS))
        result = run_process(
            self.build_argv(request),
            stdin_text=request.prompt,
            cwd=request.cwd,
            env=env,
            result_ready=request_result_probe(request),
            exit_grace_seconds=get_settings().worker_exit_grace_seconds,
        )
        if result.launch_error:
            return ProviderResponse(status="failed", summary=result.launch_error, stderr=result.stderr, exit_code=127)
        return response_from_stream(
            provider=self.name,
            stdout=result.stdout,
            stderr=result.stderr,
            returncode=result.returncode,
            terminated_after_result=result.terminated_after_result,
        )


def response_from_stream(
    *,
    provider: str,
    stdout: str,
    stderr: str,
    returncode: int | None,
    terminated_after_result: bool,
) -> ProviderResponse:
    summary_data = parse_stream(stdout)
    usage = summary_data.usage_record()
    if summary_data.model:
        usage["model"] = summary_data.model
    if terminated_after_result:
        usage["terminated_after_result"] = True
        return ProviderResponse(
            status="succeeded",
            summary="Worker submitted its result.",
            output_text=summary_data.final_text,
            stdout=stdout,
            stderr=stderr,
            provider_session_id=summary_data.session_id,
            usage=usage,
            exit_code=returncode,
            terminated_after_result=True,
        )
    exit_code = returncode if returncode is not None else -1
    if exit_code == 0 and not summary_data.error:
        first_line = next((line.strip() for line in summary_data.final_text.splitlines() if line.strip()), "")
        return ProviderResponse(
            status="succeeded",
            summary=first_line or f"{provider} completed.",
            output_text=summary_data.final_text,
            stdout=stdout,
            stderr=stderr,
            provider_session_id=summary_data.session_id,
            usage=usage,
            exit_code=exit_code,
        )
    error = summary_data.error
    if error is None and summary_data.events:
        error = f"{provider} exited with code {exit_code} before emitting a result event"
        stderr_lines = [line.strip() for line in stderr.splitlines() if line.strip()]
        if stderr_lines:
            error += f": {stderr_lines[-1][:300]}"
    return ProviderResponse(
        status="failed",
        summary=error
        or (stderr.strip().splitlines()[-1][:300] if stderr.strip() else f"{provider} exited {exit_code}."),
        output_text=summary_data.final_text,
        stdout=stdout,
        stderr=stderr,
        provider_session_id=summary_data.session_id,
        usage=usage,
        exit_code=exit_code,
    )


def missing_result_message(response: ProviderResponse) -> str:
    """Why a run ended without a submitted result, in the words most useful for triage."""
    summary = parse_stream(response.stdout)
    submit_failures = [f for f in summary.mcp_tool_failures if f["tool"].endswith("submit_worker_result")]
    if submit_failures:
        return f"The worker called submit_worker_result, but the tool call failed: {submit_failures[-1]['error']}."
    if TASQUE_MCP_SERVER_NAME in summary.failed_mcp_servers:
        return (
            f"The Tasque MCP server ({TASQUE_MCP_SERVER_NAME!r}) failed to connect at startup, "
            "so submit_worker_result was never available to the worker."
        )
    message = "The worker did not call submit_worker_result; no result was submitted."
    if summary.failed_mcp_servers:
        message += f" MCP servers that failed to connect: {', '.join(summary.failed_mcp_servers)}."
    recent = [f"{f['tool']}: {f['error']}" for f in summary.mcp_tool_failures[-3:]]
    if recent:
        message += f" Recent tool failures: {'; '.join(recent)}."
    final_text = " ".join(summary.final_text.split())
    if final_text:
        excerpt = final_text[:MISSING_RESULT_EXCERPT_CHARS]
        if len(final_text) > MISSING_RESULT_EXCERPT_CHARS:
            excerpt += "..."
        message += f" Final assistant text: {excerpt!r}"
    return message


def request_result_probe(request: ProviderRequest):
    if not request.result_token:
        return None
    from tasque2.worker import results

    token = request.result_token
    return lambda: results.peek(token)


def _trace_env(env: dict[str, str]) -> dict[str, str]:
    return {key: value for key, value in env.items() if key in {"TRACEPARENT", "TRACESTATE", "BAGGAGE"}}


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        value = value.strip()
        if value and value not in result:
            result.append(value)
    return result
