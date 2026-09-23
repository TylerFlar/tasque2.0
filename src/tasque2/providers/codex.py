"""The Codex CLI adapter (``codex exec --json``)."""

from __future__ import annotations

import os

from tasque2.config import get_settings
from tasque2.providers.base import ProviderExecutionError, ProviderRequest, ProviderResponse
from tasque2.providers.claude import request_result_probe, response_from_stream
from tasque2.providers.mcp_config import codex_mcp_args
from tasque2.providers.process import run_process
from tasque2.telemetry import TRACE_ENV_KEYS


class CodexCliProvider:
    name = "codex"

    def __init__(self, *, executable: str = "codex") -> None:
        self.executable = executable

    def build_argv(self, request: ProviderRequest) -> list[str]:
        argv = [self.executable, "exec", "--json", "--dangerously-bypass-approvals-and-sandbox"]
        trace_env = {key: value for key, value in request.env.items() if key in TRACE_ENV_KEYS.values()}
        argv.extend(codex_mcp_args(work_item_id=request.work_item_id, extra_env=trace_env))
        if request.cwd:
            argv.extend(["--cd", request.cwd])
        if request.model:
            argv.extend(["--model", request.model])
        if request.effort:
            argv.extend(["-c", f'model_reasoning_effort="{request.effort}"'])
        argv.append("-")
        return argv

    def run(self, request: ProviderRequest) -> ProviderResponse:
        if request.disallowed_tools:
            raise ProviderExecutionError("runtime_contract.disallowed_tools is enforced only by the claude provider.")
        prompt = request.prompt
        if request.system_prompt_path is not None:
            prompt = f"{request.system_prompt_path.read_text(encoding='utf-8')}\n\n{prompt}"
        result = run_process(
            self.build_argv(request),
            stdin_text=prompt,
            cwd=request.cwd,
            env={**os.environ, **request.env},
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
