"""Test providers: an in-process fake and a plain subprocess runner."""

from __future__ import annotations

import os
from typing import Any

from tasque2.providers.base import ProviderRequest, ProviderResponse
from tasque2.providers.claude import request_result_probe
from tasque2.providers.process import run_process


class FakeProvider:
    """Records requests and submits a canned result through the result inbox."""

    name = "fake"

    def __init__(
        self,
        *,
        response: ProviderResponse | None = None,
        result_payload: dict[str, Any] | None = None,
        capture_requests: list[ProviderRequest] | None = None,
        deposit_result: bool = True,
    ) -> None:
        self.response = response or ProviderResponse(
            status="succeeded",
            summary="Fake provider completed.",
            output_text="Fake provider completed.",
            stdout="Fake provider completed.",
        )
        self.result_payload = result_payload or {
            "summary": "Fake provider completed.",
            "report": "Fake provider completed.",
            "produces": {"ok": True},
        }
        self.capture_requests = capture_requests
        self.deposit_result = deposit_result

    def run(self, request: ProviderRequest) -> ProviderResponse:
        from tasque2.worker import results

        if self.capture_requests is not None:
            self.capture_requests.append(request)
        if self.deposit_result and request.result_token:
            results.deposit(
                result_token=request.result_token,
                payload={**self.result_payload, "work_item_id": request.work_item_id},
            )
        return self.response


class SubprocessProvider:
    """Runs ``runtime_contract.argv`` with the prompt on stdin; exit code 0 means success."""

    name = "subprocess"

    def run(self, request: ProviderRequest) -> ProviderResponse:
        if not request.argv:
            return ProviderResponse(
                status="failed",
                summary="No subprocess argv was provided.",
                stderr="runtime_contract.argv is required for provider.subprocess.",
                exit_code=2,
            )
        result = run_process(
            request.argv,
            stdin_text=request.prompt,
            cwd=request.cwd,
            env={**os.environ, **request.env},
            result_ready=request_result_probe(request),
        )
        if result.launch_error:
            return ProviderResponse(status="failed", summary=result.launch_error, stderr=result.stderr, exit_code=127)
        if result.terminated_after_result:
            return ProviderResponse(
                status="succeeded",
                summary="Worker submitted its result.",
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.returncode,
                terminated_after_result=True,
            )
        exit_code = result.returncode if result.returncode is not None else -1
        output = result.stdout.strip()
        return ProviderResponse(
            status="succeeded" if exit_code == 0 else "failed",
            summary=output.splitlines()[0] if output else f"Subprocess exited {exit_code}.",
            output_text=output,
            stdout=result.stdout,
            stderr=result.stderr,
            exit_code=exit_code,
        )
