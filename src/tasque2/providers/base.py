from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class ProviderRequest:
    """Everything an adapter needs to start one headless agent run."""

    provider: str
    prompt: str
    cwd: str | None = None
    model: str | None = None
    effort: str | None = None
    system_prompt_path: Path | None = None
    env: dict[str, str] = field(default_factory=dict)
    mcp_servers: list[str] | None = None
    disallowed_tools: list[str] = field(default_factory=list)
    max_turns: int | None = None
    max_budget_usd: float | None = None
    result_token: str | None = None
    work_item_id: str | None = None
    argv: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ProviderResponse:
    status: str
    summary: str
    output_text: str = ""
    stdout: str = ""
    stderr: str = ""
    provider_session_id: str | None = None
    usage: dict[str, Any] = field(default_factory=dict)
    exit_code: int | None = None
    terminated_after_result: bool = False

    @property
    def succeeded(self) -> bool:
        return self.status == "succeeded"


class ProviderAdapter(Protocol):
    name: str

    def run(self, request: ProviderRequest) -> ProviderResponse: ...


class ProviderExecutionError(RuntimeError):
    """A provider run failed in a way that is the worker's reported outcome."""


class TransientProviderError(ProviderExecutionError):
    """An infrastructure failure: crash, dropped socket, limit stop, or no submitted result.

    The queue retries these with a floor of attempts independent of ``max_attempts``.
    """


class ProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, ProviderAdapter] = {}

    def register(self, adapter: ProviderAdapter) -> None:
        self._providers[adapter.name] = adapter

    def get(self, name: str) -> ProviderAdapter:
        adapter = self._providers.get(name)
        if adapter is None:
            raise ProviderExecutionError(f"No provider adapter registered for {name!r}.")
        return adapter
