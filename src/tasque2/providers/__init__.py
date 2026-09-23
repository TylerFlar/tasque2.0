"""Provider adapters: how a work item becomes a headless agent run."""

from tasque2.config import get_settings
from tasque2.providers.base import (
    ProviderAdapter,
    ProviderExecutionError,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
    TransientProviderError,
)
from tasque2.providers.claude import ClaudeCodeProvider
from tasque2.providers.codex import CodexCliProvider
from tasque2.providers.testing import FakeProvider, SubprocessProvider

DEFAULT_PROVIDER_WORKER_KIND = "provider.default"


def default_provider_registry() -> ProviderRegistry:
    registry = ProviderRegistry()
    for adapter in (ClaudeCodeProvider(), CodexCliProvider(), FakeProvider(), SubprocessProvider()):
        registry.register(adapter)
    return registry


def provider_name_for_worker_kind(worker_kind: str) -> str:
    if worker_kind == DEFAULT_PROVIDER_WORKER_KIND:
        return get_settings().default_provider_name
    return worker_kind.removeprefix("provider.")


__all__ = [
    "DEFAULT_PROVIDER_WORKER_KIND",
    "ClaudeCodeProvider",
    "CodexCliProvider",
    "FakeProvider",
    "ProviderAdapter",
    "ProviderExecutionError",
    "ProviderRegistry",
    "ProviderRequest",
    "ProviderResponse",
    "SubprocessProvider",
    "TransientProviderError",
    "default_provider_registry",
    "provider_name_for_worker_kind",
]
