from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

MODEL_PROVIDERS = ("claude", "codex")
TEST_PROVIDERS = ("fake", "subprocess")
MODEL_PROFILES = ("low", "medium", "high", "ultra")
EFFORT_LEVELS = ("minimal", "low", "medium", "high", "xhigh", "max")

DEFAULT_CLAUDE_MODELS = {
    "low": "claude-haiku-4-5",
    "medium": "claude-sonnet-5",
    "high": "claude-opus-5-5",
    "ultra": "claude-fable-5-1",
}
DEFAULT_CLAUDE_EFFORTS = {"low": None, "medium": "medium", "high": "high", "ultra": "high"}

WORKER_BUILTIN_TOOLS = (
    "Read",
    "Write",
    "Edit",
    "Glob",
    "Grep",
    "Bash",
    "WebFetch",
    "WebSearch",
    "ToolSearch",
    "Agent",
    "SendMessage",
    "TaskStop",
)


@dataclass(frozen=True)
class ModelChoice:
    """The model and reasoning effort one work item runs with."""

    profile: str | None
    model: str | None
    effort: str | None


class Settings(BaseSettings):
    """Runtime settings read from TASQUE2_* environment variables and an optional .env file."""

    model_config = SettingsConfigDict(env_prefix="TASQUE2_", env_file=".env", extra="ignore")

    data_dir: Path = Field(default=Path("data"))
    db_path: Path | None = Field(default=None)
    memory_vault_dir: Path | None = Field(default=None)
    extensions_dir: Path = Field(default=Path("extensions"))
    project_dir: Path | None = Field(default=None)
    timezone: str = Field(default="America/Los_Angeles")
    reminder_default_time: str = Field(default="11:00")

    weather_latitude: float = Field(default=32.7157)
    weather_longitude: float = Field(default=-117.1611)
    weather_location_label: str = Field(default="San Diego, CA")

    discord_token: str | None = Field(default=None)
    discord_intake_channel_id: str | None = Field(default=None)
    discord_ops_channel_id: str | None = Field(default=None)
    discord_jobs_channel_id: str | None = Field(default=None)
    discord_chains_channel_id: str | None = Field(default=None)
    discord_dlq_channel_id: str | None = Field(default=None)
    discord_output_poll_seconds: float = Field(default=5.0)
    discord_allowed_user_ids: str | None = Field(default=None)
    discord_max_attachment_bytes: int = Field(default=25 * 1024 * 1024)

    daemon_concurrency: int = Field(default=1)
    daemon_lease_seconds: int = Field(default=600)
    daemon_tick_seconds: float = Field(default=5.0)
    daemon_max_claims_per_tick: int = Field(default=10)
    daemon_stale_seconds: int = Field(default=90)

    default_provider: str = Field(default="claude")
    default_model_profile: str = Field(default="medium")
    allow_test_providers: bool = Field(default=False)
    claude_model_low: str | None = Field(default=None)
    claude_model_medium: str | None = Field(default=None)
    claude_model_high: str | None = Field(default=None)
    claude_model_ultra: str | None = Field(default=None)
    claude_effort_low: str | None = Field(default=None)
    claude_effort_medium: str | None = Field(default=None)
    claude_effort_high: str | None = Field(default=None)
    claude_effort_ultra: str | None = Field(default=None)
    codex_model_low: str | None = Field(default=None)
    codex_model_medium: str | None = Field(default=None)
    codex_model_high: str | None = Field(default=None)
    codex_model_ultra: str | None = Field(default=None)
    codex_effort_low: str | None = Field(default=None)
    codex_effort_medium: str | None = Field(default=None)
    codex_effort_high: str | None = Field(default=None)
    codex_effort_ultra: str | None = Field(default=None)

    worker_tools: str | None = Field(default=None)
    worker_disallowed_tools: str | None = Field(default=None)
    worker_auto_memory: bool = Field(default=False)
    worker_exit_grace_seconds: float = Field(default=20.0)
    default_mcp_servers: str | None = Field(default=None)

    embedding_provider: str = Field(default="auto")
    embedding_model: str = Field(default="text-embedding-3-small")
    embedding_dim: int = Field(default=256)
    openai_api_key: str | None = Field(default=None)
    memory_hybrid_retrieval: bool = Field(default=True)
    memory_ttl_interval_seconds: int = Field(default=6 * 60 * 60)

    artifact_retention_days: int = Field(default=30)
    artifact_retention_kinds: str = Field(default="provider_stream")
    artifact_retention_interval_seconds: int = Field(default=6 * 60 * 60)
    scratch_retention_days: int = Field(default=7)

    telemetry: str = Field(default="auto")
    telemetry_worker_export: bool = Field(default=True)
    telemetry_worker_traces: bool = Field(default=False)
    telemetry_stack: str | None = Field(default=None)
    telemetry_stack_dashboard: str = Field(default="http://localhost:3000")
    telemetry_stack_open: bool = Field(default=False)
    telemetry_stack_timeout_seconds: int = Field(default=300)

    @property
    def resolved_data_dir(self) -> Path:
        return self.data_dir.expanduser().resolve()

    @property
    def database_path(self) -> Path:
        if self.db_path is not None:
            return self.db_path.expanduser().resolve()
        return self.resolved_data_dir / "tasque2.sqlite3"

    @property
    def resolved_memory_vault_dir(self) -> Path:
        if self.memory_vault_dir is not None:
            return self.memory_vault_dir.expanduser().resolve()
        return self.resolved_data_dir / "memory-vault"

    @property
    def resolved_extensions_dir(self) -> Path:
        return self.extensions_dir.expanduser().resolve()

    @property
    def resolved_project_dir(self) -> Path:
        return (self.project_dir or Path.cwd()).expanduser().resolve()

    @property
    def resolved_scratch_dir(self) -> Path:
        return self.resolved_data_dir / "scratch"

    @property
    def resolved_artifact_dir(self) -> Path:
        return self.resolved_data_dir / "artifacts"

    @property
    def artifact_retention_kind_list(self) -> list[str]:
        return _csv(self.artifact_retention_kinds)

    @property
    def allowed_discord_user_ids(self) -> set[str]:
        return set(_csv(self.discord_allowed_user_ids))

    @property
    def worker_tool_list(self) -> list[str]:
        if self.worker_tools is not None:
            return _csv(self.worker_tools)
        tools = list(WORKER_BUILTIN_TOOLS)
        if os.name == "nt":
            tools.append("PowerShell")
        return tools

    @property
    def worker_disallowed_tool_list(self) -> list[str]:
        return _csv(self.worker_disallowed_tools)

    @property
    def default_mcp_server_list(self) -> list[str] | None:
        if self.default_mcp_servers is None or not self.default_mcp_servers.strip():
            return None
        return _csv(self.default_mcp_servers)

    @property
    def default_provider_name(self) -> str:
        provider = self.default_provider.strip().removeprefix("provider.")
        allowed = set(MODEL_PROVIDERS) | (set(TEST_PROVIDERS) if self.allow_test_providers else set())
        if provider not in allowed:
            raise ValueError(f"TASQUE2_DEFAULT_PROVIDER must be one of: {', '.join(sorted(allowed))}.")
        return provider

    def normalize_model_profile(self, profile: str | None) -> str | None:
        if profile is None:
            return None
        normalized = str(profile).strip().lower()
        if not normalized:
            return None
        if normalized not in MODEL_PROFILES:
            raise ValueError(f"model_profile must be one of: {', '.join(MODEL_PROFILES)}.")
        return normalized

    def model_choice(
        self,
        provider: str,
        profile: str | None = None,
        *,
        model: str | None = None,
        effort: str | None = None,
    ) -> ModelChoice:
        """Resolve the model and effort for ``provider`` from a profile plus explicit overrides."""
        profile_name = self.normalize_model_profile(profile) or self.normalize_model_profile(self.default_model_profile)
        explicit_effort = _normalize_effort(effort)
        if provider not in MODEL_PROVIDERS:
            return ModelChoice(profile=profile_name, model=model, effort=explicit_effort)
        tier_model = getattr(self, f"{provider}_model_{profile_name}") if profile_name else None
        tier_effort = getattr(self, f"{provider}_effort_{profile_name}") if profile_name else None
        if provider == "claude" and profile_name:
            tier_model = tier_model or DEFAULT_CLAUDE_MODELS[profile_name]
            if tier_effort is None:
                tier_effort = DEFAULT_CLAUDE_EFFORTS[profile_name]
        resolved_model = (model or tier_model or "").strip() or None
        if resolved_model is None and profile_name:
            env_name = f"TASQUE2_{provider.upper()}_MODEL_{profile_name.upper()}"
            raise ValueError(f"{env_name} is required for model_profile={profile_name}.")
        return ModelChoice(
            profile=profile_name,
            model=resolved_model,
            effort=explicit_effort or _normalize_effort(tier_effort),
        )


def _normalize_effort(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized or normalized in {"default", "none"}:
        return None
    if normalized not in EFFORT_LEVELS:
        raise ValueError(f"effort must be one of: {', '.join(EFFORT_LEVELS)}.")
    return normalized


def _csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings() -> None:
    get_settings.cache_clear()
