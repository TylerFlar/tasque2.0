from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_MODEL_PROVIDERS = {"codex", "claude"}
TEST_MODEL_PROVIDERS = {"fake", "subprocess"}
MODEL_PROFILES = {"low", "medium", "high"}
MODEL_PROFILE_ALIASES = {
    "cheap": "low",
    "code": "high",
    "coding": "high",
    "routine": "medium",
    "deep": "high",
    "fast": "low",
    "reasoning": "high",
    "summarize": "medium",
    "summary": "medium",
    "vision": "high",
    "browser": "medium",
}


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables and optional .env."""

    model_config = SettingsConfigDict(
        env_prefix="TASQUE2_",
        env_file=".env",
        extra="ignore",
    )

    data_dir: Path = Field(default=Path("data"))
    db_path: Path | None = Field(default=None)
    memory_vault_dir: Path | None = Field(default=None)
    timezone: str = Field(default="America/Los_Angeles")
    discord_token: str | None = Field(default=None)
    discord_intake_channel_id: str | None = Field(default=None)
    discord_ops_channel_id: str | None = Field(default=None)
    discord_jobs_channel_id: str | None = Field(default=None)
    discord_chains_channel_id: str | None = Field(default=None)
    discord_dlq_channel_id: str | None = Field(default=None)
    discord_output_poll_seconds: float = Field(default=5.0)
    discord_allowed_user_ids: str | None = Field(default=None)
    discord_max_attachment_bytes: int = Field(default=25 * 1024 * 1024)
    default_provider: str = Field(default="codex")
    native_worker_model_profile: str | None = Field(default=None)
    codex_model_low: str | None = Field(default=None)
    codex_model_medium: str | None = Field(default=None)
    codex_model_high: str | None = Field(default=None)
    claude_model_low: str | None = Field(default=None)
    claude_model_medium: str | None = Field(default=None)
    claude_model_high: str | None = Field(default=None)
    allow_test_providers: bool = Field(default=False)

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
    def default_provider_name(self) -> str:
        provider = self.default_provider.strip()
        if provider.startswith("provider."):
            provider = provider.removeprefix("provider.")
        if not provider:
            raise ValueError("TASQUE2_DEFAULT_PROVIDER cannot be empty.")
        allowed = set(DEFAULT_MODEL_PROVIDERS)
        if self.allow_test_providers:
            allowed.update(TEST_MODEL_PROVIDERS)
        if provider not in allowed:
            if self.allow_test_providers:
                allowed_text = ", ".join(sorted(allowed))
                raise ValueError(f"TASQUE2_DEFAULT_PROVIDER must be one of: {allowed_text}.")
            raise ValueError("TASQUE2_DEFAULT_PROVIDER must be codex or claude.")
        return provider

    def model_for_profile(self, provider: str, profile: str | None = None) -> str | None:
        provider_name = provider.strip()
        if provider_name.startswith("provider."):
            provider_name = provider_name.removeprefix("provider.")
        if provider_name not in DEFAULT_MODEL_PROVIDERS:
            raise ValueError("Model profiles are supported only for codex or claude.")

        profile_name = self._normalize_model_profile(profile)
        if profile_name is None:
            return None

        setting_name = f"{provider_name}_model_{profile_name}"
        model = getattr(self, setting_name)
        if model is None or not model.strip():
            env_name = f"TASQUE2_{provider_name.upper()}_MODEL_{profile_name.upper()}"
            raise ValueError(f"{env_name} is required for model_profile={profile_name}.")
        return model.strip()

    def normalize_model_profile(self, profile: str | None) -> str | None:
        return self._normalize_model_profile(profile)

    def _normalize_model_profile(self, profile: str | None) -> str | None:
        if profile is None:
            return None
        normalized = str(profile).strip().lower()
        if not normalized:
            return None
        if normalized.startswith("hint:"):
            normalized = normalized.removeprefix("hint:").strip()
        normalized = MODEL_PROFILE_ALIASES.get(normalized, normalized)
        if normalized not in MODEL_PROFILES:
            allowed = ", ".join(sorted(MODEL_PROFILES))
            raise ValueError(f"model_profile must be one of: {allowed}.")
        return normalized


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings() -> None:
    get_settings.cache_clear()
