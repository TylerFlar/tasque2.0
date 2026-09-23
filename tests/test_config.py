from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import tasque2.config as config_module
from tasque2.config import WORKER_BUILTIN_TOOLS, ModelChoice, Settings


@pytest.mark.parametrize(
    ("profile", "expected"),
    [
        ("low", ModelChoice(profile="low", model="claude-haiku-4-5", effort=None)),
        ("medium", ModelChoice(profile="medium", model="claude-sonnet-5", effort="medium")),
        ("high", ModelChoice(profile="high", model="claude-opus-5-5", effort="high")),
        ("ultra", ModelChoice(profile="ultra", model="claude-fable-5-1", effort="high")),
    ],
)
def test_model_choice_maps_each_claude_profile_to_its_default_tier(profile: str, expected: ModelChoice) -> None:
    assert Settings().model_choice("claude", profile) == expected


def test_model_choice_without_a_profile_uses_the_default_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    assert Settings().model_choice("claude").profile == "medium"

    monkeypatch.setenv("TASQUE2_DEFAULT_MODEL_PROFILE", "high")

    assert Settings().model_choice("claude") == ModelChoice(profile="high", model="claude-opus-5-5", effort="high")


def test_model_choice_normalizes_profile_spelling() -> None:
    assert Settings().model_choice("claude", " ULTRA ").profile == "ultra"


def test_explicit_model_and_effort_override_the_tier() -> None:
    choice = Settings().model_choice("claude", "low", model="claude-custom", effort="MAX")

    assert choice == ModelChoice(profile="low", model="claude-custom", effort="max")


def test_tier_settings_override_the_claude_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_CLAUDE_MODEL_HIGH", "claude-opus-next")
    monkeypatch.setenv("TASQUE2_CLAUDE_EFFORT_HIGH", "xhigh")
    monkeypatch.setenv("TASQUE2_CLAUDE_EFFORT_ULTRA", "default")

    settings = Settings()

    assert settings.model_choice("claude", "high") == ModelChoice(
        profile="high", model="claude-opus-next", effort="xhigh"
    )
    assert settings.model_choice("claude", "ultra").effort is None


def test_codex_tiers_come_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="TASQUE2_CODEX_MODEL_MEDIUM is required"):
        Settings().model_choice("codex", "medium")

    monkeypatch.setenv("TASQUE2_CODEX_MODEL_MEDIUM", "gpt-codex")
    monkeypatch.setenv("TASQUE2_CODEX_EFFORT_MEDIUM", "high")

    assert Settings().model_choice("codex", "medium") == ModelChoice(profile="medium", model="gpt-codex", effort="high")
    assert Settings().model_choice("codex", "low", model="gpt-small").model == "gpt-small"


def test_test_providers_take_only_explicit_models() -> None:
    choice = Settings().model_choice("fake", "high", model="anything", effort="low")

    assert choice == ModelChoice(profile="high", model="anything", effort="low")
    assert Settings().model_choice("subprocess", "low") == ModelChoice(profile="low", model=None, effort=None)


def test_invalid_effort_is_rejected() -> None:
    with pytest.raises(ValueError, match="effort must be one of"):
        Settings().model_choice("claude", "high", effort="turbo")


def test_invalid_profile_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="model_profile must be one of"):
        Settings().model_choice("claude", "extreme")

    monkeypatch.setenv("TASQUE2_DEFAULT_MODEL_PROFILE", "enormous")

    with pytest.raises(ValueError, match="model_profile must be one of"):
        Settings().model_choice("claude")


def test_default_provider_name_strips_the_prefix_and_gates_test_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DEFAULT_PROVIDER", "provider.codex")
    assert Settings().default_provider_name == "codex"

    monkeypatch.setenv("TASQUE2_DEFAULT_PROVIDER", "fake")
    assert Settings().default_provider_name == "fake"

    monkeypatch.setenv("TASQUE2_ALLOW_TEST_PROVIDERS", "false")
    with pytest.raises(ValueError, match="TASQUE2_DEFAULT_PROVIDER must be one of: claude, codex"):
        _ = Settings().default_provider_name


def test_worker_tool_list_adds_powershell_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings()

    monkeypatch.setattr(config_module, "os", SimpleNamespace(name="nt"))
    assert settings.worker_tool_list == [*WORKER_BUILTIN_TOOLS, "PowerShell"]

    monkeypatch.setattr(config_module, "os", SimpleNamespace(name="posix"))
    assert settings.worker_tool_list == list(WORKER_BUILTIN_TOOLS)


def test_worker_tool_list_setting_replaces_the_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_WORKER_TOOLS", "Read, Grep,,Bash ")
    monkeypatch.setenv("TASQUE2_WORKER_DISALLOWED_TOOLS", "WebFetch")

    settings = Settings()

    assert settings.worker_tool_list == ["Read", "Grep", "Bash"]
    assert settings.worker_disallowed_tool_list == ["WebFetch"]


def test_default_mcp_server_list(monkeypatch: pytest.MonkeyPatch) -> None:
    assert Settings().default_mcp_server_list is None

    monkeypatch.setenv("TASQUE2_DEFAULT_MCP_SERVERS", "   ")
    assert Settings().default_mcp_server_list is None

    monkeypatch.setenv("TASQUE2_DEFAULT_MCP_SERVERS", "tasque, google-workspace")
    assert Settings().default_mcp_server_list == ["tasque", "google-workspace"]


def test_paths_resolve_from_the_data_directory(isolated: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = (isolated / "data").resolve()
    settings = Settings()

    assert settings.database_path == data / "tasque2.sqlite3"
    assert settings.resolved_artifact_dir == data / "artifacts"
    assert settings.resolved_scratch_dir == data / "scratch"
    assert settings.resolved_memory_vault_dir == data / "memory-vault"

    monkeypatch.setenv("TASQUE2_DB_PATH", str(isolated / "elsewhere.sqlite3"))

    assert Settings().database_path == (isolated / "elsewhere.sqlite3").resolve()


def test_csv_settings_split_into_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DISCORD_ALLOWED_USER_IDS", "111, 222")
    monkeypatch.setenv("TASQUE2_ARTIFACT_RETENTION_KINDS", "provider_stream,worker_report")

    settings = Settings()

    assert settings.allowed_discord_user_ids == {"111", "222"}
    assert settings.artifact_retention_kind_list == ["provider_stream", "worker_report"]
