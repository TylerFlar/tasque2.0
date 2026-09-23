from __future__ import annotations

import os
from pathlib import Path

import pytest

from tasque2.config import Settings
from tasque2.telemetry import stack


class Clock:
    """A fake monotonic clock whose sleep advances time instead of waiting."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def compose_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # Registered so the endpoint the stack sets is removed again after each test.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    path = tmp_path / "docker-compose.yml"
    path.write_text("services: {}\n", encoding="utf-8")
    return path


def _up(monkeypatch: pytest.MonkeyPatch, *, answering: bool = True) -> list[Path]:
    started: list[Path] = []
    monkeypatch.setattr(stack, "docker_ready", lambda: True)
    monkeypatch.setattr(stack, "compose_up", lambda path, timeout: started.append(path) or None)
    monkeypatch.setattr(stack, "answers", lambda url: answering)
    return started


def test_nothing_happens_without_a_stack(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stack, "docker_ready", lambda: pytest.fail("docker was checked"))

    assert stack.ensure_telemetry_stack(Settings()) is False


def test_the_stack_comes_up_and_telemetry_points_at_it(monkeypatch: pytest.MonkeyPatch, compose_file: Path) -> None:
    started = _up(monkeypatch)
    opened: list[str] = []
    monkeypatch.setattr(stack.webbrowser, "open", opened.append)
    settings = Settings(telemetry_stack=str(compose_file), telemetry_stack_open=True)

    assert stack.ensure_telemetry_stack(settings, interactive=True) is True
    assert started == [compose_file]
    assert os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://localhost:4318"
    assert opened == ["http://localhost:3000"]


def test_the_dashboard_opens_only_when_asked_and_in_a_terminal(
    monkeypatch: pytest.MonkeyPatch, compose_file: Path
) -> None:
    _up(monkeypatch)
    opened: list[str] = []
    monkeypatch.setattr(stack.webbrowser, "open", opened.append)

    wanted = Settings(telemetry_stack=str(compose_file), telemetry_stack_open=True)
    stack.ensure_telemetry_stack(wanted, interactive=False)
    stack.ensure_telemetry_stack(Settings(telemetry_stack=str(compose_file)), interactive=True)

    assert opened == []


def test_an_endpoint_already_set_is_kept(monkeypatch: pytest.MonkeyPatch, compose_file: Path) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    _up(monkeypatch)

    assert stack.ensure_telemetry_stack(Settings(telemetry_stack=str(compose_file)), interactive=False) is True
    assert os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://collector:4318"


def test_a_stopped_docker_is_started_and_waited_for(monkeypatch: pytest.MonkeyPatch, compose_file: Path) -> None:
    states = iter([False, False, True])
    launched: list[bool] = []
    started = _up(monkeypatch)
    monkeypatch.setattr(stack, "docker_ready", lambda: next(states))
    monkeypatch.setattr(stack, "start_docker_desktop", lambda: launched.append(True) or True)
    clock = Clock()

    settings = Settings(telemetry_stack=str(compose_file))
    assert stack.ensure_telemetry_stack(settings, interactive=False, sleep=clock.sleep, clock=clock) is True
    assert launched == [True]
    assert started == [compose_file]


def test_the_daemon_runs_without_the_stack_when_docker_never_starts(
    monkeypatch: pytest.MonkeyPatch, compose_file: Path
) -> None:
    monkeypatch.setattr(stack, "docker_ready", lambda: False)
    monkeypatch.setattr(stack, "start_docker_desktop", lambda: True)
    monkeypatch.setattr(stack, "compose_up", lambda path, timeout: pytest.fail("compose ran"))
    clock = Clock()

    settings = Settings(telemetry_stack=str(compose_file), telemetry_stack_timeout_seconds=30)
    assert stack.ensure_telemetry_stack(settings, sleep=clock.sleep, clock=clock) is False
    assert os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == ""


def test_no_docker_or_a_failed_compose_leaves_telemetry_off(
    monkeypatch: pytest.MonkeyPatch, compose_file: Path
) -> None:
    settings = Settings(telemetry_stack=str(compose_file))
    monkeypatch.setattr(stack, "docker_ready", lambda: False)
    monkeypatch.setattr(stack, "start_docker_desktop", lambda: False)
    assert stack.ensure_telemetry_stack(settings) is False

    monkeypatch.setattr(stack, "docker_ready", lambda: True)
    monkeypatch.setattr(stack, "compose_up", lambda path, timeout: "pull access denied")
    assert stack.ensure_telemetry_stack(settings) is False
    assert os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == ""
    assert stack.ensure_telemetry_stack(Settings(telemetry_stack=str(compose_file.parent / "missing.yml"))) is False


def test_a_slow_stack_still_gets_the_endpoint(monkeypatch: pytest.MonkeyPatch, compose_file: Path) -> None:
    _up(monkeypatch, answering=False)
    clock = Clock()

    settings = Settings(telemetry_stack=str(compose_file), telemetry_stack_timeout_seconds=20)
    assert stack.ensure_telemetry_stack(settings, interactive=False, sleep=clock.sleep, clock=clock) is True
    assert os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://localhost:4318"


def test_a_relative_compose_path_resolves_against_the_project(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, compose_file: Path
) -> None:
    (tmp_path / "deploy").mkdir()
    (tmp_path / "deploy" / "stack.yml").write_text("services: {}\n", encoding="utf-8")
    started = _up(monkeypatch)

    settings = Settings(telemetry_stack="deploy/stack.yml", project_dir=tmp_path)
    assert stack.ensure_telemetry_stack(settings, interactive=False) is True
    assert started == [tmp_path / "deploy" / "stack.yml"]
