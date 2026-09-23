from __future__ import annotations

import logging

import pytest
from opentelemetry import baggage, trace
from opentelemetry import context as otel_context
from opentelemetry.trace import SpanKind, StatusCode

import tasque2
from tasque2.config import Settings, reset_settings
from tasque2.telemetry import (
    TelemetryMode,
    clean_attributes,
    configure_telemetry,
    context_from_env,
    context_from_traceparent,
    current_traceparent,
    flush_telemetry,
    get_tracer,
    inject_trace_env,
    instruments,
    resolve_telemetry_mode,
    shutdown_telemetry,
    span,
    telemetry_active,
)
from tasque2.telemetry import setup as telemetry_setup
from tasque2.telemetry.setup import _TelemetryState


class RecordingProvider:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[str, int | None]] = []
        self.fail = fail

    def force_flush(self, timeout_millis: int | None = None) -> None:
        self.calls.append(("flush", timeout_millis))
        if self.fail:
            raise RuntimeError("exporter down")

    def shutdown(self) -> None:
        self.calls.append(("shutdown", None))
        if self.fail:
            raise RuntimeError("exporter down")


def _finished(spans, name: str):
    return next(item for item in spans.get_finished_spans() if item.name == name)


@pytest.mark.parametrize(
    ("value", "mode"),
    [
        ("auto", TelemetryMode.OFF),
        ("", TelemetryMode.OFF),
        ("off", TelemetryMode.OFF),
        ("otlp", TelemetryMode.OTLP),
        ("console", TelemetryMode.CONSOLE),
        (" Console ", TelemetryMode.CONSOLE),
    ],
)
def test_telemetry_setting_picks_the_mode(value: str, mode: TelemetryMode) -> None:
    assert resolve_telemetry_mode(Settings(telemetry=value)) is mode


@pytest.mark.parametrize(
    "variable",
    [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    ],
)
def test_auto_exports_over_otlp_when_an_endpoint_is_configured(variable: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(variable, "http://localhost:4318")

    assert resolve_telemetry_mode() is TelemetryMode.OTLP


def test_auto_ignores_a_blank_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "  ")

    assert resolve_telemetry_mode() is TelemetryMode.OFF


def test_mode_is_read_from_the_tasque_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_TELEMETRY", "console")
    reset_settings()

    assert resolve_telemetry_mode() is TelemetryMode.CONSOLE


def test_sdk_disabled_always_turns_telemetry_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_SDK_DISABLED", "TRUE")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

    assert resolve_telemetry_mode(Settings(telemetry="console")) is TelemetryMode.OFF
    assert resolve_telemetry_mode(Settings(telemetry="auto")) is TelemetryMode.OFF


def test_otel_settings_in_the_env_file_reach_the_environment(isolated, monkeypatch: pytest.MonkeyPatch) -> None:
    (isolated / ".env").write_text(
        "TASQUE2_TIMEZONE=UTC\n"
        "OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318\n"
        "OTEL_EXPORTER_OTLP_HEADERS=x-token=from-file\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-token=from-shell")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    copied = telemetry_setup.export_dotenv_otel_settings()

    assert copied == ["OTEL_EXPORTER_OTLP_ENDPOINT"]
    assert telemetry_setup.os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://collector:4318"
    assert telemetry_setup.os.environ["OTEL_EXPORTER_OTLP_HEADERS"] == "x-token=from-shell"
    assert "TASQUE2_TIMEZONE" not in telemetry_setup.os.environ
    assert resolve_telemetry_mode(Settings(telemetry="auto")) is TelemetryMode.OTLP
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT")


def test_unknown_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="TASQUE2_TELEMETRY must be one of: auto, otlp, console, off."):
        resolve_telemetry_mode(Settings(telemetry="jaeger"))


def test_configure_telemetry_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry_setup, "_state", None)
    tracer_provider = trace.get_tracer_provider()

    assert configure_telemetry("daemon") is TelemetryMode.OFF
    assert telemetry_active() is False
    monkeypatch.setenv("TASQUE2_TELEMETRY", "console")
    reset_settings()
    assert configure_telemetry("daemon") is TelemetryMode.OFF
    assert trace.get_tracer_provider() is tracer_provider


def test_configure_telemetry_keeps_an_active_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry_setup, "_state", _TelemetryState(mode=TelemetryMode.OTLP))

    assert configure_telemetry("cli") is TelemetryMode.OTLP
    assert telemetry_active() is True


def test_flush_telemetry_flushes_every_provider_and_tolerates_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    providers = [RecordingProvider(), RecordingProvider(fail=True), RecordingProvider()]
    state = _TelemetryState(
        mode=TelemetryMode.CONSOLE,
        tracer_provider=providers[0],
        meter_provider=providers[1],
        logger_provider=providers[2],
    )
    monkeypatch.setattr(telemetry_setup, "_state", state)

    flush_telemetry(timeout_millis=250)

    assert [provider.calls for provider in providers] == [[("flush", 250)]] * 3


def test_flush_telemetry_does_nothing_while_off(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = RecordingProvider()
    monkeypatch.setattr(telemetry_setup, "_state", _TelemetryState(mode=TelemetryMode.OFF, tracer_provider=provider))
    flush_telemetry()
    monkeypatch.setattr(telemetry_setup, "_state", None)
    flush_telemetry()

    assert provider.calls == []


def test_shutdown_telemetry_detaches_the_log_handler_and_stops_the_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = logging.NullHandler()
    logging.getLogger().addHandler(handler)
    tracer_provider, meter_provider = RecordingProvider(), RecordingProvider(fail=True)
    state = _TelemetryState(
        mode=TelemetryMode.OTLP, tracer_provider=tracer_provider, meter_provider=meter_provider, log_handler=handler
    )
    monkeypatch.setattr(telemetry_setup, "_state", state)

    shutdown_telemetry()

    assert handler not in logging.getLogger().handlers
    assert tracer_provider.calls == [("shutdown", None)]
    assert meter_provider.calls == [("shutdown", None)]
    assert telemetry_active() is False


def test_resource_names_the_service_after_the_role(monkeypatch: pytest.MonkeyPatch) -> None:
    daemon = telemetry_setup._resource("daemon").attributes
    mcp = telemetry_setup._resource("mcp").attributes

    assert daemon["service.name"] == "tasque2"
    assert mcp["service.name"] == "tasque2-mcp"
    assert mcp["tasque.role"] == "mcp"
    assert mcp["service.version"] == tasque2.__version__
    monkeypatch.setenv("OTEL_SERVICE_NAME", "home-tasque")
    assert telemetry_setup._resource("cli").attributes["service.name"] == "home-tasque"


def test_current_traceparent_names_the_active_span() -> None:
    assert current_traceparent() is None

    with span("outer") as outer:
        traceparent = current_traceparent()

    context = outer.get_span_context()
    assert traceparent.split("-")[:3] == ["00", f"{context.trace_id:032x}", f"{context.span_id:016x}"]


def test_inject_trace_env_writes_the_active_trace_context() -> None:
    env = {"KEEP": "1"}

    assert inject_trace_env(env) is env
    assert env == {"KEEP": "1"}
    with span("outer"):
        inject_trace_env(env)
        expected = current_traceparent()

    assert env == {"KEEP": "1", "TRACEPARENT": expected}


def test_baggage_travels_with_the_trace_env() -> None:
    token = otel_context.attach(baggage.set_baggage("tasque.lane", "finance"))
    try:
        env = inject_trace_env({})
    finally:
        otel_context.detach(token)

    assert env == {"BAGGAGE": "tasque.lane=finance"}
    assert baggage.get_baggage("tasque.lane", context_from_env(env)) == "finance"


def test_context_from_env_parents_spans_under_the_injected_span(spans, monkeypatch: pytest.MonkeyPatch) -> None:
    with span("parent") as parent:
        env = inject_trace_env({})
    with span("child", context=context_from_env(env)):
        pass
    monkeypatch.setenv("TRACEPARENT", env["TRACEPARENT"])
    with span("from process env", context=context_from_env()):
        pass

    for name in ("child", "from process env"):
        finished = _finished(spans, name)
        assert finished.context.trace_id == parent.get_span_context().trace_id
        assert finished.parent.span_id == parent.get_span_context().span_id
        assert finished.parent.is_remote is True


def test_context_from_env_without_a_trace_starts_a_new_trace(spans) -> None:
    with span("ambient"), span("detached", context=context_from_env({})):
        pass

    assert _finished(spans, "detached").parent is None


def test_context_from_traceparent_restores_a_stored_parent(spans) -> None:
    assert context_from_traceparent(None) is None
    assert context_from_traceparent("") is None

    with span("enqueue") as enqueue:
        stored = current_traceparent()
    with span("run", context=context_from_traceparent(stored)):
        pass

    run = _finished(spans, "run")
    assert run.parent.span_id == enqueue.get_span_context().span_id
    assert run.context.trace_id == enqueue.get_span_context().trace_id


def test_span_records_an_escaping_exception_and_reraises(spans) -> None:
    with pytest.raises(KeyError), span("failing", attributes={"tasque.work.id": "w1", "skipped": None}):
        raise KeyError("missing")

    failing = _finished(spans, "failing")
    assert failing.status.status_code is StatusCode.ERROR
    assert failing.attributes["error.type"] == "KeyError"
    assert failing.attributes["tasque.work.id"] == "w1"
    assert "skipped" not in failing.attributes
    assert [event.name for event in failing.events] == ["exception"]


def test_span_takes_a_kind_and_uses_the_tasque_tracer(spans) -> None:
    with span("consumer", kind=SpanKind.CONSUMER):
        pass
    with get_tracer().start_as_current_span("direct"):
        pass

    consumer = _finished(spans, "consumer")
    assert consumer.kind is SpanKind.CONSUMER
    assert consumer.status.status_code is StatusCode.UNSET
    assert consumer.instrumentation_scope.name == "tasque2"
    assert _finished(spans, "direct").instrumentation_scope.version == tasque2.__version__


def test_clean_attributes_drops_none_and_stringifies_what_otel_cannot_store() -> None:
    assert clean_attributes(
        {"a": None, "b": 1, "c": True, "d": 1.5, "e": "x", "f": ["x", "y"], "g": ("x",), "h": [1, 2], "i": {"k": 1}}
    ) == {"b": 1, "c": True, "d": 1.5, "e": "x", "f": ["x", "y"], "g": ["x"], "h": "[1, 2]", "i": "{'k': 1}"}
    assert clean_attributes(None) == {}


def test_instruments_are_shared_by_the_process() -> None:
    assert instruments() is instruments()
