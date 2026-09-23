from __future__ import annotations

import atexit
import logging
import os
import threading
from dataclasses import dataclass
from enum import StrEnum

from opentelemetry import metrics, trace
from opentelemetry.sdk.resources import Resource

import tasque2
from tasque2.config import Settings, get_settings

logger = logging.getLogger(__name__)

_OTLP_ENDPOINT_VARS = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
)


class TelemetryMode(StrEnum):
    OFF = "off"
    OTLP = "otlp"
    CONSOLE = "console"


@dataclass
class _TelemetryState:
    mode: TelemetryMode
    tracer_provider: object | None = None
    meter_provider: object | None = None
    logger_provider: object | None = None
    log_handler: logging.Handler | None = None


_state: _TelemetryState | None = None
_lock = threading.Lock()


def resolve_telemetry_mode(settings: Settings | None = None) -> TelemetryMode:
    """Pick the exporter mode from TASQUE2_TELEMETRY and the standard OTEL_* variables.

    ``auto`` exports over OTLP when an OTLP endpoint is configured and stays off
    otherwise. ``OTEL_SDK_DISABLED=true`` always wins.
    """
    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return TelemetryMode.OFF
    value = (settings or get_settings()).telemetry.strip().lower()
    if value in {"", "auto"}:
        has_endpoint = any(os.environ.get(name, "").strip() for name in _OTLP_ENDPOINT_VARS)
        return TelemetryMode.OTLP if has_endpoint else TelemetryMode.OFF
    try:
        return TelemetryMode(value)
    except ValueError:
        raise ValueError("TASQUE2_TELEMETRY must be one of: auto, otlp, console, off.") from None


def export_dotenv_otel_settings(path: str = ".env") -> list[str]:
    """Copy OTEL_* entries from the .env file into the environment, where the SDK and workers read them.

    Variables already set in the environment win. Returns the names that were copied.
    """
    from dotenv import dotenv_values

    copied = []
    for name, value in dotenv_values(path).items():
        if name.startswith("OTEL_") and value and name not in os.environ:
            os.environ[name] = value
            copied.append(name)
    return copied


def telemetry_active() -> bool:
    return _state is not None and _state.mode is not TelemetryMode.OFF


def configure_telemetry(role: str, *, settings: Settings | None = None) -> TelemetryMode:
    """Install tracer, meter and logger providers for this process. Idempotent."""
    global _state
    with _lock:
        if _state is not None:
            return _state.mode
        export_dotenv_otel_settings()
        mode = resolve_telemetry_mode(settings)
        _state = _TelemetryState(mode=mode)
        if mode is TelemetryMode.OFF:
            return mode
        resource = _resource(role)
        _state.tracer_provider = _install_tracing(mode, resource)
        _state.meter_provider = _install_metrics(mode, resource)
        _state.logger_provider, _state.log_handler = _install_logging(mode, resource)
        atexit.register(shutdown_telemetry)
        logger.info("Telemetry enabled (%s) for role %s", mode.value, role)
        return mode


def flush_telemetry(timeout_millis: int = 5000) -> None:
    """Export anything buffered now; used by short-lived processes before they can be killed."""
    state = _state
    if state is None or state.mode is TelemetryMode.OFF:
        return
    for provider in (state.tracer_provider, state.meter_provider, state.logger_provider):
        flush = getattr(provider, "force_flush", None)
        if flush is None:
            continue
        try:
            flush(timeout_millis=timeout_millis)
        except Exception:  # noqa: BLE001 - telemetry must never break the caller
            logger.debug("Telemetry flush failed", exc_info=True)


def shutdown_telemetry() -> None:
    global _state
    with _lock:
        state = _state
        _state = None
    if state is None or state.mode is TelemetryMode.OFF:
        return
    if state.log_handler is not None:
        logging.getLogger().removeHandler(state.log_handler)
    for provider in (state.tracer_provider, state.meter_provider, state.logger_provider):
        shutdown = getattr(provider, "shutdown", None)
        if shutdown is None:
            continue
        try:
            shutdown()
        except Exception:  # noqa: BLE001 - shutdown is best effort
            logger.debug("Telemetry shutdown failed", exc_info=True)


def _resource(role: str) -> Resource:
    attributes: dict[str, str] = {
        "service.version": tasque2.__version__,
        "tasque.role": role,
    }
    if not os.environ.get("OTEL_SERVICE_NAME"):
        attributes["service.name"] = "tasque2" if role == "daemon" else f"tasque2-{role}"
    return Resource.create(attributes)


def _install_tracing(mode: TelemetryMode, resource: Resource):
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    provider = TracerProvider(resource=resource)
    if mode is TelemetryMode.OTLP:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        exporter = OTLPSpanExporter()
    else:
        exporter = ConsoleSpanExporter()
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return provider


def _install_metrics(mode: TelemetryMode, resource: Resource):
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import (
        ConsoleMetricExporter,
        PeriodicExportingMetricReader,
    )

    if mode is TelemetryMode.OTLP:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

        exporter = OTLPMetricExporter()
    else:
        exporter = ConsoleMetricExporter()
    provider = MeterProvider(resource=resource, metric_readers=[PeriodicExportingMetricReader(exporter)])
    metrics.set_meter_provider(provider)
    return provider


def _install_logging(mode: TelemetryMode, resource: Resource):
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogExporter

    if mode is TelemetryMode.OTLP:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        exporter = OTLPLogExporter()
    else:
        exporter = ConsoleLogExporter()
    provider = LoggerProvider(resource=resource)
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    set_logger_provider(provider)
    handler = LoggingHandler(level=logging.INFO, logger_provider=provider)
    logging.getLogger().addHandler(handler)
    return provider, handler
