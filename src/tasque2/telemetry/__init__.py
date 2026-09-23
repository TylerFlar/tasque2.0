"""OpenTelemetry integration: one setup call per process, shared tracer and instruments.

Every Tasque process calls :func:`configure_telemetry` once at startup with its role
(``daemon``, ``mcp``, ``cli``). When telemetry is off, the OpenTelemetry API hands out
no-op tracers and meters, so instrumented code never needs to check whether it is enabled.
"""

from tasque2.telemetry.context import (
    TRACE_ENV_KEYS,
    context_from_env,
    context_from_traceparent,
    current_traceparent,
    inject_trace_env,
)
from tasque2.telemetry.instruments import instruments, reset_instruments
from tasque2.telemetry.setup import (
    TelemetryMode,
    configure_telemetry,
    flush_telemetry,
    resolve_telemetry_mode,
    shutdown_telemetry,
    telemetry_active,
)
from tasque2.telemetry.tracing import clean_attributes, get_tracer, record_exception, span

__all__ = [
    "TRACE_ENV_KEYS",
    "TelemetryMode",
    "clean_attributes",
    "configure_telemetry",
    "context_from_env",
    "context_from_traceparent",
    "current_traceparent",
    "flush_telemetry",
    "get_tracer",
    "inject_trace_env",
    "instruments",
    "record_exception",
    "reset_instruments",
    "resolve_telemetry_mode",
    "shutdown_telemetry",
    "span",
    "telemetry_active",
]
