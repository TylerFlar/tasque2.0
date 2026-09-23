"""W3C trace-context propagation through environment variables.

A provider run starts a subprocess tree (the agent CLI, which starts the Tasque MCP
server). The daemon writes the active span's context into ``TRACEPARENT`` /
``TRACESTATE`` so both subprocesses parent their own spans under the run span.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping

from opentelemetry import context as otel_context
from opentelemetry import propagate

TRACE_ENV_KEYS = {"traceparent": "TRACEPARENT", "tracestate": "TRACESTATE", "baggage": "BAGGAGE"}


def inject_trace_env(env: MutableMapping[str, str]) -> MutableMapping[str, str]:
    """Write the current trace context into ``env`` as TRACEPARENT/TRACESTATE/BAGGAGE."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    for header, variable in TRACE_ENV_KEYS.items():
        value = carrier.get(header)
        if value:
            env[variable] = value
    return env


def context_from_env(environ: Mapping[str, str] | None = None) -> otel_context.Context:
    """The parent context described by TRACEPARENT/TRACESTATE/BAGGAGE in ``environ``."""
    source = os.environ if environ is None else environ
    carrier = {header: source[variable] for header, variable in TRACE_ENV_KEYS.items() if source.get(variable)}
    return propagate.extract(carrier)


def current_traceparent() -> str | None:
    """The W3C traceparent of the active span, or None when nothing is being traced."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


def context_from_traceparent(traceparent: str | None) -> otel_context.Context | None:
    """A parent context for a stored traceparent, or None when there is none."""
    if not traceparent:
        return None
    return propagate.extract({"traceparent": traceparent})
