"""Test isolation: each test runs in its own directory with its own settings and database.

Tests never read the repository's ``.env``, data directory or extensions. Spans and metrics
go to in-memory exporters installed once per session; ``spans`` and ``metric_points``
read them.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tasque2.config import get_settings
from tasque2.db import create_schema, reset_engine
from tasque2.extensions import reset_registry
from tasque2.logs import reset_logging
from tasque2.memory.embeddings import reset_embedder_cache
from tasque2.telemetry import shutdown_telemetry
from tasque2.work.retry import capacity_gate

_SPAN_EXPORTER = InMemorySpanExporter()
_METRIC_READER = InMemoryMetricReader()
_ISOLATED_PREFIXES = ("TASQUE2_", "OTEL_", "CLAUDE_CODE_")
_ISOLATED_NAMES = ("TRACEPARENT", "TRACESTATE", "BAGGAGE", "OPENAI_API_KEY")


def pytest_configure(config: pytest.Config) -> None:
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
    trace.set_tracer_provider(tracer_provider)
    metrics.set_meter_provider(MeterProvider(metric_readers=[_METRIC_READER]))


@pytest.fixture(autouse=True)
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    for name in list(os.environ):
        if name.startswith(_ISOLATED_PREFIXES) or name in _ISOLATED_NAMES:
            monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASQUE2_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TASQUE2_EXTENSIONS_DIR", str(tmp_path / "extensions"))
    monkeypatch.setenv("TASQUE2_ALLOW_TEST_PROVIDERS", "true")
    _reset_process_state()
    _SPAN_EXPORTER.clear()
    yield tmp_path
    _reset_process_state()


@pytest.fixture()
def fresh_db(isolated: Path) -> Path:
    create_schema()
    return get_settings().database_path


@pytest.fixture()
def spans() -> InMemorySpanExporter:
    """Finished spans recorded during the test."""
    return _SPAN_EXPORTER


@pytest.fixture()
def metric_points() -> Callable[[str], list[Any]]:
    """Data points recorded so far for a metric name (cumulative across the session)."""

    def read(name: str) -> list[Any]:
        data = _METRIC_READER.get_metrics_data()
        points: list[Any] = []
        for resource_metrics in data.resource_metrics if data else []:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if metric.name == name:
                        points.extend(metric.data.data_points)
        return points

    return read


def _reset_process_state() -> None:
    shutdown_telemetry()
    reset_logging()
    reset_engine()
    reset_registry()
    reset_embedder_cache()
    capacity_gate.reset()
