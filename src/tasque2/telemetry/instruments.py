"""Metric instruments shared by every Tasque component.

Names follow the OpenTelemetry semantic conventions where one exists
(``gen_ai.client.token.usage``, ``mcp.server.operation.duration``); Tasque-specific
metrics live under the ``tasque.`` prefix.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

import tasque2

_DURATION_BUCKETS = (0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200)
_TOKEN_BUCKETS = (
    1_000,
    5_000,
    10_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    5_000_000,
    20_000_000,
    100_000_000,
)


class Instruments:
    def __init__(self) -> None:
        meter = metrics.get_meter("tasque2", tasque2.__version__)
        self.work_runs = meter.create_counter(
            "tasque.work.runs",
            unit="{run}",
            description="Finished work attempts by outcome.",
        )
        self.work_duration = meter.create_histogram(
            "tasque.work.duration",
            unit="s",
            description="Wall-clock duration of one work attempt.",
            explicit_bucket_boundaries_advisory=_DURATION_BUCKETS,
        )
        self.limit_stops = meter.create_counter(
            "tasque.provider.limit_stops",
            unit="{stop}",
            description="Provider runs stopped by a usage or session limit.",
        )
        self.schedule_occurrences = meter.create_counter(
            "tasque.schedule.occurrences",
            unit="{occurrence}",
            description="Schedule occurrences launched.",
        )
        self.workflow_runs = meter.create_counter(
            "tasque.workflow.runs",
            unit="{run}",
            description="Workflow runs that reached a terminal state.",
        )
        self.token_usage = meter.create_histogram(
            "gen_ai.client.token.usage",
            unit="{token}",
            description="Input and output tokens used per provider run.",
            explicit_bucket_boundaries_advisory=_TOKEN_BUCKETS,
        )
        self.tokens = meter.create_counter(
            "tasque.provider.tokens",
            unit="{token}",
            description="Provider tokens by type, including prompt-cache reads and writes.",
        )
        self.cost = meter.create_counter(
            "tasque.provider.cost",
            unit="USD",
            description="Estimated API-equivalent cost of provider runs.",
        )
        self.turns = meter.create_histogram(
            "tasque.provider.turns",
            unit="{message}",
            description="Model messages per provider run, subagents included.",
            explicit_bucket_boundaries_advisory=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000),
        )
        self.worker_tool_calls = meter.create_counter(
            "tasque.worker.tool_calls",
            unit="{call}",
            description="Tool calls made by workers, as seen in the provider stream.",
        )
        self.mcp_operation_duration = meter.create_histogram(
            "mcp.server.operation.duration",
            unit="s",
            description="Duration of Tasque MCP tool calls.",
            explicit_bucket_boundaries_advisory=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
        )
        self.discord_messages = meter.create_counter(
            "tasque.discord.messages",
            unit="{message}",
            description="Discord messages received and sent.",
        )
        self.tick_duration = meter.create_histogram(
            "tasque.daemon.tick.duration",
            unit="s",
            description="Duration of one daemon tick.",
            explicit_bucket_boundaries_advisory=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
        )
        self.memory_operations = meter.create_counter(
            "tasque.memory.operations",
            unit="{operation}",
            description="Memory writes by operation.",
        )
        self.retention_pruned = meter.create_counter(
            "tasque.retention.pruned",
            unit="{item}",
            description="Artifacts, scratch directories and memories removed by retention passes.",
        )
        self._meter = meter
        self._gauges: list[object] = []

    def observe_queue(self, read_counts: Callable[[], dict[str, int]]) -> None:
        """Report work-item counts by status whenever metrics are collected."""

        def callback(_options: CallbackOptions) -> Iterable[Observation]:
            try:
                counts = read_counts()
            except Exception:  # noqa: BLE001 - a failed read skips one observation
                return []
            return [Observation(count, {"tasque.work.status": status}) for status, count in counts.items()]

        self._gauges.append(
            self._meter.create_observable_gauge(
                "tasque.work.queue.size",
                callbacks=[callback],
                unit="{item}",
                description="Work items by status.",
            )
        )


_instruments: Instruments | None = None
_lock = threading.Lock()


def instruments() -> Instruments:
    global _instruments
    if _instruments is None:
        with _lock:
            if _instruments is None:
                _instruments = Instruments()
    return _instruments


def reset_instruments() -> None:
    global _instruments
    with _lock:
        _instruments = None
