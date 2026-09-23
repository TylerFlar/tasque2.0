# Observability

Tasque emits OpenTelemetry traces, metrics and logs from the daemon, the CLI and the MCP server
it starts for each worker. Nothing is exported until an endpoint is configured.

## Turn it on

Any OTLP/HTTP endpoint works: a collector, Grafana Cloud, Honeycomb, Jaeger. For a local stack:

```powershell
docker compose -f deploy/observability/docker-compose.yml up -d
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:4318"
uv run tasque2 telemetry-check      # prints the trace id of a test span
uv run tasque2 daemon
```

Open Grafana at http://localhost:3000: it opens on the Tasque dashboard, and Explore has the raw
traces (Tempo), metrics (Prometheus) and logs (Loki). The stack listens on this computer only
(127.0.0.1) and lets local visitors in without a login.

## The Tasque dashboard

`deploy/observability/grafana/dashboards/tasque-overview.json` is provisioned into Grafana's
"Tasque" folder and set as its home page. A Lane filter and the time picker at the top scope
every panel:

- **At a glance:** cost, runs, failed runs, runs waiting on you, work ready now, usage-limit stops.
- **Cost:** per day, by lane, by model.
- **Runs:** per day by outcome, by lane, and each lane's 90th-percentile run time.
- **Tools:** the tools workers call most, and Tasque tool calls that returned errors.
- **Recent activity:** the latest runs and the ones that failed, need you, or ran over 10 minutes
  (click one for its full trace), and warning and error logs from Tasque and its workers.

Edit the JSON file to change it for good; edits saved in the Grafana UI last until the file
changes.

To have the daemon do all of that on every start, put the compose file in `.env`:

```
TASQUE2_TELEMETRY_STACK=deploy/observability/docker-compose.yml
TASQUE2_TELEMETRY_STACK_OPEN=true
```

`tasque2 daemon` then starts Docker Desktop if the engine isn't running, runs
`docker compose up -d`, points the OTLP endpoint at `http://localhost:4318` unless one is set,
waits until the stack answers, and opens the dashboard when it runs in a terminal. If Docker or
the stack can't come up, the daemon logs why and runs without telemetry. `tasque2 daemon
--no-stack` skips it for one run; `TASQUE2_TELEMETRY_STACK_TIMEOUT_SECONDS` (default 300) bounds
the wait, which is longest the first time the image is pulled.

Settings:

- `TASQUE2_TELEMETRY`: `auto` (export when an OTLP endpoint is set), `otlp`, `console`, `off`.
  `OTEL_SDK_DISABLED=true` always turns it off.
- The standard `OTEL_EXPORTER_OTLP_*` variables pick the endpoint, headers and protocol details.
- Service names: `tasque2` (daemon), `tasque2-cli`, `tasque2-mcp`; `OTEL_SERVICE_NAME` overrides.

## One trace per piece of work

A trace starts where the work starts and follows it through every process:

```
tasque.schedule.fire            (daemon; or tasque.cli queue, tasque.discord.receive, tasque.workflow.start)
└─ tasque.work.run              (daemon, when a worker claims it, possibly minutes later)
   ├─ tasque.worker.context     (building the context packet)
   └─ invoke_agent <lane>       (the Claude Code run: model, tokens, cost)
      ├─ tools/call memory_recall        (the Tasque MCP server)
      ├─ tools/call finance_state
      └─ tools/call submit_worker_result
```

The link across time is the work item's stored `traceparent`; the link across processes is the
`TRACEPARENT` environment variable handed to the agent CLI and the MCP server. Replies in a
Discord thread start their own trace at `tasque.discord.receive`.

Useful span attributes:

| Attribute | On | Meaning |
|---|---|---|
| `tasque.work.lane` | `tasque.work.run` | the lane (schedule or workflow name, `discord-intake`, ...) |
| `tasque.work.status` | `tasque.work.run` | the work item's status after the attempt |
| `gen_ai.request.model`, `tasque.model.profile`, `tasque.model.effort` | `invoke_agent` | what the run used |
| `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.usage.cache_read.input_tokens` | `invoke_agent` | tokens |
| `tasque.provider.estimated_cost_usd` | `invoke_agent` | cost at API list prices |
| `gen_ai.tool.name`, `error.type` | `tools/call` | the MCP tool and whether it failed |

Every recorded event (claimed, retried, dead-lettered, posted to Discord, ...) is also a span
event on whatever span was active, so a trace reads as the run's timeline.

TraceQL examples (Tempo):

```
{ name = "tasque.work.run" && span.tasque.work.lane = "finance-daily" }
{ name =~ "invoke_agent.*" && span.tasque.provider.estimated_cost_usd > 1 }
{ name =~ "tools/call.*" && status = error }
```

## Metrics

| Metric | Type | Labels |
|---|---|---|
| `tasque.work.runs` | counter | lane, outcome, worker kind |
| `tasque.work.duration` | histogram (s) | lane, outcome, worker kind |
| `tasque.work.queue.size` | gauge | status |
| `gen_ai.client.token.usage` | histogram | provider, model, lane, token type |
| `tasque.provider.tokens` / `tasque.provider.cost` | counters | lane, model, token type |
| `tasque.provider.limit_stops` | counter | lane |
| `tasque.worker.tool_calls` | counter | lane, tool, server |
| `mcp.server.operation.duration` | histogram (s) | tool, error type |
| `tasque.schedule.occurrences` | counter | schedule, target |
| `tasque.workflow.runs` | counter | workflow, outcome |
| `tasque.discord.messages` | counter | direction, route |
| `tasque.daemon.tick.duration` | histogram (s) | |
| `tasque.retention.pruned` | counter | kind |
| `tasque.memory.operations` | counter | operation, namespace |

In Prometheus the dots become underscores, counters gain `_total`, and units become suffixes:

```promql
sum by (tasque_work_lane) (increase(tasque_provider_cost_USD_total[7d]))
sum by (tasque_work_lane, tasque_work_outcome) (increase(tasque_work_runs_total[1d]))
histogram_quantile(0.9, sum by (le, tasque_work_lane) (rate(tasque_work_duration_seconds_bucket[1d])))
max(tasque_work_queue_size{tasque_work_status="ready"})
```

## Worker telemetry

With `TASQUE2_TELEMETRY_WORKER_EXPORT=true` (the default) and telemetry on, each Claude Code
worker exports its own metrics and log events (API requests, tool results, token and cost
counters) to the same endpoint, with `tasque.work.lane`, `tasque.work.id` and
`tasque.work.attempt` as resource attributes. `TASQUE2_TELEMETRY_WORKER_TRACES=true` also turns
on Claude Code's per-tool trace spans, nested under `invoke_agent`.

## Logs

Tasque logs go to stderr and, when telemetry is on, to the OTLP logs endpoint with the active
trace and span ids, so a log line in Loki links to its trace. `TASQUE2_LOG_LEVEL` sets the level.
