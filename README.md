# Tasque

Tasque runs your recurring and on-demand AI work on your own machine. Each piece of work is a
headless Claude Code (or Codex) run with a task, a model tier, the MCP tools it may use, and a
compact packet of what it needs to know. Results come back as Discord threads you can reply to,
and every run is traced with OpenTelemetry.

The pieces:

- **Work items**: one run of one task, queued, retried with backoff, dead-lettered when it keeps failing.
- **Schedules**: cron, interval or one-time triggers that queue work items or start workflows.
- **Workflows**: small graphs of work (`work`, `fan_out`, `join`, `gate` nodes) defined in JSON.
- **Lanes**: the name tying a schedule, its workflow nodes and its Discord thread together; usage,
  traces and tiers are all reported per lane.
- **Memory**: canonical documents (a lane's standing rules, "doctrine") plus atomic facts, with
  keyword and vector recall. Workers read and write it through MCP tools.
- **Discord**: plain messages in the intake channel become work; replies in a result's thread
  become follow-up runs of the same lane.
- **Extensions**: your own domains (ledgers, tools, computed digests) as local Python packages.

Everything lives in one SQLite database and a data directory.

## Setup

```powershell
uv sync
Copy-Item .env.example .env   # then fill in Discord ids and any overrides
uv run tasque2 doctor
```

Workers are started with the `claude` CLI, logged in with your subscription or API key. Run
`uv run tasque2 provider-smoke claude` once to see a real run end to end.

Start the daemon (schedules, workflows, workers, retention, Discord):

```powershell
uv run tasque2 daemon
```

Stop it gracefully from another terminal. It finishes the runs in flight, claims nothing new,
and exits:

```powershell
uv run tasque2 daemon-stop
```

## Usage

### A daily brief in its own Discord thread

Write the task as a Markdown template, then schedule it. The template is read each time the
schedule fires, so editing the file changes the next run.

```powershell
uv run tasque2 schedule-create "morning-brief" --type cron --expr "30 6 * * *" `
  --template data/work-templates/brief/brief.template.md `
  --profile medium --thread <discord-thread-id>
```

Every morning the brief posts into that thread. Reply to it ("move the dentist to Friday") and
the reply runs as a follow-up in the same lane, with the same model tier and memory scope.

### One-off work from anywhere

From Discord, post in the intake channel: *"Compare these two lease offers and tell me which is
cheaper over 24 months"* with the PDFs attached. From a terminal:

```powershell
uv run tasque2 queue "Lease comparison" "Compare the attached offers over 24 months." --profile high
uv run tasque2 show <work-item-id>
```

### A research workflow that fans out and waits for you

`data/workflows/job-scout.workflow.json` scouts several sources in parallel, merges the
results, and waits for your go-ahead before acting:

```json
{
  "name": "job-scout",
  "definition": {
    "nodes": [
      {"key": "scout", "kind": "fan_out", "items": [{"name": "remote boards"}, {"name": "local companies"}],
       "child_title_template": "Scout: {item[name]}", "child_worker_kind": "provider.default",
       "child_task_template_path": "../work-templates/job-scout/scout.template.md",
       "runtime_contract": {"model_profile": "medium", "mcp_servers": ["autopilot"]},
       "tolerate_child_failures": true},
      {"key": "merge", "kind": "join", "depends_on": ["scout"]},
      {"key": "shortlist", "title": "Pick the shortlist", "worker_kind": "provider.default",
       "task_template_path": "../work-templates/job-scout/merge.template.md",
       "runtime_contract": {"model_profile": "high"}, "depends_on": ["merge"]},
      {"key": "approve", "kind": "gate", "prompt": "Apply to this shortlist?", "depends_on": ["shortlist"]}
    ]
  }
}
```

```powershell
uv run tasque2 workflow-register data/workflows/job-scout.workflow.json
uv run tasque2 schedule-workflow-create "job-scout" --type cron --expr "0 8 * * MON,WED,FRI" --workflow job-scout
```

Answer the gate with the button on the run's Discord panel, or with
`uv run tasque2 workflow-answer <run-id> approve "yes"`.

### Edit a lane's rules as files

Lanes keep their standing rules as canonical documents that workers update when you tell them
to. To review or rewrite them in an editor:

```powershell
uv run tasque2 doctrine-export doctrine/
# edit doctrine/<namespace>/<key>.md
uv run tasque2 doctrine-apply doctrine/ --dry-run
uv run tasque2 doctrine-apply doctrine/
```

A document a worker changed after the export is reported as drifted and left alone. Each
applied document archives the previous version.

Every rule has one home. When you correct a lane in Discord, its worker rewrites the rule where
it lives and deletes restatements; when two documents disagree, your most recent word decides. A
weekly consolidation pass per namespace (`data/work-templates/memory-consolidate/`) reconciles
what slips through, and leaves an `Open for the user's decision:` line in the lane's state
document when your messages don't settle it. The lane asks you about it once.

### Configure lanes from files and watch what they cost

A lane file, `data/work-templates/<lane>/context.json`, holds what a lane's runs start from: the
documents they pin, the digests they get, and how replies in its thread run (template, model
tier, MCP servers). It is read each time the lane's work is created or a reply is routed, so an
edit changes the next run. A manifest binds schedules and threads to their lane files and sets
tiers, MCP server allowlists, tool bans, and which schedules are on:

```json
{
  "schedules": {
    "finance-daily": {"profile": "high", "context_file": "data/work-templates/finance/context.json",
                      "mcp_servers": ["autopilot", "google-workspace"]}
  },
  "workflows": {"daily-gmail-cleanup": {"report": "low"}},
  "threads": {"<discord-thread-id>": {"context_file": "data/work-templates/finance/context.json"}}
}
```

```powershell
uv run tasque2 lanes-apply data/lanes.json --dry-run
uv run tasque2 lanes-apply data/lanes.json
uv run tasque2 packet --schedule finance-daily   # the packet the next run starts from, with sizes
```

Profiles map to models: `low` Haiku 4.5, `medium` Sonnet 5, `high` Opus 5.5, `ultra` Fable 5.1
(override in `.env`). See every lane's tier, then the last two weeks of tokens and estimated cost:

```powershell
uv run tasque2 lanes
uv run tasque2 usage --days 14
```

### Reminders that cost nothing

A worker that hears "remind me to pay the card on the 26th" calls `reminder_set`; the reminder is
a one-shot schedule whose `function.notify` worker posts `Reminder: pay the card` into that thread
at its time, with no model run. `reminder_list` shows what is coming (a morning brief can list
today's), `reminder_cancel` drops one, and past reminders are pruned after a month.

### Run a daily job only when it has something to do

An extension registers a gate, and a schedule names it in its payload:

```python
registry.add_schedule_gate("finance_due", lambda session, schedule, when: None if bills_due(session) else "nothing due")
```

```powershell
uv run tasque2 schedule-edit <schedule-id> --payload-json '{"gate": "finance_due", ...}'
```

When the gate returns a reason, the occurrence is recorded as skipped and no worker starts. A gate
that fails lets the run go, and `schedule-fire-now` ignores gates.

### Trace a run end to end

Point Tasque at any OTLP endpoint and every schedule fire, work run, model invocation and MCP
tool call lands in one trace. [docs/observability.md](docs/observability.md) has a one-command
local Grafana stack and what to look for.

```powershell
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:4318"
uv run tasque2 telemetry-check
```

## Commands

| Area | Commands |
|---|---|
| Daemon | `daemon`, `daemon-status`, `daemon-stop`, `tick` |
| Work | `queue`, `list`, `show`, `events`, `pause`, `resume`, `cancel`, `retry`, `run-next`, `report-work` |
| Schedules | `schedule-create`, `schedule-workflow-create`, `schedule-list`, `schedule-show`, `schedule-edit`, `schedule-enable`, `schedule-disable`, `schedule-delete`, `schedule-fire-now` |
| Workflows | `workflow-register`, `workflow-validate`, `workflow-start`, `workflow-list`, `workflow-runs`, `workflow-show`, `workflow-answer`, `workflow-cancel` |
| Memory | `memory-add`, `memory-search`, `memory-show`, `memory-archive`, `memory-delete`, `memory-ingest-text`, `memory-embed`, `memory-prune`, `doctrine-export`, `doctrine-apply` |
| Artifacts | `artifact-list`, `artifact-capture`, `artifact-archive` |
| Operations | `doctor`, `status`, `usage`, `lanes`, `lanes-apply`, `smoke`, `provider-smoke`, `telemetry-check`, `discord-output-simulate`, `migrate`, `db-status`, `backup-create`, `backup-restore`, `reset-jobs` |

`uv run tasque2 <command> --help` shows each command's options.

## Layout

- `src/tasque2/`: the application (`work/`, `daemon/`, `worker/`, `providers/`, `discord/`,
  `memory/`, `mcp/`, `telemetry/`, `ops/`, `cli/`)
- `alembic/`: the core schema migrations
- `extensions/`: local extension packages ([extensions/README.md](extensions/README.md))
- `data/`: the database, artifacts, templates and workflow files (not tracked)
- `docs/`: [observability](docs/observability.md), [workflows](docs/workflows.md)

Tests: `uv run pytest` (core) and `uv run pytest extensions/<name>/tests` (an extension).
