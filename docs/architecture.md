# Architecture

Tasque runs recurring and on-demand AI work on one machine: each piece of work is a headless agent
run with a task, a model tier, the tools it may use, and a compact packet of what it needs to know.
A personal assistant is one thing to build on it. A research pipeline that fans out and waits for a
decision, a watch that reads an inbox every quarter hour, or a codebase that maintains itself are
others. This page says what the core holds, where its edges are, and where the code is going, so
that someone setting Tasque up for their own use knows what is theirs to add or leave out. A change
that moves something updates it.

## The core

Everything under `src/tasque2` is the public core: the part every use needs. By package:

| Package | What it holds |
|---|---|
| `config.py`, `db.py`, `models.py`, `migrations.py` | the settings (`TASQUE2_*` variables, with an optional env file), the SQLite database, every table, and the Alembic upgrade that runs the core's revisions and each extension's together |
| `work/` | work items: the queue (claims, leases, retries, dead letters), the repository that creates them, the retry policy, and the runner that hands a claimed item to a provider or to a function worker (in-process work with no model) |
| `schedules.py`, `reminders.py` | cron, interval and one-time triggers that queue work or start workflows; a reminder is a one-time schedule that posts a line with no model run |
| `workflows.py` | workflow definitions and runs: graphs of work, fan-out, join and gate nodes, with rounds and rewinds |
| `worker/` | one provider run end to end: the context packet, the prompt, the runtime, the result inbox |
| `providers/` | the adapters that turn a work item into a headless agent run (Claude Code, Codex, and test doubles), their event streams, pricing and MCP configuration |
| `mcp/` | the stdio MCP server a run talks to, and the core tools: memory, artifacts, work, schedules, workflows, reminders, images, system |
| `memory/` | durable memory: canonical documents (a lane's standing rules, its doctrine), atomic facts, keyword and vector recall, text ingestion, the vault |
| `artifacts.py`, `scratch.py` | files a run was given or produced, and each run's scratch directory |
| `daemon/` | the daemon: the tick loop (recover, schedule, advance workflows, dispatch, keep house), the worker pool, graceful stop, restarts |
| `discord/` | the Discord interface: the bot, inbound routing, outbound posting, panels and cards, uploads, voice |
| `cli/` | the `tasque2` command line |
| `ops/` | operator tooling: status, doctor, reports, usage, lanes, backups, releases, the config repository, the privacy scan, the rehearsal |
| `telemetry/` | OpenTelemetry setup, tracing and instruments |
| `workshop/` | the Workshop: how Tasque changes itself (the sweep, the ledger, triage, idea cards, the monthly look, goal reviews, the weekly refactor pass, the change pipeline and its checks) |
| the rest at the top | extension discovery (`extensions.py`), lane files (`lane_context.py`), local dates (`localtime.py`), sticky notes (`sticky.py`), weather (`weather.py`), and small helpers for templates, text, events and logs |

## The interfaces

Two ways in and out today:

- **Discord.** Plain messages in an intake channel become work; replies in a result's thread become
  follow-up runs of the same lane; finished work posts into threads, with panels and cards for gates.
  Voice messages are transcribed on the machine first.
- **The command line.** `tasque2 queue`, `show`, `schedule-create`, `workflow-start`, `workflow-answer`
  and the rest create and inspect everything the daemon runs, with no Discord at all. Results stay in
  the database and the artifacts.

Discord is configured by settings; without them the daemon runs its schedules, workflows and workers
and posts nothing.

## Extensions and the data directory

A deployment's own domains are extensions: Python packages under `extensions/` (ignored by git) that
register tables, migrations, MCP tools, context digests, canonical-key resolvers, function workers,
schedule gates and signal sources. [extensions/README.md](../extensions/README.md) shows each hook.

The data directory (`data/`, not tracked by the core) holds the database, artifacts, scratch and
backups beside the config a deployment edits: work templates and lane files
(`work-templates/<lane>/`), workflow files (`workflows/`), the lanes manifest (`lanes.json`),
doctrine exported as files (`doctrine/`), and the privacy denylist. That config can be its own
private git repository (`tasque2 data-repo-init`), which the Workshop's releases and their undo
rely on.

## What the core holds that not every use needs

Tasque grew as one person's assistant, and the core still carries pieces of that first deployment:

- **Weather** (`weather.py`, the `weather_now` tool), with a home location as its default setting.
- **Goal reviews and the monthly look** in the Workshop, which assume a person with goals, weekly
  reviews and a journal; and the Workshop itself is something a deployment may not want to run.
- **Discord-only pieces** such as sticky notes and voice transcription, and the assumption in routing
  and posting that a result has a Discord thread to go to.
- **The intake prompt**, which handles a message as a general assistant's errands (reminders,
  calendar entries, drafts, trips) and names a doctrine document only an assistant deployment has.
- **Defaults** that are one person's: the timezone, the reminder time, the home location.

None of this breaks another use. It is in the way of someone reading the core for the first time,
and it is where the refactoring starts.

## Where the code is going

In order, one part at a time, each step a change of its own that ships with the first deployment
still working (the same tools, settings, schedules, threads and commands; a rename keeps the old
name working):

1. **Weather out of the core**, as the first optional part. This sets the pattern a deployment uses
   to turn a part on or off, and every later part follows it.
2. **Assistant defaults into config**: the intake prompt's errands and the document it names, the
   timezone, the reminder time and the rest read from the deployment's own config, not the code.
3. **Goal reviews, then the Workshop, as parts**: the sweep, triage, ideas and the change pipeline
   serve any deployment that wants Tasque to maintain itself; the goal reviews and the monthly look
   belong to an assistant deployment.
4. **Discord behind an interface seam**, with the command line working alone: routing, posting,
   cards and sticky notes behind one interface another channel can implement.
5. **A newcomer's setup without Discord**: the README's setup, `tasque2 doctor` and the smoke run
   work end to end with no channel configured.
6. **Later, more than one person on one install.**
