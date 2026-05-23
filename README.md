# Tasque 2.0

Tasque is a local-first task and workflow runner.

It keeps durable work, schedules, workflow runs, memory, artifacts, and Discord
interaction in a local SQLite database. Model-backed work can run through
provider adapters such as Codex or Claude.

## Setup

Install dependencies with `uv`:

```powershell
uv sync
```

Create a local environment file:

```powershell
Copy-Item .env.example .env
```

Edit `.env` with your local data path, provider settings, and Discord settings.

## Run

Start the daemon:

```powershell
uv run tasque2 daemon
```

The daemon runs migrations on startup, polls schedules, advances workflows,
processes queued work, and handles Discord intake/output when configured.

## Useful Commands

```powershell
uv run tasque2 doctor
uv run tasque2 db-status
uv run tasque2 provider-smoke codex
uv run tasque2 provider-smoke claude
uv run tasque2 runbook-smoke
uv run pytest
uv run ruff check .
```

## Example Worker And Workflow

Queue a one-shot model-backed work item:

```powershell
uv run tasque2 queue "Morning check" "Review today's priorities and return three next actions." --worker-kind provider.default
uv run tasque2 run-next
```

Or create a small workflow file at `data/workflows/morning-check.workflow.json`:

```json
{
  "name": "morning-check",
  "version": "1",
  "definition": {
    "nodes": [
      {
        "key": "review",
        "kind": "work",
        "title": "Review priorities",
        "task_instruction": "Review today's priorities and list the top three.",
        "worker_kind": "provider.default"
      },
      {
        "key": "plan",
        "kind": "work",
        "title": "Make a plan",
        "task_instruction": "Use the review context to write a short action plan.",
        "worker_kind": "provider.default",
        "depends_on": ["review"]
      }
    ]
  }
}
```

Validate and start it:

```powershell
uv run tasque2 workflow-validate-file .\data\workflows\morning-check.workflow.json
uv run tasque2 workflow-start-file .\data\workflows\morning-check.workflow.json --name "Morning check"
```

Keep personal workflow files under `data/`; that directory is ignored by git.

## Project Layout

- `src/tasque2/` - application code
- `alembic/` - database migrations
- `tests/` - test suite
- `.env.example` - local configuration template
