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

## Local Files

The `data/` directory is intentionally ignored. It may contain private workflow
templates, local SQLite databases, artifacts, memory mirrors, and analysis
workspaces.

The `mcps/` directory is also ignored because it is used for local MCP checkouts
and builds.

Alembic files are committed because the application imports them for database
migrations.

## Project Layout

- `src/tasque2/` - application code
- `alembic/` - database migrations
- `tests/` - test suite
- `.env.example` - local configuration template
