# Tasque extensions

Everything in this directory except this README is ignored by git: it is where your own domains
live. The core stays generic; your domains plug in here.

An extension is a Python package:

```
extensions/
  my_domains/
    __init__.py      # exposes register(registry)
    models.py        # optional: SQLAlchemy tables on the core Base
    tools.py         # optional: MCP tools
    migrations/      # optional: Alembic revisions for your tables
    tests/           # optional: pytest suite (run: uv run pytest extensions/my_domains/tests)
```

Tasque loads every package here at startup (`TASQUE2_EXTENSIONS_DIR` moves the directory) and
calls its `register(registry)`:

```python
# extensions/my_domains/__init__.py
from pathlib import Path


def register(registry) -> None:
    from . import models  # noqa: F401 - puts the tables on Base.metadata
    from . import tools
    from .reading_log import build_reading_log, ingest_reading_produces, resolve_shelf_keys, wants_reading_log

    # Alembic revisions for your tables. The first revision's down_revision is a core revision
    # ("core_0001"); core and extension revisions upgrade together.
    registry.add_migration_location(Path(__file__).resolve().parent / "migrations")

    # A code-computed digest added to the context packet of runs whose context asks for it:
    # wants(context) decides, build(session) computes.
    registry.add_context_digest("reading_log", wants_reading_log, build_reading_log)

    # Canonical documents chosen per run, for pinned sets where only one member matters to a
    # given run. resolve(session, context) returns keys; when it cannot decide it returns the
    # whole set, because a missing document is worse than a large packet.
    registry.add_canonical_keys(wants_reading_log, resolve_shelf_keys)

    # MCP tools served next to the core tools; name and docstring are the schema.
    registry.add_mcp_tools(tools.reading_log_entry, tools.reading_history)

    # Runs after every successfully completed attempt, e.g. to record ledger rows from its produces.
    registry.add_attempt_ingestor("reading_log", ingest_reading_produces)
```

Patterns that work well:

- **Append-only ledgers.** Tables in `models.py` (use `Base`, `TimestampMixin`, `new_id` from
  `tasque2.models`); workers write through a validated MCP tool; code computes the digest; workers
  quote computed numbers instead of keeping their own counts in memory.
- **Tools** built with `tasque2.mcp.toolkit` (`run_json`, `session_scope`, `calling_work_item`,
  `required`, `optional_string`, ...) so they behave like core tools and are traced the same way.
- **Local dates** from `tasque2.localtime.local_today` / `local_date`, so every ledger agrees on
  what "today" is.

A broken extension stops startup with an error rather than running without its tools.
