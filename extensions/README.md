# Tasque Extensions

Everything in this directory except this README is gitignored: it is where
your personal domain modules live. The core stays generic; your domains plug
in here.

An extension is a plain Python package directory:

```
extensions/
  my_domains/
    __init__.py      # must expose register(registry)
    models.py        # optional: SQLAlchemy tables on the core Base
    tools.py         # optional: MCP tools
    migrations/      # optional: Alembic revisions for your tables
    tests/           # optional: pytest suite (run it explicitly, or add a
                     # local pytest.ini with both testpaths)
```

Tasque discovers every package in this directory at startup (override the
location with `TASQUE2_EXTENSIONS_DIR`) and calls its `register(registry)`:

```python
# extensions/my_domains/__init__.py
from pathlib import Path


def register(registry) -> None:
    from . import models  # noqa: F401 - put your tables on Base.metadata
    from . import tools
    from .reading_log import build_reading_log, wants_reading_log, ingest_reading_produces

    # Alembic revisions for your tables. Chain your first revision's
    # down_revision off the current core head; core and extension histories
    # then upgrade together as one graph ("heads").
    registry.add_migration_location(Path(__file__).resolve().parent / "migrations")

    # A code-computed digest injected into matching work items' context
    # packets: wants(context) decides, build(session) computes.
    registry.add_context_digest("reading_log", wants_reading_log, build_reading_log)

    # MCP tools served alongside the core tools (name/docstring = schema).
    registry.add_mcp_tools(tools.reading_log_entry, tools.reading_history)

    # Optional fallback recorder run after every finished attempt, e.g. to
    # recover ledger rows from machine-readable `produces`.
    registry.add_attempt_ingestor("reading_log", ingest_reading_produces)
```

Conventions the core supports well:

- **Append-only ledger tables** (`models.py` importing `Base`,
  `TimestampMixin`, `new_id` from `tasque2.models`): workers write through a
  validated MCP tool, code computes the digest, and the worker quotes the
  computed numbers instead of self-attesting state in memory.
- **Tools** written with `tasque2.mcp.toolkit` (`run_json`, `session_scope`,
  `calling_work_item`, ...) so they behave exactly like core tools.
- **Local-calendar dates** via `tasque2.localtime.local_today` /
  `local_date` so every ledger agrees on what "today" means.

A broken extension fails loudly at startup rather than silently dropping its
tools and digests.
