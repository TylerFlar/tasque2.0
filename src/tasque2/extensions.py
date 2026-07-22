"""Discovery and registry for local extension packages.

Tasque's core is generic: work queue, schedules, workflows, memory,
artifacts, Discord, providers. Personal domains (a workout ledger, a pantry
tracker, ...) plug in as *extensions*: plain Python packages dropped into the
``extensions/`` directory (gitignored, ``TASQUE2_EXTENSIONS_DIR`` to
relocate). Each package exposes ``register(registry)`` and may contribute:

- SQLAlchemy models on the core ``Base`` (import them inside ``register``),
- an Alembic migration directory (``add_migration_location``) whose revisions
  chain off any core revision — core and extension histories upgrade together,
- MCP tools (``add_mcp_tools``) served alongside the core tools,
- worker-context digests (``add_context_digest``): code-computed state blocks
  injected into matching work items' context packets,
- attempt ingestors (``add_attempt_ingestor``): fallback hooks that run after
  every finished attempt to recover structured state from ``produces``.

Loading is lazy and happens once per process, triggered by the first caller
(migrations, the MCP server, the worker-context builder, or the runtime). A
broken extension raises: a daemon silently missing its domain tools would
corrupt runs far worse than a loud startup failure.
"""

from __future__ import annotations

import importlib
import logging
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# wants(context) -> bool, build(session) -> digest payload
DigestWants = Callable[[dict[str, Any]], bool]
DigestBuild = Callable[[Any], dict[str, Any]]
# ingest(session, work_item, attempt) -> Any; must be safe to run on every attempt
AttemptIngestor = Callable[[Any, Any, Any], Any]


class ExtensionError(RuntimeError):
    """Raised when an extension package cannot be loaded or registered."""


@dataclass
class ExtensionRegistry:
    """Everything the loaded extension packages contributed to the core."""

    context_digests: list[tuple[str, DigestWants, DigestBuild]] = field(default_factory=list)
    mcp_tools: list[Callable[..., str]] = field(default_factory=list)
    attempt_ingestors: list[tuple[str, AttemptIngestor]] = field(default_factory=list)
    migration_locations: list[Path] = field(default_factory=list)
    extension_names: list[str] = field(default_factory=list)

    def add_context_digest(self, key: str, wants: DigestWants, build: DigestBuild) -> None:
        """Inject ``build(session)`` as ``packet[key]`` when ``wants(context)`` is true."""
        self.context_digests.append((key, wants, build))

    def add_mcp_tools(self, *tools: Callable[..., str]) -> None:
        """Serve these callables as MCP tools (name/docstring become the schema)."""
        self.mcp_tools.extend(tools)

    def add_attempt_ingestor(self, name: str, ingestor: AttemptIngestor) -> None:
        """Run ``ingestor(session, work_item, attempt)`` after each finished attempt."""
        self.attempt_ingestors.append((name, ingestor))

    def add_migration_location(self, path: Path | str) -> None:
        """Add an Alembic version directory scanned together with the core one."""
        self.migration_locations.append(Path(path))


_registry: ExtensionRegistry | None = None
_lock = threading.Lock()


def extensions_dir() -> Path:
    from tasque2.config import get_settings

    return get_settings().resolved_extensions_dir


def registry() -> ExtensionRegistry:
    """The process-wide registry; loads every extension on first use."""
    global _registry
    if _registry is None:
        with _lock:
            if _registry is None:
                _registry = _load()
    return _registry


def reset_registry() -> None:
    """Forget loaded extensions (tests only; modules stay imported)."""
    global _registry
    with _lock:
        _registry = None


def _load() -> ExtensionRegistry:
    reg = ExtensionRegistry()
    directory = extensions_dir()
    if not directory.is_dir():
        return reg
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))
    for child in sorted(directory.iterdir()):
        if not child.is_dir() or child.name.startswith((".", "_")):
            continue
        if not (child / "__init__.py").is_file():
            continue
        try:
            module = importlib.import_module(child.name)
        except Exception as exc:
            raise ExtensionError(f"Extension '{child.name}' failed to import: {exc}") from exc
        register = getattr(module, "register", None)
        if not callable(register):
            raise ExtensionError(
                f"Extension '{child.name}' has no register(registry) function; "
                "expose one in its __init__.py."
            )
        try:
            register(reg)
        except Exception as exc:
            raise ExtensionError(f"Extension '{child.name}' failed to register: {exc}") from exc
        reg.extension_names.append(child.name)
        logger.info("Loaded Tasque extension: %s", child.name)
    return reg
