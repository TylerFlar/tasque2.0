"""Discovery and registry for local extension packages.

The core is generic: queue, schedules, workflows, memory, artifacts, providers, Discord,
MCP. Personal domains plug in as extensions: plain Python packages under the extensions
directory (``TASQUE2_EXTENSIONS_DIR``, default ``extensions/``), each exposing
``register(registry)``. An extension may contribute:

- SQLAlchemy models on the core ``Base`` (import them inside ``register``);
- an Alembic migration directory whose revisions chain off a core revision;
- MCP tools served alongside the core tools;
- context digests: code-computed state injected into matching work items' packets;
- canonical-key resolvers that choose pinned documents per run;
- attempt ingestors that run after every successfully completed attempt.

Extensions load once per process on first use. A broken extension raises: a daemon that
silently lost its domain tools would corrupt runs far worse than a loud startup failure.
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

DigestWants = Callable[[dict[str, Any]], bool]
DigestBuild = Callable[[Any], dict[str, Any]]
AttemptIngestor = Callable[[Any, Any, Any], Any]
CanonicalKeyResolver = Callable[[Any, dict[str, Any]], list[str]]


class ExtensionError(RuntimeError):
    """An extension package failed to import or register."""


@dataclass
class ExtensionRegistry:
    """Everything the loaded extension packages contributed to the core."""

    context_digests: list[tuple[str, DigestWants, DigestBuild]] = field(default_factory=list)
    canonical_key_resolvers: list[tuple[DigestWants, CanonicalKeyResolver]] = field(default_factory=list)
    mcp_tools: list[Callable[..., str]] = field(default_factory=list)
    attempt_ingestors: list[tuple[str, AttemptIngestor]] = field(default_factory=list)
    migration_locations: list[Path] = field(default_factory=list)
    extension_names: list[str] = field(default_factory=list)

    def add_context_digest(self, key: str, wants: DigestWants, build: DigestBuild) -> None:
        """Inject ``build(session)`` as ``packet[key]`` whenever ``wants(context)`` is true."""
        self.context_digests.append((key, wants, build))

    def add_canonical_keys(self, wants: DigestWants, resolve: CanonicalKeyResolver) -> None:
        """Pin canonical documents chosen per run instead of from a static context list.

        ``resolve(session, context)`` returns keys to load next to the context's own
        ``memory_canonical_keys``. It must fail safe: when it cannot decide, return the
        full set, because a silently missing document is worse than a large packet.
        """
        self.canonical_key_resolvers.append((wants, resolve))

    def add_mcp_tools(self, *tools: Callable[..., str]) -> None:
        """Serve these callables as MCP tools; name and docstring become the schema."""
        self.mcp_tools.extend(tools)

    def add_attempt_ingestor(self, name: str, ingestor: AttemptIngestor) -> None:
        """Run ``ingestor(session, work_item, attempt)`` after each attempt that completes successfully."""
        self.attempt_ingestors.append((name, ingestor))

    def add_migration_location(self, path: Path | str) -> None:
        """Add an Alembic version directory that upgrades together with the core one."""
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
    """Forget loaded extensions (modules stay imported)."""
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
                f"Extension '{child.name}' has no register(registry) function; expose one in its __init__.py."
            )
        try:
            register(reg)
        except Exception as exc:
            raise ExtensionError(f"Extension '{child.name}' failed to register: {exc}") from exc
        reg.extension_names.append(child.name)
        logger.info("Loaded extension: %s", child.name)
    return reg
