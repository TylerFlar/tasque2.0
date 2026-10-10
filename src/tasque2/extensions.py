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
- attempt ingestors that run after every successfully completed attempt;
- schedule gates: code that decides, before a scheduled run is launched, whether it is needed;
- sticky sections: fields a thread's sticky note shows below its notes, kept current in place;
- signal sources: issues the Workshop's sweep should know about (a decision that took its default).

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
ScheduleGate = Callable[[Any, Any, Any], "str | None"]
StickySection = Callable[[Any, str, Any], "list[dict[str, str]] | None"]
SignalSource = Callable[[Any, Any], "list[dict[str, Any]]"]
FunctionWorker = Callable[[Any], Any]


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
    schedule_gates: dict[str, ScheduleGate] = field(default_factory=dict)
    function_workers: dict[str, FunctionWorker] = field(default_factory=dict)
    sticky_sections: list[tuple[str, StickySection]] = field(default_factory=list)
    signal_sources: list[tuple[str, SignalSource]] = field(default_factory=list)
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

    def add_schedule_gate(self, name: str, gate: ScheduleGate) -> None:
        """Let schedules whose payload names ``gate: name`` skip runs that are not needed.

        ``gate(session, schedule, scheduled_for)`` returns None to launch the run, or a short
        reason to skip it. A gate that raises lets the run launch: a missed check costs one run,
        a wrong skip can cost a missed bill.
        """
        self.schedule_gates[name] = gate

    def add_function_worker(self, worker_kind: str, worker: FunctionWorker) -> None:
        """Run work items of ``worker_kind`` in-process with ``worker(work_item)``, no model.

        The kind must start with ``function.``; a work item or schedule names it as its
        ``worker_kind``. The worker returns what a built-in function worker returns (a
        ``WorkerResult``, a dict or a string); ``produces.silent`` keeps a quiet run off Discord.
        """
        if not worker_kind.startswith("function."):
            raise ValueError(f"Function worker kinds start with 'function.': {worker_kind!r}")
        self.function_workers[worker_kind] = worker

    def add_sticky_section(self, name: str, section: StickySection) -> None:
        """Show fields on a thread's sticky note, below its notes and above its upcoming runs.

        ``section(session, thread_id, now)`` returns ``[{"name": ..., "value": ...}]`` for a thread it
        serves and None for any other. It runs on every output pass, so it reads the ledgers and never
        calls out; the note is edited in place whenever what it returns changes. One that raises is left
        out of that pass.
        """
        self.sticky_sections.append((name, section))

    def add_signal_source(self, name: str, source: SignalSource) -> None:
        """Tell the Workshop's sweep (``tasque2.workshop.sweep``) about issues only this extension can see.

        ``source(session, now)`` returns one ``{"key", "title", "evidence"}`` per distinct issue that stands
        now (``key`` stable for as long as it stands, ``evidence`` a few short lines; ``since`` when it began,
        when known). It reads the ledgers and never calls out. One that raises is left out of that sweep.
        """
        self.signal_sources.append((name, source))

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
