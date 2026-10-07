from __future__ import annotations

import logging
import os
import sys

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
_NOISY_LOGGERS = ("alembic", "discord.http", "httpx", "httpcore", "mcp.server.lowlevel.server")
_handlers: list[logging.Handler] = []
_previous_level: int | None = None


def configure_logging(level: str | None = None) -> None:
    """Send Tasque logs to stderr once per process; TASQUE2_LOG_LEVEL sets the level.

    ERROR records also go to the fault ledger (``tasque2.ops.faults``), where health checks
    find code faults that callers caught and logged instead of raising.
    """
    global _previous_level
    if _handlers:
        return
    from tasque2.ops.faults import FaultLedgerHandler

    resolved = (level or os.environ.get("TASQUE2_LOG_LEVEL") or "INFO").upper()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root = logging.getLogger()
    _previous_level = root.level
    for added in (handler, FaultLedgerHandler()):
        root.addHandler(added)
        _handlers.append(added)
    root.setLevel(resolved)
    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)


def reset_logging() -> None:
    """Remove the handlers ``configure_logging`` added and restore the root level."""
    if not _handlers:
        return
    root = logging.getLogger()
    for handler in _handlers:
        root.removeHandler(handler)
    _handlers.clear()
    if _previous_level is not None:
        root.setLevel(_previous_level)
