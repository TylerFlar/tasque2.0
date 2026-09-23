from __future__ import annotations

import logging
import os
import sys

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
_NOISY_LOGGERS = ("alembic", "discord.http", "httpx", "httpcore", "mcp.server.lowlevel.server")
_handler: logging.Handler | None = None
_previous_level: int | None = None


def configure_logging(level: str | None = None) -> None:
    """Send Tasque logs to stderr once per process; TASQUE2_LOG_LEVEL sets the level."""
    global _handler, _previous_level
    if _handler is not None:
        return
    resolved = (level or os.environ.get("TASQUE2_LOG_LEVEL") or "INFO").upper()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root = logging.getLogger()
    _previous_level = root.level
    root.addHandler(handler)
    root.setLevel(resolved)
    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _handler = handler


def reset_logging() -> None:
    """Remove the handler ``configure_logging`` added and restore the root level."""
    global _handler
    if _handler is None:
        return
    root = logging.getLogger()
    root.removeHandler(_handler)
    if _previous_level is not None:
        root.setLevel(_previous_level)
    _handler = None
