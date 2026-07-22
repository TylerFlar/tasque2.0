"""Sanctioned helper surface for extension-provided MCP tools.

Extension packages write tools in the same shape as the core ones: a public
wrapper whose name/docstring become the MCP schema, delegating through
``run_json`` to a private implementation that opens a ``session_scope``.
These re-exports are the supported way to do that from outside the core;
import from here, not from ``tasque2.mcp.tools`` internals.
"""

from __future__ import annotations

from tasque2.db import session_scope
from tasque2.mcp.tools import (
    _calling_work_item as calling_work_item,
)
from tasque2.mcp.tools import (
    _json as json_payload,
)
from tasque2.mcp.tools import (
    _optional_int as optional_int,
)
from tasque2.mcp.tools import (
    _optional_string as optional_string,
)
from tasque2.mcp.tools import (
    _required as required,
)
from tasque2.mcp.tools import (
    _run_json as run_json,
)
from tasque2.mcp.tools import (
    _string_list as string_list,
)

__all__ = [
    "calling_work_item",
    "json_payload",
    "optional_int",
    "optional_string",
    "required",
    "run_json",
    "session_scope",
    "string_list",
]
