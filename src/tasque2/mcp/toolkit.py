"""The supported helper surface for extension MCP tools.

Extension tools follow the core shape: a public wrapper whose name and docstring become
the tool schema, delegating through ``run_json`` to a private body that opens a
``session_scope``. Import these helpers from here rather than from core tool modules.
"""

from __future__ import annotations

from tasque2.db import session_scope
from tasque2.mcp.tools._shared import (
    calling_work_item,
    clamp,
    inherit_reply_config,
    json_payload,
    optional_int,
    optional_string,
    required,
    run_json,
    string_list,
)

__all__ = [
    "calling_work_item",
    "clamp",
    "inherit_reply_config",
    "json_payload",
    "optional_int",
    "optional_string",
    "required",
    "run_json",
    "session_scope",
    "string_list",
]
