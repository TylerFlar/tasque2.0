"""Tasque's built-in optional parts: domains a deployment turns on or leaves out.

A part is a module (or package) here exposing ``register(registry)``, the hook extensions use
(``tasque2.extensions``): it adds its MCP tools, digests, gates or function workers the same way.
``TASQUE2_PARTS`` names the parts a deployment runs (comma-separated, default ``weather``; blank runs
none). Each registers, in that order, before the extensions do; a name that is not a part here, or a
part that fails to import or register, stops startup as a broken extension does.

A part owns its settings: it reads its own ``TASQUE2_<PART>_*`` variables, and the core ``Settings``
holds none of them. To add a part, add its module here and name it in ``TASQUE2_PARTS``.
"""

from __future__ import annotations

import pkgutil


def available() -> list[str]:
    """The names of the parts Tasque ships, sorted."""
    return sorted(module.name for module in pkgutil.iter_modules(__path__) if not module.name.startswith("_"))
