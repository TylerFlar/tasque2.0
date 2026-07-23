"""Drive Adobe Photoshop's scripting engine (ExtendScript / JSX) over COM.

Windows-only, and Photoshop must already be running. This is the heavy-edit
escalation above the pure-Pillow darkroom (``tasque2.imaging``): anything
Photoshop can script — true lens blur, Camera Raw, curves, dodge/burn,
frequency-separation retouch, recorded actions — by supplying a JSX body that
operates on the opened document.

Deliberately lazy and graceful. If pywin32 isn't installed or Photoshop isn't
running, the tools return a clean ``available: false`` / error instead of
hanging or launching anything, so callers fall back to ``image_edit``. It uses
``GetActiveObject`` (attach to a RUNNING instance), never ``Dispatch`` (which
would launch Photoshop unattended) — opening Photoshop stays the user's choice.

The whole open → edit → export → close cycle is one JSX round-trip so the
document is always closed even on error, and the original file is never saved
over.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Photoshop JPEG quality is 0-12.
DEFAULT_JPEG_QUALITY = 11

# One round-trip: open the source, run the caller's body against `doc`, flatten,
# export a JPEG copy, and always close without touching the original. Returns
# "ok" or "ERROR: <message>" so JSX failures surface as clean Python errors.
_EDIT_WRAPPER = """
var __result__ = "ok";
var doc = null;
try {
    var srcFile = new File("__SRC__");
    doc = app.open(srcFile);
    (function (doc) {
__BODY__
    })(doc);
    if (doc.layers.length > 1) { doc.flatten(); }
    var outFile = new File("__OUT__");
    var opts = new JPEGSaveOptions();
    opts.quality = __QUALITY__;
    doc.saveAs(outFile, opts, true, Extension.LOWERCASE);
} catch (e) {
    __result__ = "ERROR: " + e.toString();
} finally {
    if (doc !== null) { try { doc.close(SaveOptions.DONOTSAVECHANGES); } catch (e2) {} }
}
__result__;
"""


class PhotoshopError(RuntimeError):
    """Photoshop is unavailable, or a script failed."""


def _jsx_string(value: str) -> str:
    """Escape a Python string for embedding inside a JSX double-quoted literal."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def get_app() -> Any:
    """Attach to a RUNNING Photoshop instance (never launches one). Tests patch this."""
    try:
        import win32com.client  # type: ignore
    except ImportError as exc:  # pragma: no cover - environment-dependent
        raise PhotoshopError(
            "pywin32 is not installed — run `uv pip install pywin32` to enable Photoshop."
        ) from exc
    try:
        return win32com.client.GetActiveObject("Photoshop.Application")
    except Exception as exc:  # noqa: BLE001 - COM raises a variety of errors when PS is closed
        raise PhotoshopError(
            "Adobe Photoshop is not running. Open Photoshop, then try again "
            "(image_edit is the fallback when it's closed)."
        ) from exc


def run_jsx(script: str) -> str:
    """Execute a JSX script in the running Photoshop and return its result string."""
    app = get_app()
    try:
        # win32com's late binding otherwise mistakes DoJavascript for a property.
        if hasattr(app, "_FlagAsMethod"):
            app._FlagAsMethod("DoJavascript")
        result = app.DoJavascript(script)
    except Exception as exc:  # noqa: BLE001 - surface COM/JSX failures as PhotoshopError
        raise PhotoshopError(f"Photoshop script failed: {exc}") from exc
    return "" if result is None else str(result)


def status() -> dict[str, Any]:
    """Report whether Photoshop is reachable, with version + open-document count."""
    try:
        app = get_app()
    except PhotoshopError as exc:
        return {"available": False, "reason": str(exc)}
    info: dict[str, Any] = {"available": True}
    for key, attr in (("app", "Name"), ("version", "Version")):
        try:
            info[key] = str(getattr(app, attr))
        except Exception:  # noqa: BLE001 - attributes are best-effort
            info[key] = None
    try:
        info["open_documents"] = int(app.Documents.Count)
    except Exception:  # noqa: BLE001
        info["open_documents"] = None
    return info


def edit_file(
    source_path: str | Path,
    output_path: str | Path,
    edit_jsx: str,
    *,
    quality: int = DEFAULT_JPEG_QUALITY,
    runner: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    """Open ``source_path`` in Photoshop, run ``edit_jsx`` against ``doc``, export a JPEG copy.

    ``edit_jsx`` is an ExtendScript body with the opened document bound to
    ``doc`` (e.g. ``doc.activeLayer.applyLensBlur(...)`` or Action-Manager
    calls). The original is never modified. ``runner`` overrides the JSX
    executor (tests inject a fake).
    """
    src = Path(source_path).expanduser()
    if not src.is_file():
        raise PhotoshopError(f"No file at {src}")
    out = Path(output_path).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    clamped_quality = max(0, min(12, int(quality)))
    script = (
        _EDIT_WRAPPER.replace("__SRC__", _jsx_string(str(src)))
        .replace("__OUT__", _jsx_string(str(out)))
        .replace("__QUALITY__", str(clamped_quality))
        .replace("__BODY__", edit_jsx or "")
    )
    execute = runner or run_jsx
    result = execute(script)
    if result.startswith("ERROR:"):
        raise PhotoshopError(f"Photoshop edit failed: {result[6:].strip()}")
    if not out.is_file():
        raise PhotoshopError("Photoshop reported success but produced no output file.")
    return {"output_path": str(out), "result": result}
