"""Android device automation over adb, for app-only surfaces.

Some services have no web client (dating apps are the canonical case), so the
only automatable surface is the Android app itself. This module drives one
Android device — a physical phone on USB/wireless adb or an emulator, they
look identical to adb — with the primitives a vision-capable worker needs:
screenshots it can Read, taps/swipes/keys, text input, a parsed view
hierarchy, app launch, and pushing photos into the device gallery.

Exposed as the ``android_*`` MCP tools; any worker may call them. Configure
``TASQUE2_ANDROID_ADB_PATH`` (default ``adb`` on PATH) and, when more than one
device is attached, ``TASQUE2_ANDROID_SERIAL`` (e.g. ``127.0.0.1:5555`` for an
emulator or wireless phone). Screenshots land under ``data/android/screens/``.

A single device is a serially-owned resource: every acting tool call takes a
short-lived lease keyed by the calling work item, so two workers cannot
interleave taps mid-flow. The lease auto-expires (or releases as soon as the
owning work item stops running) — no explicit release step.

Text-input reality (device-side ``input text``): ASCII only, no newlines in
one call (typed line-by-line here), and no emoji/unicode unless the
ADBKeyBoard IME (https://github.com/senzhk/ADBKeyBoard) is installed on the
device — when present it is used automatically for non-ASCII text.
"""

from __future__ import annotations

import base64
import json
import logging
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from xml.etree import ElementTree

from tasque2.config import get_settings

logger = logging.getLogger(__name__)

LEASE_TTL_SECONDS = 600
ADBKEYBOARD_IME = "com.android.adbkeyboard/.AdbIME"
DEVICE_PHOTO_DIR = "/sdcard/Pictures/tasque"
UI_DUMP_DEVICE_PATH = "/sdcard/window_dump.xml"
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"

KEYCODES = {
    "home": 3,
    "back": 4,
    "power": 26,
    "tab": 61,
    "space": 62,
    "enter": 66,
    "delete": 67,
    "backspace": 67,
    "menu": 82,
    "page_up": 92,
    "page_down": 93,
    "escape": 111,
    "move_home": 122,
    "move_end": 123,
    "app_switch": 187,
    "wakeup": 224,
    "paste": 279,
    "dpad_up": 19,
    "dpad_down": 20,
    "dpad_left": 21,
    "dpad_right": 22,
}

_BOUNDS_RE = re.compile(r"\[(-?\d+),(-?\d+)\]\[(-?\d+),(-?\d+)\]")


class AndroidError(RuntimeError):
    """A device command failed or the device is not in a usable state."""


# --- adb plumbing -----------------------------------------------------------


def _adb_base_args() -> list[str]:
    settings = get_settings()
    args = [settings.android_adb_path]
    serial = (settings.android_serial or "").strip()
    if serial:
        args.extend(["-s", serial])
    return args


def _run_adb_raw(args: list[str], *, timeout: float, binary: bool) -> bytes:
    """Execute one adb invocation; tests monkeypatch this seam."""
    try:
        completed = subprocess.run(
            args,
            capture_output=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise AndroidError(
            f"adb executable not found at {args[0]!r}; install Android platform-tools "
            "and set TASQUE2_ANDROID_ADB_PATH."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise AndroidError(f"adb timed out after {timeout:.0f}s: {' '.join(args[1:])}") from exc
    if completed.returncode != 0:
        stderr = (completed.stderr or b"").decode("utf-8", errors="replace").strip()
        stdout = b"" if binary else (completed.stdout or b"")
        detail = stderr or stdout.decode("utf-8", errors="replace").strip()
        raise AndroidError(
            f"adb failed (exit {completed.returncode}): {' '.join(args[1:])}: {detail[:400]}"
        )
    return completed.stdout or b""


def run_adb(*args: str, timeout: float = 30.0, binary: bool = False) -> str | bytes:
    """Run adb against the configured device; text (stripped) unless ``binary``."""
    raw = _run_adb_raw([*_adb_base_args(), *args], timeout=timeout, binary=binary)
    if binary:
        return raw
    return raw.decode("utf-8", errors="replace").strip()


def _shell_quote(value: str) -> str:
    """Quote one argument for the DEVICE-side shell (adb shell re-parses args)."""
    return "'" + value.replace("'", "'\\''") + "'"


# --- device lease -----------------------------------------------------------


def _lease_path() -> Path:
    return get_settings().resolved_data_dir / "android" / "lease.json"


def _read_lease() -> dict[str, Any] | None:
    path = _lease_path()
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_lease(owner: str, now: datetime) -> dict[str, Any]:
    record = {"owner": owner, "refreshed_at": now.isoformat()}
    path = _lease_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(record), encoding="utf-8")
    tmp.replace(path)
    return record


def ensure_lease(
    owner: str,
    *,
    now: datetime | None = None,
    is_owner_active: Callable[[str], bool] | None = None,
) -> dict[str, Any]:
    """Take or refresh the device lease for ``owner``; raise if someone else holds it.

    A holder blocks others only while its lease is fresh (< LEASE_TTL_SECONDS)
    AND — when ``is_owner_active`` is provided — its work item is still
    running; a finished session hands the device over immediately.
    """
    now = now or datetime.now(UTC)
    record = _read_lease()
    if record and record.get("owner") != owner:
        try:
            refreshed = datetime.fromisoformat(str(record.get("refreshed_at")))
        except ValueError:
            refreshed = None
        fresh = (
            refreshed is not None
            and (now - refreshed).total_seconds() < LEASE_TTL_SECONDS
        )
        holder = str(record.get("owner"))
        holder_active = is_owner_active(holder) if is_owner_active is not None else True
        if fresh and holder_active:
            age = int((now - refreshed).total_seconds()) if refreshed else 0
            raise AndroidError(
                f"Android device is leased by work item {holder} (refreshed {age}s ago). "
                "One session drives the device at a time; retry after it finishes."
            )
    return _write_lease(owner, now)


def lease_status() -> dict[str, Any] | None:
    return _read_lease()


# --- primitives -------------------------------------------------------------


def screen_size() -> tuple[int, int]:
    out = str(run_adb("shell", "wm", "size"))
    override = re.search(r"Override size:\s*(\d+)x(\d+)", out)
    physical = re.search(r"Physical size:\s*(\d+)x(\d+)", out)
    match = override or physical
    if not match:
        raise AndroidError(f"Could not parse screen size from: {out[:200]}")
    return int(match.group(1)), int(match.group(2))


def take_screenshot(label: str | None = None) -> dict[str, Any]:
    """Capture the screen to a PNG under data/android/screens/ and return its path."""
    data = run_adb("exec-out", "screencap", "-p", timeout=60.0, binary=True)
    if not isinstance(data, bytes) or not data.startswith(PNG_MAGIC):
        raise AndroidError(
            "screencap did not return a PNG; is the device attached and unlocked? "
            "(adb devices should list it as 'device')"
        )
    now = datetime.now(UTC)
    stem = now.strftime("%H%M%S_%f")[:-3]
    if label:
        slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")[:60]
        if slug:
            stem = f"{stem}_{slug}"
    directory = get_settings().resolved_data_dir / "android" / "screens" / now.strftime("%Y%m%d")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stem}.png"
    path.write_bytes(data)
    width = height = None
    try:
        from PIL import Image

        with Image.open(path) as image:
            width, height = image.size
    except Exception:  # noqa: BLE001 - size is a convenience, not a contract
        logger.debug("Could not read screenshot dimensions for %s", path)
    return {"path": str(path), "width": width, "height": height, "bytes": len(data)}


def tap(x: int, y: int) -> None:
    run_adb("shell", "input", "tap", str(int(round(x))), str(int(round(y))))


def swipe(x1: int, y1: int, x2: int, y2: int, duration_ms: int = 300) -> None:
    run_adb(
        "shell",
        "input",
        "swipe",
        str(int(round(x1))),
        str(int(round(y1))),
        str(int(round(x2))),
        str(int(round(y2))),
        str(max(1, int(duration_ms))),
    )


def press_key(key: str | int) -> int:
    if isinstance(key, str):
        name = key.strip().lower()
        if name.isdigit():
            code = int(name)
        elif name in KEYCODES:
            code = KEYCODES[name]
        else:
            known = ", ".join(sorted(KEYCODES))
            raise AndroidError(f"Unknown key {key!r}; use a keycode int or one of: {known}")
    else:
        code = int(key)
    run_adb("shell", "input", "keyevent", str(code))
    return code


def adbkeyboard_available() -> bool:
    try:
        out = str(run_adb("shell", "ime", "list", "-s"))
    except AndroidError:
        return False
    return ADBKEYBOARD_IME in out


def type_text(text: str) -> dict[str, Any]:
    """Type ``text`` into the focused field; picks the right mechanism per content.

    ASCII goes through ``input text`` line-by-line (ENTER between lines).
    Non-ASCII requires the ADBKeyBoard IME on the device; without it this
    raises rather than silently mangling the message.
    """
    if text == "":
        return {"method": "noop", "characters": 0}
    if text.isascii():
        lines = text.split("\n")
        for index, line in enumerate(lines):
            if index:
                press_key("enter")
            if not line:
                continue
            encoded = line.replace(" ", "%s")
            run_adb("shell", "input", "text", _shell_quote(encoded))
        return {"method": "input_text", "characters": len(text)}
    if not adbkeyboard_available():
        raise AndroidError(
            "Text contains non-ASCII characters and the ADBKeyBoard IME is not on the "
            "device. Install it once (adb install ADBKeyboard.apk from "
            "github.com/senzhk/ADBKeyBoard) or keep the text ASCII."
        )
    run_adb("shell", "ime", "enable", ADBKEYBOARD_IME)
    run_adb("shell", "ime", "set", ADBKEYBOARD_IME)
    time.sleep(0.4)
    payload = base64.b64encode(text.encode("utf-8")).decode("ascii")
    run_adb("shell", "am", "broadcast", "-a", "ADB_INPUT_B64", "--es", "msg", payload)
    return {"method": "adbkeyboard_b64", "characters": len(text)}


def ui_dump(max_nodes: int = 150) -> dict[str, Any]:
    """Dump the current view hierarchy, parsed to labelled tappable elements.

    uiautomator sometimes captures only an overlay/dialog and misses the real
    content — treat this as an assist for finding exact tap targets; the
    screenshot is the source of truth.
    """
    run_adb("shell", "uiautomator", "dump", UI_DUMP_DEVICE_PATH, timeout=60.0)
    raw = run_adb("exec-out", "cat", UI_DUMP_DEVICE_PATH, timeout=30.0, binary=True)
    xml_text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        raise AndroidError(f"Could not parse uiautomator dump: {exc}") from exc
    nodes: list[dict[str, Any]] = []
    total = 0
    for node in root.iter("node"):
        total += 1
        text = (node.get("text") or "").strip()
        desc = (node.get("content-desc") or "").strip()
        resource_id = (node.get("resource-id") or "").strip()
        clickable = node.get("clickable") == "true"
        if not (text or desc or resource_id or clickable):
            continue
        match = _BOUNDS_RE.match(node.get("bounds") or "")
        if not match:
            continue
        left, top, right, bottom = (int(match.group(i)) for i in range(1, 5))
        entry: dict[str, Any] = {
            "bounds": [left, top, right, bottom],
            "center": [(left + right) // 2, (top + bottom) // 2],
        }
        if text:
            entry["text"] = text[:200]
        if desc:
            entry["desc"] = desc[:200]
        if resource_id:
            entry["id"] = resource_id
        if clickable:
            entry["clickable"] = True
        class_name = node.get("class") or ""
        if class_name:
            entry["class"] = class_name.rsplit(".", 1)[-1]
        nodes.append(entry)
        if len(nodes) >= max(1, max_nodes):
            break
    return {"nodes": nodes, "kept": len(nodes), "total_nodes": total}


def is_locked() -> bool:
    """True when the keyguard (lock screen) is up, blocking UI interaction."""
    out = str(run_adb("shell", "dumpsys window | grep -m1 isKeyguardShowing"))
    match = re.search(r"isKeyguardShowing=(true|false)", out)
    if match:
        return match.group(1) == "true"
    # Fallback for ROMs that word it differently.
    out = str(run_adb("shell", "dumpsys window | grep -m1 -iE 'mShowingLockscreen|Dreaming'"))
    match = re.search(r"(?:mShowingLockscreen|mDreamingLockscreen)=(true|false)", out)
    return bool(match and match.group(1) == "true")


def unlock(pin: str | None = None, *, settle: float = 0.6) -> dict[str, Any]:
    """Wake the device and, if locked, dismiss the keyguard with a numeric PIN.

    The PIN comes from ``TASQUE2_ANDROID_UNLOCK_PIN`` when not passed. A no-op
    (beyond waking) if the screen is already unlocked. Raises if the device is
    locked with no PIN available, if the PIN is non-numeric, or if the screen
    is still locked after entry (wrong PIN, or a biometric-only keyguard).
    """
    resolved = pin if pin is not None else get_settings().android_unlock_pin
    press_key("wakeup")
    time.sleep(0.3)
    if not is_locked():
        return {"already_unlocked": True, "locked": False}
    resolved = (resolved or "").strip()
    if not resolved:
        raise AndroidError(
            "Device is locked and no unlock PIN is set. Put the digits in "
            "TASQUE2_ANDROID_UNLOCK_PIN (in .env), or unlock the phone by hand."
        )
    if not resolved.isdigit():
        raise AndroidError("Unlock PIN must be digits only for scripted entry.")
    width, height = screen_size()
    # Swipe up to raise the PIN bouncer, then type into it and submit.
    swipe(width // 2, int(height * 0.85), width // 2, int(height * 0.25), duration_ms=250)
    time.sleep(settle)
    run_adb("shell", "input", "text", resolved)
    press_key("enter")
    # Poll rather than checking once: the keyguard-dismiss animation takes a
    # beat, and a single immediate read reports a stale "locked".
    for _ in range(6):
        time.sleep(settle)
        if not is_locked():
            return {"already_unlocked": False, "locked": False, "unlocked": True}
    raise AndroidError(
        "Still locked after entering the PIN — wrong PIN, or this keyguard needs "
        "biometrics. Unlock the phone by hand for this session."
    )


def launch_app(package: str, *, relaunch: bool = False) -> None:
    package = package.strip()
    if not re.fullmatch(r"[A-Za-z0-9_.]+", package):
        raise AndroidError(f"Suspicious package name: {package!r}")
    if relaunch:
        run_adb("shell", "am", "force-stop", package)
    run_adb(
        "shell",
        "monkey",
        "-p",
        package,
        "-c",
        "android.intent.category.LAUNCHER",
        "1",
        timeout=60.0,
    )


def list_packages(query: str | None = None) -> list[str]:
    out = str(run_adb("shell", "pm", "list", "packages"))
    packages = [
        line.removeprefix("package:").strip()
        for line in out.splitlines()
        if line.strip().startswith("package:")
    ]
    if query:
        needle = query.strip().lower()
        packages = [package for package in packages if needle in package.lower()]
    return sorted(packages)


def push_photo(local_path: str | Path, name: str | None = None) -> dict[str, Any]:
    """Push an image to the device gallery so app photo pickers can see it."""
    source = Path(local_path).expanduser()
    if not source.is_file():
        raise AndroidError(f"No file at {source}")
    filename = (name or source.name).strip().replace(" ", "_")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", filename):
        raise AndroidError(f"Unsafe device filename: {filename!r}")
    device_path = f"{DEVICE_PHOTO_DIR}/{filename}"
    run_adb("shell", "mkdir", "-p", DEVICE_PHOTO_DIR)
    run_adb("push", str(source), device_path, timeout=120.0)
    # MEDIA_SCANNER_SCAN_FILE is deprecated since Android 10; scan_volume is the
    # reliable way to make MediaStore (and the photo picker) index the file.
    scan_method = "scan_volume"
    try:
        run_adb(
            "shell",
            "content",
            "call",
            "--uri",
            "content://media/external",
            "--method",
            "scan_volume",
            "--arg",
            "external_primary",
            timeout=60.0,
        )
    except AndroidError:
        scan_method = "media_scanner_broadcast"
        run_adb(
            "shell",
            "am",
            "broadcast",
            "-a",
            "android.intent.action.MEDIA_SCANNER_SCAN_FILE",
            "-d",
            f"file://{device_path}",
        )
    return {"device_path": device_path, "scan_method": scan_method}


def ensure_connected() -> dict[str, Any]:
    """(Re)establish the wireless adb link if the configured serial has dropped.

    A no-op for USB serials (no ``:`` port) or when the wireless device is
    already ``device``. Otherwise runs ``adb connect <serial>`` — which heals a
    wifi blip or a slept-phone drop as long as the phone's adb daemon is still
    listening. It cannot revive a link after a reboot that reset ``adb tcpip``
    mode; that needs Android's persistent Wireless Debugging (see dating_ops).
    """
    settings = get_settings()
    serial = (settings.android_serial or "").strip()
    if not serial or ":" not in serial:
        return {"wireless": False, "reconnected": False}
    try:
        raw = _run_adb_raw([settings.android_adb_path, "devices"], timeout=15.0, binary=False)
    except AndroidError:
        raw = b""
    for line in raw.decode("utf-8", errors="replace").splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 2 and parts[0] == serial:
            if parts[1] == "device":
                return {"wireless": True, "reconnected": False, "already_connected": True}
            break  # offline / unauthorized — fall through and try to reconnect
    try:
        out = _run_adb_raw(
            [settings.android_adb_path, "connect", serial], timeout=20.0, binary=False
        )
        text = out.decode("utf-8", errors="replace").strip()
    except AndroidError as exc:
        return {"wireless": True, "reconnected": False, "error": str(exc)}
    return {"wireless": True, "reconnected": "connected" in text.lower(), "output": text}


def device_status() -> dict[str, Any]:
    """Connectivity + capability snapshot; safe to call with no device attached."""
    settings = get_settings()
    status: dict[str, Any] = {
        "adb_path": settings.android_adb_path,
        "configured_serial": settings.android_serial,
        "lease": lease_status(),
    }
    status["reconnect"] = ensure_connected()
    try:
        raw = _run_adb_raw(
            [settings.android_adb_path, "devices", "-l"], timeout=15.0, binary=False
        )
        lines = raw.decode("utf-8", errors="replace").strip().splitlines()
        devices = []
        for line in lines[1:]:
            parts = line.split()
            if len(parts) >= 2:
                devices.append({"serial": parts[0], "state": parts[1]})
        status["devices"] = devices
    except AndroidError as exc:
        status["devices"] = []
        status["error"] = str(exc)
        return status
    try:
        width, height = screen_size()
        status["screen"] = {"width": width, "height": height}
    except AndroidError as exc:
        status["screen"] = None
        status["screen_error"] = str(exc)
    try:
        status["locked"] = is_locked()
    except AndroidError:
        status["locked"] = None
    status["unlock_pin_configured"] = bool((settings.android_unlock_pin or "").strip())
    try:
        status["adbkeyboard_installed"] = adbkeyboard_available()
    except AndroidError:
        status["adbkeyboard_installed"] = None
    return status
