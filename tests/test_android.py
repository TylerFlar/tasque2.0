from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tasque2 import android
from tasque2.config import reset_settings

ONE_BY_ONE_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)

SAMPLE_UI_XML = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<hierarchy rotation="0">
  <node text="" content-desc="" resource-id="" class="android.widget.FrameLayout"
        clickable="false" bounds="[0,0][1080,2400]">
    <node text="Like" content-desc="" resource-id="co.example:id/like_button"
          class="android.widget.Button" clickable="true" bounds="[900,2000][1060,2160]"/>
    <node text="" content-desc="Pass" resource-id="" class="android.widget.ImageView"
          clickable="true" bounds="[20,2000][180,2160]"/>
  </node>
</hierarchy>
"""


class FakeAdb:
    """Route fake responses by substring of the joined adb invocation."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.responses: list[tuple[str, object]] = []

    def add(self, needle: str, response: object) -> None:
        self.responses.append((needle, response))

    def add_sequence(self, needle: str, responses: list[object]) -> None:
        """Return each response in turn for successive matches (last one sticks)."""
        self.responses.append((needle, list(responses)))

    def __call__(self, args: list[str], *, timeout: float, binary: bool) -> bytes:
        self.calls.append(list(args))
        joined = " ".join(args)
        for index, (needle, response) in enumerate(self.responses):
            if needle in joined:
                if isinstance(response, list):
                    current = response.pop(0) if len(response) > 1 else response[0]
                    self.responses[index] = (needle, response)
                    response = current
                if isinstance(response, Exception):
                    raise response
                return response if isinstance(response, bytes) else str(response).encode()
        return b""

    def joined_calls(self) -> list[str]:
        return [" ".join(call) for call in self.calls]


@pytest.fixture()
def fake_adb(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeAdb:
    monkeypatch.setenv("TASQUE2_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TASQUE2_ANDROID_SERIAL", "")
    monkeypatch.setenv("TASQUE2_ANDROID_ADB_PATH", "adb")
    reset_settings()
    fake = FakeAdb()
    monkeypatch.setattr(android, "_run_adb_raw", fake)
    monkeypatch.setattr(android.time, "sleep", lambda _s: None)
    yield fake
    reset_settings()


def test_serial_flag_included_when_configured(
    fake_adb: FakeAdb, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_ANDROID_SERIAL", "127.0.0.1:5555")
    reset_settings()
    android.tap(10, 20)
    assert fake_adb.calls[0][:3] == ["adb", "-s", "127.0.0.1:5555"]


def test_tap_and_swipe_round_to_ints(fake_adb: FakeAdb) -> None:
    android.tap(10.6, 20.2)  # type: ignore[arg-type]
    android.swipe(1.2, 2.8, 3.5, 4.4, duration_ms=250)  # type: ignore[arg-type]
    joined = fake_adb.joined_calls()
    assert joined[0].endswith("shell input tap 11 20")
    assert joined[1].endswith("shell input swipe 1 3 4 4 250")


def test_screenshot_writes_png(fake_adb: FakeAdb) -> None:
    fake_adb.add("screencap", ONE_BY_ONE_PNG)
    result = android.take_screenshot("first card")
    path = Path(result["path"])
    assert path.is_file()
    assert path.read_bytes().startswith(android.PNG_MAGIC)
    assert "first-card" in path.name
    assert result["width"] in (1, None)  # None only if Pillow cannot be imported


def test_screenshot_rejects_non_png(fake_adb: FakeAdb) -> None:
    fake_adb.add("screencap", b"error: no devices/emulators found")
    with pytest.raises(android.AndroidError, match="PNG"):
        android.take_screenshot()


def test_type_ascii_encodes_spaces_and_quotes(fake_adb: FakeAdb) -> None:
    result = android.type_text("saw the climbing pic - which gym?")
    assert result["method"] == "input_text"
    joined = fake_adb.joined_calls()
    assert joined[0].endswith("shell input text 'saw%sthe%sclimbing%spic%s-%swhich%sgym?'")


def test_type_multiline_presses_enter_between_lines(fake_adb: FakeAdb) -> None:
    android.type_text("line one\nline two")
    joined = fake_adb.joined_calls()
    assert "input text 'line%sone'" in joined[0]
    assert joined[1].endswith("input keyevent 66")
    assert "input text 'line%stwo'" in joined[2]


def test_type_unicode_without_adbkeyboard_raises(fake_adb: FakeAdb) -> None:
    fake_adb.add("ime list", b"com.android.inputmethod.latin/.LatinIME")
    with pytest.raises(android.AndroidError, match="ADBKeyBoard"):
        android.type_text("nice café pick")
    assert not any("broadcast" in call for call in fake_adb.joined_calls())


def test_type_unicode_uses_adbkeyboard_base64(fake_adb: FakeAdb) -> None:
    fake_adb.add("ime list", android.ADBKEYBOARD_IME.encode())
    result = android.type_text("nice café pick")
    assert result["method"] == "adbkeyboard_b64"
    broadcast = next(call for call in fake_adb.calls if "broadcast" in call)
    payload = broadcast[broadcast.index("msg") + 1]
    assert base64.b64decode(payload).decode("utf-8") == "nice café pick"


def test_ui_dump_parses_labelled_nodes(fake_adb: FakeAdb) -> None:
    fake_adb.add("uiautomator dump", b"UI hierchary dumped to: /sdcard/window_dump.xml")
    fake_adb.add("cat /sdcard/window_dump.xml", SAMPLE_UI_XML.encode())
    result = android.ui_dump()
    assert result["total_nodes"] == 3
    assert result["kept"] == 2
    like = next(node for node in result["nodes"] if node.get("text") == "Like")
    assert like["id"] == "co.example:id/like_button"
    assert like["center"] == [980, 2080]
    assert like["clickable"] is True
    passes = next(node for node in result["nodes"] if node.get("desc") == "Pass")
    assert passes["center"] == [100, 2080]


def test_push_photo_scan_volume_with_broadcast_fallback(
    fake_adb: FakeAdb, tmp_path: Path
) -> None:
    photo = tmp_path / "new pic.jpg"
    photo.write_bytes(b"jpegdata")
    fake_adb.add("content call", android.AndroidError("scan_volume unsupported"))
    result = android.push_photo(photo)
    assert result["device_path"] == f"{android.DEVICE_PHOTO_DIR}/new_pic.jpg"
    assert result["scan_method"] == "media_scanner_broadcast"
    joined = fake_adb.joined_calls()
    assert any("mkdir -p" in call for call in joined)
    assert any("MEDIA_SCANNER_SCAN_FILE" in call for call in joined)


def test_push_photo_missing_file_raises(fake_adb: FakeAdb, tmp_path: Path) -> None:
    with pytest.raises(android.AndroidError, match="No file"):
        android.push_photo(tmp_path / "absent.jpg")


def test_launch_app_rejects_suspicious_package(fake_adb: FakeAdb) -> None:
    with pytest.raises(android.AndroidError, match="package"):
        android.launch_app("co.hinge.app; rm -rf /")


def test_press_key_by_name_and_code(fake_adb: FakeAdb) -> None:
    assert android.press_key("back") == 4
    assert android.press_key(66) == 66
    joined = fake_adb.joined_calls()
    assert joined[0].endswith("keyevent 4")
    assert joined[1].endswith("keyevent 66")
    with pytest.raises(android.AndroidError, match="Unknown key"):
        android.press_key("frobnicate")


def test_lease_conflict_handover_and_expiry(fake_adb: FakeAdb) -> None:
    now = datetime.now(UTC)
    android.ensure_lease("work-a", now=now)
    # Fresh lease + active holder blocks a different owner.
    with pytest.raises(android.AndroidError, match="leased by work item work-a"):
        android.ensure_lease("work-b", now=now + timedelta(seconds=30))
    # Same owner refreshes freely.
    android.ensure_lease("work-a", now=now + timedelta(seconds=60))
    # A finished holder hands over immediately.
    android.ensure_lease(
        "work-b",
        now=now + timedelta(seconds=90),
        is_owner_active=lambda owner: False,
    )
    assert android.lease_status()["owner"] == "work-b"
    # And a stale lease expires even for an "active" holder.
    android.ensure_lease(
        "work-c",
        now=now + timedelta(seconds=90 + android.LEASE_TTL_SECONDS + 1),
    )
    assert android.lease_status()["owner"] == "work-c"


def test_is_locked_parses_keyguard(fake_adb: FakeAdb) -> None:
    fake_adb.add("isKeyguardShowing", b"    isKeyguardShowing=true")
    assert android.is_locked() is True
    fake_adb.responses.clear()
    fake_adb.add("isKeyguardShowing", b"    isKeyguardShowing=false")
    assert android.is_locked() is False


def test_unlock_noop_when_already_unlocked(fake_adb: FakeAdb) -> None:
    fake_adb.add("isKeyguardShowing", b"isKeyguardShowing=false")
    result = android.unlock(pin="1234")
    assert result == {"already_unlocked": True, "locked": False}
    assert not any("input text" in call for call in fake_adb.joined_calls())
    assert not any("input swipe" in call for call in fake_adb.joined_calls())


def test_unlock_enters_pin_and_submits(fake_adb: FakeAdb) -> None:
    # Locked before the swipe/type, unlocked after.
    fake_adb.add_sequence(
        "isKeyguardShowing",
        [b"isKeyguardShowing=true", b"isKeyguardShowing=false"],
    )
    fake_adb.add("wm size", b"Physical size: 1080x2404")
    result = android.unlock(pin="8391")
    assert result == {"already_unlocked": False, "locked": False, "unlocked": True}
    joined = fake_adb.joined_calls()
    assert any("input swipe" in call for call in joined)
    assert any(call.endswith("input text 8391") for call in joined)
    assert any(call.endswith("input keyevent 66") for call in joined)


def test_unlock_still_locked_raises(fake_adb: FakeAdb) -> None:
    fake_adb.add("isKeyguardShowing", b"isKeyguardShowing=true")  # never clears
    fake_adb.add("wm size", b"Physical size: 1080x2404")
    with pytest.raises(android.AndroidError, match="Still locked"):
        android.unlock(pin="0000")


def test_unlock_requires_numeric_pin(fake_adb: FakeAdb) -> None:
    fake_adb.add("isKeyguardShowing", b"isKeyguardShowing=true")
    with pytest.raises(android.AndroidError, match="digits only"):
        android.unlock(pin="pass!")


def test_unlock_no_pin_configured_raises(
    fake_adb: FakeAdb, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_ANDROID_UNLOCK_PIN", "")
    reset_settings()
    fake_adb.add("isKeyguardShowing", b"isKeyguardShowing=true")
    with pytest.raises(android.AndroidError, match="no unlock PIN"):
        android.unlock()


def test_ensure_connected_skips_usb_serial(
    fake_adb: FakeAdb, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_ANDROID_SERIAL", "5A090DLCQ00347")
    reset_settings()
    result = android.ensure_connected()
    assert result == {"wireless": False, "reconnected": False}
    assert fake_adb.calls == []  # a USB serial never triggers adb connect


def test_ensure_connected_noop_when_already_connected(
    fake_adb: FakeAdb, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_ANDROID_SERIAL", "192.168.0.176:5555")
    reset_settings()
    fake_adb.add("devices", b"List of devices attached\n192.168.0.176:5555\tdevice\n")
    result = android.ensure_connected()
    assert result == {"wireless": True, "reconnected": False, "already_connected": True}
    assert not any("connect" in call for call in fake_adb.joined_calls())


def test_ensure_connected_reconnects_when_missing(
    fake_adb: FakeAdb, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_ANDROID_SERIAL", "192.168.0.176:5555")
    reset_settings()
    fake_adb.add("devices", b"List of devices attached\n")  # not present
    fake_adb.add("connect", b"connected to 192.168.0.176:5555")
    result = android.ensure_connected()
    assert result["reconnected"] is True
    assert any(call.endswith("connect 192.168.0.176:5555") for call in fake_adb.joined_calls())


def test_device_status_survives_missing_adb(fake_adb: FakeAdb) -> None:
    fake_adb.add("devices -l", android.AndroidError("adb executable not found"))
    status = android.device_status()
    assert status["devices"] == []
    assert "error" in status


def test_device_status_parses_devices_and_screen(fake_adb: FakeAdb) -> None:
    fake_adb.add(
        "devices -l",
        b"List of devices attached\nR58M12ABCDE            device usb:1-1 model:Moto_G32\n",
    )
    fake_adb.add("wm size", b"Physical size: 1080x2400")
    fake_adb.add("ime list", android.ADBKEYBOARD_IME.encode())
    status = android.device_status()
    assert status["devices"] == [{"serial": "R58M12ABCDE", "state": "device"}]
    assert status["screen"] == {"width": 1080, "height": 2400}
    assert status["adbkeyboard_installed"] is True
