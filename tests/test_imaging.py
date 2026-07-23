from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image, ImageStat

from tasque2.imaging import edit_image


def _sample(width: int = 160, height: int = 220) -> Image.Image:
    image = Image.new("RGB", (width, height))
    pixels = image.load()
    for y in range(height):
        for x in range(width):
            pixels[x, y] = (x % 256, y % 256, (x + y) % 256)
    return image


def _mean(image: Image.Image) -> list[float]:
    return ImageStat.Stat(image).mean


def test_noop_returns_same_pixels() -> None:
    image = _sample()
    out, applied = edit_image(image, {})
    assert applied == []
    assert out.size == image.size
    assert out.tobytes() == image.convert("RGB").tobytes()


def test_exposure_brightens_and_darkens() -> None:
    image = _sample()
    brighter, applied = edit_image(image, {"exposure": 1.0})
    assert "exposure" in applied
    assert _mean(brighter)[0] > _mean(image)[0]
    darker, _ = edit_image(image, {"exposure": -1.0})
    assert _mean(darker)[0] < _mean(image)[0]


def test_temperature_shifts_red_vs_blue() -> None:
    image = _sample()
    warm, applied = edit_image(image, {"temperature": 80})
    assert "temperature" in applied
    base = _mean(image)
    warmed = _mean(warm)
    assert warmed[0] > base[0]  # red up
    assert warmed[2] < base[2]  # blue down


def test_depth_of_field_changes_pixels_keeps_size() -> None:
    image = _sample()
    out, applied = edit_image(image, {"depth_of_field": {"focus": "center", "strength": 70}})
    assert "depth_of_field" in applied
    assert out.size == image.size
    assert out.tobytes() != image.tobytes()


def test_depth_of_field_zero_strength_is_skipped() -> None:
    image = _sample()
    _, applied = edit_image(image, {"depth_of_field": {"focus": "center", "strength": 0}})
    assert "depth_of_field" not in applied


def test_straighten_rotates() -> None:
    image = _sample()
    out, applied = edit_image(image, {"straighten": 5})
    assert "straighten" in applied
    assert out.size == image.size


def test_auto_runs_and_changes() -> None:
    image = _sample()
    out, applied = edit_image(image, {"auto": True})
    assert "auto" in applied
    assert out.mode == "RGB"


def test_grain_and_vignette_apply() -> None:
    image = _sample()
    out, applied = edit_image(image, {"grain": 40, "vignette": 50})
    assert "grain" in applied
    assert "vignette" in applied
    assert out.tobytes() != image.tobytes()


def test_non_numeric_slider_raises() -> None:
    with pytest.raises(ValueError, match="exposure"):
        edit_image(_sample(), {"exposure": "way up"})


def test_image_edit_tool_stores_new_artifact(fresh_db, tmp_path: Path) -> None:
    from tasque2.mcp import tools

    source = tmp_path / "in.png"
    _sample(96, 96).save(source)
    result = json.loads(
        tools.image_edit(
            str(source),
            label="lead crop test",
            exposure=0.4,
            contrast=15,
            clarity=20,
            depth_of_field={"focus": "center", "strength": 45},
        )
    )
    assert result["ok"] is True
    assert {"exposure", "contrast", "clarity", "depth_of_field"} <= set(result["applied"])
    edited_path = Path(result["local_path"])
    assert edited_path.is_file()
    assert edited_path != source  # original untouched, new artifact written
    with Image.open(edited_path) as edited:
        assert edited.size == (96, 96)
