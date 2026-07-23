"""A photographic "darkroom" for real images — non-generative editing.

This is deliberately not AI generation. It's the set of adjustments a
photographer reaches for in Lightroom/Camera Raw, implemented on Pillow: a
per-channel tone engine (exposure, contrast, highlights/shadows, whites/blacks,
white balance, temperature/tint), vibrance/saturation in HSV, local-contrast
clarity and sharpening, denoise, film grain, vignette, straighten, and a
depth-of-field blur that keeps a chosen region sharp and softens the rest.

Everything runs through 256-entry lookup tables (the same mechanism as a real
tone curve) and Pillow's C filters, so it's fast and dependency-free. The
``image_edit`` MCP tool wraps ``edit_image`` and stores the result as a new
artifact; the original is never touched.

Slider conventions match Lightroom intuition: most take roughly -100..100 where
0 is no change; ``exposure`` is in stops (EV, ~-5..5); ``straighten`` is degrees.
"""

from __future__ import annotations

from typing import Any

from PIL import Image, ImageEnhance, ImageFilter, ImageOps, ImageStat

# The photographic order edits are applied in, regardless of dict order.
_TONE_KEYS = (
    "exposure",
    "contrast",
    "highlights",
    "shadows",
    "whites",
    "blacks",
    "temperature",
    "tint",
    "dehaze",
)


def _clamp8(value: float) -> int:
    if value <= 0:
        return 0
    if value >= 255:
        return 255
    return int(round(value))


def _f(ops: dict[str, Any], key: str, default: float = 0.0) -> float:
    raw = ops.get(key, default)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a number, got {raw!r}") from exc


def _tone_luts(ops: dict[str, Any]) -> tuple[list[int], list[int], list[int]]:
    """Fold every tonal + white-balance adjustment into one LUT per channel."""
    exposure = _f(ops, "exposure")
    contrast = _f(ops, "contrast")
    highlights = _f(ops, "highlights")
    shadows = _f(ops, "shadows")
    whites = _f(ops, "whites")
    blacks = _f(ops, "blacks")
    temperature = _f(ops, "temperature")
    tint = _f(ops, "tint")
    dehaze = _f(ops, "dehaze")

    exposure_factor = 2.0**exposure
    contrast_factor = 1.0 + (contrast / 100.0)
    # Dehaze ~ black-point pull + local contrast + a touch of extra contrast.
    dehaze_black = max(0.0, dehaze) / 100.0 * 40.0
    contrast_factor += max(0.0, dehaze) / 100.0 * 0.25

    channel_scale = {
        "r": 1.0 + (temperature / 100.0) * 0.30,
        "g": 1.0 + (tint / 100.0) * 0.20,
        "b": 1.0 - (temperature / 100.0) * 0.30,
    }

    def build(scale: float) -> list[int]:
        table: list[int] = []
        for i in range(256):
            v = float(i)
            # Exposure (multiplicative light), then black-point from dehaze.
            v *= exposure_factor
            if dehaze_black:
                v = (v - dehaze_black) * (255.0 / max(1.0, 255.0 - dehaze_black))
            norm = min(max(v / 255.0, 0.0), 1.0)
            # Region-weighted tonal moves.
            if shadows:
                v += (shadows / 100.0) * 80.0 * (1.0 - norm) ** 2
            if blacks:
                v += (blacks / 100.0) * 60.0 * (1.0 - norm) ** 3
            if highlights:
                v += (highlights / 100.0) * 80.0 * norm**2
            if whites:
                v += (whites / 100.0) * 60.0 * norm**3
            # Global contrast around mid-grey.
            v = (v - 128.0) * contrast_factor + 128.0
            # White balance / temperature / tint per channel.
            v *= scale
            table.append(_clamp8(v))
        return table

    return build(channel_scale["r"]), build(channel_scale["g"]), build(channel_scale["b"])


def _apply_tone(image: Image.Image, ops: dict[str, Any]) -> Image.Image:
    if not any(abs(_f(ops, key)) > 1e-9 for key in _TONE_KEYS):
        return image
    lut_r, lut_g, lut_b = _tone_luts(ops)
    return image.point(lut_r + lut_g + lut_b)


def _apply_vibrance_saturation(image: Image.Image, ops: dict[str, Any]) -> Image.Image:
    vibrance = _f(ops, "vibrance")
    saturation = _f(ops, "saturation")
    if abs(vibrance) < 1e-9 and abs(saturation) < 1e-9:
        return image
    hsv = image.convert("HSV")
    h, s, v = hsv.split()
    if abs(vibrance) > 1e-9:
        amount = vibrance / 100.0
        # Weight the boost toward already-muted pixels (true vibrance behaviour).
        lut = [_clamp8(i + amount * (255 - i) * (1.0 - i / 255.0) * 1.5) for i in range(256)]
        s = s.point(lut)
    hsv = Image.merge("HSV", (h, s, v)).convert("RGB")
    if abs(saturation) > 1e-9:
        hsv = ImageEnhance.Color(hsv).enhance(1.0 + saturation / 100.0)
    return hsv


def _focus_mask(size: tuple[int, int], focus: str, feather: float) -> Image.Image:
    """A white(=keep sharp)→black(=blur) mask for depth of field."""
    width, height = size
    mask = Image.new("L", size, 0)
    from PIL import ImageDraw

    draw = ImageDraw.Draw(mask)
    focus = (focus or "center").lower()
    if focus in {"center", "radial"}:
        steps = 48
        for i in range(steps):
            v = int(255 * (i / (steps - 1)))
            fraction = 1.0 - i / steps  # largest (dark) first, smallest (bright) last
            half_w = width * 0.75 * fraction
            half_h = height * 0.9 * fraction
            cx, cy = width / 2, height * 0.42  # bias focus slightly above centre (faces)
            draw.ellipse(
                [cx - half_w, cy - half_h, cx + half_w, cy + half_h], fill=v
            )
    else:
        # Linear tilt-shift band across the frame.
        steps = max(width, height)
        for i in range(steps):
            t = i / (steps - 1)
            # Triangle peak (bright) at the focus edge/centre.
            if focus == "top":
                v = int(255 * max(0.0, 1.0 - t * 1.6))
                draw.line([(0, int(t * height)), (width, int(t * height))], fill=v)
            elif focus == "bottom":
                v = int(255 * max(0.0, 1.0 - (1.0 - t) * 1.6))
                draw.line([(0, int(t * height)), (width, int(t * height))], fill=v)
            elif focus == "left":
                v = int(255 * max(0.0, 1.0 - t * 1.6))
                draw.line([(int(t * width), 0), (int(t * width), height)], fill=v)
            else:  # right
                v = int(255 * max(0.0, 1.0 - (1.0 - t) * 1.6))
                draw.line([(int(t * width), 0), (int(t * width), height)], fill=v)
    radius = max(4.0, feather)
    return mask.filter(ImageFilter.GaussianBlur(radius))


def _apply_depth_of_field(image: Image.Image, dof: dict[str, Any]) -> Image.Image:
    strength = _f(dof, "strength", 60.0)
    if strength <= 0:
        return image
    focus = str(dof.get("focus") or "center")
    blur_radius = max(1.0, strength / 100.0 * (max(image.size) / 40.0))
    blurred = image.filter(ImageFilter.GaussianBlur(blur_radius))
    feather = min(image.size) / 12.0
    mask = _focus_mask(image.size, focus, feather)
    return Image.composite(image, blurred, mask)


def _apply_vignette(image: Image.Image, amount: float) -> Image.Image:
    if abs(amount) < 1e-9:
        return image
    mask = _focus_mask(image.size, "center", min(image.size) / 6.0)
    strength = min(max(amount, -100.0), 100.0) / 100.0
    if strength > 0:  # darken edges
        black = Image.new("RGB", image.size, (0, 0, 0))
        darkened = Image.blend(image, black, strength * 0.9)
        return Image.composite(image, darkened, mask)
    white = Image.new("RGB", image.size, (255, 255, 255))
    brightened = Image.blend(image, white, -strength * 0.9)
    return Image.composite(image, brightened, mask)


def _apply_grain(image: Image.Image, amount: float) -> Image.Image:
    if amount <= 0:
        return image
    sigma = amount / 100.0 * 32.0
    noise = Image.effect_noise(image.size, sigma).convert("L")
    noise_rgb = Image.merge("RGB", (noise, noise, noise))
    return Image.blend(image, noise_rgb, min(0.5, amount / 100.0 * 0.4))


def edit_image(image: Image.Image, ops: dict[str, Any]) -> tuple[Image.Image, list[str]]:
    """Apply the darkroom chain; return the edited image + names of ops that ran."""
    applied: list[str] = []
    result = image.convert("RGB")

    if _f(ops, "straighten"):
        angle = _f(ops, "straighten")
        result = result.rotate(-angle, resample=Image.BICUBIC, expand=False)
        applied.append("straighten")

    if ops.get("auto"):
        result = ImageOps.autocontrast(result, cutoff=1)
        # Grey-world white balance: pull channel means together.
        means = ImageStat.Stat(result).mean
        grey = sum(means) / 3.0
        luts: list[int] = []
        for mean in means:
            scale = grey / mean if mean > 1e-6 else 1.0
            luts += [_clamp8(i * scale) for i in range(256)]
        result = result.point(luts)
        applied.append("auto")

    toned = _apply_tone(result, ops)
    if toned is not result:
        applied.extend(key for key in _TONE_KEYS if abs(_f(ops, key)) > 1e-9)
        result = toned

    saturated = _apply_vibrance_saturation(result, ops)
    if saturated is not result:
        applied.extend(k for k in ("vibrance", "saturation") if abs(_f(ops, k)) > 1e-9)
        result = saturated

    if _f(ops, "clarity"):
        pct = int(min(300.0, abs(_f(ops, "clarity")) * 2.0))
        result = result.filter(ImageFilter.UnsharpMask(radius=40, percent=pct, threshold=0))
        applied.append("clarity")
    if _f(ops, "sharpen"):
        pct = int(min(300.0, abs(_f(ops, "sharpen")) * 3.0))
        result = result.filter(ImageFilter.UnsharpMask(radius=2, percent=pct, threshold=2))
        applied.append("sharpen")
    if _f(ops, "denoise"):
        result = result.filter(ImageFilter.MedianFilter(size=3))
        applied.append("denoise")

    dof = ops.get("depth_of_field")
    if isinstance(dof, dict) and _f(dof, "strength", 60.0) > 0:
        result = _apply_depth_of_field(result, dof)
        applied.append("depth_of_field")

    if _f(ops, "vignette"):
        result = _apply_vignette(result, _f(ops, "vignette"))
        applied.append("vignette")
    if _f(ops, "grain"):
        result = _apply_grain(result, _f(ops, "grain"))
        applied.append("grain")

    return result, applied
