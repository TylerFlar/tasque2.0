"""The weather part: current conditions and a daily forecast for a home location (Open-Meteo, no key).

It serves one tool, ``weather_now``. The location is the part's own setting, read like the core's
(the environment, then the env file): ``TASQUE2_WEATHER_LATITUDE`` and ``TASQUE2_WEATHER_LONGITUDE``,
with ``TASQUE2_WEATHER_LOCATION_LABEL`` naming the place (the coordinates when blank). There is no
default location: until both coordinates are set, ``weather_now`` answers ``{ok: false}`` saying what
to set. The forecast's days follow the core timezone.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tasque2.config import ENV_FILE, get_settings
from tasque2.extensions import ExtensionRegistry
from tasque2.mcp.toolkit import run_json

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
MAX_FORECAST_DAYS = 7
NO_LOCATION = (
    "No weather location is set: set TASQUE2_WEATHER_LATITUDE and TASQUE2_WEATHER_LONGITUDE "
    "(TASQUE2_WEATHER_LOCATION_LABEL names the place), or take weather out of TASQUE2_PARTS to turn it off."
)


class WeatherSettings(BaseSettings):
    """The home location, from ``TASQUE2_WEATHER_*`` variables; a blank one counts as unset."""

    model_config = SettingsConfigDict(
        env_prefix="TASQUE2_WEATHER_", env_file=ENV_FILE, env_ignore_empty=True, extra="ignore"
    )

    latitude: float | None = Field(default=None)
    longitude: float | None = Field(default=None)
    location_label: str | None = Field(default=None)


# WMO weather interpretation codes.
_WEATHER_CODES = {
    0: "clear",
    1: "mostly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "icy fog",
    51: "light drizzle",
    53: "drizzle",
    55: "heavy drizzle",
    56: "freezing drizzle",
    57: "freezing drizzle",
    61: "light rain",
    63: "rain",
    65: "heavy rain",
    66: "freezing rain",
    67: "freezing rain",
    71: "light snow",
    73: "snow",
    75: "heavy snow",
    77: "snow grains",
    80: "light showers",
    81: "showers",
    82: "violent showers",
    85: "snow showers",
    86: "heavy snow showers",
    95: "thunderstorm",
    96: "thunderstorm with hail",
    99: "thunderstorm with heavy hail",
}


def _conditions(code: Any) -> str | None:
    try:
        return _WEATHER_CODES.get(int(code))
    except (TypeError, ValueError):
        return None


def shape_forecast(payload: dict[str, Any], *, label: str) -> dict[str, Any]:
    """Shape a raw Open-Meteo response into the compact report workers read."""
    current = payload.get("current") or {}
    daily = payload.get("daily") or {}

    def _day(index: int) -> dict[str, Any] | None:
        dates = daily.get("time") or []
        if index >= len(dates):
            return None

        def value(key: str) -> Any:
            values = daily.get(key) or []
            return values[index] if index < len(values) else None

        return {
            "date": dates[index],
            "high_f": value("temperature_2m_max"),
            "low_f": value("temperature_2m_min"),
            "feels_like_high_f": value("apparent_temperature_max"),
            "feels_like_low_f": value("apparent_temperature_min"),
            "precip_chance_pct": value("precipitation_probability_max"),
            "conditions": _conditions(value("weather_code")),
            "sunset": value("sunset"),
        }

    days = []
    index = 0
    while True:
        day = _day(index)
        if day is None:
            break
        days.append(day)
        index += 1

    return {
        "location": label,
        "timezone": payload.get("timezone"),
        "current": {
            "time": current.get("time"),
            "temp_f": current.get("temperature_2m"),
            "feels_like_f": current.get("apparent_temperature"),
            "humidity_pct": current.get("relative_humidity_2m"),
            "precipitation_in": current.get("precipitation"),
            "wind_mph": current.get("wind_speed_10m"),
            "conditions": _conditions(current.get("weather_code")),
        },
        "today": days[0] if days else None,
        "upcoming": days[1:],
    }


def fetch_local_weather(*, days: int = 3) -> dict[str, Any]:
    """Fetch and shape the local forecast for the configured coordinates; with none set, raise before any call."""
    location = WeatherSettings()
    if location.latitude is None or location.longitude is None:
        raise ValueError(NO_LOCATION)
    import httpx

    forecast_days = max(1, min(int(days or 3), MAX_FORECAST_DAYS))
    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "current": (
            "temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,weather_code,wind_speed_10m"
        ),
        "daily": (
            "temperature_2m_max,temperature_2m_min,apparent_temperature_max,"
            "apparent_temperature_min,precipitation_probability_max,weather_code,sunset"
        ),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": get_settings().timezone,
        "forecast_days": forecast_days,
    }
    with httpx.Client(timeout=20.0) as client:
        response = client.get(OPEN_METEO_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    label = (location.location_label or "").strip() or f"{location.latitude}, {location.longitude}"
    return shape_forecast(payload, label=label)


def weather_now(days: int = 3, intent: str = "") -> str:
    """Current conditions and a daily forecast (high/low, feels-like, rain chance, sunset)
    for the configured home location; ``days`` is 1-7."""
    return run_json(lambda: {"ok": True, "weather": fetch_local_weather(days=days)}, intent=intent)


def register(registry: ExtensionRegistry) -> None:
    registry.add_mcp_tools(weather_now)
