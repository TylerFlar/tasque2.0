"""Local weather for outfit decisions, via the Open-Meteo forecast API.

The stylist factors the actual day (temperature, feels-like, rain chance,
evening cool-off) into looks instead of guessing from the season. Exposed as
the ``weather_now`` MCP tool; any worker may call it. Open-Meteo needs no API
key. Coordinates default to San Diego and are configurable via
``TASQUE2_WEATHER_LATITUDE`` / ``TASQUE2_WEATHER_LONGITUDE`` /
``TASQUE2_WEATHER_LOCATION_LABEL``.

:func:`shape_forecast` is a pure payload -> report function so the shaping
logic is testable without the network.
"""

from __future__ import annotations

from typing import Any

from tasque2.config import get_settings

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
MAX_FORECAST_DAYS = 7

# WMO weather interpretation codes -> short text.
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
        "note": (
            "Dress for feels-like, not the number: sun and humidity read warmer, "
            "wind and coastal evenings cooler. Check today's low + sunset for an "
            "evening-layer call."
        ),
    }


def fetch_local_weather(*, days: int = 3) -> dict[str, Any]:
    """Fetch and shape the local forecast for the configured coordinates."""
    import httpx

    settings = get_settings()
    forecast_days = max(1, min(int(days or 3), MAX_FORECAST_DAYS))
    params = {
        "latitude": settings.weather_latitude,
        "longitude": settings.weather_longitude,
        "current": (
            "temperature_2m,apparent_temperature,relative_humidity_2m,"
            "precipitation,weather_code,wind_speed_10m"
        ),
        "daily": (
            "temperature_2m_max,temperature_2m_min,apparent_temperature_max,"
            "apparent_temperature_min,precipitation_probability_max,weather_code,sunset"
        ),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": settings.timezone,
        "forecast_days": forecast_days,
    }
    with httpx.Client(timeout=20.0) as client:
        response = client.get(OPEN_METEO_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    return shape_forecast(payload, label=settings.weather_location_label)
