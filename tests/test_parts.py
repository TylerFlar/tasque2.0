"""Built-in optional parts: ``TASQUE2_PARTS`` names the ones that run, each registering through the hook
extensions use. Weather is the first: its tool, its own location settings, and how the operator sees it."""

from __future__ import annotations

import asyncio
import importlib.util
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

import tasque2.parts.weather as weather_part
from tasque2 import parts
from tasque2.config import ENV_FILE, Settings, reset_settings
from tasque2.db import session_scope
from tasque2.extensions import ExtensionError, extensions_dir, registry, reset_registry
from tasque2.mcp.server import build_server
from tasque2.mcp.tools import CORE_TOOLS
from tasque2.models import ProviderRun, WorkAttempt, WorkItem, utc_now
from tasque2.ops.doctor import HealthCheck, run_doctor
from tasque2.parts.weather import OPEN_METEO_URL, shape_forecast, weather_now
from tasque2.workshop import sweep

NOW = utc_now()
LOCATION = ("TASQUE2_WEATHER_LATITUDE", "TASQUE2_WEATHER_LONGITUDE")
PAYLOAD = {
    "timezone": "UTC",
    "current": {
        "time": "2031-03-01T09:00",
        "temperature_2m": 61.5,
        "apparent_temperature": 59.0,
        "relative_humidity_2m": 70,
        "precipitation": 0.02,
        "weather_code": 61,
        "wind_speed_10m": 11.2,
    },
    "daily": {
        "time": ["2031-03-01", "2031-03-02", "2031-03-03"],
        "temperature_2m_max": [64.0, 58.5, 60.1],
        "temperature_2m_min": [50.2, 47.9, 49.0],
        "apparent_temperature_max": [62.8, 55.0, 59.3],
        "apparent_temperature_min": [48.1, 44.6, 47.7],
        "precipitation_probability_max": [80, 35, 5],
        "weather_code": [61, 3, 42],
        "sunset": ["2031-03-01T17:51", "2031-03-02T17:52"],
    },
}


def _parts(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Run the parts ``value`` names (the tests otherwise run with none)."""
    monkeypatch.setenv("TASQUE2_PARTS", value)
    reset_settings()
    reset_registry()


def _served() -> dict[str, Any]:
    return {tool.name: tool for tool in asyncio.run(build_server().list_tools())}


def _settings_file(text: str) -> None:
    """The settings file in the directory Tasque starts in: each test runs in its own."""
    Path(ENV_FILE).write_text(text, encoding="utf-8")


def _extensions_check() -> HealthCheck:
    report = run_doctor(which=lambda name: f"/fake/bin/{name}")
    return next(check for check in report.checks if check.name == "extensions")


# --- the loader ------------------------------------------------------------------------------------------


def test_parts_default_to_weather_and_a_blank_setting_runs_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TASQUE2_PARTS")
    assert Settings().part_list == ["weather"]

    monkeypatch.setenv("TASQUE2_PARTS", " weather, ,weather ")
    assert Settings().part_list == ["weather"]

    monkeypatch.setenv("TASQUE2_PARTS", "")
    assert Settings().part_list == []
    assert "weather" in parts.available() and not [name for name in parts.available() if name.startswith("_")]


def test_a_named_part_registers_and_is_listed_with_no_extensions_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    _parts(monkeypatch, "weather")

    loaded = registry()

    assert not extensions_dir().is_dir()
    assert (loaded.part_names, loaded.extension_names) == (["weather"], [])
    assert loaded.mcp_tools == [weather_now]


def test_blank_parts_load_none() -> None:
    loaded = registry()

    assert loaded.part_names == [] and loaded.mcp_tools == []


def test_parts_register_before_the_extensions(isolated: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = isolated / "extensions" / "parts_order_ext"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(
        "def register(registry):\n"
        "    def ledger_total(intent: str = '') -> str:\n"
        "        '''The ledger's total.'''\n"
        "        return '{\"ok\": true}'\n"
        "\n"
        "    registry.add_mcp_tools(ledger_total)\n",
        encoding="utf-8",
    )
    _parts(monkeypatch, "weather")

    loaded = registry()

    assert (loaded.part_names, loaded.extension_names) == (["weather"], ["parts_order_ext"])
    assert [tool.__name__ for tool in loaded.mcp_tools] == ["weather_now", "ledger_total"]


def test_an_unknown_part_stops_the_load_and_names_the_parts_there_are(monkeypatch: pytest.MonkeyPatch) -> None:
    _parts(monkeypatch, "weather,tide_tables")

    with pytest.raises(ExtensionError, match=r"Part 'tide_tables' in TASQUE2_PARTS .*they are: .*weather"):
        registry()


def test_a_part_that_fails_to_register_stops_the_load(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(_registry: Any) -> None:
        raise RuntimeError("no forecast service")

    monkeypatch.setattr(weather_part, "register", broken)
    _parts(monkeypatch, "weather")

    with pytest.raises(ExtensionError, match="Part 'weather' failed to register: no forecast service"):
        registry()


# --- the weather part ------------------------------------------------------------------------------------


def test_weather_now_is_served_with_its_schema_when_the_part_is_on_and_absent_when_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    off = _served()
    _parts(monkeypatch, "weather")
    on = _served()

    assert "weather_now" not in off and set(off) == {tool.__name__ for tool in CORE_TOOLS}
    assert set(on) == set(off) | {"weather_now"}
    served = on["weather_now"]
    assert served.description == weather_now.__doc__
    assert list(served.input_schema["properties"]) == ["days", "intent"]
    assert served.input_schema["properties"]["days"]["default"] == 3
    assert not served.input_schema.get("required")


def test_the_core_holds_no_weather() -> None:
    assert "weather_now" not in {tool.__name__ for tool in CORE_TOOLS}
    assert importlib.util.find_spec("tasque2.weather") is None
    assert not [name for name in Settings.model_fields if name.startswith("weather")]


def test_weather_now_without_a_location_says_what_to_set_and_calls_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_network(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("weather_now called out with no location set")

    monkeypatch.setattr(httpx, "Client", no_network)
    answers = [json.loads(weather_now())]
    for name in LOCATION:
        monkeypatch.setenv(name, "")
    answers.append(json.loads(weather_now()))
    _settings_file("TASQUE2_WEATHER_LONGITUDE=-45.25\nTASQUE2_WEATHER_LOCATION_LABEL=Sample Bay\n")
    answers.append(json.loads(weather_now(days=2, intent="dress for the day")))

    for answer in answers:  # unset, blank, and only one coordinate set
        assert (answer["ok"], answer["error_type"]) == (False, "ValueError")
        assert all(name in answer["error"] for name in (*LOCATION, "TASQUE2_PARTS"))


def test_weather_now_reads_its_location_as_the_core_reads_its_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    class Response:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict[str, Any]:
            return PAYLOAD

    class Client:
        def __init__(self, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> Client:
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

        def get(self, url: str, params: dict[str, Any]) -> Response:
            calls.append({"url": url, **params})
            return Response()

    monkeypatch.setattr(httpx, "Client", Client)
    monkeypatch.setenv("TASQUE2_TIMEZONE", "Etc/GMT+3")
    reset_settings()
    _settings_file(
        "TASQUE2_WEATHER_LATITUDE=12.5\nTASQUE2_WEATHER_LONGITUDE=-45.25\nTASQUE2_WEATHER_LOCATION_LABEL=Sample Bay\n"
    )
    from_file = json.loads(weather_now(days=9, intent="plan the walk"))
    _settings_file("TASQUE2_WEATHER_LATITUDE=12.5\nTASQUE2_WEATHER_LONGITUDE=-45.25\nTASQUE2_WEATHER_LOCATION_LABEL=\n")
    monkeypatch.setenv("TASQUE2_WEATHER_LATITUDE", "40")
    overridden = json.loads(weather_now())

    assert (from_file["ok"], from_file["_intent"]) == (True, "plan the walk")
    assert from_file["weather"]["location"] == "Sample Bay"
    assert from_file["weather"]["today"]["date"] == "2031-03-01"
    assert calls[0]["url"] == OPEN_METEO_URL
    assert (calls[0]["latitude"], calls[0]["longitude"]) == (12.5, -45.25)
    assert (calls[0]["timezone"], calls[0]["forecast_days"]) == ("Etc/GMT+3", 7)
    # The environment wins over the file, and a blank label names the place by its coordinates.
    assert (calls[1]["latitude"], calls[1]["forecast_days"]) == (40.0, 3)
    assert overridden["weather"]["location"] == "40.0, -45.25"


def test_shape_forecast_compacts_an_open_meteo_payload() -> None:
    report = shape_forecast(PAYLOAD, label="Sample Bay")

    assert (report["location"], report["timezone"]) == ("Sample Bay", "UTC")
    assert report["current"] == {
        "time": "2031-03-01T09:00",
        "temp_f": 61.5,
        "feels_like_f": 59.0,
        "humidity_pct": 70,
        "precipitation_in": 0.02,
        "wind_mph": 11.2,
        "conditions": "light rain",
    }
    assert report["today"] == {
        "date": "2031-03-01",
        "high_f": 64.0,
        "low_f": 50.2,
        "feels_like_high_f": 62.8,
        "feels_like_low_f": 48.1,
        "precip_chance_pct": 80,
        "conditions": "light rain",
        "sunset": "2031-03-01T17:51",
    }
    assert [(day["date"], day["conditions"]) for day in report["upcoming"]] == [
        ("2031-03-02", "overcast"),
        ("2031-03-03", None),  # a code the table does not know
    ]
    assert report["upcoming"][-1]["sunset"] is None  # a value the payload left out
    assert shape_forecast({}, label="Sample Bay")["today"] is None


# --- the operator's view ---------------------------------------------------------------------------------


def test_doctor_lists_the_parts_that_are_on(monkeypatch: pytest.MonkeyPatch) -> None:
    off = _extensions_check()
    _parts(monkeypatch, "weather")
    on = _extensions_check()
    _parts(monkeypatch, "tide_tables")
    unknown = _extensions_check()

    assert (off.status, off.summary, off.details["parts"]) == ("ok", "0 extension(s) loaded; no parts on.", [])
    assert (on.status, on.summary) == ("ok", "0 extension(s) loaded; parts on: weather.")
    assert on.details == {"parts": ["weather"], "names": [], "tools": 1, "digests": 0}
    assert unknown.status == "fail" and "'tide_tables'" in unknown.summary


def test_the_sweep_names_a_part_as_the_owner_of_its_idle_tools(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _parts(monkeypatch, "weather")
    with session_scope() as session:
        for days_ago, calls in ((29, {"mcp__tasque2__memory_recall": 1}), (5, {"mcp__tasque2__work_list": 2})):
            item = WorkItem(title="A run", task_instruction="x", worker_kind="provider.default", lane="desk")
            session.add(item)
            session.flush()
            attempt = WorkAttempt(work_item_id=item.id, attempt_number=1, status="succeeded", worker_kind="provider")
            session.add(attempt)
            session.flush()
            session.add(
                ProviderRun(
                    attempt_id=attempt.id,
                    provider="claude",
                    usage={"tool_calls": calls},
                    created_at=NOW - timedelta(days=days_ago),
                )
            )
        session.flush()
        found = {signal.key: signal for signal in sweep.unused_tools(session, NOW)}

    assert sorted(found) == ["unused_tools:core", "unused_tools:weather"]
    idle = found["unused_tools:weather"]
    assert idle.title == "1 weather tool(s): no run called them in 30 days"
    assert idle.evidence == ["2 runs in 30 days called none of: weather_now"]
    assert not any("weather_now" in line for line in found["unused_tools:core"].evidence)
