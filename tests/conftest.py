from __future__ import annotations

from pathlib import Path

import pytest

from tasque2.daemon import reset_background_pool, reset_bookkeeping_clocks
from tasque2.db import create_schema, reset_engine
from tasque2.extensions import registry as load_extension_registry
from tasque2.queue import reset_provider_limit_gate


@pytest.fixture()
def fresh_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "tasque2.sqlite3"
    monkeypatch.setenv("TASQUE2_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()
    # Load the extension packages BEFORE creating the schema so their tables are
    # part of it. Otherwise whichever test first touched the registry decided
    # whether later CLI paths saw an "unversioned" schema (order-dependent flake).
    load_extension_registry()
    create_schema()
    # The limit gate, the bookkeeping clocks and the background work pool are all
    # process-local daemon state; a fresh database stands in for a fresh daemon
    # process, so each must start clean.
    reset_provider_limit_gate()
    reset_background_pool()
    reset_bookkeeping_clocks()
    yield db_path
    reset_background_pool()
    reset_engine()
    reset_provider_limit_gate()
    reset_bookkeeping_clocks()
