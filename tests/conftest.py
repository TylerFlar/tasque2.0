from __future__ import annotations

from pathlib import Path

import pytest

from tasque2.daemon import reset_background_pool
from tasque2.db import create_schema, reset_engine
from tasque2.queue import reset_provider_limit_gate


@pytest.fixture()
def fresh_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "tasque2.sqlite3"
    monkeypatch.setenv("TASQUE2_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()
    create_schema()
    # The limit gate and the background work pool are process-local daemon
    # state; a fresh database stands in for a fresh daemon process, so both
    # must start clean.
    reset_provider_limit_gate()
    reset_background_pool()
    yield db_path
    reset_background_pool()
    reset_engine()
    reset_provider_limit_gate()
