from __future__ import annotations

from pathlib import Path

import pytest

from epl_lakehouse.config import Settings


def test_defaults_point_at_the_public_api() -> None:
    settings = Settings.from_env({})
    assert settings.fpl_base_url == "https://fantasy.premierleague.com/api"
    assert settings.ingest_player_history is True


def test_env_values_are_typed() -> None:
    settings = Settings.from_env(
        {
            "EPL_MAX_WORKERS": "8",
            "EPL_HTTP_TIMEOUT_SECONDS": "12.5",
            "EPL_INGEST_PLAYER_HISTORY": "false",
            "EPL_DATA_ROOT": "/tmp/lake",
            "EPL_LOG_LEVEL": "",  # empty means "use the default"
        }
    )
    assert settings.max_workers == 8
    assert settings.http_timeout_seconds == 12.5
    assert settings.ingest_player_history is False
    assert settings.data_root == "/tmp/lake"
    assert settings.log_level == "INFO"


def test_overrides_win_over_env() -> None:
    settings = Settings.from_env({"EPL_MAX_WORKERS": "8"}, max_workers=2)
    assert settings.max_workers == 2


def test_invalid_boolean_is_rejected() -> None:
    with pytest.raises(ValueError, match="not a boolean"):
        Settings.from_env({"EPL_INGEST_PLAYER_HISTORY": "maybe"})


def test_table_paths_are_layered(tmp_path: Path) -> None:
    settings = Settings(data_root=str(tmp_path))
    assert settings.table_path("silver", "teams") == str(tmp_path / "silver" / "teams")
    with pytest.raises(ValueError, match="unknown layer"):
        settings.table_path("platinum", "teams")
