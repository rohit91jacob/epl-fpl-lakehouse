from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from epl_lakehouse.ingestion.client import FileTransport, FplApiError
from epl_lakehouse.ingestion.landing import RawLandingZone, safe_run_id
from epl_lakehouse.ingestion.pipeline import (
    IngestionError,
    derive_season,
    new_run_id,
    run_ingestion,
)
from tests.helpers import GW5_CLOCK, GW6_CLOCK, fixed_clock, lake_settings

PLAYERS = 300
GAMEWEEKS = 5


class FailingTransport(FileTransport):
    def __init__(self, root: Path, fail_on: str) -> None:
        super().__init__(root)
        self.fail_on = fail_on

    def get(self, path: str) -> tuple[bytes, str]:
        if path == self.fail_on:
            raise FplApiError("simulated outage")
        return super().get(path)


def test_derive_season() -> None:
    assert derive_season({"events": [{"deadline_time": "2026-08-14T17:30:00Z"}]}) == "2026-27"
    assert derive_season({"events": [{"deadline_time": "1999-08-07T10:00:00Z"}]}) == "1999-00"


def test_run_ids_are_path_safe() -> None:
    assert (
        safe_run_id("scheduled__2026-10-07T06:00:00+00:00")
        == "scheduled__2026-10-07T06_00_00_00_00"
    )
    assert ":" not in new_run_id()


def test_first_run_lands_everything_with_checksums(tmp_path: Path, sample_api: Path) -> None:
    settings = lake_settings(tmp_path, sample_api)
    result = run_ingestion(settings, run_id="r1", clock=fixed_clock(GW5_CLOCK))

    assert result.season == "2026-27"
    assert result.landed == 2 + GAMEWEEKS + PLAYERS
    zone = RawLandingZone(settings.raw_root)
    manifest = zone.read_manifest("r1")
    assert manifest.status == "succeeded"
    by_endpoint = {f.endpoint for f in manifest.files}
    assert by_endpoint == {"bootstrap-static", "fixtures", "event-live", "element-summary"}
    for landed in manifest.files[:10]:
        content = (zone.root / landed.path).read_bytes()
        assert hashlib.sha256(content).hexdigest() == landed.sha256
        assert "ingest_date=2026-09-14" in landed.path
    live = [f for f in manifest.files if f.endpoint == "event-live"]
    assert all(f.is_final for f in live)
    assert zone.load_state("2026-27").final_gameweeks == set(range(1, GAMEWEEKS + 1))


def test_second_run_skips_unchanged_and_final_resources(tmp_path: Path, sample_api: Path) -> None:
    settings = lake_settings(tmp_path, sample_api)
    run_ingestion(settings, run_id="r1", clock=fixed_clock(GW5_CLOCK))
    again = run_ingestion(settings, run_id="r2", clock=fixed_clock(GW5_CLOCK))
    # Final gameweeks are not refetched; everything else is byte-identical.
    assert again.landed == 0
    assert again.skipped_unchanged == 2 + PLAYERS

    refreshed = run_ingestion(settings, run_id="r3", full_refresh=True)
    assert refreshed.landed == 2 + GAMEWEEKS + PLAYERS


def test_season_progress_lands_only_new_data(
    tmp_path: Path, sample_api: Path, sample_api_gw6: Path
) -> None:
    run_ingestion(lake_settings(tmp_path, sample_api), run_id="r1", clock=fixed_clock(GW5_CLOCK))
    later = run_ingestion(
        lake_settings(tmp_path, sample_api_gw6), run_id="r2", clock=fixed_clock(GW6_CLOCK)
    )
    zone = RawLandingZone(tmp_path / "raw")
    live = [f.resource_id for f in zone.read_manifest("r2").files if f.endpoint == "event-live"]
    assert live == ["6"]
    assert later.landed > 3  # bootstrap, fixtures, GW6 live and changed player histories


def test_partial_failure_fails_the_run_and_keeps_state(tmp_path: Path, sample_api: Path) -> None:
    settings = lake_settings(tmp_path, sample_api)
    transport = FailingTransport(sample_api, fail_on="element-summary/17/")
    with pytest.raises(IngestionError, match="element-summary/17"):
        run_ingestion(settings, run_id="r1", transport=transport)
    zone = RawLandingZone(settings.raw_root)
    assert zone.read_manifest("r1").status == "failed"
    assert zone.load_state("2026-27").final_gameweeks == set()
    assert zone.list_manifests() == []  # nothing for bronze to load

    retried = run_ingestion(settings, run_id="r1")  # orchestrator retry, same run id
    assert retried.landed == 2 + GAMEWEEKS + PLAYERS
    assert zone.read_manifest("r1").status == "succeeded"


def test_player_history_can_be_disabled(tmp_path: Path, sample_api: Path) -> None:
    result = run_ingestion(
        lake_settings(tmp_path, sample_api), run_id="r1", with_player_history=False
    )
    assert result.landed == 2 + GAMEWEEKS
