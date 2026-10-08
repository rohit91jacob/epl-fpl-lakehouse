"""Ingestion run: decide what to fetch, land it, and record a manifest.

Incremental rules:
* ``bootstrap-static`` and ``fixtures`` are fetched every run.
* ``event/<gw>/live`` is fetched for gameweeks that have started, until the gameweek is
  final (``finished`` and ``data_checked``); after that it never changes and is skipped.
* ``element-summary/<id>`` (per-player history, optional) is fetched for every player.
* A response byte-identical to the last landed one for the same resource is not landed again.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from epl_lakehouse.config import Settings
from epl_lakehouse.ingestion.client import (
    FetchResult,
    FplApiError,
    FplClient,
    Transport,
    transport_from_settings,
)
from epl_lakehouse.ingestion.landing import LandedFile, Manifest, RawLandingZone, state_key

log = logging.getLogger(__name__)


class IngestionError(RuntimeError):
    pass


@dataclass(frozen=True)
class IngestionResult:
    run_id: str
    season: str
    landed: int
    skipped_unchanged: int
    manifest_path: Path


def derive_season(bootstrap: dict[str, Any]) -> str:
    """FPL ids reset every season, so every row is keyed by season, e.g. ``"2026-27"``."""
    first_deadline = min(e["deadline_time"] for e in bootstrap["events"] if e["deadline_time"])
    year = datetime.fromisoformat(first_deadline.replace("Z", "+00:00")).year
    return f"{year}-{(year + 1) % 100:02d}"


def new_run_id(now: datetime | None = None) -> str:
    moment = now or datetime.now(UTC)
    return "manual__" + moment.strftime("%Y%m%dT%H%M%S%fZ")


def run_ingestion(
    settings: Settings,
    *,
    run_id: str,
    full_refresh: bool = False,
    with_player_history: bool | None = None,
    transport: Transport | None = None,
    clock: Callable[[], datetime] | None = None,
) -> IngestionResult:
    """Fetch and land one ingestion run. ``transport``/``clock`` are injectable for tests."""
    transport = transport or transport_from_settings(settings)
    client = FplClient(transport, clock) if clock else FplClient(transport)
    zone = RawLandingZone(settings.raw_root)
    started_at = datetime.now(UTC).isoformat()
    include_history = (
        settings.ingest_player_history if with_player_history is None else with_player_history
    )

    bootstrap = client.bootstrap_static()
    season = derive_season(bootstrap.data)
    state = zone.load_state(season)
    landed: list[LandedFile] = []
    new_sha: dict[str, str] = {}
    skipped = 0

    def land(fetch: FetchResult, is_final: bool | None = None) -> None:
        nonlocal skipped
        key = state_key(fetch.endpoint, fetch.resource_id)
        digest = hashlib.sha256(fetch.content).hexdigest()
        if not full_refresh and state.last_sha256.get(key) == digest:
            skipped += 1
            return
        landed.append(zone.land(fetch, run_id=run_id, season=season, is_final=is_final))
        new_sha[key] = digest

    errors: list[str] = []
    newly_final: set[int] = set()
    try:
        land(bootstrap)
        land(client.fixtures())
        for event in bootstrap.data["events"]:
            gameweek = int(event["id"])
            if not (event["finished"] or event["is_current"]):
                continue
            if gameweek in state.final_gameweeks and not full_refresh:
                continue
            is_final = bool(event["finished"] and event["data_checked"])
            land(client.event_live(gameweek), is_final=is_final)
            if is_final:
                newly_final.add(gameweek)

        if include_history:
            player_ids = sorted(int(e["id"]) for e in bootstrap.data["elements"])
            with ThreadPoolExecutor(max_workers=max(settings.max_workers, 1)) as pool:
                futures = {pool.submit(client.element_summary, pid): pid for pid in player_ids}
                for future in as_completed(futures):
                    try:
                        land(future.result())
                    except FplApiError as exc:
                        errors.append(f"element-summary/{futures[future]}: {exc}")
    except FplApiError as exc:
        errors.append(str(exc))

    manifest = Manifest(
        run_id=run_id,
        status="failed" if errors else "succeeded",
        season=season,
        started_at=started_at,
        finished_at=datetime.now(UTC).isoformat(),
        files=sorted(landed, key=lambda f: f.path),
        skipped_unchanged=skipped,
        errors=sorted(errors),
    )
    manifest_path = zone.write_manifest(manifest)
    log_fields = {
        "run_id": run_id,
        "season": season,
        "landed": len(landed),
        "skipped_unchanged": skipped,
        "errors": len(errors),
    }
    if errors:
        log.error("ingestion failed", extra=log_fields)
        raise IngestionError(f"{len(errors)} request(s) failed, first: {sorted(errors)[0]}")

    # State is advanced only after a fully successful run, so a failed run is retried in full.
    state.final_gameweeks |= newly_final
    state.last_sha256.update(new_sha)
    zone.save_state(season, state)
    log.info("ingestion succeeded", extra=log_fields)
    return IngestionResult(run_id, season, len(landed), skipped, manifest_path)
