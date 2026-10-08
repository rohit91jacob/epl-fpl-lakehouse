"""Immutable raw landing zone: API responses stored byte-for-byte, plus run manifests.

Layout under ``<data_root>/raw``::

    fpl/<endpoint>/season=<season>/ingest_date=<date>/run_id=<run>/<name>.json
    _manifests/run_id=<run>.json      # what a run landed, with checksums
    _state/season=<season>.json       # incremental state (final gameweeks, last checksums)
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from epl_lakehouse.ingestion.client import FetchResult

_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def safe_run_id(run_id: str) -> str:
    """Make an orchestrator run id safe for file paths (Hadoop paths reject ':')."""
    return _UNSAFE.sub("_", run_id)


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        Path(tmp).replace(path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _file_name(fetch: FetchResult) -> str:
    if fetch.endpoint == "event-live":
        return f"gw-{fetch.resource_id}.json"
    if fetch.endpoint == "element-summary":
        return f"player-{fetch.resource_id}.json"
    return f"{fetch.endpoint}.json"


def state_key(endpoint: str, resource_id: str | None) -> str:
    return endpoint if resource_id is None else f"{endpoint}/{resource_id}"


@dataclass(frozen=True)
class LandedFile:
    endpoint: str
    resource_id: str | None
    season: str
    path: str  # relative to the raw root
    source_url: str
    fetched_at: str
    sha256: str
    size_bytes: int
    is_final: bool | None = None


@dataclass
class Manifest:
    run_id: str
    status: str
    season: str | None
    started_at: str
    finished_at: str
    files: list[LandedFile] = field(default_factory=list)
    skipped_unchanged: int = 0
    errors: list[str] = field(default_factory=list)

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Manifest:
        files = [LandedFile(**f) for f in data.get("files", [])]
        return cls(**{**data, "files": files})


@dataclass
class IngestionState:
    final_gameweeks: set[int] = field(default_factory=set)
    last_sha256: dict[str, str] = field(default_factory=dict)


class RawLandingZone:
    def __init__(self, raw_root: Path) -> None:
        self.root = raw_root

    def land(
        self, fetch: FetchResult, *, run_id: str, season: str, is_final: bool | None = None
    ) -> LandedFile:
        ingest_date = fetch.fetched_at.date().isoformat()
        relative = (
            Path("fpl")
            / fetch.endpoint
            / f"season={season}"
            / f"ingest_date={ingest_date}"
            / f"run_id={safe_run_id(run_id)}"
            / _file_name(fetch)
        )
        _atomic_write(self.root / relative, fetch.content)
        return LandedFile(
            endpoint=fetch.endpoint,
            resource_id=fetch.resource_id,
            season=season,
            path=relative.as_posix(),
            source_url=fetch.source_url,
            fetched_at=fetch.fetched_at.isoformat(),
            sha256=hashlib.sha256(fetch.content).hexdigest(),
            size_bytes=len(fetch.content),
            is_final=is_final,
        )

    # -- manifests -------------------------------------------------------------------------

    def _manifest_path(self, run_id: str) -> Path:
        return self.root / "_manifests" / f"run_id={safe_run_id(run_id)}.json"

    def write_manifest(self, manifest: Manifest) -> Path:
        path = self._manifest_path(manifest.run_id)
        _atomic_write(path, json.dumps(asdict(manifest), indent=2).encode())
        return path

    def read_manifest(self, run_id: str) -> Manifest:
        return Manifest.from_json(json.loads(self._manifest_path(run_id).read_text()))

    def list_manifests(self, status: str | None = "succeeded") -> list[Manifest]:
        manifests = [
            Manifest.from_json(json.loads(p.read_text()))
            for p in sorted((self.root / "_manifests").glob("run_id=*.json"))
        ]
        manifests.sort(key=lambda m: m.started_at)
        return [m for m in manifests if status is None or m.status == status]

    # -- incremental state -----------------------------------------------------------------

    def _state_path(self, season: str) -> Path:
        return self.root / "_state" / f"season={season}.json"

    def load_state(self, season: str) -> IngestionState:
        path = self._state_path(season)
        if not path.exists():
            return IngestionState()
        data = json.loads(path.read_text())
        return IngestionState(set(data["final_gameweeks"]), dict(data["last_sha256"]))

    def save_state(self, season: str, state: IngestionState) -> None:
        payload = {
            "final_gameweeks": sorted(state.final_gameweeks),
            "last_sha256": dict(sorted(state.last_sha256.items())),
        }
        _atomic_write(self._state_path(season), json.dumps(payload, indent=2).encode())
