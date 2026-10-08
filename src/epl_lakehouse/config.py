"""Runtime configuration, read from ``EPL_*`` environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

from epl_lakehouse import __version__

DEFAULT_USER_AGENT = (
    f"epl-fpl-lakehouse/{__version__} (+https://github.com/rohit91jacob/epl-fpl-lakehouse)"
)


def _parse_bool(value: str) -> bool:
    normalised = value.strip().lower()
    if normalised in {"1", "true", "yes", "on"}:
        return True
    if normalised in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"not a boolean: {value!r}")


@dataclass(frozen=True)
class Settings:
    """All tunables for the pipeline. Every field maps to ``EPL_<FIELD_NAME>``."""

    data_root: str = "./data"
    fpl_base_url: str = "https://fantasy.premierleague.com/api"
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 5
    http_backoff_seconds: float = 1.0
    max_requests_per_second: float = 4.0
    max_workers: int = 4
    user_agent: str = DEFAULT_USER_AGENT
    ingest_player_history: bool = True
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"
    spark_shuffle_partitions: int = 8
    spark_jars: str = ""
    log_level: str = "INFO"
    log_format: str = "json"
    expected_team_count: int = 20
    vacuum_retention_hours: int = 168

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None, **overrides: Any) -> Settings:
        env = os.environ if environ is None else environ
        values: dict[str, Any] = {}
        for f in fields(cls):
            raw = env.get(f"EPL_{f.name.upper()}")
            if raw is None or raw == "":
                continue
            if f.type in ("bool", bool):
                values[f.name] = _parse_bool(raw)
            elif f.type in ("int", int):
                values[f.name] = int(raw)
            elif f.type in ("float", float):
                values[f.name] = float(raw)
            else:
                values[f.name] = raw
        return replace(cls(**values), **overrides)

    @property
    def root(self) -> Path:
        return Path(self.data_root).expanduser().resolve()

    @property
    def raw_root(self) -> Path:
        return self.root / "raw"

    def table_path(self, layer: str, name: str) -> str:
        """Filesystem location of a Delta table, e.g. ``table_path("silver", "teams")``."""
        if layer not in {"bronze", "silver", "gold", "ops"}:
            raise ValueError(f"unknown layer: {layer}")
        return str(self.root / layer / name)
