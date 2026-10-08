"""Shared test helpers (importable, unlike conftest)."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from epl_lakehouse.config import Settings

GW5_CLOCK = datetime(2026, 9, 14, 6, 0, tzinfo=UTC)  # the Monday after gameweek 5
GW6_CLOCK = datetime(2026, 9, 21, 6, 0, tzinfo=UTC)


def lake_settings(root: Path, api_dir: Path, **overrides: Any) -> Settings:
    return Settings(
        data_root=str(root),
        fpl_base_url=api_dir.as_uri(),
        max_workers=4,
        max_requests_per_second=0,
        spark_master="local[2]",
        spark_shuffle_partitions=2,
        spark_driver_memory="1g",
        log_format="text",
        **overrides,
    )


def fixed_clock(moment: datetime) -> Callable[[], datetime]:
    return lambda: moment


def load_api(api_dir: Path, path: str) -> Any:
    return json.loads((api_dir / f"{path}.json").read_text())
