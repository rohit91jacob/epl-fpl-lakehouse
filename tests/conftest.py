from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from epl_lakehouse.sample import SampleConfig, write_sample_api
from tests.helpers import lake_settings


@pytest.fixture(scope="session")
def sample_api(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return write_sample_api(tmp_path_factory.mktemp("api-gw5"), SampleConfig(finished_gameweeks=5))


@pytest.fixture(scope="session")
def sample_api_gw6(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return write_sample_api(tmp_path_factory.mktemp("api-gw6"), SampleConfig(finished_gameweeks=6))


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    from epl_lakehouse.spark_session import build_spark

    root = tmp_path_factory.mktemp("spark")
    session = build_spark(lake_settings(root, root), app_name="epl-tests")
    yield session
    session.stop()
