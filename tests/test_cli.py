from __future__ import annotations

import json
from pathlib import Path

import pytest

from epl_lakehouse import cli
from epl_lakehouse.quality.results import CheckResult, DataQualityError, Severity


def test_help_lists_every_stage(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    for command in (
        "ingest",
        "bronze",
        "silver",
        "gold",
        "quality",
        "run",
        "maintenance",
        "show",
        "report",
    ):
        assert command in out


def test_sample_data_then_ingest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        cli.main(["sample-data", "--out", str(tmp_path / "api"), "--finished-gameweeks", "2"]) == 0
    )
    base_url = capsys.readouterr().out.strip()
    assert base_url.startswith("file://")

    monkeypatch.setenv("EPL_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("EPL_FPL_BASE_URL", base_url)
    monkeypatch.setenv("EPL_MAX_REQUESTS_PER_SECOND", "0")
    assert cli.main(["ingest", "--run-id", "cli-1", "--no-player-history"]) == 0
    manifest = json.loads((tmp_path / "data/raw/_manifests/run_id=cli-1.json").read_text())
    assert manifest["status"] == "succeeded"
    assert len(manifest["files"]) == 4  # bootstrap, fixtures, 2 gameweeks


def test_failures_map_to_exit_codes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("EPL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("EPL_FPL_BASE_URL", (tmp_path / "nowhere").as_uri())
    assert cli.main(["ingest"]) == cli.EXIT_FAILURE

    failure = CheckResult("silver.teams", "unique", Severity.ERROR, False, 2, "")

    def broken(*_: object) -> int:
        raise DataQualityError([failure])

    # build_parser() resolves command functions at call time, so patching the module works.
    monkeypatch.setattr(cli, "cmd_ingest", broken)
    assert cli.main(["ingest"]) == cli.EXIT_DATA_QUALITY
