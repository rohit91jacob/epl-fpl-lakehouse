"""``epl`` command line: every pipeline stage is a subcommand, so any scheduler can run it."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from epl_lakehouse.config import Settings
from epl_lakehouse.ingestion.pipeline import new_run_id, run_ingestion
from epl_lakehouse.logging_utils import configure_logging
from epl_lakehouse.quality.results import DataQualityError

log = logging.getLogger("epl_lakehouse.cli")

EXIT_OK, EXIT_FAILURE, EXIT_DATA_QUALITY = 0, 1, 2


def _spark(settings: Settings):  # type: ignore[no-untyped-def]
    from epl_lakehouse.spark_session import build_spark

    return build_spark(settings)


def cmd_ingest(settings: Settings, args: argparse.Namespace) -> int:
    run_ingestion(
        settings,
        run_id=args.run_id,
        full_refresh=args.full_refresh,
        with_player_history=args.player_history,
    )
    return EXIT_OK


def cmd_bronze(settings: Settings, args: argparse.Namespace) -> int:
    from epl_lakehouse.bronze import run_bronze

    run_bronze(_spark(settings), settings, [args.run_id] if args.only_run else None)
    return EXIT_OK


def cmd_silver(settings: Settings, args: argparse.Namespace) -> int:
    from epl_lakehouse.silver import run_silver

    run_silver(_spark(settings), settings, full_refresh=args.full_refresh)
    return EXIT_OK


def cmd_gold(settings: Settings, args: argparse.Namespace) -> int:
    from epl_lakehouse.gold import run_gold

    run_gold(_spark(settings), settings, args.season or None)
    return EXIT_OK


def cmd_quality(settings: Settings, args: argparse.Namespace) -> int:
    from epl_lakehouse.quality.suites import run_suite

    spark = _spark(settings)
    layers = ["silver", "gold"] if args.layer == "all" else [args.layer]
    for layer in layers:
        run_suite(spark, settings, layer, args.run_id)
    return EXIT_OK


def cmd_run(settings: Settings, args: argparse.Namespace) -> int:
    """The whole pipeline in one process: ingest -> bronze -> silver -> DQ -> gold -> DQ."""
    from epl_lakehouse.bronze import run_bronze
    from epl_lakehouse.gold import run_gold
    from epl_lakehouse.quality.suites import run_suite
    from epl_lakehouse.silver import run_silver

    run_ingestion(
        settings,
        run_id=args.run_id,
        full_refresh=args.full_refresh,
        with_player_history=args.player_history,
    )
    spark = _spark(settings)
    run_bronze(spark, settings)
    run_silver(spark, settings, full_refresh=args.full_refresh)
    run_suite(spark, settings, "silver", args.run_id)
    run_gold(spark, settings)
    run_suite(spark, settings, "gold", args.run_id)
    return EXIT_OK


def cmd_maintenance(settings: Settings, args: argparse.Namespace) -> int:
    from epl_lakehouse.maintenance import run_maintenance

    run_maintenance(_spark(settings), settings, args.retention_hours)
    return EXIT_OK


def cmd_show(settings: Settings, args: argparse.Namespace) -> int:
    from epl_lakehouse import delta_io

    layer, name = args.table.split(".", 1)
    df = delta_io.read(_spark(settings), settings.table_path(layer, name))
    if args.where:
        df = df.filter(args.where)
    if args.order_by:
        df = df.orderBy(*[_sort_column(c) for c in args.order_by.split(",")])
    if args.columns:
        df = df.select(*[c.strip() for c in args.columns.split(",")])
    df.show(args.limit, truncate=False)
    return EXIT_OK


def _sort_column(spec: str):  # type: ignore[no-untyped-def]
    """``"points"`` -> ascending, ``"points DESC"`` -> descending."""
    from pyspark.sql import functions as F

    name, _, direction = spec.strip().partition(" ")
    column = F.col(name)
    return column.desc() if direction.strip().lower() == "desc" else column.asc()


def cmd_report(settings: Settings, args: argparse.Namespace) -> int:
    from epl_lakehouse.report import build_report

    page = build_report(_spark(settings), settings, Path(args.out))
    print(page.resolve().as_uri())
    return EXIT_OK


def cmd_sample_data(settings: Settings, args: argparse.Namespace) -> int:
    from epl_lakehouse.sample import SampleConfig, write_sample_api

    out = write_sample_api(
        Path(args.out), SampleConfig(seed=args.seed, finished_gameweeks=args.finished_gameweeks)
    )
    print(out.resolve().as_uri())
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="epl", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add(name: str, func, help_: str) -> argparse.ArgumentParser:  # type: ignore[no-untyped-def]
        p = sub.add_parser(name, help=help_, description=help_)
        p.set_defaults(func=func)
        return p

    def with_run_id(p: argparse.ArgumentParser) -> None:
        p.add_argument("--run-id", default=new_run_id(), help="orchestrator run id (lineage)")

    def with_ingest_flags(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--full-refresh",
            action="store_true",
            help="ignore incremental state and refetch/reprocess everything",
        )
        p.add_argument(
            "--player-history",
            action=argparse.BooleanOptionalAction,
            default=None,
            help="fetch per-player element-summary history (default: EPL_INGEST_PLAYER_HISTORY)",
        )

    p = add("ingest", cmd_ingest, "Fetch the FPL API into the raw landing zone.")
    with_run_id(p)
    with_ingest_flags(p)

    p = add("bronze", cmd_bronze, "Load landed runs into the bronze Delta table.")
    with_run_id(p)
    p.add_argument("--only-run", action="store_true", help="load only --run-id")

    p = add("silver", cmd_silver, "Parse new bronze payloads into silver tables.")
    p.add_argument("--full-refresh", action="store_true", help="reprocess all of bronze")

    p = add("gold", cmd_gold, "Rebuild the gold model and marts.")
    p.add_argument("--season", action="append", help="limit to a season such as 2026-27")

    p = add("quality", cmd_quality, "Run data-quality suites (exit code 2 on failure).")
    with_run_id(p)
    p.add_argument("--layer", choices=["silver", "gold", "all"], default="all")

    p = add("run", cmd_run, "Run the full pipeline end to end.")
    with_run_id(p)
    with_ingest_flags(p)

    p = add("maintenance", cmd_maintenance, "OPTIMIZE and VACUUM every Delta table.")
    p.add_argument("--retention-hours", type=int, default=None)

    p = add("show", cmd_show, "Print a table, e.g. `epl show gold.mart_league_table`.")
    p.add_argument("table", help="<layer>.<table>")
    p.add_argument("--where", help="SQL filter")
    p.add_argument("--order-by", help='comma-separated, e.g. "points DESC,team_name"')
    p.add_argument("--columns", help="comma-separated columns to show")
    p.add_argument("--limit", type=int, default=20)

    p = add("report", cmd_report, "Write the static results site (index.html + data.json).")
    p.add_argument("--out", default="site", help="output directory")

    p = add("sample-data", cmd_sample_data, "Write a synthetic FPL API snapshot for offline runs.")
    p.add_argument("--out", required=True)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--finished-gameweeks", type=int, default=5)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    try:
        return int(args.func(settings, args))
    except DataQualityError as exc:
        log.error(str(exc))
        return EXIT_DATA_QUALITY
    except Exception:
        log.exception("command failed", extra={"command": args.command})
        return EXIT_FAILURE


if __name__ == "__main__":
    sys.exit(main())
