"""Static results site (league table, form, value picks, fixture ticker, team xG) from gold.

Writes ``index.html`` (self-contained: no scripts, no external assets) and ``data.json``
with the same numbers for programmatic use. Colour never carries meaning alone: fixture
cells print the opponent and difficulty number, form badges print W/D/L.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from epl_lakehouse import delta_io
from epl_lakehouse.config import Settings

REPO_URL = "https://github.com/rohit91jacob/epl-fpl-lakehouse"

# Validated with the dataviz skill's validator: ordinal blue ramp (light 250..650,
# dark 600..200) and a blue/red polarity pair for W/L; draws are outlined neutral.
CSS = """
:root { color-scheme: light dark; }
.viz-root {
  --surface-1: #fcfcfb; --surface-2: #f0efec; --border: #dedcd5;
  --text-primary: #0b0b0b; --text-secondary: #52514e;
  --d1: #86b6ef; --d2: #5598e7; --d3: #2a78d6; --d4: #1c5cab; --d5: #104281;
  --d1-ink: #0b0b0b; --d2-ink: #0b0b0b; --d3-ink: #ffffff; --d4-ink: #ffffff; --d5-ink: #ffffff;
  --win: #2a78d6; --loss: #e34948;
}
@media (prefers-color-scheme: dark) {
  .viz-root {
    --surface-1: #1a1a19; --surface-2: #262624; --border: #383835;
    --text-primary: #ffffff; --text-secondary: #c3c2b7;
    --d1: #184f95; --d2: #256abf; --d3: #3987e5; --d4: #6da7ec; --d5: #9ec5f4;
    --d1-ink: #ffffff; --d2-ink: #ffffff; --d3-ink: #0b0b0b; --d4-ink: #0b0b0b; --d5-ink: #0b0b0b;
    --win: #3987e5; --loss: #e66767;
  }
}
* { box-sizing: border-box; }
body { margin: 0; font: 15px/1.45 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
.viz-root { background: var(--surface-1); color: var(--text-primary); min-height: 100vh; }
main { max-width: 1100px; margin: 0 auto; padding: 24px 16px 48px; }
h1 { font-size: 1.6rem; margin: 0 0 4px; }
h2 { font-size: 1.15rem; margin: 36px 0 4px; }
p.meta, p.note, footer { color: var(--text-secondary); }
p.note { margin: 0 0 10px; font-size: .9rem; }
.scroll { overflow-x: auto; border: 1px solid var(--border); border-radius: 8px; }
table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
th, td { padding: 6px 10px; text-align: right; white-space: nowrap; }
th { color: var(--text-secondary); font-weight: 600; border-bottom: 1px solid var(--border); }
th.l, td.l { text-align: left; }
th.c { text-align: center; }
tbody tr:nth-child(even) { background: var(--surface-2); }
.badge { display: inline-block; width: 1.6em; margin-right: 2px; border-radius: 4px;
  text-align: center; font-weight: 700; font-size: .8rem; line-height: 1.6em; }
.W { background: var(--win); color: #fff; } .L { background: var(--loss); color: #fff; }
.D { background: var(--surface-1); color: var(--text-primary); box-shadow: inset 0 0 0 1px var(--text-secondary); }
td.fx { text-align: center; border-left: 2px solid var(--surface-1); font-size: .85rem; }
.fx1 { background: var(--d1); color: var(--d1-ink); } .fx2 { background: var(--d2); color: var(--d2-ink); }
.fx3 { background: var(--d3); color: var(--d3-ink); } .fx4 { background: var(--d4); color: var(--d4-ink); }
.fx5 { background: var(--d5); color: var(--d5-ink); }
.legend { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; font-size: .85rem;
  color: var(--text-secondary); margin: 0 0 8px; }
.legend span.sw { padding: 1px 8px; border-radius: 4px; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; }
footer { margin-top: 40px; font-size: .85rem; }
a { color: inherit; }
"""

POSITIONS = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}


def _rows(df: DataFrame) -> list[dict[str, Any]]:
    return [r.asDict(recursive=True) for r in df.collect()]


def _num(value: Any, digits: int = 0) -> str:
    if value is None:
        return "–"
    return f"{float(value):.{digits}f}" if digits else str(int(value))


def collect(spark: SparkSession, settings: Settings) -> dict[str, Any]:
    """Pull the small result sets the page needs from gold (latest season only)."""

    def gold(name: str) -> DataFrame:
        return delta_io.read(spark, settings.table_path("gold", name))

    def silver(name: str) -> DataFrame:
        return delta_io.read(spark, settings.table_path("silver", name))

    season = gold("dim_gameweek").agg(F.max("season")).first()[0]  # type: ignore[index]
    gws = gold("dim_gameweek").filter(F.col("season") == season)
    as_of = (
        gold("mart_league_table").filter(F.col("season") == season).agg(F.max("as_of_gameweek"))
    ).first()[0]  # type: ignore[index]
    finished = gws.filter("finished").agg(F.max("gameweek_id")).first()[0]  # type: ignore[index]
    data_as_of = silver("players").agg(F.max("fetched_at")).first()[0]  # type: ignore[index]
    teams = (
        gold("dim_team")
        .filter(F.col("season") == season)
        .select("team_id", "team_name", "short_name")
    )

    league = (
        gold("mart_league_table")
        .filter((F.col("season") == season) & (F.col("as_of_gameweek") == as_of))
        .orderBy("position")
    )

    form = (
        gold("mart_player_form")
        .filter((F.col("season") == season) & (F.col("gameweek_id") == finished))
        .join(
            silver("players").filter(F.col("season") == season).select("player_id", "team_id"),
            "player_id",
        )
        .join(teams.select("team_id", "short_name"), "team_id")
    )
    in_form = form.orderBy(F.desc("points_last5"), F.desc("xgi_last5"), "web_name").limit(10)
    value_rank = Window.partitionBy("position_id").orderBy(
        F.desc("points_per_million_last5"), F.desc("points_last5"), "web_name"
    )
    value = (
        form.filter(F.col("minutes_last5") >= 180)
        .withColumn("_rk", F.row_number().over(value_rank))
        .filter("_rk <= 3")
        .orderBy("position_id", "_rk")
    )

    ticker = (
        gold("mart_fixture_ticker")
        .filter(F.col("season") == season)
        .join(teams, "team_id")
        .orderBy("avg_difficulty", "team_name")
    )

    xg = (
        gold("mart_team_gameweek")
        .filter(F.col("season") == season)
        .groupBy("team_id")
        .agg(
            F.sum("goals_for").alias("goals_for"),
            F.sum("xg_for").alias("xg_for"),
            F.sum("goals_against").alias("goals_against"),
            F.sum("xg_against").alias("xg_against"),
        )
        .withColumn("goals_minus_xg", F.col("goals_for") - F.col("xg_for"))
        .join(teams, "team_id")
        .orderBy(F.desc("xg_for"))
    )

    return {
        "season": season,
        "as_of_gameweek": as_of,
        "latest_finished_gameweek": finished,
        "data_as_of": data_as_of.isoformat() if data_as_of else None,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "league_table": _rows(league),
        "in_form": _rows(in_form),
        "value_picks": _rows(value),
        "fixture_ticker": _rows(ticker),
        "team_xg": _rows(xg),
    }


def _signed(value: int) -> str:
    return f"{value:+d}" if value else "0"


def _form_badges(form: str) -> str:
    names = {"W": "win", "D": "draw", "L": "loss"}
    return "".join(
        f'<span class="badge {c}" title="{names.get(c, c)}">{escape(c)}</span>' for c in form
    )


def _table(headers: list[tuple[str, str]], body: list[str]) -> str:
    head = "".join(f'<th class="{cls}" scope="col">{escape(h)}</th>' for h, cls in headers)
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def render(data: dict[str, Any]) -> str:
    league_rows = [
        "<tr>"
        f'<td>{r["position"]}</td><td class="l">{escape(r["team_name"])}</td>'
        f"<td>{r['played']}</td><td>{r['won']}</td><td>{r['drawn']}</td><td>{r['lost']}</td>"
        f"<td>{r['goals_for']}</td><td>{r['goals_against']}</td>"
        f"<td>{_signed(r['goal_difference'])}</td><td><strong>{r['points']}</strong></td>"
        f'<td class="l">{_form_badges(r["form_last5"] or "")}</td>'
        "</tr>"
        for r in data["league_table"]
    ]
    league = _table(
        [("#", ""), ("Club", "l"), ("P", ""), ("W", ""), ("D", ""), ("L", ""), ("GF", ""),
         ("GA", ""), ("GD", ""), ("Pts", ""), ("Form (oldest → newest)", "l")],
        league_rows,
    )  # fmt: skip

    def player_row(r: dict[str, Any]) -> str:
        return (
            "<tr>"
            f'<td class="l">{escape(r["web_name"])}</td><td class="l">{escape(r["short_name"])}</td>'
            f'<td class="l">{POSITIONS.get(r["position_id"], "?")}</td>'
            f"<td>£{_num(r['price_m'], 1)}m</td><td><strong>{r['points_last5']}</strong></td>"
            f"<td>{r['minutes_last5']}</td><td>{_num(r['xgi_last5'], 2)}</td>"
            f"<td>{_num(r['points_per_million_last5'], 2)}</td>"
            "</tr>"
        )

    player_headers = [("Player", "l"), ("Club", "l"), ("Pos", "l"), ("Price", ""),
                      ("Pts (5 GW)", ""), ("Mins", ""), ("xGI", ""), ("Pts / £m", "")]  # fmt: skip
    in_form = _table(player_headers, [player_row(r) for r in data["in_form"]])
    value = _table(player_headers, [player_row(r) for r in data["value_picks"]])

    horizon = max((len(r["fixtures"]) for r in data["fixture_ticker"]), default=0)
    first_gw = data["fixture_ticker"][0]["from_gameweek"] if data["fixture_ticker"] else None
    ticker_rows = []
    for r in data["fixture_ticker"]:
        by_gw: dict[int, list[dict[str, Any]]] = {}
        for fx in r["fixtures"]:
            by_gw.setdefault(fx["gameweek_id"], []).append(fx)
        cells = []
        for gw in range(first_gw or 0, (first_gw or 0) + max(horizon, 5)):
            fxs = by_gw.get(gw, [])
            if not fxs:
                cells.append('<td class="fx" title="Blank gameweek">–</td>')
                continue
            label = " + ".join(f"{fx['opponent']} ({fx['venue']}) {fx['difficulty']}" for fx in fxs)
            hardest = max(int(fx["difficulty"] or 3) for fx in fxs)
            tip = "; ".join(
                f"GW{gw} vs {fx['opponent']} {'home' if fx['venue'] == 'H' else 'away'}, difficulty {fx['difficulty']}"
                for fx in fxs
            )
            cells.append(
                f'<td class="fx fx{min(max(hardest, 1), 5)}" title="{escape(tip)}">{escape(label)}</td>'
            )
        ticker_rows.append(
            f'<tr><td class="l">{escape(r["team_name"])}</td><td>{_num(r["avg_difficulty"], 2)}</td>{"".join(cells)}</tr>'
        )
    ticker = _table(
        [("Club", "l"), ("Avg", "")]
        + [(f"GW{g}", "c") for g in range(first_gw or 0, (first_gw or 0) + max(horizon, 5))],
        ticker_rows,
    )
    legend = (
        '<p class="legend">FPL difficulty: '
        + "".join(f'<span class="sw fx{i}">{i}</span>' for i in range(1, 6))
        + " (1 easiest · 5 hardest; each cell shows opponent, venue and rating)</p>"
    )

    xg_rows = [
        "<tr>"
        f'<td class="l">{escape(r["team_name"])}</td><td>{r["goals_for"]}</td><td>{_num(r["xg_for"], 2)}</td>'
        f"<td>{_num(r['goals_minus_xg'], 2)}</td><td>{r['goals_against']}</td><td>{_num(r['xg_against'], 2)}</td>"
        "</tr>"
        for r in data["team_xg"]
    ]
    xg = _table(
        [("Club", "l"), ("Goals", ""), ("xG", ""), ("Goals − xG", ""), ("Conceded", ""),
         ("xGA*", "")],
        xg_rows,
    )  # fmt: skip

    as_of = data["data_as_of"] or data["generated_at"]
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Premier League {escape(str(data["season"]))} — EPL FPL Lakehouse</title>
<meta name="description" content="Premier League standings, form, value picks and fixture difficulty, rebuilt daily from the Fantasy Premier League API.">
<style>{CSS}</style></head>
<body class="viz-root"><main>
<h1>Premier League {escape(str(data["season"]))}</h1>
<p class="meta">Standings after gameweek {data["as_of_gameweek"]} · data fetched {escape(as_of[:16].replace("T", " "))} UTC ·
rebuilt daily by <a href="{REPO_URL}">epl-fpl-lakehouse</a></p>

<h2>League table</h2>
<p class="note">Ranked by points, goal difference, goals scored.</p>
{league}

<div class="grid">
<section><h2>In form</h2><p class="note">Most FPL points over the last 5 gameweeks.</p>{in_form}</section>
<section><h2>Best value by position</h2><p class="note">Points per £m over the last 5 gameweeks (min. 180 minutes).</p>{value}</section>
</div>

<h2>Fixture ticker</h2>
<p class="note">Next gameweeks, easiest run first.</p>
{legend}
{ticker}

<h2>Attack and defence</h2>
<p class="note">Season to date. *xGA is approximated from the largest expected-goals-conceded among each club's players.</p>
{xg}

<footer>Data: Fantasy Premier League API (© Premier League), fetched for personal, educational use; this site is not
affiliated with the Premier League. Numbers: <a href="data.json">data.json</a> · Code: <a href="{REPO_URL}">GitHub</a>.</footer>
</main></body></html>
"""


def build_report(spark: SparkSession, settings: Settings, out_dir: Path) -> Path:
    data = collect(spark, settings)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "index.html").write_text(render(data), encoding="utf-8")
    (out_dir / "data.json").write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    (out_dir / ".nojekyll").write_text("", encoding="utf-8")
    return out_dir / "index.html"
