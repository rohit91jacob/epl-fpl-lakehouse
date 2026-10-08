"""Deterministic synthetic FPL API snapshot, for tests, CI smoke runs and offline demos.

The output mirrors the real API's paths and field names, so it can be served through
``EPL_FPL_BASE_URL=file:///path/to/dir``. Clubs and players are fictional.

Everything is seeded per fixture / per player, so a snapshot generated "later in the
season" (more finished gameweeks) agrees exactly with an earlier one on past results,
which lets tests exercise incremental runs.
"""

from __future__ import annotations

import json
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

CLUBS = [
    ("Ashford Rovers", "ASH"),
    ("Bramley United", "BRA"),
    ("Castleton City", "CAS"),
    ("Dunmore Athletic", "DUN"),
    ("Eastbrook Town", "EAS"),
    ("Fairhaven", "FAI"),
    ("Glenwood Wanderers", "GLE"),
    ("Hartfield Albion", "HAR"),
    ("Ironbridge County", "IRO"),
    ("Kingsley Park", "KIN"),
    ("Lakeside Rangers", "LAK"),
    ("Marlow Forest", "MAR"),
    ("Northgate Villa", "NOR"),
    ("Oakham Orient", "OAK"),
    ("Pemberton Palace", "PEM"),
    ("Queensbury Hotspur", "QUE"),
    ("Redcliffe Rovers", "RED"),
    ("Stanmore Wednesday", "STA"),
    ("Thornbury Argyle", "THO"),
    ("Westfield Harriers", "WES"),
]
FIRST_NAMES = [
    "Alex", "Ben", "Callum", "Dan", "Eli", "Femi", "George", "Hugo", "Isaac", "Jay",
    "Kai", "Leo", "Max", "Noah", "Oscar", "Pablo", "Reece", "Sam", "Theo", "Yusuf",
]  # fmt: skip
SURNAMES = [
    "Adebayo", "Barnes", "Clarke", "Dawson", "Evans", "Fletcher", "Grant", "Hughes",
    "Ibrahim", "Jensen", "Kowalski", "Lindqvist", "Moreno", "Nwosu", "Okafor", "Patel",
    "Quinn", "Reyes", "Silva", "Turner", "Uzoma", "Varga", "Walsh", "Yilmaz", "Zielinski",
]  # fmt: skip
POSITIONS = [
    # id, singular, short, plural, squad_select, min_play, max_play
    (1, "Goalkeeper", "GKP", "Goalkeepers", 2, 1, 1),
    (2, "Defender", "DEF", "Defenders", 5, 3, 5),
    (3, "Midfielder", "MID", "Midfielders", 5, 2, 5),
    (4, "Forward", "FWD", "Forwards", 3, 1, 3),
]
SQUAD = [1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 4]  # 15 per club
STARTERS = [0, 2, 3, 4, 5, 7, 8, 9, 10, 12, 13]  # squad indexes in the starting XI
SUBS = [6, 11, 14]  # come off the bench
BASE_PRICE = {1: 45, 2: 47, 3: 60, 4: 70}
GOAL_POINTS = {1: 10, 2: 6, 3: 5, 4: 4}
CLEAN_SHEET_POINTS = {1: 4, 2: 4, 3: 1, 4: 0}
DEF_CONTRIBUTION_THRESHOLD = {2: 10, 3: 12, 4: 12}


@dataclass(frozen=True)
class SampleConfig:
    seed: int = 7
    finished_gameweeks: int = 5
    first_deadline: datetime = datetime(2026, 8, 14, 17, 30, tzinfo=UTC)
    # A gameweek-3 fixture is moved to gameweek 5 (blank GW3 + double GW5 for two clubs),
    # and a gameweek-8 fixture is postponed with no new date yet.
    moved_fixture: tuple[int, int] = (3, 5)
    unscheduled_gameweek: int = 8


def _iso(moment: datetime | None) -> str | None:
    return None if moment is None else moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _round_robin(n: int) -> list[list[tuple[int, int]]]:
    """Double round robin (circle method): 2*(n-1) rounds of n/2 (home, away) pairs.

    Venues are assigned greedily so clubs alternate home and away where possible; the
    second half mirrors the first with venues swapped.
    """
    teams = list(range(1, n + 1))
    last_home: dict[int, bool] = {}
    home_games = dict.fromkeys(teams, 0)
    rounds = []
    for _ in range(n - 1):
        pairs = []
        for i in range(n // 2):
            a, b = teams[i], teams[n - 1 - i]
            a_wants = (not last_home.get(a, False), -home_games[a], -a)
            b_wants = (not last_home.get(b, False), -home_games[b], -b)
            home, away = (a, b) if a_wants > b_wants else (b, a)
            last_home[home], last_home[away] = True, False
            home_games[home] += 1
            pairs.append((home, away))
        rounds.append(pairs)
        teams = [teams[0], teams[-1], *teams[1:-1]]
    return rounds + [[(b, a) for a, b in pairs] for pairs in rounds]


@dataclass
class _Player:
    id: int
    code: int
    team: int
    position: int
    first: str
    last: str
    prices: list[int]  # price in tenths before each gameweek (index 0 = GW1)


def _players(cfg: SampleConfig) -> list[_Player]:
    out = []
    for team in range(1, len(CLUBS) + 1):
        for idx, pos in enumerate(SQUAD):
            pid = (team - 1) * len(SQUAD) + idx + 1
            rng = random.Random(f"{cfg.seed}:player:{pid}")
            price = BASE_PRICE[pos] + rng.choice([0, 0, 5, 10, 15, 20, 30])
            prices = []
            for _ in range(39):
                prices.append(price)
                price = max(39, price + rng.choice([0, 0, 0, 0, 0, 0, 1, -1]))
            out.append(
                _Player(
                    id=pid,
                    code=100000 + pid,
                    team=team,
                    position=pos,
                    first=rng.choice(FIRST_NAMES),
                    last=rng.choice(SURNAMES),
                    prices=prices,
                )
            )
    return out


def _schedule(cfg: SampleConfig) -> list[dict[str, Any]]:
    fixtures, fid = [], 1
    for gw, pairs in enumerate(_round_robin(len(CLUBS)), start=1):
        deadline = cfg.first_deadline + timedelta(days=7 * (gw - 1))
        for i, (home, away) in enumerate(pairs):
            fixtures.append(
                {
                    "id": fid,
                    "event": gw,
                    "kickoff_time": deadline + timedelta(hours=2 + 2 * (i % 4)),
                    "team_h": home,
                    "team_a": away,
                }
            )
            fid += 1
    moved_from, moved_to = cfg.moved_fixture
    moved = next(f for f in fixtures if f["event"] == moved_from)
    moved["event"] = moved_to
    moved["kickoff_time"] = cfg.first_deadline + timedelta(days=7 * (moved_to - 1), hours=26)
    unscheduled = next(f for f in fixtures if f["event"] == cfg.unscheduled_gameweek)
    unscheduled["event"] = None
    unscheduled["kickoff_time"] = None
    return fixtures


def _weighted(rng: random.Random, candidates: list[_Player], weights: dict[int, int]) -> _Player:
    return rng.choices(candidates, weights=[weights[p.position] for p in candidates])[0]


def _play_fixture(
    cfg: SampleConfig, fixture: dict[str, Any], squads: dict[int, list[_Player]]
) -> dict[int, dict[str, Any]]:
    """Simulate one match; returns per-player stat lines keyed by player id."""
    rng = random.Random(f"{cfg.seed}:fixture:{fixture['id']}")
    lines: dict[int, dict[str, Any]] = {}
    on_pitch: dict[int, list[_Player]] = {}
    for team in (fixture["team_h"], fixture["team_a"]):
        squad = squads[team]
        played = []
        subbed = rng.sample(STARTERS[1:], 3)
        for slot in STARTERS:
            minutes = rng.randint(58, 85) if slot in subbed else 90
            lines[squad[slot].id] = {"minutes": minutes, "starts": 1}
            played.append(squad[slot])
        for off, slot in zip(subbed, SUBS, strict=True):
            lines[squad[slot].id] = {"minutes": 90 - lines[squad[off].id]["minutes"], "starts": 0}
            played.append(squad[slot])
        on_pitch[team] = played

    goals = {fixture["team_h"]: 0, fixture["team_a"]: 0}
    for team, opp in (
        (fixture["team_h"], fixture["team_a"]),
        (fixture["team_a"], fixture["team_h"]),
    ):
        for _ in range(rng.choices([0, 1, 2, 3, 4], weights=[25, 33, 24, 12, 6])[0]):
            goals[team] += 1
            if rng.random() < 0.05:
                defender = _weighted(rng, on_pitch[opp], {1: 1, 2: 6, 3: 2, 4: 1})
                lines[defender.id]["own_goals"] = lines[defender.id].get("own_goals", 0) + 1
                continue
            outfield = [p for p in on_pitch[team] if p.position != 1]
            scorer = _weighted(rng, outfield, {2: 1, 3: 3, 4: 5})
            lines[scorer.id]["goals_scored"] = lines[scorer.id].get("goals_scored", 0) + 1
            if rng.random() < 0.75:
                helpers = [p for p in outfield if p.id != scorer.id]
                assister = _weighted(rng, helpers, {2: 2, 3: 4, 4: 2})
                lines[assister.id]["assists"] = lines[assister.id].get("assists", 0) + 1

    fixture["team_h_score"], fixture["team_a_score"] = (
        goals[fixture["team_h"]],
        goals[fixture["team_a"]],
    )
    for team, opp in (
        (fixture["team_h"], fixture["team_a"]),
        (fixture["team_a"], fixture["team_h"]),
    ):
        conceded = goals[opp]
        for p in on_pitch[team]:
            line = lines[p.id]
            line["goals_conceded"] = conceded
            line["clean_sheets"] = int(conceded == 0 and line["minutes"] >= 60)
            line["yellow_cards"] = int(rng.random() < 0.08)
            line["red_cards"] = int(rng.random() < 0.004)
            line["saves"] = rng.randint(0, 6) if p.position == 1 else 0
            line["defensive_contribution"] = (
                0 if p.position == 1 else rng.randint(0, {2: 14, 3: 14, 4: 8}[p.position])
            )
            xg = line.get("goals_scored", 0) * 0.35 + rng.random() * (
                0.6 if p.position == 4 else 0.25
            )
            xa = line.get("assists", 0) * 0.3 + rng.random() * 0.2
            line["expected_goals"] = round(xg if p.position != 1 else 0.0, 2)
            line["expected_assists"] = round(xa if p.position != 1 else 0.0, 2)
            line["expected_goals_conceded"] = round(conceded * 0.8 + rng.random(), 2)
            line["bps"] = (
                (6 if line["minutes"] >= 60 else 3)
                + 24 * line.get("goals_scored", 0)
                + 9 * line.get("assists", 0)
                + 12 * line["clean_sheets"] * (p.position <= 2)
                + 2 * line["saves"]
                + rng.randint(-3, 12)
            )
    ranked = sorted(lines, key=lambda pid: (-lines[pid]["bps"], pid))
    for pid, bonus in zip(ranked[:3], (3, 2, 1), strict=True):
        lines[pid]["bonus"] = bonus
    return lines


def _best_score(item: tuple[int, int]) -> tuple[int, int]:
    """Sort key: highest points first, then lowest player id."""
    player_id, points = item
    return (-points, player_id)


def _explain(position: int, line: dict[str, Any]) -> list[dict[str, Any]]:
    """FPL scoring rules (2025/26) as an ``explain`` breakdown for one fixture."""
    items: list[tuple[str, int, int]] = []
    minutes = line["minutes"]
    if minutes > 0:
        items.append(("minutes", minutes, 2 if minutes >= 60 else 1))
    for stat, per in (("goals_scored", GOAL_POINTS[position]), ("assists", 3)):
        if line.get(stat):
            items.append((stat, line[stat], line[stat] * per))
    if line.get("clean_sheets") and CLEAN_SHEET_POINTS[position]:
        items.append(("clean_sheets", 1, CLEAN_SHEET_POINTS[position]))
    if position <= 2 and line.get("goals_conceded", 0) >= 2 and minutes > 0:
        items.append(("goals_conceded", line["goals_conceded"], -(line["goals_conceded"] // 2)))
    if line.get("saves", 0) >= 3:
        items.append(("saves", line["saves"], line["saves"] // 3))
    for stat, per in (("own_goals", -2), ("yellow_cards", -1), ("red_cards", -3), ("bonus", 1)):
        if line.get(stat):
            items.append((stat, line[stat], line[stat] * per))
    threshold = DEF_CONTRIBUTION_THRESHOLD.get(position)
    if threshold and line.get("defensive_contribution", 0) >= threshold:
        items.append(("defensive_contribution", line["defensive_contribution"], 2))
    return [
        {"identifier": i, "points": pts, "value": val, "points_modification": 0}
        for i, val, pts in items
    ]


STAT_KEYS = [
    "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded", "own_goals",
    "penalties_saved", "penalties_missed", "yellow_cards", "red_cards", "saves", "bonus",
    "bps", "starts", "defensive_contribution",
]  # fmt: skip
DECIMAL_KEYS = ["expected_goals", "expected_assists", "expected_goals_conceded"]


def _stats_block(
    lines: list[dict[str, Any]], explains: list[list[dict[str, Any]]]
) -> dict[str, Any]:
    stats: dict[str, Any] = {k: sum(int(line.get(k, 0)) for line in lines) for k in STAT_KEYS}
    for k in DECIMAL_KEYS:
        stats[k] = f"{sum(float(line.get(k, 0.0)) for line in lines):.2f}"
    stats["expected_goal_involvements"] = (
        f"{float(stats['expected_goals']) + float(stats['expected_assists']):.2f}"
    )
    ict = stats["bps"] * 0.4
    stats.update(
        influence=f"{ict:.1f}",
        creativity=f"{ict * 0.5:.1f}",
        threat=f"{ict * 0.6:.1f}",
        ict_index=f"{ict * 0.21:.1f}",
        total_points=sum(item["points"] for ex in explains for item in ex),
        in_dreamteam=False,
    )
    return stats


def build_sample_api(cfg: SampleConfig | None = None) -> dict[str, Any]:
    """Return ``{api_path: payload}`` for bootstrap, fixtures, live and element-summary."""
    cfg = cfg or SampleConfig()
    players = _players(cfg)
    squads: dict[int, list[_Player]] = defaultdict(list)
    for p in players:
        squads[p.team].append(p)
    by_id = {p.id: p for p in players}
    fixtures = _schedule(cfg)
    finished_gw = cfg.finished_gameweeks

    # Simulate finished fixtures in kickoff order.
    lines_by_fixture: dict[int, dict[int, dict[str, Any]]] = {}
    for f in fixtures:
        f["finished"] = f["event"] is not None and f["event"] <= finished_gw
        if f["finished"]:
            lines_by_fixture[f["id"]] = _play_fixture(cfg, f, squads)

    # event/<gw>/live
    live: dict[int, dict[str, Any]] = {}
    per_player_gw: dict[tuple[int, int], int] = {}
    for gw in range(1, finished_gw + 1):
        gw_fixtures = [f for f in fixtures if f["event"] == gw]
        elements = []
        for p in players:
            mine = [f for f in gw_fixtures if p.team in (f["team_h"], f["team_a"])]
            lines, explains = [], []
            for f in mine:
                line = lines_by_fixture[f["id"]].get(p.id, {"minutes": 0, "starts": 0})
                lines.append(line)
                explains.append(_explain(p.position, line))
            stats = _stats_block(lines, explains)
            per_player_gw[(p.id, gw)] = stats["total_points"]
            elements.append(
                {
                    "id": p.id,
                    "stats": stats,
                    "explain": [
                        {"fixture": f["id"], "stats": ex}
                        for f, ex in zip(mine, explains, strict=True)
                    ],
                    "modified": False,
                }
            )
        live[gw] = {"elements": elements}

    # fixtures/ (with per-player stats arrays for finished fixtures)
    fixture_payload = []
    for f in sorted(fixtures, key=lambda x: (x["event"] is None, x["event"] or 0, x["id"])):
        fixture_stats: list[dict[str, Any]] = []
        if f["finished"]:
            match_lines = lines_by_fixture[f["id"]]
            for identifier in [
                "goals_scored", "assists", "own_goals", "penalties_saved", "penalties_missed",
                "yellow_cards", "red_cards", "saves", "bonus", "bps", "defensive_contribution",
            ]:  # fmt: skip
                sides: dict[str, list[dict[str, int]]] = {"h": [], "a": []}
                for pid, line in sorted(match_lines.items()):
                    value = line.get(identifier, 0)
                    if value:
                        side = "h" if by_id[pid].team == f["team_h"] else "a"
                        sides[side].append({"value": value, "element": pid})
                fixture_stats.append({"identifier": identifier, **sides})
        fixture_payload.append(
            {
                "code": 900000 + f["id"],
                "event": f["event"],
                "finished": f["finished"],
                "finished_provisional": f["finished"],
                "id": f["id"],
                "kickoff_time": _iso(f["kickoff_time"]),
                "minutes": 90 if f["finished"] else 0,
                "provisional_start_time": False,
                "started": f["finished"],
                "team_a": f["team_a"],
                "team_a_score": f.get("team_a_score"),
                "team_h": f["team_h"],
                "team_h_score": f.get("team_h_score"),
                "stats": fixture_stats,
                "team_h_difficulty": 2 + (f["team_a"] % 4),
                "team_a_difficulty": 2 + (f["team_h"] % 4),
                "pulse_id": 500000 + f["id"],
            }
        )

    # element-summary/<id>
    summaries: dict[int, dict[str, Any]] = {}
    totals: dict[int, dict[str, Any]] = {}
    for p in players:
        history, total = [], dict.fromkeys(STAT_KEYS, 0)
        played_fixtures = sorted(
            (f for f in fixtures if f["finished"] and p.team in (f["team_h"], f["team_a"])),
            key=lambda f: f["kickoff_time"],
        )
        for f in played_fixtures:
            line = lines_by_fixture[f["id"]].get(p.id, {"minutes": 0, "starts": 0})
            ex = _explain(p.position, line)
            row = _stats_block([line], [ex])
            for k in STAT_KEYS:
                total[k] += row[k]
            history.append(
                {
                    "element": p.id,
                    "fixture": f["id"],
                    "opponent_team": f["team_a"] if f["team_h"] == p.team else f["team_h"],
                    "was_home": f["team_h"] == p.team,
                    "kickoff_time": _iso(f["kickoff_time"]),
                    "team_h_score": f["team_h_score"],
                    "team_a_score": f["team_a_score"],
                    "round": f["event"],
                    "modified": False,
                    "value": p.prices[f["event"] - 1],
                    "transfers_balance": 0,
                    "selected": 10000 + p.id * 37,
                    "transfers_in": 0,
                    "transfers_out": 0,
                    **row,
                }
            )
        upcoming = [
            {
                "id": f["id"],
                "event": f["event"],
                "is_home": f["team_h"] == p.team,
                "kickoff_time": _iso(f["kickoff_time"]),
                "difficulty": 3,
            }
            for f in fixtures
            if not f["finished"] and p.team in (f["team_h"], f["team_a"])
        ]
        rng = random.Random(f"{cfg.seed}:past:{p.id}")
        past = [
            {
                "season_name": f"{2025 - k}/{(26 - k) % 100:02d}",
                "element_code": p.code,
                "start_cost": p.prices[0] - k,
                "end_cost": p.prices[0] - k + rng.choice([-2, 0, 3]),
                "total_points": rng.randint(20, 220),
                **dict.fromkeys(STAT_KEYS, 0),
                **dict.fromkeys([*DECIMAL_KEYS, "expected_goal_involvements"], "0.00"),
                "influence": "0.0",
                "creativity": "0.0",
                "threat": "0.0",
                "ict_index": "0.0",
            }
            for k in range(rng.randint(0, 2))
        ]
        summaries[p.id] = {"fixtures": upcoming, "history": history, "history_past": past}
        totals[p.id] = total

    # bootstrap-static
    events = []
    for gw in range(1, 39):
        finished = gw <= finished_gw
        scores = [per_player_gw.get((p.id, gw), 0) for p in players]
        gw_points = {p.id: per_player_gw.get((p.id, gw), 0) for p in players}
        top_id = min(gw_points.items(), key=_best_score)[0]
        events.append(
            {
                "id": gw,
                "name": f"Gameweek {gw}",
                "deadline_time": _iso(cfg.first_deadline + timedelta(days=7 * (gw - 1))),
                "finished": finished,
                "data_checked": finished,
                "is_previous": gw == finished_gw - 1,
                "is_current": gw == finished_gw,
                "is_next": gw == finished_gw + 1,
                "average_entry_score": (sum(scores) // len(scores)) if finished else 0,
                "highest_score": max(scores) if finished else None,
                "most_selected": players[0].id if finished else None,
                "most_transferred_in": players[1].id if finished else None,
                "most_captained": top_id if finished else None,
                "most_vice_captained": players[2].id if finished else None,
                "top_element": top_id if finished else None,
                "transfers_made": 1000 * gw if finished else 0,
                "chip_plays": (
                    [
                        {"chip_name": "bboost", "num_played": 100 + gw},
                        {"chip_name": "3xc", "num_played": 50 + gw},
                    ]
                    if finished
                    else []
                ),
            }
        )
    teams = [
        {
            "code": 300 + i,
            "draw": 0,
            "form": None,
            "id": i,
            "loss": 0,
            "name": name,
            "played": 0,
            "points": 0,
            "position": 0,
            "short_name": short,
            "strength": 2 + i % 4,
            "strength_overall_home": 1000 + 10 * i,
            "strength_overall_away": 1000 + 10 * i,
            "strength_attack_home": 1000 + 5 * i,
            "strength_attack_away": 1000 + 5 * i,
            "strength_defence_home": 1000 + 7 * i,
            "strength_defence_away": 1000 + 7 * i,
            "pulse_id": 700 + i,
        }
        for i, (name, short) in enumerate(CLUBS, start=1)
    ]
    elements = []
    for p in players:
        t = totals[p.id]
        points = sum(per_player_gw.get((p.id, gw), 0) for gw in range(1, finished_gw + 1))
        recent = [
            per_player_gw.get((p.id, gw), 0)
            for gw in range(max(1, finished_gw - 3), finished_gw + 1)
        ]
        status = "i" if p.id % 29 == 0 else ("d" if p.id % 41 == 0 else "a")
        elements.append(
            {
                "id": p.id,
                "code": p.code,
                "first_name": p.first,
                "second_name": p.last,
                "web_name": p.last,
                "team": p.team,
                "team_code": 300 + p.team,
                "element_type": p.position,
                "status": status,
                "now_cost": p.prices[finished_gw],
                "cost_change_start": p.prices[finished_gw] - p.prices[0],
                "selected_by_percent": f"{(p.id * 7) % 450 / 10:.1f}",
                "form": f"{sum(recent) / max(len(recent), 1):.1f}",
                "points_per_game": f"{points / max(finished_gw, 1):.1f}",
                "total_points": points,
                "event_points": per_player_gw.get((p.id, finished_gw), 0),
                "transfers_in": p.id * 101,
                "transfers_out": p.id * 99,
                "transfers_in_event": p.id * 11,
                "transfers_out_event": p.id * 9,
                "chance_of_playing_next_round": 25
                if status == "d"
                else (0 if status == "i" else None),
                "news": "Knock - 25% chance of playing" if status == "d" else "",
                "news_added": _iso(cfg.first_deadline) if status != "a" else None,
                "birth_date": f"{1990 + p.id % 15}-0{1 + p.id % 9}-1{p.id % 9}",
                "team_join_date": "2024-07-01",
                "opta_code": f"p{p.code}",
                "removed": False,
                "can_select": True,
                **t,
                "influence": f"{t['bps'] * 0.4:.1f}",
                "creativity": f"{t['bps'] * 0.2:.1f}",
                "threat": f"{t['bps'] * 0.24:.1f}",
                "ict_index": f"{t['bps'] * 0.084:.1f}",
                "expected_goals": "0.00",
                "expected_assists": "0.00",
                "expected_goal_involvements": "0.00",
                "expected_goals_conceded": "0.00",
            }
        )
    bootstrap = {
        "events": events,
        "teams": teams,
        "element_types": [
            {
                "id": i,
                "singular_name": s,
                "singular_name_short": sh,
                "plural_name": pl,
                "plural_name_short": sh,
                "squad_select": sel,
                "squad_min_play": mn,
                "squad_max_play": mx,
                "element_count": SQUAD.count(i) * len(CLUBS),
            }
            for i, s, sh, pl, sel, mn, mx in POSITIONS
        ],
        "elements": elements,
        "total_players": 1_000_000,
    }

    api: dict[str, Any] = {"bootstrap-static": bootstrap, "fixtures": fixture_payload}
    api.update({f"event/{gw}/live": payload for gw, payload in live.items()})
    api.update({f"element-summary/{pid}": payload for pid, payload in summaries.items()})
    return api


def write_sample_api(out: Path, cfg: SampleConfig | None = None) -> Path:
    for path, payload in build_sample_api(cfg).items():
        file = out / f"{path}.json"
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    return out
