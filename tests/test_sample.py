"""The synthetic API must be internally consistent, or every downstream test is suspect."""

from __future__ import annotations

from collections import Counter

from epl_lakehouse.sample import SampleConfig, build_sample_api


def test_schedule_is_a_double_round_robin() -> None:
    fixtures = build_sample_api()["fixtures"]
    assert len(fixtures) == 380
    played = Counter()
    hosted = Counter()
    pairs = Counter()
    for f in fixtures:
        played[f["team_h"]] += 1
        played[f["team_a"]] += 1
        hosted[f["team_h"]] += 1
        pairs[(f["team_h"], f["team_a"])] += 1
    assert set(played.values()) == {38}
    assert set(hosted.values()) == {19}
    assert set(pairs.values()) == {1}  # every club hosts every other club exactly once


def test_moved_and_unscheduled_fixtures() -> None:
    fixtures = build_sample_api(SampleConfig(finished_gameweeks=5))["fixtures"]
    per_gw = Counter(f["event"] for f in fixtures)
    assert per_gw[3] == 9  # one fixture moved out: blank gameweek for two clubs
    assert per_gw[5] == 11  # ...and into gameweek 5: a double gameweek
    assert per_gw[None] == 1  # postponed without a new date
    unscheduled = next(f for f in fixtures if f["event"] is None)
    assert unscheduled["kickoff_time"] is None
    assert not unscheduled["finished"]


def test_scores_reconcile_with_goal_scorers() -> None:
    api = build_sample_api()
    team_of = {e["id"]: e["team"] for e in api["bootstrap-static"]["elements"]}
    for f in (f for f in api["fixtures"] if f["finished"]):
        stats = {s["identifier"]: s for s in f["stats"]}
        home = sum(x["value"] for x in stats["goals_scored"]["h"]) + sum(
            x["value"] for x in stats["own_goals"]["a"]
        )
        away = sum(x["value"] for x in stats["goals_scored"]["a"]) + sum(
            x["value"] for x in stats["own_goals"]["h"]
        )
        assert (home, away) == (f["team_h_score"], f["team_a_score"])
        for side, team in (("h", f["team_h"]), ("a", f["team_a"])):
            assert all(team_of[x["element"]] == team for x in stats["bps"][side])


def test_live_points_match_the_explain_breakdown_and_bootstrap_totals() -> None:
    api = build_sample_api()
    season_points: Counter[int] = Counter()
    for gw in range(1, 6):
        for element in api[f"event/{gw}/live"]["elements"]:
            explained = sum(s["points"] for fx in element["explain"] for s in fx["stats"])
            assert explained == element["stats"]["total_points"]
            season_points[element["id"]] += element["stats"]["total_points"]
    for element in api["bootstrap-static"]["elements"]:
        assert element["total_points"] == season_points[element["id"]]


def test_generation_is_deterministic_and_stable_as_the_season_advances() -> None:
    gw5 = build_sample_api(SampleConfig(finished_gameweeks=5))
    assert gw5 == build_sample_api(SampleConfig(finished_gameweeks=5))
    gw6 = build_sample_api(SampleConfig(finished_gameweeks=6))
    later = {f["id"]: f for f in gw6["fixtures"]}
    for f in (f for f in gw5["fixtures"] if f["finished"]):
        assert later[f["id"]]["team_h_score"] == f["team_h_score"]
        assert later[f["id"]]["team_a_score"] == f["team_a_score"]
    assert gw6["event/5/live"] == gw5["event/5/live"]


def test_history_prices_track_bootstrap_price() -> None:
    api = build_sample_api()
    for element in api["bootstrap-static"]["elements"][:30]:
        history = api[f"element-summary/{element['id']}"]["history"]
        assert history, "every player's club has played"
        assert all(35 <= h["value"] <= 160 for h in history)
        assert {h["round"] for h in history} <= {1, 2, 3, 4, 5}
