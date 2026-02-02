import time

import pandas as pd
import pytest

from entity_resolution_engine.matchers.players_matcher import match_players


@pytest.mark.performance
def test_player_matcher_perf_smoke():
    alpha_players = pd.DataFrame(
        [
            {
                "player_id": idx,
                "name": f"Player {idx}",
                "dob": None,
                "nationality": "England",
                "height_cm": 180,
                "foot": "Right",
                "team_id": 1,
            }
            for idx in range(100)
        ]
    )
    beta_players = pd.DataFrame(
        [
            {
                "id": idx,
                "full_name": f"Player {idx}",
                "birth_year": 1990,
                "nationality": "EN",
                "height_cm": 180,
                "footedness": "R",
                "team_name": "City FC",
            }
            for idx in range(100)
        ]
    )
    beta_teams = pd.DataFrame(
        [
            {"id": 1, "display_name": "City FC"},
        ]
    )

    start = time.perf_counter()
    match_players(alpha_players, beta_players, {1: 1}, beta_teams)
    duration = time.perf_counter() - start

    assert duration < 3.0
