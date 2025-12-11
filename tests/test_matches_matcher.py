import datetime as dt

import pandas as pd

from entity_resolution_engine.matchers.matches_matcher import match_matches


def test_match_matches_skips_when_team_mappings_do_not_align():
    alpha_matches = pd.DataFrame(
        [
            {
                "match_id": 1,
                "competition_id": 1,
                "season_id": 1,
                "home_team_id": 10,
                "away_team_id": 20,
                "match_date": dt.date(2024, 5, 1),
            }
        ]
    )

    beta_matches = pd.DataFrame(
        [
            {
                "id": 100,
                "competition_id": 1,
                "season_id": 1,
                "home_team_id": 30,
                "away_team_id": 40,
                "match_date": dt.date(2024, 5, 1),
            }
        ]
    )

    matches = match_matches(
        alpha_matches,
        beta_matches,
        alpha_team_map={10: 11, 20: 22},
        competition_map={1: 1},
        season_map={1: 1},
    )

    assert matches == []
