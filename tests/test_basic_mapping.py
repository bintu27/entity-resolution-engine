import datetime as dt

import pandas as pd

from entity_resolution_engine.matchers.players_matcher import match_players
from entity_resolution_engine.normalizers.season_normalizer import normalize_season
from entity_resolution_engine.normalizers.name_normalizer import (
    normalize_name,
    token_sort_ratio,
)
from entity_resolution_engine.ues_writer.writer import generate_ues_id


def test_season_normalization():
    assert normalize_season("2020/21") == (2020, 2021)
    assert normalize_season("20-21") == (2020, 2021)
    assert normalize_season("2020") == (2020, 2021)


def test_name_normalizer_and_similarity():
    a = normalize_name("City FC")
    b = normalize_name("City Football Club")
    assert token_sort_ratio(a, b) > 0.8


def test_player_matching_positive_case():
    alpha_players = pd.DataFrame(
        [
            {
                "player_id": 1,
                "name": "John Doe",
                "dob": dt.date(1995, 4, 10),
                "nationality": "England",
                "height_cm": 182,
                "foot": "Right",
                "team_id": 1,
            }
        ]
    )
    beta_players = pd.DataFrame(
        [
            {
                "id": 10,
                "full_name": "Jon Doe",
                "birth_year": 1995,
                "nationality": "EN",
                "height_cm": 181,
                "footedness": "R",
                "team_name": "City FC",
            }
        ]
    )
    beta_teams = pd.DataFrame(
        [
            {"id": 1, "display_name": "City FC"},
        ]
    )
    matches = match_players(alpha_players, beta_players, {1: 1}, beta_teams)
    assert len(matches) == 1
    assert matches[0]["confidence"] >= 0.85


def test_generate_ues_id_is_deterministic():
    first = generate_ues_id("UESP", 1, 2)
    second = generate_ues_id("UESP", 1, 2)
    assert first == second
