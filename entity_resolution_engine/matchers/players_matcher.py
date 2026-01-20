from typing import Dict, List

import pandas as pd
import yaml

from entity_resolution_engine.normalizers.name_normalizer import (
    normalize_name,
    token_sort_ratio,
)

CONFIG_PATH = (
    __import__("pathlib").Path(__file__).resolve().parents[1]
    / "config"
    / "thresholds.yml"
)
with CONFIG_PATH.open() as f:
    THRESHOLDS = yaml.safe_load(f)

WEIGHTS = {
    "name": 0.6,
    "dob": 0.3,
    "team": 0.1,
}


def _dob_similarity(alpha_dob, beta_birth_year) -> float:
    if pd.isna(alpha_dob) or pd.isna(beta_birth_year):
        return 0.0
    if alpha_dob.year == int(beta_birth_year):
        return 1.0
    if abs(alpha_dob.year - int(beta_birth_year)) == 1:
        return THRESHOLDS.get("DOB_PARTIAL_SCORE", 0.6)
    return 0.0


def match_players(
    alpha_players: pd.DataFrame,
    beta_players: pd.DataFrame,
    team_map: Dict[int, int],
    beta_teams: pd.DataFrame,
) -> List[Dict]:
    beta_team_lookup = {
        normalize_name(row["display_name"]): row["id"]
        for _, row in beta_teams.iterrows()
    }
    matches: List[Dict] = []
    for _, alpha_row in alpha_players.iterrows():
        norm_alpha_name = normalize_name(alpha_row["name"])
        best_score = 0.0
        best_match = None
        for _, beta_row in beta_players.iterrows():
            norm_beta_name = normalize_name(beta_row["full_name"])
            name_score = token_sort_ratio(norm_alpha_name, norm_beta_name)
            dob_score = _dob_similarity(
                alpha_row.get("dob"), beta_row.get("birth_year")
            )
            beta_team_norm = normalize_name(beta_row.get("team_name"))
            beta_team_id = beta_team_lookup.get(beta_team_norm)
            team_score = (
                1.0
                if beta_team_id
                and team_map.get(alpha_row.get("team_id")) == beta_team_id
                else 0.0
            )
            confidence = (
                WEIGHTS["name"] * name_score
                + WEIGHTS["dob"] * dob_score
                + WEIGHTS["team"] * team_score
            )
            if confidence > best_score:
                best_score = confidence
                best_match = beta_row
                best_breakdown = {
                    "name_similarity": name_score,
                    "dob_similarity": dob_score,
                    "team_similarity": team_score,
                }
        if best_match is not None and best_score >= THRESHOLDS.get(
            "CONFIDENCE_AUTOPASS", 0.85
        ):
            matches.append(
                {
                    "alpha_player_id": alpha_row["player_id"],
                    "beta_player_id": best_match["id"],
                    "confidence": best_score,
                    "breakdown": best_breakdown,
                }
            )
    return matches
