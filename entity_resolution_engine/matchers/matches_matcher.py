from typing import Dict, List

import pandas as pd
import yaml

CONFIG_PATH = (
    __import__("pathlib").Path(__file__).resolve().parents[1]
    / "config"
    / "thresholds.yml"
)
with CONFIG_PATH.open() as f:
    THRESHOLDS = yaml.safe_load(f)


def _date_similarity(alpha_date, beta_date) -> float:
    if pd.isna(alpha_date) or pd.isna(beta_date):
        return 0.0
    delta = abs(alpha_date - beta_date)
    if delta.days == 0:
        return 1.0
    if delta.days <= 1:
        return 0.8
    return 0.0


def match_matches(
    alpha_matches: pd.DataFrame,
    beta_matches: pd.DataFrame,
    alpha_team_map: Dict[int, int],
    competition_map: Dict[int, int],
    season_map: Dict[int, int],
) -> List[Dict]:
    matches: List[Dict] = []
    for _, alpha_row in alpha_matches.iterrows():
        best_score = 0.0
        best_match = None
        for _, beta_row in beta_matches.iterrows():
            comp_match = competition_map.get(alpha_row["competition_id"])
            if comp_match != beta_row["competition_id"]:
                continue
            season_match = season_map.get(alpha_row["season_id"])
            if season_match != beta_row["season_id"]:
                continue
            home_team_match = alpha_team_map.get(alpha_row["home_team_id"])
            away_team_match = alpha_team_map.get(alpha_row["away_team_id"])

            if home_team_match is None or away_team_match is None:
                continue

            teams_align = (
                home_team_match == beta_row["home_team_id"]
                and away_team_match == beta_row["away_team_id"]
            )

            if not teams_align:
                continue

            team_score = 1.0
            date_score = _date_similarity(
                alpha_row.get("match_date"), beta_row.get("match_date")
            )
            confidence = 0.4 * team_score + 0.3 * date_score + 0.3
            if confidence > best_score:
                best_score = confidence
                best_match = beta_row
        if best_match is not None and best_score >= THRESHOLDS.get(
            "CONFIDENCE_REVIEW", 0.6
        ):
            matches.append(
                {
                    "alpha_match_id": alpha_row["match_id"],
                    "beta_match_id": best_match["id"],
                    "confidence": best_score,
                }
            )
    return matches
