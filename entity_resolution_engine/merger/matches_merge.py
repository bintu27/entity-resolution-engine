from typing import Dict, List

import pandas as pd

from entity_resolution_engine.lineage.lineage_builder import build_lineage
from entity_resolution_engine.ues_writer.writer import generate_ues_id


def merge_matches(
    matches: List[Dict],
    alpha_matches: pd.DataFrame,
    beta_matches: pd.DataFrame,
    team_ues_map: Dict[int, str],
    competition_ues_map: Dict[int, str],
    season_ues_map: Dict[int, str],
) -> List[Dict]:
    records: List[Dict] = []
    alpha_lookup = {row["match_id"]: row for _, row in alpha_matches.iterrows()}
    beta_lookup = {row["id"]: row for _, row in beta_matches.iterrows()}

    for match in matches:
        alpha_row = alpha_lookup.get(match["alpha_match_id"])
        beta_row = beta_lookup.get(match["beta_match_id"])
        ues_id = generate_ues_id("UESM", match["alpha_match_id"], match["beta_match_id"])
        lineage = build_lineage(
            source_type="match",
            alpha_id=match["alpha_match_id"],
            beta_id=match["beta_match_id"],
            confidence=match["confidence"],
            breakdown={"team": match["confidence"]},
        )
        records.append(
            {
                "ues_match_id": ues_id,
                "home_team_ues_id": team_ues_map.get(alpha_row.get("home_team_id")),
                "away_team_ues_id": team_ues_map.get(alpha_row.get("away_team_id")),
                "season_ues_id": season_ues_map.get(alpha_row.get("season_id")),
                "competition_ues_id": competition_ues_map.get(alpha_row.get("competition_id")),
                "match_date": alpha_row.get("match_date"),
                "merge_confidence": match["confidence"],
                "lineage": lineage,
            }
        )
    return records
