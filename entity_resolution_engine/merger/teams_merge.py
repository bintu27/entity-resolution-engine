from typing import Dict, List, Tuple

import pandas as pd

from entity_resolution_engine.lineage.lineage_builder import build_lineage
from entity_resolution_engine.ues_writer.writer import generate_ues_id


def merge_teams(matches: List[Dict], alpha_teams: pd.DataFrame, beta_teams: pd.DataFrame) -> Tuple[List[Dict], Dict[int, str], Dict[int, str]]:
    records: List[Dict] = []
    alpha_map: Dict[int, str] = {}
    beta_map: Dict[int, str] = {}
    alpha_lookup = {row["team_id"]: row for _, row in alpha_teams.iterrows()}
    beta_lookup = {row["id"]: row for _, row in beta_teams.iterrows()}

    for match in matches:
        alpha_row = alpha_lookup.get(match["alpha_team_id"])
        beta_row = beta_lookup.get(match["beta_team_id"])
        ues_id = generate_ues_id("UEST", match["alpha_team_id"], match["beta_team_id"])
        lineage = build_lineage(
            source_type="team",
            alpha_id=match["alpha_team_id"],
            beta_id=match["beta_team_id"],
            confidence=match["confidence"],
            breakdown={"name_similarity": match["confidence"]},
        )
        canonical_name = alpha_row["name"] if alpha_row is not None else beta_row.get("display_name")
        canonical_country = alpha_row.get("country") if alpha_row is not None else beta_row.get("region")
        records.append(
            {
                "ues_team_id": ues_id,
                "name": canonical_name,
                "country": canonical_country,
                "merge_confidence": match["confidence"],
                "lineage": lineage,
            }
        )
        alpha_map[match["alpha_team_id"]] = ues_id
        beta_map[match["beta_team_id"]] = ues_id
    return records, alpha_map, beta_map
