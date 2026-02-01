from typing import Dict, List, Tuple

import pandas as pd

from entity_resolution_engine.lineage.lineage_builder import build_lineage
from entity_resolution_engine.normalizers.nationality_normalizer import (
    normalize_country,
)
from entity_resolution_engine.ues_writer.writer import generate_ues_id


def merge_players(
    matches: List[Dict],
    alpha_players: pd.DataFrame,
    beta_players: pd.DataFrame,
    team_ues_map: Dict[int, str],
) -> Tuple[List[Dict], Dict[int, str], Dict[int, str]]:
    records: List[Dict] = []
    alpha_map: Dict[int, str] = {}
    beta_map: Dict[int, str] = {}
    alpha_lookup = {row["player_id"]: row for _, row in alpha_players.iterrows()}
    beta_lookup = {row["id"]: row for _, row in beta_players.iterrows()}

    for match in matches:
        alpha_row = alpha_lookup.get(match["alpha_player_id"])
        beta_row = beta_lookup.get(match["beta_player_id"])
        if alpha_row is None or beta_row is None:
            continue
        ues_id = generate_ues_id(
            "UESP", match["alpha_player_id"], match["beta_player_id"]
        )
        lineage = build_lineage(
            source_type="player",
            alpha_id=match["alpha_player_id"],
            beta_id=match["beta_player_id"],
            confidence=match["confidence"],
            breakdown=match.get("breakdown", {}),
        )
        canonical_name = alpha_row.get("name") or beta_row.get("full_name")
        canonical_nationality = normalize_country(
            alpha_row.get("nationality") or beta_row.get("nationality")
        )
        canonical_foot = beta_row.get("footedness") or alpha_row.get("foot")
        canonical_height = alpha_row.get("height_cm") or beta_row.get("height_cm")
        team_ues_id = team_ues_map.get(alpha_row.get("team_id"))
        records.append(
            {
                "ues_player_id": ues_id,
                "canonical_name": canonical_name,
                "dob": alpha_row.get("dob"),
                "birth_year": beta_row.get("birth_year"),
                "nationality": canonical_nationality,
                "height_cm": canonical_height,
                "foot": (
                    canonical_foot.lower()
                    if isinstance(canonical_foot, str)
                    else canonical_foot
                ),
                "team_ues_id": team_ues_id,
                "merge_confidence": match["confidence"],
                "lineage": lineage,
            }
        )
        alpha_map[match["alpha_player_id"]] = ues_id
        beta_map[match["beta_player_id"]] = ues_id
    return records, alpha_map, beta_map
