from typing import Dict, List, Tuple

import pandas as pd
import yaml

from entity_resolution_engine.normalizers.season_normalizer import normalize_season
from entity_resolution_engine.lineage.lineage_builder import build_lineage
from entity_resolution_engine.ues_writer.writer import generate_ues_id

CONFIG_PATH = (
    __import__("pathlib").Path(__file__).resolve().parents[1] / "config" / "thresholds.yml"
)
with CONFIG_PATH.open() as f:
    THRESHOLDS = yaml.safe_load(f)


def match_seasons(
    alpha_seasons: pd.DataFrame,
    beta_seasons: pd.DataFrame,
    competition_map: Dict[int, int],
) -> List[Dict]:
    results: List[Dict] = []
    for _, alpha_row in alpha_seasons.iterrows():
        alpha_start, alpha_end = normalize_season(alpha_row["name"])
        for _, beta_row in beta_seasons.iterrows():
            comp_match = competition_map.get(alpha_row["competition_id"])
            if comp_match != beta_row["competition_id"]:
                continue
            beta_start, beta_end = normalize_season(beta_row["label"])
            if alpha_start and beta_start and abs(alpha_start - beta_start) <= 0:
                confidence = 1.0
            elif alpha_start and beta_start and abs(alpha_start - beta_start) == 1:
                confidence = 0.7
            else:
                confidence = 0.0
            if confidence >= THRESHOLDS.get("CONFIDENCE_REVIEW", 0.6):
                results.append(
                    {
                        "alpha_season_id": alpha_row["season_id"],
                        "beta_season_id": beta_row["id"],
                        "confidence": confidence,
                        "start_year": alpha_start or beta_start,
                        "end_year": alpha_end or beta_end,
                        "alpha_competition_id": alpha_row["competition_id"],
                        "beta_competition_id": beta_row["competition_id"],
                    }
                )
    return results


def build_season_entities(
    matches: List[Dict],
    competition_ues_map: Dict[int, str],
) -> Tuple[List[Dict], Dict[int, str], Dict[int, str]]:
    entities: List[Dict] = []
    alpha_map: Dict[int, str] = {}
    beta_map: Dict[int, str] = {}
    for match in matches:
        ues_id = generate_ues_id("UESS", match["alpha_season_id"], match["beta_season_id"])
        lineage = build_lineage(
            source_type="season",
            alpha_id=match["alpha_season_id"],
            beta_id=match["beta_season_id"],
            confidence=match["confidence"],
            breakdown={"start_year": match.get("start_year"), "end_year": match.get("end_year")},
        )
        record = {
            "ues_season_id": ues_id,
            "start_year": match.get("start_year"),
            "end_year": match.get("end_year"),
            "competition_ues_id": competition_ues_map.get(
                match.get("alpha_competition_id")
            ),
            "merge_confidence": match["confidence"],
            "lineage": lineage,
        }
        entities.append(record)
        alpha_map[match["alpha_season_id"]] = ues_id
        beta_map[match["beta_season_id"]] = ues_id
    return entities, alpha_map, beta_map
