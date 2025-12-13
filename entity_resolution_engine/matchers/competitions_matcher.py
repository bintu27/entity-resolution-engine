from typing import Dict, List, Tuple

import pandas as pd
import yaml

from entity_resolution_engine.normalizers.competition_normalizer import normalize_competition
from entity_resolution_engine.normalizers.name_normalizer import token_sort_ratio
from entity_resolution_engine.normalizers.nationality_normalizer import normalize_country
from entity_resolution_engine.lineage.lineage_builder import build_lineage
from entity_resolution_engine.ues_writer.writer import generate_ues_id

CONFIG_PATH = (
    __import__("pathlib").Path(__file__).resolve().parents[1] / "config" / "thresholds.yml"
)
with CONFIG_PATH.open() as f:
    THRESHOLDS = yaml.safe_load(f)

COMP_THRESHOLD = THRESHOLDS.get("COMP_SIM_THRESHOLD", 0.75)


def match_competitions(alpha_comp: pd.DataFrame, beta_comp: pd.DataFrame) -> List[Dict]:
    results: List[Dict] = []
    for _, alpha_row in alpha_comp.iterrows():
        norm_alpha = normalize_competition(alpha_row["name"])
        best = None
        best_score = 0.0
        for _, beta_row in beta_comp.iterrows():
            norm_beta = normalize_competition(beta_row["title"])
            score = token_sort_ratio(norm_alpha, norm_beta)
            if score > best_score:
                best_score = score
                best = beta_row
        if best is not None and best_score >= COMP_THRESHOLD:
            results.append(
                {
                    "alpha_competition_id": alpha_row["competition_id"],
                    "beta_competition_id": best["id"],
                    "confidence": best_score,
                    "name": alpha_row["name"],
                    "country": normalize_country(alpha_row.get("country") or best.get("locale")),
                }
            )
    return results


def build_competition_entities(matches: List[Dict]) -> Tuple[List[Dict], Dict[int, str], Dict[int, str]]:
    entities: List[Dict] = []
    alpha_map: Dict[int, str] = {}
    beta_map: Dict[int, str] = {}
    for match in matches:
        ues_id = generate_ues_id("UESC", match["alpha_competition_id"], match["beta_competition_id"])
        lineage = build_lineage(
            source_type="competition",
            alpha_id=match["alpha_competition_id"],
            beta_id=match["beta_competition_id"],
            confidence=match["confidence"],
            breakdown={"name_similarity": match["confidence"]},
        )
        record = {
            "ues_competition_id": ues_id,
            "name": match["name"],
            "country": match.get("country"),
            "merge_confidence": match["confidence"],
            "lineage": lineage,
        }
        entities.append(record)
        alpha_map[match["alpha_competition_id"]] = ues_id
        beta_map[match["beta_competition_id"]] = ues_id
    return entities, alpha_map, beta_map
