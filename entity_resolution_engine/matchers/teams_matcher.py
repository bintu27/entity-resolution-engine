from typing import Dict, List, Tuple

import pandas as pd
import yaml

from entity_resolution_engine.normalizers.name_normalizer import (
    normalize_name,
    token_sort_ratio,
)
from entity_resolution_engine.lineage.lineage_builder import build_lineage
from entity_resolution_engine.ues_writer.writer import generate_ues_id

CONFIG_PATH = (
    __import__("pathlib").Path(__file__).resolve().parents[1]
    / "config"
    / "thresholds.yml"
)
RULES_PATH = (
    __import__("pathlib").Path(__file__).resolve().parents[1]
    / "config"
    / "mapping_rules.yml"
)
with CONFIG_PATH.open() as f:
    THRESHOLDS = yaml.safe_load(f)
with RULES_PATH.open() as f:
    RULES = yaml.safe_load(f)

TEAM_THRESHOLD = THRESHOLDS.get("TEAM_SIM_THRESHOLD", 0.7)
ALIASES = {k.lower(): v for k, v in RULES.get("team_name_aliases", {}).items()}


def _apply_alias(name: str) -> str:
    return ALIASES.get(name.lower(), name)


def match_teams(alpha_teams: pd.DataFrame, beta_teams: pd.DataFrame) -> List[Dict]:
    matches: List[Dict] = []
    for _, alpha_row in alpha_teams.iterrows():
        alpha_name = _apply_alias(alpha_row["name"])
        norm_alpha = normalize_name(alpha_name)
        best = None
        best_score = 0.0
        for _, beta_row in beta_teams.iterrows():
            beta_name = _apply_alias(beta_row["display_name"])
            norm_beta = normalize_name(beta_name)
            score = token_sort_ratio(norm_alpha, norm_beta)
            if score > best_score:
                best_score = score
                best = beta_row
        if best is not None and best_score >= TEAM_THRESHOLD:
            matches.append(
                {
                    "alpha_team_id": alpha_row["team_id"],
                    "beta_team_id": best["id"],
                    "confidence": best_score,
                    "name": alpha_row["name"],
                    "country": alpha_row.get("country") or best.get("region"),
                }
            )
    return matches


def build_team_entities(
    matches: List[Dict],
) -> Tuple[List[Dict], Dict[int, str], Dict[int, str]]:
    entities: List[Dict] = []
    alpha_map: Dict[int, str] = {}
    beta_map: Dict[int, str] = {}
    for match in matches:
        ues_id = generate_ues_id("UEST", match["alpha_team_id"], match["beta_team_id"])
        lineage = build_lineage(
            source_type="team",
            alpha_id=match["alpha_team_id"],
            beta_id=match["beta_team_id"],
            confidence=match["confidence"],
            breakdown={"name_similarity": match["confidence"]},
        )
        record = {
            "ues_team_id": ues_id,
            "name": match["name"],
            "country": match.get("country"),
            "merge_confidence": match["confidence"],
            "lineage": lineage,
        }
        entities.append(record)
        alpha_map[match["alpha_team_id"]] = ues_id
        beta_map[match["beta_team_id"]] = ues_id
    return entities, alpha_map, beta_map
