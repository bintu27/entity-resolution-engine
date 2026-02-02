from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from entity_resolution_engine.normalizers.competition_normalizer import (
    normalize_competition,
)
from entity_resolution_engine.normalizers.name_normalizer import normalize_name
from entity_resolution_engine.normalizers.nationality_normalizer import (
    normalize_country,
)
from entity_resolution_engine.normalizers.season_normalizer import normalize_season


@dataclass
class ValidationCandidate:
    left_id: str
    right_id: str
    left_source: str
    right_source: str
    left: Dict[str, Any]
    right: Dict[str, Any]
    matcher_score: float
    signals: Dict[str, Any]


def _lookup(df: pd.DataFrame, id_field: str) -> Dict[Any, Dict[str, Any]]:
    return {row[id_field]: row.to_dict() for _, row in df.iterrows()}


def _conflict_flags(*flags: Optional[str]) -> List[str]:
    return [flag for flag in flags if flag]


def _normalize_country(value: Any) -> str:
    return normalize_country(str(value)) if value is not None else ""


def _coerce_int(value: Any) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    return int(value)


def adapt_team_match(
    match: Dict[str, Any], alpha_teams: pd.DataFrame, beta_teams: pd.DataFrame
) -> ValidationCandidate:
    alpha_lookup = _lookup(alpha_teams, "team_id")
    beta_lookup = _lookup(beta_teams, "id")
    alpha_row = alpha_lookup[match["alpha_team_id"]]
    beta_row = beta_lookup[match["beta_team_id"]]
    alpha_name = normalize_name(alpha_row.get("name", ""))
    beta_name = normalize_name(beta_row.get("display_name", ""))
    alpha_country = _normalize_country(alpha_row.get("country"))
    beta_country = _normalize_country(beta_row.get("region"))
    conflict = (
        "country_mismatch"
        if alpha_country and beta_country and alpha_country != beta_country
        else None
    )
    return ValidationCandidate(
        left_id=str(match["alpha_team_id"]),
        right_id=str(match["beta_team_id"]),
        left_source="ALPHA",
        right_source="BETA",
        left={"id": str(match["alpha_team_id"]), "name": alpha_name},
        right={"id": str(match["beta_team_id"]), "name": beta_name},
        matcher_score=float(match["confidence"]),
        signals={
            "name_similarity": float(match["confidence"]),
            "country_match": alpha_country == beta_country if alpha_country else None,
            "conflict_flags": _conflict_flags(conflict),
        },
    )


def adapt_competition_match(
    match: Dict[str, Any], alpha_comp: pd.DataFrame, beta_comp: pd.DataFrame
) -> ValidationCandidate:
    alpha_lookup = _lookup(alpha_comp, "competition_id")
    beta_lookup = _lookup(beta_comp, "id")
    alpha_row = alpha_lookup[match["alpha_competition_id"]]
    beta_row = beta_lookup[match["beta_competition_id"]]
    alpha_name = normalize_competition(alpha_row.get("name", ""))
    beta_name = normalize_competition(beta_row.get("title", ""))
    alpha_country = _normalize_country(alpha_row.get("country"))
    beta_country = _normalize_country(beta_row.get("locale"))
    conflict = (
        "country_mismatch"
        if alpha_country and beta_country and alpha_country != beta_country
        else None
    )
    return ValidationCandidate(
        left_id=str(match["alpha_competition_id"]),
        right_id=str(match["beta_competition_id"]),
        left_source="ALPHA",
        right_source="BETA",
        left={"id": str(match["alpha_competition_id"]), "name": alpha_name},
        right={"id": str(match["beta_competition_id"]), "name": beta_name},
        matcher_score=float(match["confidence"]),
        signals={
            "name_similarity": float(match["confidence"]),
            "country_match": alpha_country == beta_country if alpha_country else None,
            "conflict_flags": _conflict_flags(conflict),
        },
    )


def adapt_season_match(
    match: Dict[str, Any], alpha_seasons: pd.DataFrame, beta_seasons: pd.DataFrame
) -> ValidationCandidate:
    alpha_lookup = _lookup(alpha_seasons, "season_id")
    beta_lookup = _lookup(beta_seasons, "id")
    alpha_row = alpha_lookup[match["alpha_season_id"]]
    beta_row = beta_lookup[match["beta_season_id"]]
    alpha_start, alpha_end = normalize_season(alpha_row.get("name", ""))
    beta_start, beta_end = normalize_season(beta_row.get("label", ""))
    conflict = (
        "season_year_mismatch"
        if alpha_start and beta_start and abs(int(alpha_start) - int(beta_start)) > 1
        else None
    )
    return ValidationCandidate(
        left_id=str(match["alpha_season_id"]),
        right_id=str(match["beta_season_id"]),
        left_source="ALPHA",
        right_source="BETA",
        left={
            "id": str(match["alpha_season_id"]),
            "start_year": alpha_start,
            "end_year": alpha_end,
        },
        right={
            "id": str(match["beta_season_id"]),
            "start_year": beta_start,
            "end_year": beta_end,
        },
        matcher_score=float(match["confidence"]),
        signals={
            "start_year_delta": (
                abs(int(alpha_start) - int(beta_start))
                if alpha_start and beta_start
                else None
            ),
            "conflict_flags": _conflict_flags(conflict),
        },
    )


def adapt_player_match(
    match: Dict[str, Any], alpha_players: pd.DataFrame, beta_players: pd.DataFrame
) -> ValidationCandidate:
    alpha_lookup = _lookup(alpha_players, "player_id")
    beta_lookup = _lookup(beta_players, "id")
    alpha_row = alpha_lookup[match["alpha_player_id"]]
    beta_row = beta_lookup[match["beta_player_id"]]
    alpha_name = normalize_name(alpha_row.get("name", ""))
    beta_name = normalize_name(beta_row.get("full_name", ""))
    alpha_year = (
        int(alpha_row["dob"].year)
        if alpha_row.get("dob") is not None and not pd.isna(alpha_row.get("dob"))
        else None
    )
    beta_year = _coerce_int(beta_row.get("birth_year"))
    conflict = (
        "dob_mismatch"
        if alpha_year and beta_year and abs(alpha_year - beta_year) > 1
        else None
    )
    breakdown = match.get("breakdown") or {}
    return ValidationCandidate(
        left_id=str(match["alpha_player_id"]),
        right_id=str(match["beta_player_id"]),
        left_source="ALPHA",
        right_source="BETA",
        left={"id": str(match["alpha_player_id"]), "name": alpha_name},
        right={"id": str(match["beta_player_id"]), "name": beta_name},
        matcher_score=float(match["confidence"]),
        signals={
            "name_similarity": breakdown.get("name_similarity"),
            "dob_similarity": breakdown.get("dob_similarity"),
            "team_similarity": breakdown.get("team_similarity"),
            "birth_year_alpha": alpha_year,
            "birth_year_beta": beta_year,
            "conflict_flags": _conflict_flags(conflict),
        },
    )


def adapt_match_match(
    match: Dict[str, Any], alpha_matches: pd.DataFrame, beta_matches: pd.DataFrame
) -> ValidationCandidate:
    alpha_lookup = _lookup(alpha_matches, "match_id")
    beta_lookup = _lookup(beta_matches, "id")
    alpha_row = alpha_lookup[match["alpha_match_id"]]
    beta_row = beta_lookup[match["beta_match_id"]]
    alpha_date = alpha_row.get("match_date")
    beta_date = beta_row.get("match_date")
    date_delta = None
    if (
        alpha_date is not None
        and beta_date is not None
        and not pd.isna(alpha_date)
        and not pd.isna(beta_date)
    ):
        date_delta = abs((alpha_date - beta_date).days)
    conflict = "date_mismatch" if date_delta is not None and date_delta > 2 else None
    return ValidationCandidate(
        left_id=str(match["alpha_match_id"]),
        right_id=str(match["beta_match_id"]),
        left_source="ALPHA",
        right_source="BETA",
        left={"id": str(match["alpha_match_id"]), "match_date": str(alpha_date)},
        right={"id": str(match["beta_match_id"]), "match_date": str(beta_date)},
        matcher_score=float(match["confidence"]),
        signals={
            "date_delta_days": date_delta,
            "conflict_flags": _conflict_flags(conflict),
        },
    )
