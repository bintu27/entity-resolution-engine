from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from entity_resolution_engine.validation.adapters import (
    ValidationCandidate,
    adapt_competition_match,
    adapt_match_match,
    adapt_player_match,
    adapt_season_match,
    adapt_team_match,
)
from entity_resolution_engine.validation.config import (
    LLMValidationConfig,
    get_llm_validation_config,
)
from entity_resolution_engine.validation.llm_validator import validate_pair
from entity_resolution_engine.validation.schemas import ValidationResult


@dataclass
class RoutingOutcome:
    approved_matches: List[Dict[str, Any]]
    rejected_matches: List[Dict[str, Any]]
    review_items: List[Dict[str, Any]]
    metrics: Dict[str, Any]


def _decision_from_result(result: ValidationResult) -> str:
    if result.decision == "MATCH":
        return "approved"
    if result.decision == "NO_MATCH":
        return "rejected"
    return "review"


def _route_matches(
    entity_type: str,
    matches: List[Dict[str, Any]],
    adapter: Callable[[Dict[str, Any]], ValidationCandidate],
    config: Optional[LLMValidationConfig],
    run_id: str,
) -> RoutingOutcome:
    config = config or get_llm_validation_config()
    threshold = config.threshold_for(entity_type)
    approved: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    review_items: List[Dict[str, Any]] = []

    gray_zone_sent = 0
    llm_match = 0
    llm_no_match = 0
    llm_review = 0

    for match in matches:
        candidate = adapter(match)
        score = candidate.matcher_score
        if score < threshold.low:
            rejected.append(match)
            continue
        if score >= threshold.high and not candidate.signals.get("conflict_flags"):
            approved.append(match)
            continue

        gray_zone_sent += 1
        result = validate_pair(
            entity_type,
            candidate.left,
            candidate.right,
            candidate.matcher_score,
            candidate.signals,
            config=config,
        )
        decision = _decision_from_result(result)
        if decision == "approved":
            approved.append(match)
            llm_match += 1
        elif decision == "rejected":
            rejected.append(match)
            llm_no_match += 1
        else:
            llm_review += 1
            review_items.append(
                {
                    "run_id": run_id,
                    "entity_type": entity_type,
                    "left_source": candidate.left_source,
                    "left_id": candidate.left_id,
                    "right_source": candidate.right_source,
                    "right_id": candidate.right_id,
                    "matcher_score": candidate.matcher_score,
                    "signals": candidate.signals,
                    "llm_decision": result.decision,
                    "llm_confidence": result.confidence,
                    "reasons": result.reasons,
                    "risk_flags": result.risk_flags,
                    "status": "PENDING",
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            )

    metrics = {
        "run_id": run_id,
        "entity_type": entity_type,
        "started_at": None,
        "finished_at": None,
        "total_candidates": len(matches),
        "auto_match_count": len(approved) - llm_match,
        "auto_reject_count": len(rejected) - llm_no_match,
        "gray_zone_sent_count": gray_zone_sent,
        "llm_match_count": llm_match,
        "llm_no_match_count": llm_no_match,
        "llm_review_count": llm_review,
    }
    return RoutingOutcome(approved, rejected, review_items, metrics)


def route_team_matches(
    matches: List[Dict[str, Any]],
    alpha_teams: pd.DataFrame,
    beta_teams: pd.DataFrame,
    run_id: str,
    config: Optional[LLMValidationConfig] = None,
) -> RoutingOutcome:
    return _route_matches(
        "team",
        matches,
        lambda match: adapt_team_match(match, alpha_teams, beta_teams),
        config,
        run_id,
    )


def route_competition_matches(
    matches: List[Dict[str, Any]],
    alpha_comp: pd.DataFrame,
    beta_comp: pd.DataFrame,
    run_id: str,
    config: Optional[LLMValidationConfig] = None,
) -> RoutingOutcome:
    return _route_matches(
        "competition",
        matches,
        lambda match: adapt_competition_match(match, alpha_comp, beta_comp),
        config,
        run_id,
    )


def route_season_matches(
    matches: List[Dict[str, Any]],
    alpha_seasons: pd.DataFrame,
    beta_seasons: pd.DataFrame,
    run_id: str,
    config: Optional[LLMValidationConfig] = None,
) -> RoutingOutcome:
    return _route_matches(
        "season",
        matches,
        lambda match: adapt_season_match(match, alpha_seasons, beta_seasons),
        config,
        run_id,
    )


def route_player_matches(
    matches: List[Dict[str, Any]],
    alpha_players: pd.DataFrame,
    beta_players: pd.DataFrame,
    run_id: str,
    config: Optional[LLMValidationConfig] = None,
) -> RoutingOutcome:
    return _route_matches(
        "player",
        matches,
        lambda match: adapt_player_match(match, alpha_players, beta_players),
        config,
        run_id,
    )


def route_match_matches(
    matches: List[Dict[str, Any]],
    alpha_matches: pd.DataFrame,
    beta_matches: pd.DataFrame,
    run_id: str,
    config: Optional[LLMValidationConfig] = None,
) -> RoutingOutcome:
    return _route_matches(
        "match",
        matches,
        lambda match: adapt_match_match(match, alpha_matches, beta_matches),
        config,
        run_id,
    )
