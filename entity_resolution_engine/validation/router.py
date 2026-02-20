from __future__ import annotations

import logging
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, List, Optional

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
from entity_resolution_engine.validation.llm_client import LLMClient
from entity_resolution_engine.validation.llm_validator import validate_pair
from entity_resolution_engine.validation.schemas import ValidationResult

logger = logging.getLogger(__name__)


@dataclass
class RoutingOutcome:
    approved_matches: List[Dict[str, Any]]
    rejected_matches: List[Dict[str, Any]]
    review_items: List[Dict[str, Any]]
    metrics: Dict[str, Any]


def _increment_llm_decision_counts(
    result: ValidationResult,
    llm_match: int,
    llm_no_match: int,
    llm_review: int,
) -> tuple[int, int, int]:
    if result.decision == "MATCH":
        llm_match += 1
    elif result.decision == "NO_MATCH":
        llm_no_match += 1
    else:
        llm_review += 1
    return llm_match, llm_no_match, llm_review


def _llm_validation_available(config: LLMValidationConfig) -> bool:
    if not config.enabled:
        return False
    return all(
        os.getenv(env_var, "")
        for env_var in (config.provider_env, config.model_env, config.api_key_env)
    )


def _build_review_item(
    run_id: str,
    entity_type: str,
    candidate: ValidationCandidate,
    result: ValidationResult,
) -> Dict[str, Any]:
    return {
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


def _fallback_decision(fallback_mode: str) -> ValidationResult:
    if fallback_mode == "review":
        return ValidationResult(
            decision="REVIEW",
            confidence=0.0,
            reasons=["LLM unavailable - fallback mode set to review"],
            risk_flags=["llm_fallback"],
        )
    return ValidationResult(
        decision="MATCH",
        confidence=0.0,
        reasons=["LLM unavailable - fallback mode auto-approved"],
        risk_flags=["llm_fallback"],
    )


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
    llm_call_count = 0
    llm_error_count = 0
    llm_invalid_json_retry_count = 0
    llm_total_latency_ms = 0.0
    llm_disabled_reason: Optional[str] = None
    fallback_mode = config.fallback_mode_when_llm_unhealthy
    circuit_breaker = config.circuit_breaker
    circuit_window: Deque[Dict[str, bool]] = deque(maxlen=circuit_breaker.window)

    llm_client: Optional[LLMClient] = None
    llm_available = _llm_validation_available(config)
    if llm_available:
        llm_client = LLMClient(
            provider=os.getenv(config.provider_env, ""),
            model=os.getenv(config.model_env, ""),
            api_key=os.getenv(config.api_key_env, ""),
        )
    else:
        llm_disabled_reason = "llm_unavailable"

    def _record_llm_outcome(result: ValidationResult) -> None:
        nonlocal llm_error_count, llm_invalid_json_retry_count, llm_total_latency_ms
        error_flag = "llm_error" in result.risk_flags
        invalid_retry = "llm_invalid_json_retry" in result.risk_flags
        if error_flag:
            llm_error_count += 1
        if invalid_retry:
            llm_invalid_json_retry_count += 1
        if llm_client and llm_client.last_latency_ms is not None:
            llm_total_latency_ms += llm_client.last_latency_ms
        circuit_window.append(
            {"success": not error_flag, "invalid_json_retry": invalid_retry}
        )

    def _circuit_open() -> bool:
        if len(circuit_window) < circuit_breaker.window:
            return False
        failures = sum(1 for entry in circuit_window if not entry["success"])
        invalid_retries = sum(
            1 for entry in circuit_window if entry["invalid_json_retry"]
        )
        fail_rate = failures / len(circuit_window)
        invalid_rate = invalid_retries / len(circuit_window)
        return (
            fail_rate >= circuit_breaker.max_fail_rate
            or invalid_rate >= circuit_breaker.max_invalid_json_rate
        )

    for match in matches:
        candidate = adapter(match)
        score = candidate.matcher_score
        if score < threshold.low:
            rejected.append(match)
            continue
        if score >= threshold.high and not candidate.signals.get("conflict_flags"):
            approved.append(match)
            continue

        if llm_disabled_reason:
            fallback_result = _fallback_decision(fallback_mode)
            llm_match, llm_no_match, llm_review = _increment_llm_decision_counts(
                fallback_result,
                llm_match,
                llm_no_match,
                llm_review,
            )
            review_items.append(
                _build_review_item(run_id, entity_type, candidate, fallback_result)
            )
            continue

        if llm_call_count >= config.max_calls_per_entity_type_per_run:
            llm_disabled_reason = "max_calls_exceeded"
            fallback_result = _fallback_decision(fallback_mode)
            llm_match, llm_no_match, llm_review = _increment_llm_decision_counts(
                fallback_result,
                llm_match,
                llm_no_match,
                llm_review,
            )
            review_items.append(
                _build_review_item(run_id, entity_type, candidate, fallback_result)
            )
            continue

        gray_zone_sent += 1
        llm_call_count += 1
        result = validate_pair(
            entity_type,
            candidate.left,
            candidate.right,
            candidate.matcher_score,
            candidate.signals,
            config=config,
            llm_client=llm_client,
        )
        _record_llm_outcome(result)
        if _circuit_open():
            llm_disabled_reason = "circuit_breaker_open"
        llm_match, llm_no_match, llm_review = _increment_llm_decision_counts(
            result,
            llm_match,
            llm_no_match,
            llm_review,
        )
        review_items.append(_build_review_item(run_id, entity_type, candidate, result))

    llm_avg_latency_ms = (
        llm_total_latency_ms / llm_call_count if llm_call_count else None
    )
    metrics = {
        "run_id": run_id,
        "entity_type": entity_type,
        "started_at": None,
        "finished_at": None,
        "total_candidates": len(matches),
        "auto_match_count": len(approved),
        "auto_reject_count": len(rejected),
        "gray_zone_sent_count": gray_zone_sent,
        "llm_match_count": llm_match,
        "llm_no_match_count": llm_no_match,
        "llm_review_count": llm_review,
        "llm_call_count": llm_call_count,
        "llm_error_count": llm_error_count,
        "llm_invalid_json_retry_count": llm_invalid_json_retry_count,
        "llm_avg_latency_ms": llm_avg_latency_ms,
        "llm_fallback_mode": fallback_mode,
        "llm_disabled_reason": llm_disabled_reason,
    }
    logger.info(
        "LLM routing summary run_id=%s entity_type=%s total=%s llm_calls=%s "
        "errors=%s fallback_mode=%s",
        run_id,
        entity_type,
        len(matches),
        llm_call_count,
        llm_error_count,
        fallback_mode,
    )
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
