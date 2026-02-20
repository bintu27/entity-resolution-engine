import pandas as pd

from entity_resolution_engine.validation.config import (
    CircuitBreakerConfig,
    GrayZoneThreshold,
    LLMValidationConfig,
)
from entity_resolution_engine.validation.router import route_team_matches
from entity_resolution_engine.validation.schemas import ValidationResult
from entity_resolution_engine.validation import router as router_module


def test_route_team_matches_gray_zone():
    alpha = pd.DataFrame(
        [
            {"team_id": 1, "name": "Alpha FC", "country": "US"},
            {"team_id": 2, "name": "Beta FC", "country": "US"},
            {"team_id": 3, "name": "Gamma FC", "country": "US"},
        ]
    )
    beta = pd.DataFrame(
        [
            {"id": 10, "display_name": "Alpha FC", "region": "US"},
            {"id": 20, "display_name": "Beta FC", "region": "US"},
            {"id": 30, "display_name": "Gamma FC", "region": "US"},
        ]
    )
    matches = [
        {"alpha_team_id": 1, "beta_team_id": 10, "confidence": 0.95},
        {"alpha_team_id": 2, "beta_team_id": 20, "confidence": 0.8},
        {"alpha_team_id": 3, "beta_team_id": 30, "confidence": 0.6},
    ]
    config = LLMValidationConfig(
        enabled=False,
        gray_zone={"team": GrayZoneThreshold(low=0.7, high=0.9)},
        internal_api_key_env="INTERNAL_API_KEY",
        provider_env="LLM_PROVIDER",
        model_env="LLM_MODEL",
        api_key_env="LLM_API_KEY",
        max_calls_per_entity_type_per_run=200,
        circuit_breaker=CircuitBreakerConfig(
            window=50, max_fail_rate=0.2, max_invalid_json_rate=0.1
        ),
        fallback_mode_when_llm_unhealthy="auto_approve",
    )

    outcome = route_team_matches(matches, alpha, beta, run_id="run-1", config=config)

    assert len(outcome.approved_matches) == 1
    assert len(outcome.rejected_matches) == 1
    assert len(outcome.review_items) == 1
    assert outcome.review_items[0]["llm_decision"] == "MATCH"
    assert outcome.metrics["gray_zone_sent_count"] == 0


def _sample_team_frames():
    alpha = pd.DataFrame(
        [
            {"team_id": 1, "name": "Alpha FC", "country": "US"},
            {"team_id": 2, "name": "Beta FC", "country": "US"},
            {"team_id": 3, "name": "Gamma FC", "country": "US"},
        ]
    )
    beta = pd.DataFrame(
        [
            {"id": 10, "display_name": "Alpha FC", "region": "US"},
            {"id": 20, "display_name": "Beta FC", "region": "US"},
            {"id": 30, "display_name": "Gamma FC", "region": "US"},
        ]
    )
    return alpha, beta


def test_router_respects_max_calls(monkeypatch):
    alpha, beta = _sample_team_frames()
    matches = [
        {"alpha_team_id": 1, "beta_team_id": 10, "confidence": 0.8},
        {"alpha_team_id": 2, "beta_team_id": 20, "confidence": 0.8},
    ]
    config = LLMValidationConfig(
        enabled=True,
        gray_zone={"team": GrayZoneThreshold(low=0.7, high=0.9)},
        internal_api_key_env="INTERNAL_API_KEY",
        provider_env="LLM_PROVIDER",
        model_env="LLM_MODEL",
        api_key_env="LLM_API_KEY",
        max_calls_per_entity_type_per_run=1,
        circuit_breaker=CircuitBreakerConfig(
            window=5, max_fail_rate=0.5, max_invalid_json_rate=0.5
        ),
        fallback_mode_when_llm_unhealthy="auto_approve",
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "gpt-test")
    monkeypatch.setenv("LLM_API_KEY", "key")

    def fake_validate(*_args, **kwargs):
        llm_client = kwargs.get("llm_client")
        if llm_client:
            llm_client.last_latency_ms = 5.0
        return ValidationResult(
            decision="MATCH",
            confidence=0.9,
            reasons=[],
            risk_flags=[],
        )

    monkeypatch.setattr(router_module, "validate_pair", fake_validate)
    outcome = route_team_matches(matches, alpha, beta, run_id="run-2", config=config)

    assert outcome.metrics["llm_call_count"] == 1
    assert outcome.metrics["llm_disabled_reason"] == "max_calls_exceeded"
    assert len(outcome.review_items) == 2
    assert len(outcome.approved_matches) == 0
    assert all(item["llm_decision"] == "MATCH" for item in outcome.review_items)


def test_router_circuit_breaker_trips(monkeypatch):
    alpha, beta = _sample_team_frames()
    matches = [
        {"alpha_team_id": 1, "beta_team_id": 10, "confidence": 0.8},
        {"alpha_team_id": 2, "beta_team_id": 20, "confidence": 0.8},
        {"alpha_team_id": 3, "beta_team_id": 30, "confidence": 0.8},
    ]
    config = LLMValidationConfig(
        enabled=True,
        gray_zone={"team": GrayZoneThreshold(low=0.7, high=0.9)},
        internal_api_key_env="INTERNAL_API_KEY",
        provider_env="LLM_PROVIDER",
        model_env="LLM_MODEL",
        api_key_env="LLM_API_KEY",
        max_calls_per_entity_type_per_run=10,
        circuit_breaker=CircuitBreakerConfig(
            window=2, max_fail_rate=0.5, max_invalid_json_rate=0.5
        ),
        fallback_mode_when_llm_unhealthy="review",
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "gpt-test")
    monkeypatch.setenv("LLM_API_KEY", "key")

    def fake_validate(*_args, **kwargs):
        llm_client = kwargs.get("llm_client")
        if llm_client:
            llm_client.last_latency_ms = 3.0
        return ValidationResult(
            decision="REVIEW",
            confidence=0.0,
            reasons=["LLM error"],
            risk_flags=["llm_error"],
        )

    monkeypatch.setattr(router_module, "validate_pair", fake_validate)
    outcome = route_team_matches(matches, alpha, beta, run_id="run-3", config=config)

    assert outcome.metrics["llm_call_count"] == 2
    assert outcome.metrics["llm_disabled_reason"] == "circuit_breaker_open"
    assert len(outcome.review_items) == 3


def test_router_fallback_mode_review_when_unavailable():
    alpha, beta = _sample_team_frames()
    matches = [{"alpha_team_id": 1, "beta_team_id": 10, "confidence": 0.8}]
    config = LLMValidationConfig(
        enabled=False,
        gray_zone={"team": GrayZoneThreshold(low=0.7, high=0.9)},
        internal_api_key_env="INTERNAL_API_KEY",
        provider_env="LLM_PROVIDER",
        model_env="LLM_MODEL",
        api_key_env="LLM_API_KEY",
        max_calls_per_entity_type_per_run=200,
        circuit_breaker=CircuitBreakerConfig(
            window=5, max_fail_rate=0.5, max_invalid_json_rate=0.5
        ),
        fallback_mode_when_llm_unhealthy="review",
    )

    outcome = route_team_matches(matches, alpha, beta, run_id="run-4", config=config)

    assert len(outcome.review_items) == 1
    assert outcome.metrics["llm_fallback_mode"] == "review"
    assert outcome.metrics["llm_disabled_reason"] == "llm_unavailable"
