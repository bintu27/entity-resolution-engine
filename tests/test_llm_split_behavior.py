import pandas as pd
from sqlalchemy import create_engine, text

from entity_resolution_engine.monitoring.llm_triage import generate_triage_report
from entity_resolution_engine.validation import router as router_module
from entity_resolution_engine.validation.config import (
    CircuitBreakerConfig,
    GrayZoneThreshold,
    LLMValidationConfig,
)
from entity_resolution_engine.validation.router import route_team_matches


def _config(enabled: bool = True, **kwargs) -> LLMValidationConfig:
    return LLMValidationConfig(
        enabled=enabled,
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
        **kwargs,
    )


def test_mapping_llm_disabled_even_when_global_enabled(monkeypatch):
    alpha = pd.DataFrame([{"team_id": 1, "name": "Alpha FC", "country": "US"}])
    beta = pd.DataFrame([{"id": 10, "display_name": "Alpha FC", "region": "US"}])
    matches = [{"alpha_team_id": 1, "beta_team_id": 10, "confidence": 0.8}]
    config = _config(mapping_enabled=False, reporting_enabled=True)

    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "gpt-test")
    monkeypatch.setenv("LLM_API_KEY", "key")

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("validate_pair should not be called for mapping")

    monkeypatch.setattr(router_module, "validate_pair", fail_if_called)
    outcome = route_team_matches(
        matches, alpha, beta, run_id="run-split", config=config
    )

    assert outcome.metrics["llm_call_count"] == 0
    assert outcome.metrics["llm_disabled_reason"] == "llm_unavailable"
    assert len(outcome.review_items) == 1
    assert outcome.review_items[0]["llm_decision"] == "MATCH"


def test_reporting_llm_enabled_even_when_global_disabled(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE anomaly_events (
                    run_id TEXT,
                    entity_type TEXT,
                    metric_name TEXT,
                    z_score REAL,
                    created_at TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE llm_match_reviews (
                    run_id TEXT,
                    entity_type TEXT,
                    left_id TEXT,
                    right_id TEXT,
                    matcher_score REAL,
                    signals TEXT,
                    created_at TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE anomaly_triage_reports (
                    run_id TEXT,
                    entity_type TEXT,
                    report TEXT
                )
                """
            )
        )

    config = _config(enabled=False, mapping_enabled=False, reporting_enabled=True)
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "gpt-test")
    monkeypatch.setenv("LLM_API_KEY", "key")

    class StubClient:
        def request_json(self, *_args, **_kwargs):
            return {
                "summary": "LLM triage used",
                "likely_causes": ["none"],
                "impact": "low",
                "suggested_actions": ["monitor"],
                "queries_to_run": [],
            }

    report = generate_triage_report(
        engine,
        run_id="run-split",
        entity_type="team",
        config=config,
        llm_client=StubClient(),
    )

    assert report["summary"] == "LLM triage used"
