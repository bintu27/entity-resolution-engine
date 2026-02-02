import pandas as pd

from entity_resolution_engine.validation.config import (
    GrayZoneThreshold,
    LLMValidationConfig,
)
from entity_resolution_engine.validation.router import route_team_matches


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
    )

    outcome = route_team_matches(matches, alpha, beta, run_id="run-1", config=config)

    assert len(outcome.approved_matches) == 2
    assert len(outcome.rejected_matches) == 1
    assert len(outcome.review_items) == 0
    assert outcome.metrics["gray_zone_sent_count"] == 0
