from entity_resolution_engine.validation.config import (
    CircuitBreakerConfig,
    GrayZoneThreshold,
    LLMValidationConfig,
)


def _base_config(enabled: bool = False, **kwargs) -> LLMValidationConfig:
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


def test_mapping_and_reporting_flags_fallback_to_enabled():
    config = _base_config(enabled=True)
    assert config.mapping_llm_enabled is True
    assert config.reporting_llm_enabled is True

    config = _base_config(enabled=False)
    assert config.mapping_llm_enabled is False
    assert config.reporting_llm_enabled is False


def test_mapping_and_reporting_flags_override_enabled():
    config = _base_config(
        enabled=False,
        mapping_enabled=False,
        reporting_enabled=True,
    )
    assert config.mapping_llm_enabled is False
    assert config.reporting_llm_enabled is True
