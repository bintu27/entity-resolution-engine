from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "llm_validation.yml"


@dataclass(frozen=True)
class GrayZoneThreshold:
    low: float
    high: float


@dataclass(frozen=True)
class CircuitBreakerConfig:
    window: int
    max_fail_rate: float
    max_invalid_json_rate: float


@dataclass(frozen=True)
class LLMValidationConfig:
    enabled: bool
    gray_zone: Dict[str, GrayZoneThreshold]
    internal_api_key_env: str
    provider_env: str
    model_env: str
    api_key_env: str
    max_calls_per_entity_type_per_run: int
    circuit_breaker: CircuitBreakerConfig
    fallback_mode_when_llm_unhealthy: str
    mapping_enabled: Optional[bool] = None
    reporting_enabled: Optional[bool] = None

    @property
    def mapping_llm_enabled(self) -> bool:
        if self.mapping_enabled is None:
            return self.enabled
        return self.mapping_enabled

    @property
    def reporting_llm_enabled(self) -> bool:
        if self.reporting_enabled is None:
            return self.enabled
        return self.reporting_enabled

    def threshold_for(self, entity_type: str) -> GrayZoneThreshold:
        return self.gray_zone.get(entity_type, GrayZoneThreshold(low=0.0, high=1.0))


@lru_cache
def get_llm_validation_config(path: Path = CONFIG_PATH) -> LLMValidationConfig:
    data = yaml.safe_load(path.read_text()) if path.exists() else {}
    gray_zone = {
        key: GrayZoneThreshold(**value)
        for key, value in (data.get("gray_zone") or {}).items()
    }
    circuit_breaker_data = data.get("circuit_breaker") or {}
    circuit_breaker = CircuitBreakerConfig(
        window=int(circuit_breaker_data.get("window", 50)),
        max_fail_rate=float(circuit_breaker_data.get("max_fail_rate", 0.2)),
        max_invalid_json_rate=float(
            circuit_breaker_data.get("max_invalid_json_rate", 0.1)
        ),
    )
    return LLMValidationConfig(
        enabled=bool(data.get("enabled", False)),
        mapping_enabled=data.get("mapping_enabled"),
        reporting_enabled=data.get("reporting_enabled"),
        gray_zone=gray_zone,
        internal_api_key_env=data.get("internal_api_key_env", "INTERNAL_API_KEY"),
        provider_env=data.get("provider_env", "LLM_PROVIDER"),
        model_env=data.get("model_env", "LLM_MODEL"),
        api_key_env=data.get("api_key_env", "LLM_API_KEY"),
        max_calls_per_entity_type_per_run=int(
            data.get("max_calls_per_entity_type_per_run", 200)
        ),
        circuit_breaker=circuit_breaker,
        fallback_mode_when_llm_unhealthy=data.get(
            "fallback_mode_when_llm_unhealthy", "auto_approve"
        ),
    )
