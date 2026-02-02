from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict

import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "llm_validation.yml"


@dataclass(frozen=True)
class GrayZoneThreshold:
    low: float
    high: float


@dataclass(frozen=True)
class LLMValidationConfig:
    enabled: bool
    gray_zone: Dict[str, GrayZoneThreshold]
    internal_api_key_env: str
    provider_env: str
    model_env: str
    api_key_env: str

    def threshold_for(self, entity_type: str) -> GrayZoneThreshold:
        return self.gray_zone.get(entity_type, GrayZoneThreshold(low=0.0, high=1.0))


@lru_cache
def get_llm_validation_config(path: Path = CONFIG_PATH) -> LLMValidationConfig:
    data = yaml.safe_load(path.read_text()) if path.exists() else {}
    gray_zone = {
        key: GrayZoneThreshold(**value)
        for key, value in (data.get("gray_zone") or {}).items()
    }
    return LLMValidationConfig(
        enabled=bool(data.get("enabled", False)),
        gray_zone=gray_zone,
        internal_api_key_env=data.get("internal_api_key_env", "INTERNAL_API_KEY"),
        provider_env=data.get("provider_env", "LLM_PROVIDER"),
        model_env=data.get("model_env", "LLM_MODEL"),
        api_key_env=data.get("api_key_env", "LLM_API_KEY"),
    )
