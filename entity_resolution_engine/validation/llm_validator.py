from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from entity_resolution_engine.validation.config import (
    LLMValidationConfig,
    get_llm_validation_config,
)
from entity_resolution_engine.validation.llm_client import LLMClient
from entity_resolution_engine.validation.schemas import ValidationResult

SYSTEM_PROMPT = (
    "You are a strict entity-resolution validator. "
    "Return JSON with decision MATCH, NO_MATCH, or REVIEW."
)


def validate_pair(
    entity_type: str,
    left: Dict[str, Any],
    right: Dict[str, Any],
    matcher_score: float,
    signals: Dict[str, Any],
    config: Optional[LLMValidationConfig] = None,
    llm_client: Optional[LLMClient] = None,
) -> ValidationResult:
    config = config or get_llm_validation_config()
    if not config.enabled:
        return ValidationResult(
            decision="REVIEW",
            confidence=0.0,
            reasons=["LLM validation disabled"],
            risk_flags=[],
        )

    provider = os.getenv(config.provider_env, "")
    model = os.getenv(config.model_env, "")
    api_key = os.getenv(config.api_key_env, "")
    if not (provider and model and api_key):
        return ValidationResult(
            decision="REVIEW",
            confidence=0.0,
            reasons=["LLM provider not configured"],
            risk_flags=["llm_unavailable"],
        )

    llm_client = llm_client or LLMClient(provider=provider, model=model, api_key=api_key)
    payload = {
        "entity_type": entity_type,
        "matcher_score": matcher_score,
        "left": left,
        "right": right,
        "signals": signals,
        "response_schema": {
            "decision": "MATCH|NO_MATCH|REVIEW",
            "confidence": "0..1",
            "reasons": "list[str]",
            "risk_flags": "list[str]",
        },
    }
    user_prompt = json.dumps(payload, sort_keys=True)
    try:
        response = llm_client.request_json(SYSTEM_PROMPT, user_prompt)
        return ValidationResult.model_validate(response)
    except Exception:
        return ValidationResult(
            decision="REVIEW",
            confidence=0.0,
            reasons=["LLM validation failed"],
            risk_flags=["llm_error"],
        )
