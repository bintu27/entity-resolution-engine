from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine

from entity_resolution_engine.validation.config import (
    LLMValidationConfig,
    get_llm_validation_config,
)
from entity_resolution_engine.validation.llm_client import LLMClient


class TriageReport(BaseModel):
    summary: str
    likely_causes: List[str] = Field(default_factory=list)
    impact: str
    suggested_actions: List[str] = Field(default_factory=list)
    queries_to_run: List[str] = Field(default_factory=list)


SYSTEM_PROMPT = (
    "You are a data quality analyst. "
    "Return JSON with summary, likely_causes, impact, suggested_actions, queries_to_run."
)


def _fallback_report(anomalies: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary = "No anomalies detected." if not anomalies else "Anomalies detected."
    likely_causes = [
        f"{item['metric_name']} drift (z={item['z_score']:.2f})"
        for item in anomalies
    ]
    return {
        "summary": summary,
        "likely_causes": likely_causes,
        "impact": "Review pipeline metrics and LLM decisions.",
        "suggested_actions": ["Inspect recent matcher thresholds", "Sample review items"],
        "queries_to_run": [
            "SELECT * FROM pipeline_run_metrics WHERE run_id = '<RUN_ID>';",
            "SELECT * FROM llm_match_reviews WHERE run_id = '<RUN_ID>' LIMIT 50;",
        ],
    }


def generate_triage_report(
    engine: Engine,
    run_id: str,
    entity_type: str,
    config: Optional[LLMValidationConfig] = None,
    llm_client: Optional[LLMClient] = None,
) -> Dict[str, Any]:
    config = config or get_llm_validation_config()
    with engine.connect() as conn:
        anomalies = conn.execute(
            text(
                """
                SELECT * FROM anomaly_events
                WHERE run_id = :run_id AND entity_type = :entity_type
                ORDER BY created_at DESC
                """
            ),
            {"run_id": run_id, "entity_type": entity_type},
        ).mappings().all()
        reviews = conn.execute(
            text(
                """
                SELECT left_id, right_id, matcher_score, signals
                FROM llm_match_reviews
                WHERE run_id = :run_id AND entity_type = :entity_type
                ORDER BY created_at DESC
                LIMIT 20
                """
            ),
            {"run_id": run_id, "entity_type": entity_type},
        ).mappings().all()

    anomalies_payload = [dict(item) for item in anomalies]
    reviews_payload = [dict(item) for item in reviews]

    if not config.enabled:
        report = _fallback_report(anomalies_payload)
    else:
        provider = os.getenv(config.provider_env, "")
        model = os.getenv(config.model_env, "")
        api_key = os.getenv(config.api_key_env, "")
        if not (provider and model and api_key):
            report = _fallback_report(anomalies_payload)
        else:
            llm_client = llm_client or LLMClient(
                provider=provider, model=model, api_key=api_key
            )
            payload = {
                "run_id": run_id,
                "entity_type": entity_type,
                "anomalies": anomalies_payload,
                "review_samples": reviews_payload,
            }
            user_prompt = json.dumps(payload, sort_keys=True)
            try:
                response = llm_client.request_json(SYSTEM_PROMPT, user_prompt)
                report = TriageReport.model_validate(response).model_dump()
            except Exception:
                report = _fallback_report(anomalies_payload)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO anomaly_triage_reports (run_id, entity_type, report)
                VALUES (:run_id, :entity_type, :report)
                """
            ),
            {"run_id": run_id, "entity_type": entity_type, "report": json.dumps(report)},
        )

    return report
