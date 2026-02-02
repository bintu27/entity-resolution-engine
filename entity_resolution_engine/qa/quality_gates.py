from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "quality_gates.yml"


@dataclass(frozen=True)
class QualityGateConfig:
    max_llm_review_rate: float
    max_gray_zone_rate: float
    fail_on_high_severity_anomalies: bool
    max_llm_error_rate: float


@lru_cache
def get_quality_gate_config(path: Path = CONFIG_PATH) -> QualityGateConfig:
    data = yaml.safe_load(path.read_text()) if path.exists() else {}
    return QualityGateConfig(
        max_llm_review_rate=float(data.get("max_llm_review_rate", 0.15)),
        max_gray_zone_rate=float(data.get("max_gray_zone_rate", 0.35)),
        fail_on_high_severity_anomalies=bool(
            data.get("fail_on_high_severity_anomalies", True)
        ),
        max_llm_error_rate=float(data.get("max_llm_error_rate", 0.05)),
    )


def _normalize_config(
    config: Optional[QualityGateConfig | Mapping[str, Any]],
) -> QualityGateConfig:
    if config is None:
        return get_quality_gate_config()
    if isinstance(config, QualityGateConfig):
        return config
    return QualityGateConfig(
        max_llm_review_rate=float(config.get("max_llm_review_rate", 0.15)),
        max_gray_zone_rate=float(config.get("max_gray_zone_rate", 0.35)),
        fail_on_high_severity_anomalies=bool(
            config.get("fail_on_high_severity_anomalies", True)
        ),
        max_llm_error_rate=float(config.get("max_llm_error_rate", 0.05)),
    )


def evaluate_quality_gates(
    engine: Engine, run_id: str, config: Optional[QualityGateConfig | Mapping[str, Any]]
) -> Dict[str, Any]:
    config = _normalize_config(config)
    with engine.connect() as conn:
        metrics_rows = (
            conn.execute(
                text("SELECT * FROM pipeline_run_metrics WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            .mappings()
            .all()
        )
        high_severity_count = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM anomaly_events
                WHERE run_id = :run_id AND severity = 'HIGH'
                """
            ),
            {"run_id": run_id},
        ).scalar()

    totals: Dict[str, float] = {
        "total_candidates": 0.0,
        "gray_zone_sent_count": 0.0,
        "llm_review_count": 0.0,
        "llm_call_count": 0.0,
        "llm_error_count": 0.0,
    }
    for row in metrics_rows:
        totals["total_candidates"] += float(row.get("total_candidates") or 0)
        totals["gray_zone_sent_count"] += float(row.get("gray_zone_sent_count") or 0)
        totals["llm_review_count"] += float(row.get("llm_review_count") or 0)
        totals["llm_call_count"] += float(row.get("llm_call_count") or 0)
        totals["llm_error_count"] += float(row.get("llm_error_count") or 0)

    total_candidates = totals["total_candidates"] or 0.0
    llm_call_count = totals["llm_call_count"] or 0.0

    gray_zone_rate = (
        totals["gray_zone_sent_count"] / total_candidates if total_candidates else 0.0
    )
    llm_review_rate = (
        totals["llm_review_count"] / total_candidates if total_candidates else 0.0
    )
    llm_error_rate = (
        totals["llm_error_count"] / llm_call_count if llm_call_count else 0.0
    )

    failed_gates = []
    if gray_zone_rate > config.max_gray_zone_rate:
        failed_gates.append("max_gray_zone_rate")
    if llm_review_rate > config.max_llm_review_rate:
        failed_gates.append("max_llm_review_rate")
    if llm_error_rate > config.max_llm_error_rate:
        failed_gates.append("max_llm_error_rate")
    if config.fail_on_high_severity_anomalies and (high_severity_count or 0) > 0:
        failed_gates.append("high_severity_anomalies")

    status = "FAIL" if failed_gates else "PASS"
    gate_values = {
        "gray_zone_rate": gray_zone_rate,
        "llm_review_rate": llm_review_rate,
        "llm_error_rate": llm_error_rate,
        "high_severity_anomaly_count": int(high_severity_count or 0),
        "total_candidates": int(total_candidates),
        "llm_call_count": int(llm_call_count),
    }
    return {
        "run_id": run_id,
        "status": status,
        "failed_gates": failed_gates,
        "gate_values": gate_values,
    }
