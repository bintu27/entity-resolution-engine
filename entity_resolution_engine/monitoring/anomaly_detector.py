from __future__ import annotations

from statistics import mean, stdev
from typing import Any, Dict, List, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _z_score(current: float, baseline: List[float]) -> Optional[float]:
    if len(baseline) < 2:
        return None
    baseline_std = stdev(baseline)
    if baseline_std == 0:
        return None
    return (current - mean(baseline)) / baseline_std


def detect_anomalies(
    engine: Engine,
    run_id: str,
    entity_type: str,
    lookback: int = 8,
    z_threshold: float = 2.0,
) -> List[Dict[str, float | str]]:
    with engine.connect() as conn:
        current = (
            conn.execute(
                text(
                    """
                SELECT * FROM pipeline_run_metrics
                WHERE run_id = :run_id AND entity_type = :entity_type
                LIMIT 1
                """
                ),
                {"run_id": run_id, "entity_type": entity_type},
            )
            .mappings()
            .first()
        )
        if not current:
            return []
        current_row: Dict[str, Any] = dict(current)
        baseline_rows = (
            conn.execute(
                text(
                    """
                SELECT * FROM pipeline_run_metrics
                WHERE entity_type = :entity_type AND run_id != :run_id
                ORDER BY finished_at DESC NULLS LAST
                LIMIT :limit
                """
                ),
                {"entity_type": entity_type, "run_id": run_id, "limit": lookback},
            )
            .mappings()
            .all()
        )

    baseline_data: List[Dict[str, Any]] = [dict(row) for row in baseline_rows]

    if len(baseline_data) < 2:
        return []

    def _rate(row: Mapping[str, Any], numerator: str) -> float:
        total = row.get("total_candidates") or 0
        if total <= 0:
            return 0.0
        return float(row.get(numerator) or 0) / float(total)

    metrics = {
        "gray_zone_rate": (
            float(current_row.get("gray_zone_sent_count") or 0)
            / max(float(current_row.get("total_candidates") or 1), 1.0)
        ),
        "llm_review_rate": _rate(current_row, "llm_review_count"),
        "auto_match_rate": _rate(current_row, "auto_match_count"),
        "auto_reject_rate": _rate(current_row, "auto_reject_count"),
    }

    baseline_metrics: Dict[str, List[float]] = {}
    for key in metrics:
        values: List[float] = []
        for row in baseline_data:
            if key == "gray_zone_rate":
                values.append(
                    float(row.get("gray_zone_sent_count") or 0)
                    / max(float(row.get("total_candidates") or 1), 1.0)
                )
            elif key == "llm_review_rate":
                values.append(_rate(row, "llm_review_count"))
            elif key == "auto_match_rate":
                values.append(_rate(row, "auto_match_count"))
            elif key == "auto_reject_rate":
                values.append(_rate(row, "auto_reject_count"))
        baseline_metrics[key] = values

    anomalies: List[Dict[str, float | str]] = []
    for metric_name, current_value in metrics.items():
        z = _z_score(current_value, baseline_metrics[metric_name])
        if z is None or abs(z) < z_threshold:
            continue
        severity = "HIGH" if abs(z) >= 3.0 else "MEDIUM"
        anomalies.append(
            {
                "run_id": run_id,
                "entity_type": entity_type,
                "metric_name": metric_name,
                "current_value": current_value,
                "baseline_value": mean(baseline_metrics[metric_name]),
                "z_score": z,
                "severity": severity,
            }
        )

    if anomalies:
        with engine.begin() as conn:
            for anomaly in anomalies:
                conn.execute(
                    text(
                        """
                        INSERT INTO anomaly_events
                        (run_id, entity_type, metric_name, current_value, baseline_value, z_score, severity)
                        VALUES (:run_id, :entity_type, :metric_name, :current_value, :baseline_value, :z_score, :severity)
                        """
                    ),
                    anomaly,
                )

    return anomalies
