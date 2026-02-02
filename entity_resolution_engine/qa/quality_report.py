from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.engine import Engine


def build_quality_report(engine: Engine, run_id: str) -> Dict[str, Any]:
    with engine.connect() as conn:
        metrics = conn.execute(
            text("SELECT * FROM pipeline_run_metrics WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).mappings().all()
        anomalies = conn.execute(
            text("SELECT * FROM anomaly_events WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).mappings().all()
        review_counts = conn.execute(
            text(
                """
                SELECT entity_type, status, COUNT(*) AS count
                FROM llm_match_reviews
                WHERE run_id = :run_id
                GROUP BY entity_type, status
                """
            ),
            {"run_id": run_id},
        ).mappings().all()

    metrics_payload = [dict(row) for row in metrics]
    anomalies_payload = [dict(row) for row in anomalies]
    reviews_by_entity: Dict[str, Dict[str, int]] = {}
    for row in review_counts:
        entity = row["entity_type"]
        reviews_by_entity.setdefault(entity, {})[row["status"]] = row["count"]

    return {
        "run_id": run_id,
        "metrics": metrics_payload,
        "anomalies": anomalies_payload,
        "review_counts": reviews_by_entity,
    }
