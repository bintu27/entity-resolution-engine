import json
import os
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import text

from entity_resolution_engine.cli.run_mapping import main as run_mapping
from entity_resolution_engine.db.connections import get_engine
from entity_resolution_engine.monitoring.anomaly_detector import detect_anomalies
from entity_resolution_engine.monitoring.llm_triage import generate_triage_report
from entity_resolution_engine.qa.quality_report import build_quality_report
from entity_resolution_engine.validation.config import get_llm_validation_config

app = FastAPI(title="Unified Entity Store API")
ues_engine = get_engine(
    "UES_DB_URL", "postgresql://postgres:pass@localhost:5435/ues_db"
)
validation_config = get_llm_validation_config()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/mapping/run")
def trigger_mapping():
    run_mapping()
    return {"status": "mapping_complete"}


def _require_internal_key(
    x_internal_api_key: Optional[str] = Header(
        default=None, alias="X-Internal-API-Key"
    ),
):
    expected = os.getenv(validation_config.internal_api_key_env)
    if not expected:
        raise HTTPException(status_code=500, detail="Internal API key not configured")
    if x_internal_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


def _fetch_player(where_clause: str, params: Dict[str, Any]):
    query = text(f"SELECT * FROM ues_players WHERE {where_clause} LIMIT 1")
    with ues_engine.connect() as conn:
        result = conn.execute(query, params).mappings().first()
        if not result:
            return None
        return dict(result)


@app.get("/ues/player/{ues_id}")
def get_player(ues_id: str):
    player = _fetch_player("ues_player_id = :pid", {"pid": ues_id})
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


@app.get("/lookup/player/by-alpha/{alpha_id}")
def lookup_by_alpha(alpha_id: str):
    query = text(
        "SELECT ues_entity_id FROM source_lineage WHERE source_system='ALPHA' AND source_id=:sid AND ues_entity_type='player'"
    )
    with ues_engine.connect() as conn:
        result = conn.execute(query, {"sid": alpha_id}).scalar()
    if not result:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return get_player(result)


@app.get("/lookup/player/by-beta/{beta_id}")
def lookup_by_beta(beta_id: str):
    query = text(
        "SELECT ues_entity_id FROM source_lineage WHERE source_system='BETA' AND source_id=:sid AND ues_entity_type='player'"
    )
    with ues_engine.connect() as conn:
        result = conn.execute(query, {"sid": beta_id}).scalar()
    if not result:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return get_player(result)


@app.get("/ues/player/{ues_id}/lineage")
def get_player_lineage(ues_id: str):
    player = _fetch_player("ues_player_id = :pid", {"pid": ues_id})
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    lineage = player.get("lineage")
    if isinstance(lineage, str):
        try:
            lineage = json.loads(lineage)
        except json.JSONDecodeError:
            pass
    return {"lineage": lineage}


def _deserialize_json_fields(row: Dict[str, Any], fields: list[str]) -> Dict[str, Any]:
    for field in fields:
        value = row.get(field)
        if isinstance(value, str):
            try:
                row[field] = json.loads(value)
            except json.JSONDecodeError:
                continue
    return row


@app.get("/validation/reviews")
def list_reviews(
    status: Optional[str] = None,
    entity_type: Optional[str] = None,
    run_id: Optional[str] = None,
    min_score: Optional[float] = None,
    max_score: Optional[float] = None,
    limit: int = 50,
    offset: int = 0,
    _: bool = Depends(_require_internal_key),
):
    clauses = []
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if status:
        clauses.append("status = :status")
        params["status"] = status
    if entity_type:
        clauses.append("entity_type = :entity_type")
        params["entity_type"] = entity_type
    if run_id:
        clauses.append("run_id = :run_id")
        params["run_id"] = run_id
    if min_score is not None:
        clauses.append("matcher_score >= :min_score")
        params["min_score"] = min_score
    if max_score is not None:
        clauses.append("matcher_score <= :max_score")
        params["max_score"] = max_score
    where_clause = " AND ".join(clauses)
    if where_clause:
        where_clause = f"WHERE {where_clause}"
    query = text(
        f"""
        SELECT * FROM llm_match_reviews
        {where_clause}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
        """
    )
    with ues_engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    reviews = [
        _deserialize_json_fields(dict(row), ["signals", "reasons", "risk_flags"])
        for row in rows
    ]
    return {"reviews": reviews}


@app.get("/validation/reviews/{review_id}")
def get_review(review_id: int, _: bool = Depends(_require_internal_key)):
    query = text("SELECT * FROM llm_match_reviews WHERE id = :rid")
    with ues_engine.connect() as conn:
        row = conn.execute(query, {"rid": review_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Review not found")
    review = _deserialize_json_fields(dict(row), ["signals", "reasons", "risk_flags"])
    return review


def _update_review_status(review_id: int, status: str) -> Dict[str, Any]:
    query = text(
        """
        UPDATE llm_match_reviews
        SET status = :status, updated_at = CURRENT_TIMESTAMP
        WHERE id = :rid
        RETURNING *
        """
    )
    with ues_engine.begin() as conn:
        row = (
            conn.execute(query, {"status": status, "rid": review_id}).mappings().first()
        )
    if not row:
        raise HTTPException(status_code=404, detail="Review not found")
    return _deserialize_json_fields(dict(row), ["signals", "reasons", "risk_flags"])


@app.post("/validation/reviews/{review_id}/approve")
def approve_review(review_id: int, _: bool = Depends(_require_internal_key)):
    return _update_review_status(review_id, "APPROVED")


@app.post("/validation/reviews/{review_id}/reject")
def reject_review(review_id: int, _: bool = Depends(_require_internal_key)):
    return _update_review_status(review_id, "REJECTED")


@app.get("/monitoring/anomalies")
def list_anomalies(
    run_id: Optional[str] = None,
    entity_type: Optional[str] = None,
    _: bool = Depends(_require_internal_key),
):
    clauses = []
    params: Dict[str, Any] = {}
    if run_id:
        clauses.append("run_id = :run_id")
        params["run_id"] = run_id
    if entity_type:
        clauses.append("entity_type = :entity_type")
        params["entity_type"] = entity_type
    where_clause = " AND ".join(clauses)
    if where_clause:
        where_clause = f"WHERE {where_clause}"
    query = text(
        f"""
        SELECT * FROM anomaly_events
        {where_clause}
        ORDER BY created_at DESC
        """
    )
    with ues_engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    return {"anomalies": [dict(row) for row in rows]}


@app.post("/monitoring/triage")
def run_triage(
    run_id: str,
    entity_type: str,
    _: bool = Depends(_require_internal_key),
):
    detect_anomalies(ues_engine, run_id, entity_type)
    report = generate_triage_report(ues_engine, run_id, entity_type)
    return report


@app.get("/monitoring/report")
def get_report(run_id: str, _: bool = Depends(_require_internal_key)):
    return build_quality_report(ues_engine, run_id)
