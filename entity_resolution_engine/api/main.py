import json
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from entity_resolution_engine.cli.run_mapping import main as run_mapping
from entity_resolution_engine.db.connections import get_engine

app = FastAPI(title="Unified Entity Store API")
ues_engine = get_engine(
    "UES_DB_URL", "postgresql://postgres:pass@localhost:5435/ues_db"
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/mapping/run")
def trigger_mapping():
    run_mapping()
    return {"status": "mapping_complete"}


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
