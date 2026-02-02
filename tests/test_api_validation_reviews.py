from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

import entity_resolution_engine.api.main as main


def _setup_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE llm_match_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    entity_type TEXT,
                    left_source TEXT,
                    left_id TEXT,
                    right_source TEXT,
                    right_id TEXT,
                    matcher_score REAL,
                    signals TEXT,
                    llm_decision TEXT,
                    llm_confidence REAL,
                    reasons TEXT,
                    risk_flags TEXT,
                    status TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO llm_match_reviews
                (run_id, entity_type, left_source, left_id, right_source, right_id, matcher_score, signals, llm_decision, llm_confidence, reasons, risk_flags, status, created_at, updated_at)
                VALUES
                ('run-1', 'team', 'ALPHA', '1', 'BETA', '2', 0.8, '{"name_similarity": 0.8}', 'REVIEW', 0.5, '["low"]', '["borderline"]', 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
            )
        )
    return engine


def test_internal_auth_and_review_updates(monkeypatch):
    engine = _setup_engine()
    monkeypatch.setattr(main, "ues_engine", engine)
    monkeypatch.setenv("INTERNAL_API_KEY", "secret")

    client = TestClient(main.app)

    unauthorized = client.get("/validation/reviews")
    assert unauthorized.status_code == 401

    headers = {"X-Internal-API-Key": "secret"}
    response = client.get("/validation/reviews", headers=headers)
    assert response.status_code == 200
    assert response.json()["reviews"][0]["status"] == "PENDING"

    approve = client.post("/validation/reviews/1/approve", headers=headers)
    assert approve.status_code == 200
    assert approve.json()["status"] == "APPROVED"

    reject = client.post("/validation/reviews/1/reject", headers=headers)
    assert reject.status_code == 200
    assert reject.json()["status"] == "REJECTED"
