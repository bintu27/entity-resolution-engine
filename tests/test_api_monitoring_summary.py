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
                CREATE TABLE pipeline_run_metrics (
                    run_id TEXT,
                    entity_type TEXT,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    total_candidates INTEGER,
                    auto_match_count INTEGER,
                    auto_reject_count INTEGER,
                    gray_zone_sent_count INTEGER,
                    llm_match_count INTEGER,
                    llm_no_match_count INTEGER,
                    llm_review_count INTEGER,
                    llm_call_count INTEGER,
                    llm_error_count INTEGER,
                    llm_invalid_json_retry_count INTEGER,
                    llm_avg_latency_ms REAL,
                    llm_fallback_mode TEXT,
                    llm_disabled_reason TEXT,
                    created_at TIMESTAMP
                )
                """
            )
        )
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
                INSERT INTO pipeline_run_metrics
                (run_id, entity_type, total_candidates, auto_match_count, auto_reject_count,
                 gray_zone_sent_count, llm_match_count, llm_no_match_count, llm_review_count,
                 llm_call_count, llm_error_count, llm_invalid_json_retry_count, llm_avg_latency_ms)
                VALUES
                ('run-1', 'team', 10, 6, 2, 2, 1, 0, 1, 2, 1, 1, 5.0),
                ('run-1', 'player', 5, 3, 1, 1, 1, 0, 0, 1, 0, 0, 7.0)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO llm_match_reviews
                (run_id, entity_type, left_source, left_id, right_source, right_id,
                 matcher_score, signals, llm_decision, llm_confidence, reasons, risk_flags, status,
                 created_at, updated_at)
                VALUES
                ('run-1', 'team', 'ALPHA', '1', 'BETA', '2', 0.8, '{}', 'REVIEW', 0.4, '[]', '[]', 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
                ('run-1', 'team', 'ALPHA', '2', 'BETA', '3', 0.8, '{}', 'REVIEW', 0.4, '[]', '[]', 'APPROVED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
            )
        )
    return engine


def test_monitoring_summary_aggregates_metrics(monkeypatch):
    engine = _setup_engine()
    monkeypatch.setattr(main, "ues_engine", engine)
    monkeypatch.setenv("INTERNAL_API_KEY", "secret")

    client = TestClient(main.app)
    headers = {"X-Internal-API-Key": "secret"}

    response = client.get("/monitoring/summary?run_id=run-1", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["totals"]["total_candidates"] == 15
    assert payload["totals"]["llm_call_count"] == 3
    assert payload["llm_health"]["llm_error_count"] == 1
    assert payload["review_counts"]["PENDING"] == 1
    assert payload["review_counts"]["APPROVED"] == 1
