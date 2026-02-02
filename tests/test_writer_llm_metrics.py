from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from entity_resolution_engine.ues_writer.writer import UESWriter


def _setup_engine():
    engine = create_engine("sqlite:///:memory:")
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
                    created_at TIMESTAMP
                )
                """
            )
        )
    return engine


def test_writer_persists_reviews_and_metrics():
    engine = _setup_engine()
    writer = UESWriter(engine=engine)
    now = datetime.now(timezone.utc)

    writer.write_llm_reviews(
        [
            {
                "run_id": "run-1",
                "entity_type": "team",
                "left_source": "ALPHA",
                "left_id": "1",
                "right_source": "BETA",
                "right_id": "2",
                "matcher_score": 0.8,
                "signals": {"name_similarity": 0.8},
                "llm_decision": "REVIEW",
                "llm_confidence": 0.5,
                "reasons": ["low confidence"],
                "risk_flags": ["borderline"],
                "status": "PENDING",
                "created_at": now,
                "updated_at": now,
            }
        ]
    )
    writer.write_run_metrics(
        {
            "run_id": "run-1",
            "entity_type": "team",
            "started_at": now,
            "finished_at": now,
            "total_candidates": 3,
            "auto_match_count": 1,
            "auto_reject_count": 1,
            "gray_zone_sent_count": 1,
            "llm_match_count": 0,
            "llm_no_match_count": 0,
            "llm_review_count": 1,
        }
    )

    with engine.connect() as conn:
        review_count = conn.execute(text("SELECT COUNT(*) FROM llm_match_reviews")).scalar()
        metrics_count = conn.execute(
            text("SELECT COUNT(*) FROM pipeline_run_metrics")
        ).scalar()

    assert review_count == 1
    assert metrics_count == 1
