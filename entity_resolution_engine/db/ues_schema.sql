CREATE TABLE IF NOT EXISTS ues_teams (
    ues_team_id TEXT PRIMARY KEY,
    name TEXT,
    country TEXT,
    merge_confidence NUMERIC,
    lineage JSONB
);

CREATE TABLE IF NOT EXISTS ues_competitions (
    ues_competition_id TEXT PRIMARY KEY,
    name TEXT,
    country TEXT,
    merge_confidence NUMERIC,
    lineage JSONB
);

CREATE TABLE IF NOT EXISTS ues_seasons (
    ues_season_id TEXT PRIMARY KEY,
    start_year INTEGER,
    end_year INTEGER,
    competition_ues_id TEXT REFERENCES ues_competitions(ues_competition_id),
    merge_confidence NUMERIC,
    lineage JSONB
);

CREATE TABLE IF NOT EXISTS ues_players (
    ues_player_id TEXT PRIMARY KEY,
    canonical_name TEXT,
    dob DATE,
    birth_year INTEGER,
    nationality TEXT,
    height_cm INTEGER,
    foot TEXT,
    team_ues_id TEXT REFERENCES ues_teams(ues_team_id),
    merge_confidence NUMERIC,
    lineage JSONB
);

CREATE TABLE IF NOT EXISTS ues_matches (
    ues_match_id TEXT PRIMARY KEY,
    home_team_ues_id TEXT REFERENCES ues_teams(ues_team_id),
    away_team_ues_id TEXT REFERENCES ues_teams(ues_team_id),
    season_ues_id TEXT REFERENCES ues_seasons(ues_season_id),
    competition_ues_id TEXT REFERENCES ues_competitions(ues_competition_id),
    match_date DATE,
    merge_confidence NUMERIC,
    lineage JSONB
);

CREATE TABLE IF NOT EXISTS source_lineage (
    source_system TEXT,
    source_id TEXT,
    ues_entity_type TEXT,
    ues_entity_id TEXT
);

CREATE TABLE IF NOT EXISTS llm_match_reviews (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    entity_type TEXT,
    left_source TEXT,
    left_id TEXT,
    right_source TEXT,
    right_id TEXT,
    matcher_score NUMERIC,
    signals JSONB,
    llm_decision TEXT,
    llm_confidence NUMERIC,
    reasons JSONB,
    risk_flags JSONB,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_run_metrics (
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
    llm_avg_latency_ms NUMERIC,
    llm_fallback_mode TEXT,
    llm_disabled_reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomaly_events (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    entity_type TEXT,
    metric_name TEXT,
    current_value NUMERIC,
    baseline_value NUMERIC,
    z_score NUMERIC,
    severity TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomaly_triage_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    entity_type TEXT,
    report JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quality_gate_results (
    run_id TEXT PRIMARY KEY,
    status TEXT,
    failed_gates JSONB,
    gate_values JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
