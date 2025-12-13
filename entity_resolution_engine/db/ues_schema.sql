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
