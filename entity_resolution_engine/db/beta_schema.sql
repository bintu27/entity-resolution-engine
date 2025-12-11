CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    display_name TEXT NOT NULL,
    region TEXT,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS competitions (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    locale TEXT
);

CREATE TABLE IF NOT EXISTS seasons (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    competition_id INTEGER REFERENCES competitions(id)
);

CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    birth_year INTEGER,
    nationality TEXT,
    height_cm INTEGER,
    footedness TEXT,
    team_name TEXT,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS matches (
    id SERIAL PRIMARY KEY,
    home_team TEXT,
    away_team TEXT,
    season_id INTEGER REFERENCES seasons(id),
    competition_id INTEGER REFERENCES competitions(id),
    match_date DATE
);

CREATE TABLE IF NOT EXISTS lineups (
    id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES matches(id),
    player_name TEXT,
    team_name TEXT,
    position TEXT
);
