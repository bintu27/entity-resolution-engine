CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS competitions (
    competition_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT
);

CREATE TABLE IF NOT EXISTS seasons (
    season_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    competition_id INTEGER REFERENCES competitions(competition_id)
);

CREATE TABLE IF NOT EXISTS players (
    player_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    dob DATE,
    nationality TEXT,
    height_cm INTEGER,
    foot TEXT,
    team_id INTEGER REFERENCES teams(team_id),
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS matches (
    match_id SERIAL PRIMARY KEY,
    home_team_id INTEGER REFERENCES teams(team_id),
    away_team_id INTEGER REFERENCES teams(team_id),
    season_id INTEGER REFERENCES seasons(season_id),
    competition_id INTEGER REFERENCES competitions(competition_id),
    match_date DATE
);

CREATE TABLE IF NOT EXISTS lineups (
    lineup_id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES matches(match_id),
    player_id INTEGER REFERENCES players(player_id),
    team_id INTEGER REFERENCES teams(team_id),
    position TEXT
);
