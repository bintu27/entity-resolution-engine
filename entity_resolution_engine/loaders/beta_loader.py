import pandas as pd

from entity_resolution_engine.db.connections import get_engine


def load_beta_data():
    engine = get_engine("SOURCE_BETA_DB_URL", "postgresql://postgres:pass@localhost:5434/source_beta_db")
    with engine.connect() as conn:
        players = pd.read_sql("SELECT * FROM players", conn)
        teams = pd.read_sql("SELECT * FROM teams", conn)
        competitions = pd.read_sql("SELECT * FROM competitions", conn)
        seasons = pd.read_sql("SELECT * FROM seasons", conn)
        matches = pd.read_sql("SELECT * FROM matches", conn)
    return {
        "players": players,
        "teams": teams,
        "competitions": competitions,
        "seasons": seasons,
        "matches": matches,
    }
