import datetime as dt

import pandas as pd
from sqlalchemy import text

from entity_resolution_engine.db.connections import get_engine, init_db


def main():
    engine = get_engine("SOURCE_BETA_DB_URL", "postgresql://postgres:pass@localhost:5434/source_beta_db")
    init_db(engine, "beta_schema.sql")

    teams = pd.DataFrame(
        [
            {"id": 1, "display_name": "City Football Club", "region": "England"},
            {"id": 2, "display_name": "Beachside Utd", "region": "USA"},
            {"id": 3, "display_name": "Rio Wanderers", "region": "Brasil"},
        ]
    )

    competitions = pd.DataFrame(
        [
            {"id": 1, "title": "Premier League presented by SportsCorp", "locale": "England"},
            {"id": 2, "title": "Coastal Championship", "locale": "USA"},
        ]
    )

    seasons = pd.DataFrame(
        [
            {"id": 1, "label": "20-21", "competition_id": 1},
            {"id": 2, "label": "2021-2022", "competition_id": 1},
            {"id": 3, "label": "2020", "competition_id": 2},
        ]
    )

    players = pd.DataFrame(
        [
            {
                "id": 10,
                "full_name": "Jon Doe",
                "birth_year": 1995,
                "nationality": "EN",
                "height_cm": 181,
                "footedness": "R",
                "team_name": "City Football Club",
            },
            {
                "id": 11,
                "full_name": "Carlos Silva",
                "birth_year": 1998,
                "nationality": "Brazil",
                "height_cm": 177,
                "footedness": "Left-footed",
                "team_name": "Rio Wanderers",
            },
            {
                "id": 12,
                "full_name": "Michael Stone",
                "birth_year": 1992,
                "nationality": "United States",
                "height_cm": 180,
                "footedness": "Right",
                "team_name": "Beachside Utd",
            },
            {
                "id": 13,
                "full_name": "J Miles",
                "birth_year": 2000,
                "nationality": "England",
                "height_cm": 186,
                "footedness": "L",
                "team_name": "City Football Club",
            },
        ]
    )

    matches = pd.DataFrame(
        [
            {
                "id": 1,
                "home_team": "City Football Club",
                "away_team": "Beachside Utd",
                "season_id": 1,
                "competition_id": 1,
                "match_date": dt.date(2021, 3, 11),
            },
            {
                "id": 2,
                "home_team": "Rio Wanderers",
                "away_team": "City Football Club",
                "season_id": 3,
                "competition_id": 2,
                "match_date": dt.date(2020, 7, 17),
            },
        ]
    )

    lineups = pd.DataFrame(
        [
            {"id": 1, "match_id": 1, "player_name": "Jon Doe", "team_name": "City Football Club", "position": "ST"},
            {"id": 2, "match_id": 1, "player_name": "Michael Stone", "team_name": "Beachside Utd", "position": "CM"},
            {"id": 3, "match_id": 2, "player_name": "Carlos Silva", "team_name": "Rio Wanderers", "position": "ST"},
        ]
    )

    with engine.begin() as conn:
        for table in ["lineups", "matches", "players", "seasons", "competitions", "teams"]:
            conn.execute(text(f"DELETE FROM {table}"))

        teams.to_sql("teams", conn, if_exists="append", index=False)
        competitions.to_sql("competitions", conn, if_exists="append", index=False)
        seasons.to_sql("seasons", conn, if_exists="append", index=False)
        players.to_sql("players", conn, if_exists="append", index=False)
        matches.to_sql("matches", conn, if_exists="append", index=False)
        lineups.to_sql("lineups", conn, if_exists="append", index=False)

    print("Seeded SourceBeta database with synthetic data")


if __name__ == "__main__":
    main()
