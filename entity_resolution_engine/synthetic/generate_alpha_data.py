import datetime as dt

import pandas as pd
from sqlalchemy import text

from entity_resolution_engine.db.connections import get_engine, init_db


def main():
    engine = get_engine("SOURCE_ALPHA_DB_URL", "postgresql://postgres:pass@localhost:5433/source_alpha_db")
    init_db(engine, "alpha_schema.sql")

    teams = pd.DataFrame(
        [
            {"team_id": 1, "name": "City FC", "country": "England"},
            {"team_id": 2, "name": "Beachside United", "country": "USA"},
            {"team_id": 3, "name": "Rio Wanderers", "country": "Brazil"},
        ]
    )
    competitions = pd.DataFrame(
        [
            {"competition_id": 1, "name": "Premier League", "country": "England"},
            {"competition_id": 2, "name": "Coastal Cup", "country": "USA"},
        ]
    )
    seasons = pd.DataFrame(
        [
            {"season_id": 1, "name": "2020/21", "competition_id": 1},
            {"season_id": 2, "name": "2021/22", "competition_id": 1},
            {"season_id": 3, "name": "2020", "competition_id": 2},
        ]
    )
    players = pd.DataFrame(
        [
            {
                "player_id": 1,
                "name": "John Doe",
                "dob": dt.date(1995, 4, 10),
                "nationality": "England",
                "height_cm": 182,
                "foot": "Right",
                "team_id": 1,
            },
            {
                "player_id": 2,
                "name": "Carlos Silva",
                "dob": dt.date(1998, 8, 23),
                "nationality": "Brasil",
                "height_cm": 176,
                "foot": "Left",
                "team_id": 3,
            },
            {
                "player_id": 3,
                "name": "Mike Stone",
                "dob": dt.date(1992, 1, 5),
                "nationality": "USA",
                "height_cm": None,
                "foot": "Right",
                "team_id": 2,
            },
            {
                "player_id": 4,
                "name": "Jordan Miles",
                "dob": dt.date(2000, 2, 14),
                "nationality": "England",
                "height_cm": 188,
                "foot": "Left",
                "team_id": 1,
            },
        ]
    )

    matches = pd.DataFrame(
        [
            {
                "match_id": 1,
                "home_team_id": 1,
                "away_team_id": 2,
                "season_id": 1,
                "competition_id": 1,
                "match_date": dt.date(2021, 3, 10),
            },
            {
                "match_id": 2,
                "home_team_id": 3,
                "away_team_id": 1,
                "season_id": 3,
                "competition_id": 2,
                "match_date": dt.date(2020, 7, 18),
            },
        ]
    )

    lineups = pd.DataFrame(
        [
            {"lineup_id": 1, "match_id": 1, "player_id": 1, "team_id": 1, "position": "FW"},
            {"lineup_id": 2, "match_id": 1, "player_id": 3, "team_id": 2, "position": "MF"},
            {"lineup_id": 3, "match_id": 2, "player_id": 2, "team_id": 3, "position": "FW"},
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

    print("Seeded SourceAlpha database with synthetic data")


if __name__ == "__main__":
    main()
