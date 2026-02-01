import datetime as dt
import random
from itertools import cycle
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import text

from entity_resolution_engine.db.connections import get_engine, init_db
from entity_resolution_engine.synthetic.base_entities import (
    COMP_BASE_NAMES,
    COUNTRIES,
    NATIONALITIES,
    PLAYER_NAME_POOL,
    POSITIONS,
    TEAM_BASE_NAMES,
)

ALPHA_TEAM_COUNT = 40
ALPHA_COMP_COUNT = 12
SEASONS_PER_COMP = 3
ALPHA_PLAYER_COUNT = 2000
ALPHA_MATCH_COUNT = 800
LINEUP_PLAYERS_PER_TEAM = 6
CHUNKSIZE = 1000
RANDOM_SEED = 42


def take_slice(pool: List[str], count: int, start: int = 0) -> List[str]:
    return pool[start : start + count]


def build_teams() -> pd.DataFrame:
    names = take_slice(TEAM_BASE_NAMES, ALPHA_TEAM_COUNT)
    country_cycle = cycle(COUNTRIES)
    rows = []
    for idx, name in enumerate(names, start=1):
        rows.append({"team_id": idx, "name": name, "country": next(country_cycle)})
    return pd.DataFrame(rows)


def build_competitions() -> pd.DataFrame:
    names = take_slice(COMP_BASE_NAMES, ALPHA_COMP_COUNT)
    country_cycle = cycle(COUNTRIES)
    rows = []
    for idx, name in enumerate(names, start=1):
        rows.append(
            {"competition_id": idx, "name": name, "country": next(country_cycle)}
        )
    return pd.DataFrame(rows)


def build_seasons(
    competitions: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[int, Dict[str, int]]]:
    rows = []
    meta: Dict[int, Dict[str, int]] = {}
    season_id = 1
    for comp in competitions.itertuples(index=False):
        base_year = 2017 + (comp.competition_id % 5)
        for offset in range(SEASONS_PER_COMP):
            start_year = base_year + offset
            name = f"{start_year}/{str(start_year + 1)[-2:]}"
            rows.append(
                {
                    "season_id": season_id,
                    "name": name,
                    "competition_id": comp.competition_id,
                }
            )
            meta[season_id] = {
                "competition_id": comp.competition_id,
                "start_year": start_year,
            }
            season_id += 1
    return pd.DataFrame(rows), meta


def build_players(teams: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, List[int]]]:
    team_cycle = cycle(teams["team_id"].tolist())
    nationality_cycle = cycle(NATIONALITIES)
    rows = []
    players_by_team: Dict[int, List[int]] = {
        team_id: [] for team_id in teams["team_id"]
    }
    for idx, full_name in enumerate(PLAYER_NAME_POOL[:ALPHA_PLAYER_COUNT], start=1):
        year = random.randint(1985, 2003)
        dob = dt.date(year, random.randint(1, 12), random.randint(1, 28))
        height = random.randint(165, 195)
        foot = random.choice(["Right", "Left"])
        team_id = next(team_cycle)
        rows.append(
            {
                "player_id": idx,
                "name": full_name,
                "dob": dob,
                "nationality": next(nationality_cycle),
                "height_cm": height,
                "foot": foot,
                "team_id": team_id,
            }
        )
        players_by_team[team_id].append(idx)
    return pd.DataFrame(rows), players_by_team


def build_matches(
    season_meta: Dict[int, Dict[str, int]], team_ids: List[int]
) -> pd.DataFrame:
    season_ids = list(season_meta.keys())
    rows = []
    for match_id in range(1, ALPHA_MATCH_COUNT + 1):
        home_team_id, away_team_id = random.sample(team_ids, 2)
        season_id = random.choice(season_ids)
        competition_id = season_meta[season_id]["competition_id"]
        start_year = season_meta[season_id]["start_year"]
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        match_date = dt.date(start_year, month, day)
        rows.append(
            {
                "match_id": match_id,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "season_id": season_id,
                "competition_id": competition_id,
                "match_date": match_date,
            }
        )
    return pd.DataFrame(rows)


def build_lineups(
    matches: pd.DataFrame, players_by_team: Dict[int, List[int]]
) -> pd.DataFrame:
    rows = []
    lineup_id = 1
    for match in matches.itertuples(index=False):
        for team_id in (match.home_team_id, match.away_team_id):
            team_players = players_by_team[team_id]
            count = min(LINEUP_PLAYERS_PER_TEAM, len(team_players))
            selected = random.sample(team_players, count)
            for player_id in selected:
                rows.append(
                    {
                        "lineup_id": lineup_id,
                        "match_id": match.match_id,
                        "player_id": player_id,
                        "team_id": team_id,
                        "position": random.choice(POSITIONS),
                    }
                )
                lineup_id += 1
    return pd.DataFrame(rows)


def write_table(conn, table: str, df: pd.DataFrame) -> None:
    df.to_sql(
        table,
        conn,
        if_exists="append",
        index=False,
        chunksize=CHUNKSIZE,
        method="multi",
    )


def main():
    random.seed(RANDOM_SEED)
    engine = get_engine(
        "SOURCE_ALPHA_DB_URL",
        "postgresql://postgres:pass@localhost:5433/source_alpha_db",
    )
    init_db(engine, "alpha_schema.sql")

    teams = build_teams()
    competitions = build_competitions()
    seasons, season_meta = build_seasons(competitions)
    players, players_by_team = build_players(teams)
    matches = build_matches(season_meta, teams["team_id"].tolist())
    lineups = build_lineups(matches, players_by_team)

    with engine.begin() as conn:
        for table in [
            "lineups",
            "matches",
            "players",
            "seasons",
            "competitions",
            "teams",
        ]:
            conn.execute(text(f"DELETE FROM {table}"))

        write_table(conn, "teams", teams)
        write_table(conn, "competitions", competitions)
        write_table(conn, "seasons", seasons)
        write_table(conn, "players", players)
        write_table(conn, "matches", matches)
        write_table(conn, "lineups", lineups)

    print("Seeded SourceAlpha database with expanded synthetic data")


if __name__ == "__main__":
    main()
