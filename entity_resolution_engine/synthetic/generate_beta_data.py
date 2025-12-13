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

BETA_TEAM_COUNT = 50
SHARED_TEAM_COUNT = 30
BETA_COMP_COUNT = 12
SHARED_COMP_COUNT = 9
SEASONS_PER_COMP = 3
BETA_PLAYER_COUNT = 2300
SHARED_PLAYER_COUNT = 1600
BETA_MATCH_COUNT = 900
LINEUP_PLAYERS_PER_TEAM = 6
CHUNKSIZE = 1000
RANDOM_SEED = 1337


def take_slice(pool: List[str], count: int, start: int = 0) -> List[str]:
    return pool[start : start + count]


def mutate_team_name(base_name: str, idx: int) -> str:
    if idx % 3 == 0:
        return f"{base_name} FC"
    if idx % 3 == 1:
        return f"{base_name} Club"
    return f"{base_name} SC"


def mutate_competition_name(base_name: str, idx: int) -> str:
    if idx % 2 == 0:
        return f"{base_name} Showcase"
    return f"{base_name} Presented"


def mutate_player_name(base_name: str, idx: int) -> str:
    parts = base_name.split()
    if len(parts) < 2:
        return base_name
    first, last = parts[0], parts[-1]
    if idx % 2 == 0 and len(first) > 3:
        first = first[:-1]
    if idx % 5 == 0:
        first = f"{first[0]}."
    if idx % 7 == 0:
        last = f"{last} Jr"
    return f"{first} {last}"


def build_teams() -> Tuple[pd.DataFrame, Dict[str, str]]:
    shared_bases = take_slice(TEAM_BASE_NAMES, SHARED_TEAM_COUNT)
    remaining_needed = BETA_TEAM_COUNT - SHARED_TEAM_COUNT
    unique_bases = take_slice(TEAM_BASE_NAMES, remaining_needed, start=SHARED_TEAM_COUNT)

    rows = []
    mapping: Dict[str, str] = {}
    region_cycle = cycle(COUNTRIES)
    team_id = 1

    for idx, base in enumerate(shared_bases, start=1):
        display = mutate_team_name(base, idx)
        mapping[base] = display
        rows.append({"id": team_id, "display_name": display, "region": next(region_cycle)})
        team_id += 1

    for idx, base in enumerate(unique_bases, start=len(shared_bases) + 1):
        display = mutate_team_name(base, idx)
        rows.append({"id": team_id, "display_name": display, "region": next(region_cycle)})
        team_id += 1

    return pd.DataFrame(rows), mapping


def build_competitions() -> pd.DataFrame:
    shared_bases = take_slice(COMP_BASE_NAMES, SHARED_COMP_COUNT)
    remaining_needed = BETA_COMP_COUNT - SHARED_COMP_COUNT
    unique_bases = take_slice(COMP_BASE_NAMES, remaining_needed, start=SHARED_COMP_COUNT)

    names = [mutate_competition_name(name, idx) for idx, name in enumerate(shared_bases, start=1)]
    names += [mutate_competition_name(name, idx + 100) for idx, name in enumerate(unique_bases, start=1)]
    locale_cycle = cycle(COUNTRIES)
    rows = []
    for idx, name in enumerate(names, start=1):
        rows.append({"id": idx, "title": name, "locale": next(locale_cycle)})
    return pd.DataFrame(rows)


def build_seasons(competitions: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, Dict[str, int]]]:
    rows = []
    meta: Dict[int, Dict[str, int]] = {}
    season_id = 1
    for comp in competitions.itertuples(index=False):
        base_year = 2017 + (comp.id % 4)
        for offset in range(SEASONS_PER_COMP):
            start_year = base_year + offset
            label = f"{start_year}-{str(start_year + 1)[-2:]}"
            rows.append({"id": season_id, "label": label, "competition_id": comp.id})
            meta[season_id] = {"competition_id": comp.id, "start_year": start_year}
            season_id += 1
    return pd.DataFrame(rows), meta


def build_players(teams: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[int]]]:
    team_cycle = cycle(teams["display_name"].tolist())
    nationality_cycle = cycle(NATIONALITIES)
    rows = []
    players_by_team: Dict[str, List[int]] = {name: [] for name in teams["display_name"]}

    shared_names = [mutate_player_name(name, idx) for idx, name in enumerate(PLAYER_NAME_POOL[:SHARED_PLAYER_COUNT], start=1)]
    remaining_needed = BETA_PLAYER_COUNT - SHARED_PLAYER_COUNT
    unique_names = PLAYER_NAME_POOL[SHARED_PLAYER_COUNT : SHARED_PLAYER_COUNT + remaining_needed]
    names = shared_names + unique_names

    for idx, full_name in enumerate(names, start=1):
        birth_year = random.randint(1985, 2003)
        height = random.randint(165, 195)
        footedness = random.choice(["Right", "Left", "Both"])
        team_name = next(team_cycle)
        rows.append(
            {
                "id": idx,
                "full_name": full_name,
                "birth_year": birth_year,
                "nationality": next(nationality_cycle),
                "height_cm": height,
                "footedness": footedness,
                "team_name": team_name,
                "is_active": True,
            }
        )
        players_by_team[team_name].append(idx)
    return pd.DataFrame(rows), players_by_team


def build_matches(season_meta: Dict[int, Dict[str, int]], team_names: List[str]) -> pd.DataFrame:
    season_ids = list(season_meta.keys())
    rows = []
    for match_id in range(1, BETA_MATCH_COUNT + 1):
        home_team, away_team = random.sample(team_names, 2)
        season_id = random.choice(season_ids)
        competition_id = season_meta[season_id]["competition_id"]
        start_year = season_meta[season_id]["start_year"]
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        match_date = dt.date(start_year, month, day)
        rows.append(
            {
                "id": match_id,
                "home_team": home_team,
                "away_team": away_team,
                "season_id": season_id,
                "competition_id": competition_id,
                "match_date": match_date,
            }
        )
    return pd.DataFrame(rows)


def build_lineups(matches: pd.DataFrame, players_by_team: Dict[str, List[int]], player_lookup: Dict[int, str]) -> pd.DataFrame:
    rows = []
    lineup_id = 1
    for match in matches.itertuples(index=False):
        for team_name in (match.home_team, match.away_team):
            team_players = players_by_team[team_name]
            count = min(LINEUP_PLAYERS_PER_TEAM, len(team_players))
            selected = random.sample(team_players, count)
            for player_id in selected:
                rows.append(
                    {
                        "id": lineup_id,
                        "match_id": match.id,
                        "player_name": player_lookup[player_id],
                        "team_name": team_name,
                        "position": random.choice(POSITIONS),
                    }
                )
                lineup_id += 1
    return pd.DataFrame(rows)


def write_table(conn, table: str, df: pd.DataFrame) -> None:
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=CHUNKSIZE, method="multi")


def main():
    random.seed(RANDOM_SEED)
    engine = get_engine("SOURCE_BETA_DB_URL", "postgresql://postgres:pass@localhost:5434/source_beta_db")
    init_db(engine, "beta_schema.sql")

    teams, _ = build_teams()
    competitions = build_competitions()
    seasons, season_meta = build_seasons(competitions)
    players, players_by_team = build_players(teams)
    player_lookup = players.set_index("id")["full_name"].to_dict()
    matches = build_matches(season_meta, teams["display_name"].tolist())
    lineups = build_lineups(matches, players_by_team, player_lookup)

    with engine.begin() as conn:
        for table in ["lineups", "matches", "players", "seasons", "competitions", "teams"]:
            conn.execute(text(f"DELETE FROM {table}"))

        write_table(conn, "teams", teams)
        write_table(conn, "competitions", competitions)
        write_table(conn, "seasons", seasons)
        write_table(conn, "players", players)
        write_table(conn, "matches", matches)
        write_table(conn, "lineups", lineups)

    print("Seeded SourceBeta database with expanded synthetic data")


if __name__ == "__main__":
    main()
