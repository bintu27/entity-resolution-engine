import hashlib
from typing import Dict, List

import pandas as pd

from entity_resolution_engine.db.connections import get_engine, init_db

DEFAULT_UES_URL = "postgresql://postgres:pass@localhost:5435/ues_db"


def generate_ues_id(prefix: str, alpha_id, beta_id) -> str:
    hash_input = f"{prefix}-{alpha_id}-{beta_id}".encode()
    digest = hashlib.md5(hash_input).hexdigest()[:8]
    return f"{prefix}-{digest}"


class UESWriter:
    def __init__(self):
        self.engine = get_engine("UES_DB_URL", DEFAULT_UES_URL)
        init_db(self.engine, "ues_schema.sql")

    def reset(self) -> None:
        with self.engine.begin() as conn:
            conn.execute("DELETE FROM source_lineage")
            conn.execute("DELETE FROM ues_matches")
            conn.execute("DELETE FROM ues_players")
            conn.execute("DELETE FROM ues_seasons")
            conn.execute("DELETE FROM ues_competitions")
            conn.execute("DELETE FROM ues_teams")

    def _write_source_lineage(self, entries: List[Dict]) -> None:
        if not entries:
            return
        df = pd.DataFrame(entries)
        df.to_sql("source_lineage", self.engine, if_exists="append", index=False)

    def write_teams(self, teams: List[Dict]) -> None:
        if not teams:
            return
        df = pd.DataFrame(teams)
        df.to_sql("ues_teams", self.engine, if_exists="append", index=False)
        lineage_entries = []
        for team in teams:
            for src in team["lineage"]["sources"]:
                lineage_entries.append(
                    {
                        "source_system": src["source"],
                        "source_id": src["id"],
                        "ues_entity_type": "team",
                        "ues_entity_id": team["ues_team_id"],
                    }
                )
        self._write_source_lineage(lineage_entries)

    def write_competitions(self, competitions: List[Dict]) -> None:
        if not competitions:
            return
        df = pd.DataFrame(competitions)
        df.to_sql("ues_competitions", self.engine, if_exists="append", index=False)
        lineage_entries = []
        for comp in competitions:
            for src in comp["lineage"]["sources"]:
                lineage_entries.append(
                    {
                        "source_system": src["source"],
                        "source_id": src["id"],
                        "ues_entity_type": "competition",
                        "ues_entity_id": comp["ues_competition_id"],
                    }
                )
        self._write_source_lineage(lineage_entries)

    def write_seasons(self, seasons: List[Dict]) -> None:
        if not seasons:
            return
        df = pd.DataFrame(seasons)
        df.to_sql("ues_seasons", self.engine, if_exists="append", index=False)
        lineage_entries = []
        for season in seasons:
            for src in season["lineage"]["sources"]:
                lineage_entries.append(
                    {
                        "source_system": src["source"],
                        "source_id": src["id"],
                        "ues_entity_type": "season",
                        "ues_entity_id": season["ues_season_id"],
                    }
                )
        self._write_source_lineage(lineage_entries)

    def write_players(self, players: List[Dict]) -> None:
        if not players:
            return
        df = pd.DataFrame(players)
        df.to_sql("ues_players", self.engine, if_exists="append", index=False)
        lineage_entries = []
        for player in players:
            for src in player["lineage"]["sources"]:
                lineage_entries.append(
                    {
                        "source_system": src["source"],
                        "source_id": src["id"],
                        "ues_entity_type": "player",
                        "ues_entity_id": player["ues_player_id"],
                    }
                )
        self._write_source_lineage(lineage_entries)

    def write_matches(self, matches: List[Dict]) -> None:
        if not matches:
            return
        df = pd.DataFrame(matches)
        df.to_sql("ues_matches", self.engine, if_exists="append", index=False)
        lineage_entries = []
        for match in matches:
            for src in match["lineage"]["sources"]:
                lineage_entries.append(
                    {
                        "source_system": src["source"],
                        "source_id": src["id"],
                        "ues_entity_type": "match",
                        "ues_entity_id": match["ues_match_id"],
                    }
                )
        self._write_source_lineage(lineage_entries)
