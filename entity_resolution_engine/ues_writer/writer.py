import hashlib
from typing import Dict, List

import pandas as pd
from sqlalchemy import JSON, text
from sqlalchemy.dialects.postgresql import JSONB

from entity_resolution_engine.db.connections import get_engine, init_db

DEFAULT_UES_URL = "postgresql://postgres:pass@localhost:5435/ues_db"


def generate_ues_id(prefix: str, alpha_id, beta_id) -> str:
    hash_input = f"{prefix}-{alpha_id}-{beta_id}".encode()
    digest = hashlib.md5(hash_input).hexdigest()[:8]
    return f"{prefix}-{digest}"


class UESWriter:
    def __init__(self, engine=None):
        self.engine = engine or get_engine("UES_DB_URL", DEFAULT_UES_URL)
        if engine is None:
            init_db(self.engine, "ues_schema.sql")

    def reset(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM anomaly_triage_reports"))
            conn.execute(text("DELETE FROM anomaly_events"))
            conn.execute(text("DELETE FROM pipeline_run_metrics"))
            conn.execute(text("DELETE FROM llm_match_reviews"))
            conn.execute(text("DELETE FROM source_lineage"))
            conn.execute(text("DELETE FROM ues_matches"))
            conn.execute(text("DELETE FROM ues_players"))
            conn.execute(text("DELETE FROM ues_seasons"))
            conn.execute(text("DELETE FROM ues_competitions"))
            conn.execute(text("DELETE FROM ues_teams"))

    def _write_source_lineage(self, entries: List[Dict]) -> None:
        if not entries:
            return
        df = pd.DataFrame(entries)
        df.to_sql("source_lineage", self.engine, if_exists="append", index=False)

    def write_teams(self, teams: List[Dict]) -> None:
        if not teams:
            return
        df = pd.DataFrame(teams)
        df.to_sql(
            "ues_teams",
            self.engine,
            if_exists="append",
            index=False,
            dtype={"lineage": JSONB},
        )
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
        df.to_sql(
            "ues_competitions",
            self.engine,
            if_exists="append",
            index=False,
            dtype={"lineage": JSONB},
        )
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
        df.to_sql(
            "ues_seasons",
            self.engine,
            if_exists="append",
            index=False,
            dtype={"lineage": JSONB},
        )
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
        df.to_sql(
            "ues_players",
            self.engine,
            if_exists="append",
            index=False,
            dtype={"lineage": JSONB},
        )
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
        df.to_sql(
            "ues_matches",
            self.engine,
            if_exists="append",
            index=False,
            dtype={"lineage": JSONB},
        )
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

    def write_llm_reviews(self, reviews: List[Dict]) -> None:
        if not reviews:
            return
        df = pd.DataFrame(reviews)
        df.to_sql(
            "llm_match_reviews",
            self.engine,
            if_exists="append",
            index=False,
            dtype={
                "signals": JSON,
                "reasons": JSON,
                "risk_flags": JSON,
            },
        )

    def write_run_metrics(self, metrics: List[Dict] | Dict) -> None:
        if not metrics:
            return
        payload = metrics if isinstance(metrics, list) else [metrics]
        df = pd.DataFrame(payload)
        df.to_sql("pipeline_run_metrics", self.engine, if_exists="append", index=False)

    def write_anomaly_events(self, events: List[Dict]) -> None:
        if not events:
            return
        df = pd.DataFrame(events)
        df.to_sql("anomaly_events", self.engine, if_exists="append", index=False)

    def write_anomaly_report(self, report: Dict) -> None:
        if not report:
            return
        df = pd.DataFrame([report])
        df.to_sql(
            "anomaly_triage_reports",
            self.engine,
            if_exists="append",
            index=False,
            dtype={"report": JSON},
        )
