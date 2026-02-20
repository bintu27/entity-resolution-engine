import os
from entity_resolution_engine.loaders.alpha_loader import load_alpha_data
from entity_resolution_engine.loaders.beta_loader import load_beta_data
from entity_resolution_engine.matchers.teams_matcher import match_teams
from entity_resolution_engine.matchers.competitions_matcher import (
    build_competition_entities,
    match_competitions,
)
from entity_resolution_engine.matchers.seasons_matcher import (
    build_season_entities,
    match_seasons,
)
from entity_resolution_engine.matchers.players_matcher import match_players
from entity_resolution_engine.matchers.matches_matcher import match_matches
from entity_resolution_engine.merger.teams_merge import merge_teams
from entity_resolution_engine.merger.players_merge import merge_players
from entity_resolution_engine.merger.matches_merge import merge_matches
from datetime import datetime, timezone
from uuid import uuid4

from entity_resolution_engine.monitoring.anomaly_detector import detect_anomalies
from entity_resolution_engine.monitoring.llm_triage import generate_triage_report
from entity_resolution_engine.qa.quality_gates import (
    evaluate_quality_gates,
    get_quality_gate_config,
)
from entity_resolution_engine.ues_writer.writer import UESWriter
from entity_resolution_engine.validation.config import get_llm_validation_config
from entity_resolution_engine.validation.router import (
    route_competition_matches,
    route_match_matches,
    route_player_matches,
    route_season_matches,
    route_team_matches,
)


def main(run_id: str | None = None) -> str:
    run_id = run_id or str(uuid4())
    validation_config = get_llm_validation_config()
    auto_triage_during_mapping = (
        os.getenv("AUTO_TRIAGE_DURING_MAPPING", "false").lower() == "true"
    )
    quality_gate_config = get_quality_gate_config()
    alpha_data = load_alpha_data()
    beta_data = load_beta_data()
    writer = UESWriter()
    writer.reset()

    team_matches = match_teams(alpha_data["teams"], beta_data["teams"])
    team_start = datetime.now(timezone.utc)
    team_outcome = route_team_matches(
        team_matches,
        alpha_data["teams"],
        beta_data["teams"],
        run_id,
        config=validation_config,
    )
    team_outcome.metrics["started_at"] = team_start
    team_outcome.metrics["finished_at"] = datetime.now(timezone.utc)
    writer.write_llm_reviews(team_outcome.review_items)
    writer.write_run_metrics(team_outcome.metrics)
    detect_anomalies(writer.engine, run_id, "team")
    if auto_triage_during_mapping:
        generate_triage_report(writer.engine, run_id, "team")
    team_entities, alpha_team_to_ues, _ = merge_teams(
        team_outcome.approved_matches, alpha_data["teams"], beta_data["teams"]
    )
    writer.write_teams(team_entities)
    alpha_team_to_beta = {
        m["alpha_team_id"]: m["beta_team_id"] for m in team_outcome.approved_matches
    }

    comp_matches = match_competitions(
        alpha_data["competitions"], beta_data["competitions"]
    )
    comp_start = datetime.now(timezone.utc)
    comp_outcome = route_competition_matches(
        comp_matches,
        alpha_data["competitions"],
        beta_data["competitions"],
        run_id,
        config=validation_config,
    )
    comp_outcome.metrics["started_at"] = comp_start
    comp_outcome.metrics["finished_at"] = datetime.now(timezone.utc)
    writer.write_llm_reviews(comp_outcome.review_items)
    writer.write_run_metrics(comp_outcome.metrics)
    detect_anomalies(writer.engine, run_id, "competition")
    if auto_triage_during_mapping:
        generate_triage_report(writer.engine, run_id, "competition")
    comp_entities, alpha_comp_to_ues, beta_comp_to_ues = build_competition_entities(
        comp_outcome.approved_matches
    )
    writer.write_competitions(comp_entities)

    comp_map = {
        m["alpha_competition_id"]: m["beta_competition_id"]
        for m in comp_outcome.approved_matches
    }
    season_matches = match_seasons(
        alpha_data["seasons"], beta_data["seasons"], comp_map
    )
    season_start = datetime.now(timezone.utc)
    season_outcome = route_season_matches(
        season_matches,
        alpha_data["seasons"],
        beta_data["seasons"],
        run_id,
        config=validation_config,
    )
    season_outcome.metrics["started_at"] = season_start
    season_outcome.metrics["finished_at"] = datetime.now(timezone.utc)
    writer.write_llm_reviews(season_outcome.review_items)
    writer.write_run_metrics(season_outcome.metrics)
    detect_anomalies(writer.engine, run_id, "season")
    if auto_triage_during_mapping:
        generate_triage_report(writer.engine, run_id, "season")
    season_entities, alpha_season_to_ues, beta_season_to_ues = build_season_entities(
        season_outcome.approved_matches, alpha_comp_to_ues
    )
    writer.write_seasons(season_entities)

    player_matches = match_players(
        alpha_data["players"],
        beta_data["players"],
        alpha_team_to_beta,
        beta_data["teams"],
    )
    player_start = datetime.now(timezone.utc)
    player_outcome = route_player_matches(
        player_matches,
        alpha_data["players"],
        beta_data["players"],
        run_id,
        config=validation_config,
    )
    player_outcome.metrics["started_at"] = player_start
    player_outcome.metrics["finished_at"] = datetime.now(timezone.utc)
    writer.write_llm_reviews(player_outcome.review_items)
    writer.write_run_metrics(player_outcome.metrics)
    detect_anomalies(writer.engine, run_id, "player")
    if auto_triage_during_mapping:
        generate_triage_report(writer.engine, run_id, "player")
    player_entities, alpha_player_to_ues, beta_player_to_ues = merge_players(
        player_outcome.approved_matches,
        alpha_data["players"],
        beta_data["players"],
        alpha_team_to_ues,
    )
    writer.write_players(player_entities)

    competition_map_raw = {
        m["alpha_competition_id"]: m["beta_competition_id"]
        for m in comp_outcome.approved_matches
    }
    season_map_raw = {
        m["alpha_season_id"]: m["beta_season_id"]
        for m in season_outcome.approved_matches
    }
    match_matches_result = match_matches(
        alpha_data["matches"],
        beta_data["matches"],
        alpha_team_to_beta,
        competition_map_raw,
        season_map_raw,
    )
    match_start = datetime.now(timezone.utc)
    match_outcome = route_match_matches(
        match_matches_result,
        alpha_data["matches"],
        beta_data["matches"],
        run_id,
        config=validation_config,
    )
    match_outcome.metrics["started_at"] = match_start
    match_outcome.metrics["finished_at"] = datetime.now(timezone.utc)
    writer.write_llm_reviews(match_outcome.review_items)
    writer.write_run_metrics(match_outcome.metrics)
    detect_anomalies(writer.engine, run_id, "match")
    if auto_triage_during_mapping:
        generate_triage_report(writer.engine, run_id, "match")
    match_entities = merge_matches(
        match_outcome.approved_matches,
        alpha_data["matches"],
        beta_data["matches"],
        alpha_team_to_ues,
        alpha_comp_to_ues,
        alpha_season_to_ues,
    )
    writer.write_matches(match_entities)

    gate_result = evaluate_quality_gates(writer.engine, run_id, quality_gate_config)
    writer.write_quality_gate_result(gate_result)

    print("Mapping pipeline completed")
    return run_id


if __name__ == "__main__":
    run_id = main()
    print(f"Run ID: {run_id}")
