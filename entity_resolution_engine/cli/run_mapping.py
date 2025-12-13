from entity_resolution_engine.loaders.alpha_loader import load_alpha_data
from entity_resolution_engine.loaders.beta_loader import load_beta_data
from entity_resolution_engine.matchers.teams_matcher import match_teams
from entity_resolution_engine.matchers.competitions_matcher import (
    build_competition_entities,
    match_competitions,
)
from entity_resolution_engine.matchers.seasons_matcher import build_season_entities, match_seasons
from entity_resolution_engine.matchers.players_matcher import match_players
from entity_resolution_engine.matchers.matches_matcher import match_matches
from entity_resolution_engine.merger.teams_merge import merge_teams
from entity_resolution_engine.merger.players_merge import merge_players
from entity_resolution_engine.merger.matches_merge import merge_matches
from entity_resolution_engine.ues_writer.writer import UESWriter


def main():
    alpha_data = load_alpha_data()
    beta_data = load_beta_data()
    writer = UESWriter()
    writer.reset()

    team_matches = match_teams(alpha_data["teams"], beta_data["teams"])
    team_entities, alpha_team_to_ues, _ = merge_teams(
        team_matches, alpha_data["teams"], beta_data["teams"]
    )
    writer.write_teams(team_entities)
    alpha_team_to_beta = {m["alpha_team_id"]: m["beta_team_id"] for m in team_matches}

    comp_matches = match_competitions(alpha_data["competitions"], beta_data["competitions"])
    comp_entities, alpha_comp_to_ues, beta_comp_to_ues = build_competition_entities(comp_matches)
    writer.write_competitions(comp_entities)

    comp_map = {m["alpha_competition_id"]: m["beta_competition_id"] for m in comp_matches}
    season_matches = match_seasons(alpha_data["seasons"], beta_data["seasons"], comp_map)
    season_entities, alpha_season_to_ues, beta_season_to_ues = build_season_entities(
        season_matches, alpha_comp_to_ues
    )
    writer.write_seasons(season_entities)

    player_matches = match_players(
        alpha_data["players"], beta_data["players"], alpha_team_to_beta, beta_data["teams"]
    )
    player_entities, alpha_player_to_ues, beta_player_to_ues = merge_players(
        player_matches,
        alpha_data["players"],
        beta_data["players"],
        alpha_team_to_ues,
    )
    writer.write_players(player_entities)

    competition_map_raw = {m["alpha_competition_id"]: m["beta_competition_id"] for m in comp_matches}
    season_map_raw = {m["alpha_season_id"]: m["beta_season_id"] for m in season_matches}
    match_matches_result = match_matches(
        alpha_data["matches"],
        beta_data["matches"],
        alpha_team_to_beta,
        competition_map_raw,
        season_map_raw,
    )
    match_entities = merge_matches(
        match_matches_result,
        alpha_data["matches"],
        beta_data["matches"],
        alpha_team_to_ues,
        alpha_comp_to_ues,
        alpha_season_to_ues,
    )
    writer.write_matches(match_entities)

    print("Mapping pipeline completed")


if __name__ == "__main__":
    main()
