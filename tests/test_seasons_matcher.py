from entity_resolution_engine.matchers.seasons_matcher import build_season_entities


def test_build_season_entities_uses_alpha_competition_ids_for_lookup():
    season_matches = [
        {
            "alpha_season_id": 1,
            "beta_season_id": 10,
            "confidence": 0.9,
            "start_year": 2024,
            "end_year": 2025,
            "alpha_competition_id": 100,
            "beta_competition_id": 200,
        }
    ]

    entities, alpha_map, beta_map = build_season_entities(
        season_matches, competition_ues_map={100: "COMP-UES-100"}
    )

    assert entities[0]["competition_ues_id"] == "COMP-UES-100"
    assert alpha_map[1] == entities[0]["ues_season_id"]
    assert beta_map[10] == entities[0]["ues_season_id"]


def test_build_season_entities_falls_back_to_beta_competition_lookup():
    season_matches = [
        {
            "alpha_season_id": 2,
            "beta_season_id": 20,
            "confidence": 0.9,
            "start_year": 2022,
            "end_year": 2023,
            "alpha_competition_id": 101,
            "beta_competition_id": 201,
        }
    ]

    entities, _, _ = build_season_entities(
        season_matches, competition_ues_map={201: "COMP-UES-201"}
    )

    assert entities[0]["competition_ues_id"] == "COMP-UES-201"
