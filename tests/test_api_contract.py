import pytest

from entity_resolution_engine.api.main import app


@pytest.mark.contract
def test_openapi_contract_contains_expected_paths():
    schema = app.openapi()

    assert schema["info"]["title"] == "Unified Entity Store API"

    paths = schema["paths"]
    expected_paths = {
        "/health": {"get"},
        "/mapping/run": {"post"},
        "/ues/player/{ues_id}": {"get"},
        "/lookup/player/by-alpha/{alpha_id}": {"get"},
        "/lookup/player/by-beta/{beta_id}": {"get"},
        "/ues/player/{ues_id}/lineage": {"get"},
        "/monitoring/summary": {"get"},
        "/monitoring/gates": {"get"},
    }

    for path, methods in expected_paths.items():
        assert path in paths
        for method in methods:
            assert method in paths[path]
            assert "responses" in paths[path][method]
            assert "200" in paths[path][method]["responses"]


@pytest.mark.contract
def test_health_contract_response_shape():
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
