from fastapi.testclient import TestClient

import entity_resolution_engine.api.main as main


def test_mapping_run_returns_run_id(monkeypatch):
    monkeypatch.setattr(main, "run_mapping", lambda: "run-123")
    client = TestClient(main.app)

    response = client.post("/mapping/run")

    assert response.status_code == 200
    assert response.json() == {"status": "mapping_complete", "run_id": "run-123"}
