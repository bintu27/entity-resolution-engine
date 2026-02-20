from fastapi.testclient import TestClient

import entity_resolution_engine.api.main as main


def test_mapping_run_wait_returns_run_id(monkeypatch):
    main._mapping_runs.clear()
    monkeypatch.setattr(main, "run_mapping", lambda run_id=None: run_id or "run-123")
    client = TestClient(main.app)

    response = client.post("/mapping/run?wait=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "mapping_complete"
    assert isinstance(payload["run_id"], str)


def test_mapping_run_starts_background_job(monkeypatch):
    main._mapping_runs.clear()

    class FakeThread:
        def __init__(self, target, args, daemon):
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self):
            return None

    monkeypatch.setattr(main, "Thread", FakeThread)
    client = TestClient(main.app)

    response = client.post("/mapping/run")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "mapping_started"
    assert isinstance(payload["run_id"], str)


def test_mapping_run_rejects_when_already_running():
    main._mapping_runs.clear()
    main._mapping_runs["run-active"] = {
        "run_id": "run-active",
        "status": "running",
        "started_at": "2026-02-20T00:00:00+00:00",
        "finished_at": None,
        "error": None,
    }
    client = TestClient(main.app)

    response = client.post("/mapping/run")

    assert response.status_code == 409
    assert "already in progress" in response.json()["detail"]
