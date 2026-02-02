from entity_resolution_engine.validation.config import (
    GrayZoneThreshold,
    LLMValidationConfig,
)
from entity_resolution_engine.validation.llm_client import LLMClient
from entity_resolution_engine.validation.llm_validator import validate_pair


def test_llm_client_retries_invalid_json(monkeypatch):
    client = LLMClient(
        provider="internal",
        model="test-model",
        api_key="test-key",
        api_url="http://example.com",
    )
    responses = [
        "not-json",
        '{"decision":"MATCH","confidence":0.9,"reasons":[],"risk_flags":[]}',
    ]
    calls = []

    def fake_send(system_prompt, user_prompt):
        calls.append(user_prompt)
        return responses.pop(0)

    monkeypatch.setattr(client, "_send_request", fake_send)
    result = client.request_json("sys", "user")

    assert result["decision"] == "MATCH"
    assert len(calls) == 2


def test_validate_pair_falls_back_on_llm_error(monkeypatch):
    config = LLMValidationConfig(
        enabled=True,
        gray_zone={"team": GrayZoneThreshold(low=0.7, high=0.9)},
        internal_api_key_env="INTERNAL_API_KEY",
        provider_env="TEST_PROVIDER",
        model_env="TEST_MODEL",
        api_key_env="TEST_KEY",
    )
    monkeypatch.setenv("TEST_PROVIDER", "internal")
    monkeypatch.setenv("TEST_MODEL", "test-model")
    monkeypatch.setenv("TEST_KEY", "test-key")

    class BrokenClient:
        def request_json(self, *_args, **_kwargs):
            raise ValueError("boom")

    result = validate_pair(
        "team",
        left={"id": "1", "name": "alpha"},
        right={"id": "2", "name": "beta"},
        matcher_score=0.8,
        signals={"conflict_flags": []},
        config=config,
        llm_client=BrokenClient(),
    )

    assert result.decision == "REVIEW"
