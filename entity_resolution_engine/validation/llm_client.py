from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import httpx


class LLMClient:
    def __init__(
        self,
        provider: str,
        model: str,
        api_key: str,
        api_url: Optional[str] = None,
        timeout_s: float = 12.0,
    ) -> None:
        self.provider = provider
        self.model = model
        self.api_key = api_key
        api_url = api_url or self._default_api_url(provider)
        if not api_url:
            raise ValueError("LLM API URL is required")
        self.api_url: str = api_url
        self.timeout_s = timeout_s

    @staticmethod
    def _default_api_url(provider: str) -> Optional[str]:
        if provider == "openai":
            return "https://api.openai.com/v1/chat/completions"
        if provider == "internal":
            return os.getenv("LLM_API_URL")
        return os.getenv("LLM_API_URL")

    def request_json(
        self, system_prompt: str, user_prompt: str, retry_on_invalid_json: bool = True
    ) -> Dict[str, Any]:
        response_text = self._send_request(system_prompt, user_prompt)
        try:
            return json.loads(response_text)
        except json.JSONDecodeError:
            if not retry_on_invalid_json:
                raise
            retry_prompt = (
                "Return valid JSON only. Do not include commentary or markdown."
            )
            response_text = self._send_request(
                system_prompt, f"{retry_prompt}\n\n{user_prompt}"
            )
            return json.loads(response_text)

    def _send_request(self, system_prompt: str, user_prompt: str) -> str:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        payload = {
            "model": self.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        with httpx.Client(timeout=self.timeout_s) as client:
            response = client.post(self.api_url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError("Unexpected LLM response format") from exc
