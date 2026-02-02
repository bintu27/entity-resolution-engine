from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional
from uuid import uuid4

import httpx

logger = logging.getLogger(__name__)


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
        api_url = self._resolve_api_url(provider, api_url)
        if not api_url:
            raise ValueError("LLM API URL is required")
        self.api_url: str = api_url
        self.timeout_s = timeout_s
        self.last_invalid_json_retry = False
        self.last_latency_ms: Optional[float] = None
        self.last_request_id: Optional[str] = None

    @staticmethod
    def _resolve_api_url(provider: str, api_url: Optional[str]) -> Optional[str]:
        env_url = os.getenv("LLM_API_URL")
        if env_url:
            return env_url
        if api_url:
            return api_url
        if provider == "openai":
            return "https://api.openai.com/v1/chat/completions"
        return None

    @staticmethod
    def _extract_content(data: Dict[str, Any]) -> Optional[str]:
        if isinstance(data.get("content"), str):
            return data["content"]
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                message = first.get("message")
                if isinstance(message, dict) and isinstance(
                    message.get("content"), str
                ):
                    return message["content"]
                if isinstance(first.get("text"), str):
                    return first["text"]
        return None

    def request_json(
        self, system_prompt: str, user_prompt: str, retry_on_invalid_json: bool = True
    ) -> Dict[str, Any]:
        request_id = str(uuid4())
        self.last_request_id = request_id
        self.last_invalid_json_retry = False
        response_text = self._send_request(system_prompt, user_prompt, request_id)
        first_latency_ms = self.last_latency_ms or 0.0
        try:
            return json.loads(response_text)
        except json.JSONDecodeError as exc:
            if not retry_on_invalid_json:
                raise ValueError(
                    f"Invalid JSON response from provider={self.provider} "
                    f"request_id={request_id}"
                ) from exc
            retry_prompt = (
                "Return valid JSON only. Do not include commentary or markdown."
            )
            self.last_invalid_json_retry = True
            response_text = self._send_request(
                system_prompt, f"{retry_prompt}\n\n{user_prompt}", request_id
            )
            self.last_latency_ms = first_latency_ms + (self.last_latency_ms or 0.0)
            try:
                return json.loads(response_text)
            except json.JSONDecodeError as retry_exc:
                raise ValueError(
                    f"Invalid JSON response from provider={self.provider} "
                    f"request_id={request_id}"
                ) from retry_exc

    def _send_request(
        self, system_prompt: str, user_prompt: str, request_id: str
    ) -> str:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        payload = {
            "model": self.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        start_time = time.monotonic()
        try:
            with httpx.Client(timeout=self.timeout_s) as client:
                response = client.post(self.api_url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as exc:
            raise ValueError(
                f"LLM request failed for provider={self.provider} "
                f"request_id={request_id}"
            ) from exc
        except ValueError as exc:
            raise ValueError(
                f"Invalid JSON response from provider={self.provider} "
                f"request_id={request_id}"
            ) from exc
        finally:
            self.last_latency_ms = (time.monotonic() - start_time) * 1000
            logger.debug(
                "LLM request completed request_id=%s provider=%s latency_ms=%.2f",
                request_id,
                self.provider,
                self.last_latency_ms,
            )
        content = self._extract_content(data)
        if content is None:
            keys = sorted(list(data.keys())) if isinstance(data, dict) else []
            raise ValueError(
                "Unexpected LLM response format from provider="
                f"{self.provider} request_id={request_id} keys={keys}"
            )
        return content
