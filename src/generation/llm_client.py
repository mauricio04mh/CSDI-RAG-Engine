from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LLMResponse:
    content: str
    model: str
    prompt_tokens: int
    completion_tokens: int


class LLMClient:
    """OpenAI-compatible HTTP client.

    Works with any provider that implements the /chat/completions endpoint:
    Groq, Ollama, OpenAI, Together AI, etc.
    Switch provider by changing LLM_BASE_URL + LLM_API_KEY + LLM_MODEL env vars.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        max_tokens: int,
        temperature: float,
        timeout: float = 60.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._max_tokens = max_tokens
        self._temperature = temperature
        self._timeout = timeout
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def chat(self, messages: list[dict]) -> LLMResponse:
        """Send a chat completion request and return the response."""
        payload = {
            "model": self._model,
            "messages": messages,
            "max_tokens": self._max_tokens,
            "temperature": self._temperature,
        }

        logger.debug("llm_request model=%s messages=%s", self._model, len(messages))

        with httpx.Client(timeout=self._timeout) as client:
            response = client.post(
                f"{self._base_url}/chat/completions",
                json=payload,
                headers=self._headers,
            )
            response.raise_for_status()

        data = response.json()
        choice = data["choices"][0]
        usage = data.get("usage", {})

        return LLMResponse(
            content=choice["message"]["content"],
            model=data.get("model", self._model),
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
        )

    def update_settings(self, model: str, temperature: float) -> None:
        """Hot-update model and temperature without restarting."""
        self._model = model
        self._temperature = temperature
        logger.info(
            "llm_settings_updated model=%s temperature=%.2f", model, temperature
        )
