from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class GenerationSettings:
    base_url: str
    api_key: str
    model: str
    max_tokens: int
    temperature: float
    context_chunks: int
    timeout: float


def load_settings() -> GenerationSettings:
    return GenerationSettings(
        base_url=os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1"),
        api_key=os.getenv("LLM_API_KEY", ""),
        model=os.getenv("LLM_MODEL", "llama-3.3-70b-versatile"),
        max_tokens=int(os.getenv("LLM_MAX_TOKENS", "1024")),
        temperature=float(os.getenv("LLM_TEMPERATURE", "0.1")),
        context_chunks=int(os.getenv("LLM_CONTEXT_CHUNKS", "5")),
        timeout=float(os.getenv("LLM_TIMEOUT", "60.0")),
    )
