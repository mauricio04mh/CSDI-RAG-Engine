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
    reranker_enabled: bool
    reranker_model: str
    reranker_candidate_k: int


def load_settings() -> GenerationSettings:
    return GenerationSettings(
        base_url=os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1"),
        api_key=os.getenv("LLM_API_KEY", ""),
        model=os.getenv("LLM_MODEL", "llama-3.3-70b-versatile"),
        max_tokens=int(os.getenv("LLM_MAX_TOKENS", "1024")),
        temperature=float(os.getenv("LLM_TEMPERATURE", "0.1")),
        context_chunks=int(os.getenv("LLM_CONTEXT_CHUNKS", "15")),
        timeout=float(os.getenv("LLM_TIMEOUT", "60.0")),
        reranker_enabled=os.getenv("RERANKER_ENABLED", "true").lower() == "true",
        reranker_model=os.getenv("RERANKER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2"),
        reranker_candidate_k=int(os.getenv("RERANKER_CANDIDATE_K", "30")),
    )
