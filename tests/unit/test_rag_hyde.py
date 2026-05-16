from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.generation.config.settings import GenerationSettings
from src.generation.rag_pipeline import RAGPipeline


def _make_settings(*, hyde_enabled: bool) -> GenerationSettings:
    return GenerationSettings(
        base_url="http://localhost",
        api_key="test",
        model="test-model",
        max_tokens=256,
        temperature=0.0,
        context_chunks=3,
        timeout=10.0,
        reranker_enabled=False,
        reranker_model="",
        reranker_candidate_k=10,
        hyde_enabled=hyde_enabled,
    )


def _make_pipeline(*, hyde_enabled: bool, llm_response: str = "hypothesis text") -> tuple[RAGPipeline, MagicMock, MagicMock]:
    llm_client = MagicMock()
    llm_client.chat.return_value = MagicMock(
        content=llm_response,
        model="test-model",
        prompt_tokens=10,
        completion_tokens=5,
    )

    retriever = MagicMock()
    retriever.search.return_value = []
    retriever._bm25_weight = 0.3
    retriever._vector_weight = 0.7

    chunk_repo = MagicMock()
    chunk_repo.get_chunks.return_value = {}

    pipeline = RAGPipeline(
        retriever=retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=_make_settings(hyde_enabled=hyde_enabled),
    )
    return pipeline, retriever, llm_client


def test_hyde_disabled_calls_retriever_without_vector_query():
    """When HYDE_ENABLED=false, retriever.search is called without vector_query."""
    pipeline, retriever, _ = _make_pipeline(hyde_enabled=False)
    pipeline.query("what is Python?")
    retriever.search.assert_called_once()
    call_kwargs = retriever.search.call_args.kwargs
    assert "vector_query" not in call_kwargs or call_kwargs.get("vector_query") is None


def test_hyde_enabled_calls_retriever_with_hypothesis():
    """When HYDE_ENABLED=true, retriever.search receives vector_query=<hypothesis>."""
    pipeline, retriever, llm_client = _make_pipeline(
        hyde_enabled=True, llm_response="Python is a high-level language."
    )
    pipeline.query("what is Python?")

    # The LLM is called twice: once for HyDE expansion, once for answer generation.
    assert llm_client.chat.call_count == 2

    # vector_query must be the hypothesis text, not the original query.
    retriever.search.assert_called_once()
    call_kwargs = retriever.search.call_args.kwargs
    assert call_kwargs["vector_query"] == "Python is a high-level language."
    assert call_kwargs["query"] == "what is Python?"


def test_hyde_llm_failure_falls_back_to_original_query():
    """If the HyDE LLM call raises, vector_query falls back to the original query."""
    pipeline, retriever, llm_client = _make_pipeline(hyde_enabled=True)

    # First call (HyDE) raises; second call (answer generation) succeeds.
    llm_client.chat.side_effect = [RuntimeError("timeout"), llm_client.chat.return_value]
    # Reset side_effect for the second call by using a list.
    answer_mock = MagicMock(content="fallback answer", model="m", prompt_tokens=5, completion_tokens=5)
    llm_client.chat.side_effect = [RuntimeError("timeout"), answer_mock]

    pipeline.query("what is Python?")

    retriever.search.assert_called_once()
    call_kwargs = retriever.search.call_args.kwargs
    # Fallback: vector_query equals the original question.
    assert call_kwargs["vector_query"] == "what is Python?"


def test_hyde_settings_default_is_false():
    """HYDE_ENABLED defaults to false — no env var set."""
    import os
    env_backup = os.environ.pop("HYDE_ENABLED", None)
    try:
        from src.generation.config.settings import load_settings
        settings = load_settings()
        assert settings.hyde_enabled is False
    finally:
        if env_backup is not None:
            os.environ["HYDE_ENABLED"] = env_backup


def test_hyde_settings_reads_env_true():
    """HYDE_ENABLED=true is parsed correctly."""
    import os
    os.environ["HYDE_ENABLED"] = "true"
    try:
        from src.generation.config import settings as settings_module
        import importlib
        importlib.reload(settings_module)
        loaded = settings_module.load_settings()
        assert loaded.hyde_enabled is True
    finally:
        del os.environ["HYDE_ENABLED"]
