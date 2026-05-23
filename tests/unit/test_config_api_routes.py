from __future__ import annotations

from dataclasses import dataclass
import sys
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from pydantic import ValidationError

if "dotenv" not in sys.modules:
    sys.modules["dotenv"] = SimpleNamespace(
        dotenv_values=lambda *_args, **_kwargs: {},
        load_dotenv=lambda *_args, **_kwargs: None,
    )

if "numpy" not in sys.modules:
    sys.modules["numpy"] = SimpleNamespace(
        ndarray=list,
        float32=float,
        asarray=lambda value, dtype=None: value,
        empty=lambda shape, dtype=None: [],
        dot=lambda left, right: sum(a * b for a, b in zip(left, right, strict=True)),
    )

if "sentence_transformers" not in sys.modules:
    sys.modules["sentence_transformers"] = SimpleNamespace(
        SentenceTransformer=lambda *_args, **_kwargs: None,
        CrossEncoder=lambda *_args, **_kwargs: None,
    )

if "faiss" not in sys.modules:
    class _FakeFaissIndex:
        ntotal = 0

        def add(self, _vectors) -> None:
            return None

        def search(self, _query, top_k: int):
            return [[0.0] * top_k], [[-1] * top_k]

    sys.modules["faiss"] = SimpleNamespace(
        Index=object,
        IndexHNSWFlat=lambda *_args, **_kwargs: _FakeFaissIndex(),
        METRIC_INNER_PRODUCT=0,
    )

if "snowballstemmer" not in sys.modules:
    sys.modules["snowballstemmer"] = SimpleNamespace(
        stemmer=lambda _language: SimpleNamespace(stemWord=lambda token: token),
    )

from src.config_api.api.routes import (
    PipelineConfigUpdate,
    _query_feedback_comparison_probability,
    get_config,
    router,
    update_config,
)


@dataclass(slots=True)
class FakeRagSettings:
    context_chunks: int = 15
    reranker_candidate_k: int = 30
    hyde_enabled: bool = False


@dataclass(slots=True)
class FakeDetectorSettings:
    confidence_threshold: float = 0.65
    min_results: int = 5
    expected_results: int = 10
    min_top_score: float = 0.35
    min_relevant_results: int = 2
    min_coverage_score: float = 0.20
    min_answerability_score: float = 0.40
    min_source_diversity: float = 0.30
    coverage_top_n: int = 5
    w_top: float = 0.10
    w_quantity: float = 0.15
    w_coverage: float = 0.35
    w_diversity: float = 0.15
    w_answerability: float = 0.25


class FakeDetector:
    def __init__(self) -> None:
        self.settings = FakeDetectorSettings()


class FakeHybridRetriever:
    def __init__(self) -> None:
        self._bm25_weight = 0.3
        self._vector_weight = 0.7

    def update_weights(self, bm25_weight: float, vector_weight: float) -> None:
        self._bm25_weight = bm25_weight
        self._vector_weight = vector_weight


class FakeLLMClient:
    def __init__(self) -> None:
        self._base_url = "https://api.groq.com/openai/v1"
        self._headers = {"Authorization": "Bearer test-key"}
        self._temperature = 0.1
        self._model = "llama-3.3-70b-versatile"
        self._max_tokens = 1024

    def update_settings(self, *, model: str, temperature: float, max_tokens: int) -> None:
        self._model = model
        self._temperature = temperature
        self._max_tokens = max_tokens

    def update_connection(self, *, base_url: str, api_key: str) -> None:
        self._base_url = base_url
        self._headers = {"Authorization": f"Bearer {api_key}"}


class FakeRagPipeline:
    def __init__(self) -> None:
        self._settings = FakeRagSettings()
        self._reranker = object()
        self._insufficiency_detector = FakeDetector()


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.hybrid_retriever = FakeHybridRetriever()
    app.state.llm_client = FakeLLMClient()
    app.state.rag_pipeline = FakeRagPipeline()
    app.state.reranker_model = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    return app


def _build_request(app: FastAPI):
    return SimpleNamespace(app=app)


def test_get_config_returns_query_feedback_comparison_probability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
):
    config_path = tmp_path / "pipeline_config.json"
    monkeypatch.setenv("PIPELINE_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("QUERY_FEEDBACK_COMPARISON_PROBABILITY", "0.4")
    app = _build_app()
    response = get_config(_build_request(app))

    assert response.query_feedback_comparison_probability == 0.4


def test_query_feedback_comparison_probability_persisted_value_has_priority_over_env(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("QUERY_FEEDBACK_COMPARISON_PROBABILITY", "0.9")

    value = _query_feedback_comparison_probability({
        "query_feedback_comparison_probability": 0.4,
    })

    assert value == 0.4


def test_query_feedback_comparison_probability_invalid_env_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("QUERY_FEEDBACK_COMPARISON_PROBABILITY", "not-a-number")

    value = _query_feedback_comparison_probability({})

    assert value == 0.25


def test_query_feedback_comparison_probability_env_below_range_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("QUERY_FEEDBACK_COMPARISON_PROBABILITY", "-0.1")

    value = _query_feedback_comparison_probability({})

    assert value == 0.25


def test_query_feedback_comparison_probability_env_above_range_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("QUERY_FEEDBACK_COMPARISON_PROBABILITY", "1.1")

    value = _query_feedback_comparison_probability({})

    assert value == 0.25


def test_query_feedback_comparison_probability_invalid_persisted_value_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("QUERY_FEEDBACK_COMPARISON_PROBABILITY", "0.8")

    value = _query_feedback_comparison_probability({
        "query_feedback_comparison_probability": "invalid",
    })

    assert value == 0.25


def test_post_config_updates_and_persists_query_feedback_comparison_probability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
):
    config_path = tmp_path / "pipeline_config.json"
    monkeypatch.setenv("PIPELINE_CONFIG_PATH", str(config_path))
    app = _build_app()
    payload = {
        "bm25_weight": 0.3,
        "vector_weight": 0.7,
        "temperature": 0.2,
        "model": "llama-3.3-70b-versatile",
        "reranker_enabled": True,
        "reranker_candidate_k": 30,
        "context_chunks": 15,
        "max_tokens": 1024,
        "hyde_enabled": False,
        "llm_base_url": "https://api.groq.com/openai/v1",
        "llm_api_key": "updated-key",
        "query_feedback_comparison_probability": 0.55,
        "insuff": {
            "confidence_threshold": 0.65,
            "min_results": 5,
            "expected_results": 10,
            "min_top_score": 0.35,
            "min_relevant_results": 2,
            "min_coverage_score": 0.2,
            "min_answerability_score": 0.4,
            "min_source_diversity": 0.3,
            "coverage_top_n": 5,
            "w_top": 0.1,
            "w_quantity": 0.15,
            "w_coverage": 0.35,
            "w_diversity": 0.15,
            "w_answerability": 0.25,
        },
    }

    response = update_config(PipelineConfigUpdate(**payload), _build_request(app))
    persisted_response = get_config(_build_request(app))

    assert response.query_feedback_comparison_probability == 0.55
    assert persisted_response.query_feedback_comparison_probability == 0.55
    assert config_path.exists()


@pytest.mark.parametrize("value", [-0.01, 1.01])
def test_pipeline_config_update_rejects_out_of_range_query_feedback_comparison_probability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    value: float,
):
    config_path = tmp_path / "pipeline_config.json"
    monkeypatch.setenv("PIPELINE_CONFIG_PATH", str(config_path))
    payload = {
        "bm25_weight": 0.3,
        "vector_weight": 0.7,
        "temperature": 0.2,
        "model": "llama-3.3-70b-versatile",
        "reranker_enabled": True,
        "reranker_candidate_k": 30,
        "context_chunks": 15,
        "max_tokens": 1024,
        "hyde_enabled": False,
        "llm_base_url": "https://api.groq.com/openai/v1",
        "llm_api_key": "updated-key",
        "query_feedback_comparison_probability": value,
        "insuff": {
            "confidence_threshold": 0.65,
            "min_results": 5,
            "expected_results": 10,
            "min_top_score": 0.35,
            "min_relevant_results": 2,
            "min_coverage_score": 0.2,
            "min_answerability_score": 0.4,
            "min_source_diversity": 0.3,
            "coverage_top_n": 5,
            "w_top": 0.1,
            "w_quantity": 0.15,
            "w_coverage": 0.35,
            "w_diversity": 0.15,
            "w_answerability": 0.25,
        },
    }

    with pytest.raises(ValidationError):
        PipelineConfigUpdate(**payload)
