from __future__ import annotations

import dataclasses
from pathlib import Path

from src.generation.config.settings import GenerationSettings
from src.generation.rag_pipeline import RAGPipeline
from src.web_search.orchestrator import WebSearchOrchestrator, WebSearchSettings
from src.web_search.schemas import WebSearchHit
from src.web_search.insufficiency_detector.config.settings import InsufficiencyDetectorSettings
from src.web_search.insufficiency_detector.detector import InsufficiencyDetector


def _generation_settings() -> GenerationSettings:
    return GenerationSettings(
        base_url="http://localhost:1234",
        api_key="test",
        model="test-model",
        max_tokens=64,
        temperature=0.1,
        context_chunks=5,
        timeout=5.0,
        reranker_enabled=False,
        reranker_model="",
        reranker_candidate_k=10,
    )


def _insufficiency_settings(**overrides) -> InsufficiencyDetectorSettings:
    base = InsufficiencyDetectorSettings(
        min_results=5,
        expected_results=10,
        min_top_score=0.35,
        relevant_overlap_threshold=0.20,
        min_relevant_results=2,
        min_coverage_score=0.20,
        min_answerability_score=0.40,
        min_source_diversity=0.30,
        confidence_threshold=0.55,
        coverage_top_n=5,
        w_top=0.25,
        w_quantity=0.15,
        w_coverage=0.25,
        w_diversity=0.20,
        w_answerability=0.15,
        env_path=Path("."),
        project_root=Path("."),
    )
    base_dict = {
        f.name: getattr(base, f.name)
        for f in dataclasses.fields(InsufficiencyDetectorSettings)
    }
    return InsufficiencyDetectorSettings(**{**base_dict, **overrides})


class _FakeRetriever:
    def __init__(self, hits: list["_Hit"]) -> None:
        self._hits = hits
        self._bm25_weight = 0.3
        self._vector_weight = 0.7
        self.calls = 0

    def search(self, query: str, top_k: int) -> list["_Hit"]:
        self.calls += 1
        return self._hits[:top_k]


class _FakeChunkRepo:
    def __init__(self, chunks: dict[str, object]) -> None:
        self._chunks = chunks

    def get_chunks(self, chunk_ids: list[str]) -> dict[str, object]:
        return {cid: self._chunks[cid] for cid in chunk_ids if cid in self._chunks}


class _FakeLLMClient:
    def __init__(self) -> None:
        self.calls = 0

    def chat(self, messages: list[dict]) -> object:
        self.calls += 1
        return _LLMResponse(
            "respuesta llm",
            "test-model",
            10,
            5,
        )


class _LLMResponse:
    def __init__(
        self,
        content: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> None:
        self.content = content
        self.model = model
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens


class _Chunk:
    def __init__(
        self,
        chunk_id: str,
        text: str,
        source_id: str = "s1",
        url: str = "https://example.com",
        title: str = "title",
        breadcrumb: str = "",
    ) -> None:
        self.chunk_id = chunk_id
        self.text = text
        self.source_id = source_id
        self.url = url
        self.title = title
        self.breadcrumb = breadcrumb


class _Hit:
    def __init__(self, doc_id: str, score: float) -> None:
        self.doc_id = doc_id
        self.score = score


class _FakeWebSearchProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def search(self, query: str, top_k: int) -> list[WebSearchHit]:
        self.calls.append((query, top_k))
        return [WebSearchHit(title="web title", url="https://web.example", snippet="web snippet")]


def _coverage_only_settings() -> InsufficiencyDetectorSettings:
    return _insufficiency_settings(
        min_results=1,
        expected_results=1,
        min_top_score=0.0,
        relevant_overlap_threshold=0.20,
        min_relevant_results=1,
        min_coverage_score=0.0,
        min_answerability_score=0.0,
        min_source_diversity=0.0,
        confidence_threshold=0.50,
        w_top=0.0,
        w_quantity=0.0,
        w_coverage=0.6,
        w_diversity=0.0,
        w_answerability=0.4,
    )


def test_rag_pipeline_falls_back_to_llm_when_detector_requires_web_but_orchestrator_is_missing() -> None:
    retriever = _FakeRetriever([_Hit(doc_id="c1", score=0.01)])
    chunk_repo = _FakeChunkRepo({"c1": _Chunk(chunk_id="c1", text="contenido no relacionado")})
    llm_client = _FakeLLMClient()
    detector = InsufficiencyDetector(settings=_insufficiency_settings(confidence_threshold=0.95))
    pipeline = RAGPipeline(
        retriever=retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=_generation_settings(),
        reranker=None,
        insufficiency_detector=detector,
    )

    result = pipeline.query("python decorators")

    assert result.answer == "respuesta llm"
    assert result.model == "test-model"
    assert result.prompt_tokens == 10
    assert result.completion_tokens == 5
    assert llm_client.calls == 1


def test_rag_pipeline_calls_llm_when_detector_allows_local_answer() -> None:
    retriever = _FakeRetriever([_Hit(doc_id="c1", score=0.02)])
    chunk_repo = _FakeChunkRepo({"c1": _Chunk(chunk_id="c1", text="python decorators examples")})
    llm_client = _FakeLLMClient()
    detector = InsufficiencyDetector(
        settings=_insufficiency_settings(
            min_results=1,
            expected_results=1,
            min_top_score=0.0,
            relevant_overlap_threshold=0.0,
            min_relevant_results=1,
            min_coverage_score=0.0,
            min_answerability_score=0.0,
            min_source_diversity=0.0,
            confidence_threshold=0.0,
        )
    )
    pipeline = RAGPipeline(
        retriever=retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=_generation_settings(),
        reranker=None,
        insufficiency_detector=detector,
    )

    result = pipeline.query("python decorators")

    assert result.answer == "respuesta llm"
    assert result.model == "test-model"
    assert result.prompt_tokens == 10
    assert result.completion_tokens == 5
    assert llm_client.calls == 1


def test_rag_pipeline_calls_web_search_orchestrator_when_detector_requires_it() -> None:
    retriever = _FakeRetriever([_Hit(doc_id="c1", score=0.01)])
    chunk_repo = _FakeChunkRepo({"c1": _Chunk(chunk_id="c1", text="contenido no relacionado")})
    llm_client = _FakeLLMClient()
    detector = InsufficiencyDetector(settings=_insufficiency_settings(confidence_threshold=0.95))
    provider = _FakeWebSearchProvider()
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=True, top_k=4),
    )
    pipeline = RAGPipeline(
        retriever=retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=_generation_settings(),
        reranker=None,
        insufficiency_detector=detector,
        web_search_orchestrator=orchestrator,
    )

    result = pipeline.query("python decorators")

    assert provider.calls == [("python decorators", 4)]
    assert result.answer == "respuesta llm"
    assert result.model == "test-model"
    assert result.web_search is not None
    assert len(result.web_search.hits) == 1
    assert llm_client.calls == 1


def test_rag_pipeline_falls_back_to_llm_when_web_search_is_disabled() -> None:
    retriever = _FakeRetriever([_Hit(doc_id="c1", score=0.01)])
    chunk_repo = _FakeChunkRepo({"c1": _Chunk(chunk_id="c1", text="contenido no relacionado")})
    llm_client = _FakeLLMClient()
    detector = InsufficiencyDetector(settings=_insufficiency_settings(confidence_threshold=0.95))
    provider = _FakeWebSearchProvider()
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=False, top_k=4),
    )
    pipeline = RAGPipeline(
        retriever=retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=_generation_settings(),
        reranker=None,
        insufficiency_detector=detector,
        web_search_orchestrator=orchestrator,
    )

    result = pipeline.query("python decorators")

    assert provider.calls == []
    assert result.answer == "respuesta llm"
    assert result.model == "test-model"
    assert result.web_search is None
    assert llm_client.calls == 1


def test_rag_pipeline_uses_web_cache_before_external_search() -> None:
    retriever = _FakeRetriever([_Hit(doc_id="c1", score=0.01)])
    chunk_repo = _FakeChunkRepo({"c1": _Chunk(chunk_id="c1", text="contenido no relacionado")})
    cache_retriever = _FakeRetriever([_Hit(doc_id="w1", score=0.02)])
    cache_repo = _FakeChunkRepo(
        {
            "w1": _Chunk(
                chunk_id="w1",
                text="python decorators examples",
                source_id="web:duckduckgo:example.com",
                url="https://web.example",
                title="web",
                breadcrumb="web-search",
            )
        }
    )
    llm_client = _FakeLLMClient()
    detector = InsufficiencyDetector(settings=_coverage_only_settings())
    provider = _FakeWebSearchProvider()
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=True, top_k=4),
    )
    pipeline = RAGPipeline(
        retriever=retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=_generation_settings(),
        reranker=None,
        insufficiency_detector=detector,
        web_search_orchestrator=orchestrator,
        web_cache_retriever=cache_retriever,
        web_cache_chunk_repo=cache_repo,
        web_cache_enabled=True,
        web_cache_top_k=5,
    )

    result = pipeline.query("python decorators")

    assert cache_retriever.calls == 1
    assert provider.calls == []
    assert result.cache_searched is True
    assert result.cache_hits == 1
    assert result.external_search_executed is False
    assert any(source.source_type == "web_cache" for source in result.sources)


def test_rag_pipeline_runs_external_when_web_cache_is_insufficient() -> None:
    retriever = _FakeRetriever([_Hit(doc_id="c1", score=0.01)])
    chunk_repo = _FakeChunkRepo({"c1": _Chunk(chunk_id="c1", text="contenido no relacionado")})
    cache_retriever = _FakeRetriever([_Hit(doc_id="w1", score=0.02)])
    cache_repo = _FakeChunkRepo(
        {
            "w1": _Chunk(
                chunk_id="w1",
                text="tambien sin relacion",
                source_id="web:duckduckgo:example.com",
                url="https://web.example",
                title="web",
                breadcrumb="web-search",
            )
        }
    )
    llm_client = _FakeLLMClient()
    detector = InsufficiencyDetector(settings=_coverage_only_settings())
    provider = _FakeWebSearchProvider()
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=True, top_k=4),
    )
    pipeline = RAGPipeline(
        retriever=retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=_generation_settings(),
        reranker=None,
        insufficiency_detector=detector,
        web_search_orchestrator=orchestrator,
        web_cache_retriever=cache_retriever,
        web_cache_chunk_repo=cache_repo,
        web_cache_enabled=True,
        web_cache_top_k=5,
    )

    result = pipeline.query("python decorators")

    assert cache_retriever.calls == 2
    assert provider.calls == [("python decorators", 4)]
    assert result.external_search_executed is True
    assert result.web_search is not None


def test_rag_pipeline_skips_cache_when_web_cache_is_disabled() -> None:
    retriever = _FakeRetriever([_Hit(doc_id="c1", score=0.01)])
    chunk_repo = _FakeChunkRepo({"c1": _Chunk(chunk_id="c1", text="contenido no relacionado")})
    cache_retriever = _FakeRetriever([_Hit(doc_id="w1", score=0.02)])
    cache_repo = _FakeChunkRepo({"w1": _Chunk(chunk_id="w1", text="python decorators examples")})
    llm_client = _FakeLLMClient()
    detector = InsufficiencyDetector(settings=_coverage_only_settings())
    provider = _FakeWebSearchProvider()
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=True, top_k=4),
    )
    pipeline = RAGPipeline(
        retriever=retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=_generation_settings(),
        reranker=None,
        insufficiency_detector=detector,
        web_search_orchestrator=orchestrator,
        web_cache_retriever=cache_retriever,
        web_cache_chunk_repo=cache_repo,
        web_cache_enabled=False,
        web_cache_top_k=5,
    )

    result = pipeline.query("python decorators")

    assert cache_retriever.calls == 1
    assert provider.calls == [("python decorators", 4)]
    assert result.external_search_executed is True
