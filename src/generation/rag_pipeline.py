from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.generation.config.settings import GenerationSettings
from src.generation.prompt_builder import build_messages
from src.web_search.orchestrator import WebSearchOrchestrator
from src.web_search.insufficiency_detector.detector import InsufficiencyDetector
from src.web_search.insufficiency_detector.schemas import RetrievedChunk
from src.web_search.schemas import WebSearchRunResult

if TYPE_CHECKING:
    from src.database.repositories.chunk_repository import ChunkRepository
    from src.generation.llm_client import LLMClient
    from src.hybrid.pipeline.hybrid_retriever import HybridRetriever
    from src.reranker.cross_encoder_reranker import CrossEncoderReranker

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RAGSource:
    chunk_id: str
    url: str
    title: str


@dataclass(slots=True)
class RAGResult:
    query: str
    answer: str
    sources: list[RAGSource]
    model: str
    prompt_tokens: int
    completion_tokens: int
    web_search: WebSearchRunResult | None = None


class RAGPipeline:
    """Retrieval-Augmented Generation pipeline.

    1. Retrieves candidate chunks via hybrid search (BM25 + vector).
    2. Optionally re-ranks candidates with a cross-encoder for precision.
    3. Builds a prompt with the top-k chunks.
    4. Calls the LLM to generate an answer.
    """

    def __init__(
        self,
        retriever: HybridRetriever,
        chunk_repo: ChunkRepository,
        llm_client: LLMClient,
        settings: GenerationSettings,
        reranker: CrossEncoderReranker | None = None,
        insufficiency_detector: InsufficiencyDetector | None = None,
        web_search_orchestrator: WebSearchOrchestrator | None = None,
    ) -> None:
        self._retriever = retriever
        self._chunk_repo = chunk_repo
        self._llm_client = llm_client
        self._settings = settings
        self._reranker = reranker
        self._insufficiency_detector = insufficiency_detector
        self._web_search_orchestrator = web_search_orchestrator

    def query(self, question: str) -> RAGResult:
        # 1. First-stage: retrieve candidate chunks
        candidate_k = (
            self._settings.reranker_candidate_k
            if self._reranker
            else self._settings.context_chunks
        )
        hits, chunks = self._retrieve_chunks(question=question, candidate_k=candidate_k)
        sources = [
            RAGSource(chunk_id=c.chunk_id, url=c.url, title=c.title)
            for c in chunks
        ]
        web_search_result: WebSearchRunResult | None = None

        if self._insufficiency_detector:
            score_by_chunk_id = {h.doc_id: h.score for h in hits}
            detector_results = [
                RetrievedChunk(
                    chunk_id=c.chunk_id,
                    text=c.text,
                    score=score_by_chunk_id.get(c.chunk_id),
                    source_id=c.source_id,
                    url=c.url,
                    title=c.title,
                    breadcrumb=c.breadcrumb,
                )
                for c in chunks
            ]
            decision = self._insufficiency_detector.evaluate(
                query=question,
                results=detector_results,
                retrieval_context={
                    "fusion": {
                        "method": "rrf",
                        "rrf_k": 60,
                        "weights": {
                            "bm25": self._retriever._bm25_weight,
                            "vector": self._retriever._vector_weight,
                        },
                    }
                },
            )
            logger.info(
                "insufficiency_detector query=%s needs_web_search=%s confidence=%.4f reasons=%s metrics=%s",
                question,
                decision.needs_web_search,
                decision.sufficiency_confidence,
                [reason.value for reason in decision.reasons],
                decision.metrics,
            )

            if decision.needs_web_search:
                if self._web_search_orchestrator and self._web_search_orchestrator.enabled:
                    web_search_result = self._web_search_orchestrator.run(question)
                    hits, chunks = self._retrieve_chunks(question=question, candidate_k=candidate_k)
                    sources = [
                        RAGSource(chunk_id=c.chunk_id, url=c.url, title=c.title)
                        for c in chunks
                    ]
                else:
                    logger.info(
                        "web_search_disabled_fallback_to_llm query=%s",
                        question,
                    )

        # 3. Build prompt and call LLM
        messages = build_messages(question, chunks)
        response = self._llm_client.chat(messages)

        logger.info(
            "rag_generation model=%s prompt_tokens=%s completion_tokens=%s",
            response.model,
            response.prompt_tokens,
            response.completion_tokens,
        )

        return RAGResult(
            query=question,
            answer=response.content,
            sources=sources,
            model=response.model,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            web_search=web_search_result,
        )

    def _retrieve_chunks(self, *, question: str, candidate_k: int):
        hits = self._retriever.search(query=question, top_k=candidate_k)
        chunk_ids = [h.doc_id for h in hits]
        chunks_map = self._chunk_repo.get_chunks(chunk_ids)

        logger.info(
            "rag_retrieval query=%s candidates=%s", question, len(chunk_ids)
        )

        # 2. Second-stage: re-rank with cross-encoder if available
        if self._reranker:
            candidates = [
                (cid, chunks_map[cid].text)
                for cid in chunk_ids
                if cid in chunks_map
            ]
            reranked = self._reranker.rerank(
                query=question,
                candidates=candidates,
                top_k=self._settings.context_chunks,
            )
            final_ids = [r.doc_id for r in reranked]
            logger.info("rag_reranked top_k=%s", len(final_ids))
        else:
            final_ids = chunk_ids[: self._settings.context_chunks]

        chunks = [chunks_map[cid] for cid in final_ids if cid in chunks_map]
        return hits, chunks
