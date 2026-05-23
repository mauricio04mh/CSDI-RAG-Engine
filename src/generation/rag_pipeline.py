from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.generation.config.settings import GenerationSettings
from src.generation.prompt_builder import build_messages
from src.positioning import build_positioned_results
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
    source_type: str = "corpus"
    retrieval_method: str = "hybrid"
    relevance_score: float = 0.0
    freshness_score: float = 0.5
    display_priority: float = 0.0
    rank: int = 0


@dataclass(slots=True)
class RAGResult:
    query: str
    answer: str
    sources: list[RAGSource]
    model: str
    prompt_tokens: int
    completion_tokens: int
    web_search: WebSearchRunResult | None = None
    cache_searched: bool = False
    cache_hits: int = 0
    external_search_executed: bool = False
    external_indexed_count: int = 0


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
        web_cache_retriever: HybridRetriever | None = None,
        web_cache_chunk_repo: ChunkRepository | None = None,
        web_cache_enabled: bool = True,
        web_cache_top_k: int = 10,
    ) -> None:
        self._retriever = retriever
        self._chunk_repo = chunk_repo
        self._llm_client = llm_client
        self._settings = settings
        self._reranker = reranker
        self._insufficiency_detector = insufficiency_detector
        self._web_search_orchestrator = web_search_orchestrator
        self._web_cache_retriever = web_cache_retriever
        self._web_cache_chunk_repo = web_cache_chunk_repo
        self._web_cache_enabled = web_cache_enabled
        self._web_cache_top_k = web_cache_top_k

    def query(self, question: str) -> RAGResult:
        # 1. First-stage: retrieve candidate chunks
        candidate_k = (
            self._settings.reranker_candidate_k
            if self._reranker
            else self._settings.context_chunks
        )
        hits, chunks = self._retrieve_chunks(question=question, candidate_k=candidate_k)
        web_search_result: WebSearchRunResult | None = None
        cache_searched = False
        cache_hits = 0
        external_search_executed = False
        external_indexed_count = 0
        final_chunks = chunks
        final_hits = hits

        if self._insufficiency_detector:
            decision = self._evaluate_sufficiency(
                query=question,
                hits=hits,
                chunks=chunks,
                retriever=self._retriever,
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
                combined_hits = list(hits)
                combined_chunks = list(chunks)

                if self._web_cache_enabled and self._web_cache_retriever and self._web_cache_chunk_repo:
                    cache_searched = True
                    web_candidate_k = max(self._web_cache_top_k, candidate_k)
                    cache_retrieval_hits, cache_chunks = self._retrieve_chunks(
                        question=question,
                        candidate_k=web_candidate_k,
                        final_k=self._web_cache_top_k,
                        retriever=self._web_cache_retriever,
                        chunk_repo=self._web_cache_chunk_repo,
                    )
                    cache_hits = len(cache_chunks)
                    combined_hits = _merge_hits(combined_hits, cache_retrieval_hits)
                    combined_chunks = _merge_chunks(combined_chunks, cache_chunks)
                    decision = self._evaluate_sufficiency(
                        query=question,
                        hits=combined_hits,
                        chunks=combined_chunks,
                        retriever=self._retriever,
                    )
                    logger.info(
                        "web_cache_sufficiency query=%s needs_web_search=%s confidence=%.4f cache_hits=%s",
                        question,
                        decision.needs_web_search,
                        decision.sufficiency_confidence,
                        cache_hits,
                    )

                if self._web_search_orchestrator and self._web_search_orchestrator.enabled:
                    if decision.needs_web_search:
                        external_search_executed = True
                        web_search_result = self._web_search_orchestrator.run(question)
                        external_indexed_count = web_search_result.indexed_count

                        if self._web_cache_retriever and self._web_cache_chunk_repo:
                            cache_searched = True
                            web_candidate_k = max(self._web_cache_top_k, candidate_k)
                            cache_retrieval_hits, cache_chunks = self._retrieve_chunks(
                                question=question,
                                candidate_k=web_candidate_k,
                                final_k=self._web_cache_top_k,
                                retriever=self._web_cache_retriever,
                                chunk_repo=self._web_cache_chunk_repo,
                            )
                            cache_hits = len(cache_chunks)
                            combined_hits = _merge_hits(hits, cache_retrieval_hits)
                            combined_chunks = _merge_chunks(chunks, cache_chunks)
                else:
                    if decision.needs_web_search:
                        logger.info(
                            "web_search_disabled_fallback_to_llm query=%s",
                            question,
                        )

                final_hits = combined_hits
                final_chunks = self._select_prompt_chunks(
                    question=question,
                    hits=combined_hits,
                    chunks=combined_chunks,
                )

        found_by_map = {h.doc_id: getattr(h, "found_by", frozenset()) for h in final_hits}
        positioned = build_positioned_results(
            question,
            final_hits,
            {c.chunk_id: c for c in final_chunks},
        )
        sources = [
            RAGSource(
                chunk_id=c.chunk_id,
                url=c.url,
                title=c.title,
                source_type=_source_type_for_chunk(c),
                retrieval_method=_retrieval_method_for_chunk(c, found_by_map),
                relevance_score=positioned[c.chunk_id].relevance_score if c.chunk_id in positioned else 0.0,
                freshness_score=positioned[c.chunk_id].freshness_score if c.chunk_id in positioned else 0.5,
                display_priority=positioned[c.chunk_id].display_priority if c.chunk_id in positioned else 0.0,
                rank=positioned[c.chunk_id].rank if c.chunk_id in positioned else 0,
            )
            for c in final_chunks
        ]
        sources.sort(key=lambda source: source.rank or 9999)

        # 3. Build prompt and call LLM — pass corpus and web chunks separately so
        #    the prompt can instruct the model to distinguish sources in the answer.
        corpus_chunks = [c for c in final_chunks if _source_type_for_chunk(c) == "corpus"]
        web_chunks = [c for c in final_chunks if _source_type_for_chunk(c) == "web_cache"]
        messages = build_messages(question, corpus_chunks, web_chunks)
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
            cache_searched=cache_searched,
            cache_hits=cache_hits,
            external_search_executed=external_search_executed,
            external_indexed_count=external_indexed_count,
        )

    def _expand_with_hyde(self, question: str) -> str:
        messages = [
            {"role": "system", "content": "Write a concise factual paragraph (2-4 sentences) that directly answers the question. Only the paragraph, no preamble."},
            {"role": "user", "content": question},
        ]
        try:
            return self._llm_client.chat(messages).content
        except Exception:
            logger.exception("hyde_expansion_failed — falling back to original query")
            return question

    def _retrieve_chunks(
        self,
        *,
        question: str,
        candidate_k: int,
        final_k: int | None = None,
        retriever: HybridRetriever | None = None,
        chunk_repo: ChunkRepository | None = None,
    ):
        active_retriever = retriever or self._retriever
        active_chunk_repo = chunk_repo or self._chunk_repo
        final_k = final_k or self._settings.context_chunks
        if self._settings.hyde_enabled:
            hypothesis = self._expand_with_hyde(question)
            hits = active_retriever.search(query=question, top_k=candidate_k, vector_query=hypothesis)
        else:
            hits = active_retriever.search(query=question, top_k=candidate_k)
        chunk_ids = [h.doc_id for h in hits]
        chunks_map = active_chunk_repo.get_chunks(chunk_ids)
        missing_chunk_ids = [chunk_id for chunk_id in chunk_ids if chunk_id not in chunks_map]

        logger.info(
            "rag_retrieval query=%s candidates=%s resolved_chunks=%s missing_chunks=%s top_hit_ids=%s",
            question,
            len(chunk_ids),
            len(chunks_map),
            len(missing_chunk_ids),
            chunk_ids[:10],
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
                top_k=final_k,
            )
            final_ids = [r.doc_id for r in reranked]
            logger.info("rag_reranked top_k=%s", len(final_ids))
        else:
            final_ids = chunk_ids[:final_k]

        chunks = [chunks_map[cid] for cid in final_ids if cid in chunks_map]
        return hits, chunks

    def _evaluate_sufficiency(self, *, query: str, hits, chunks, retriever) -> object:
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
        return self._insufficiency_detector.evaluate(
            query=query,
            results=detector_results,
            retrieval_context={
                "fusion": {
                    "method": "rrf",
                    "rrf_k": 60,
                    "weights": {
                        "bm25": getattr(retriever, "_bm25_weight", 0.3),
                        "vector": getattr(retriever, "_vector_weight", 0.7),
                    },
                }
            },
        )

    def _select_prompt_chunks(self, *, question: str, hits, chunks):
        if not self._reranker:
            return chunks[: self._settings.context_chunks]

        candidates = [(c.chunk_id, c.text) for c in chunks]
        reranked = self._reranker.rerank(
            query=question,
            candidates=candidates,
            top_k=self._settings.context_chunks,
        )
        chunk_by_id = {c.chunk_id: c for c in chunks}
        return [chunk_by_id[r.doc_id] for r in reranked if r.doc_id in chunk_by_id]


def _merge_chunks(primary, secondary):
    merged = []
    seen: set[str] = set()
    for chunk in [*primary, *secondary]:
        if chunk.chunk_id in seen:
            continue
        seen.add(chunk.chunk_id)
        merged.append(chunk)
    return merged


def _merge_hits(primary, secondary):
    merged = []
    seen: set[str] = set()
    for hit in [*primary, *secondary]:
        if hit.doc_id in seen:
            continue
        seen.add(hit.doc_id)
        merged.append(hit)
    return merged


def _source_type_for_chunk(chunk) -> str:
    source_id = str(getattr(chunk, "source_id", ""))
    breadcrumb = str(getattr(chunk, "breadcrumb", ""))
    if source_id.startswith("web:") or breadcrumb == "web-search":
        return "web_cache"
    return "corpus"


def _retrieval_method_for_chunk(chunk, found_by_map: dict) -> str:
    source_id = str(getattr(chunk, "source_id", ""))
    breadcrumb = str(getattr(chunk, "breadcrumb", ""))
    if source_id.startswith("web:") or breadcrumb == "web-search":
        return "web_cache"
    fb: frozenset = found_by_map.get(getattr(chunk, "chunk_id", ""), frozenset())
    if "bm25" in fb and "vector" in fb:
        return "hybrid"
    if "bm25" in fb:
        return "bm25"
    if "vector" in fb:
        return "vector"
    return "hybrid"
