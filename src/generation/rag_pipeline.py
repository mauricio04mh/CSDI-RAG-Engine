from __future__ import annotations

import logging
from dataclasses import dataclass, field

from src.database.repositories.chunk_repository import ChunkRepository
from src.generation.config.settings import GenerationSettings
from src.generation.llm_client import LLMClient
from src.generation.prompt_builder import build_messages
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
    ) -> None:
        self._retriever = retriever
        self._chunk_repo = chunk_repo
        self._llm_client = llm_client
        self._settings = settings
        self._reranker = reranker

    def query(self, question: str) -> RAGResult:
        # 1. First-stage: retrieve candidate chunks
        candidate_k = (
            self._settings.reranker_candidate_k
            if self._reranker
            else self._settings.context_chunks
        )
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

        # 3. Build prompt and call LLM
        messages = build_messages(question, chunks)
        response = self._llm_client.chat(messages)

        logger.info(
            "rag_generation model=%s prompt_tokens=%s completion_tokens=%s",
            response.model,
            response.prompt_tokens,
            response.completion_tokens,
        )

        sources = [
            RAGSource(chunk_id=c.chunk_id, url=c.url, title=c.title)
            for c in chunks
        ]

        return RAGResult(
            query=question,
            answer=response.content,
            sources=sources,
            model=response.model,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
        )
