from __future__ import annotations

import logging
from dataclasses import dataclass, field

from src.database.repositories.chunk_repository import ChunkRepository
from src.generation.config.settings import GenerationSettings
from src.generation.llm_client import LLMClient
from src.generation.prompt_builder import build_messages
from src.hybrid.pipeline.hybrid_retriever import HybridRetriever

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

    1. Retrieves the top-k most relevant chunks via hybrid search.
    2. Builds a prompt with the retrieved context.
    3. Calls the LLM to generate an answer.
    """

    def __init__(
        self,
        retriever: HybridRetriever,
        chunk_repo: ChunkRepository,
        llm_client: LLMClient,
        settings: GenerationSettings,
    ) -> None:
        self._retriever = retriever
        self._chunk_repo = chunk_repo
        self._llm_client = llm_client
        self._settings = settings

    def query(self, question: str) -> RAGResult:
        # 1. Retrieve relevant chunks
        hits = self._retriever.search(
            query=question, top_k=self._settings.context_chunks
        )
        chunk_ids = [h.doc_id for h in hits]
        chunks_map = self._chunk_repo.get_chunks(chunk_ids)
        chunks = [chunks_map[cid] for cid in chunk_ids if cid in chunks_map]

        logger.info(
            "rag_retrieval query=%s chunks_retrieved=%s", question, len(chunks)
        )

        # 2. Build prompt and call LLM
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
