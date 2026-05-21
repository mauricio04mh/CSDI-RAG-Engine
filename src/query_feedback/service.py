"""Services for the Query Feedback module."""

from __future__ import annotations

from src.query_feedback.expansion import QueryExpansionService
from src.query_feedback.schemas import ExpansionResult


class QueryFeedbackService:
    """Orchestrates query expansion using the existing retrieval pipeline."""

    def __init__(
        self,
        hybrid_retriever,
        chunk_repo,
        expansion_service: QueryExpansionService | None = None,
    ) -> None:
        self._hybrid_retriever = hybrid_retriever
        self._chunk_repo = chunk_repo
        self._expansion_service = expansion_service or QueryExpansionService()

    def expand_query(
        self,
        query: str,
        top_k_feedback: int = 5,
        max_expansion_terms: int = 6,
        source_ids: list[str] | None = None,
    ) -> ExpansionResult:
        if not query.strip():
            raise ValueError("query must not be empty")
        if top_k_feedback < 1:
            raise ValueError("top_k_feedback must be at least 1")
        if max_expansion_terms < 0:
            raise ValueError("max_expansion_terms must be greater than or equal to 0")

        fetch_k = self._candidate_fetch_k(top_k_feedback, source_ids)
        results = self._hybrid_retriever.search(query=query, top_k=fetch_k)
        chunk_ids = [self._result_chunk_id(result) for result in results]
        chunks_by_id = self._chunk_repo.get_chunks(chunk_ids)

        selected_chunks: list[object] = []
        for chunk_id in chunk_ids:
            chunk = chunks_by_id.get(chunk_id)
            if chunk is None:
                continue
            if source_ids is not None and getattr(chunk, "source_id", None) not in source_ids:
                continue
            selected_chunks.append(chunk)
            if len(selected_chunks) >= top_k_feedback:
                break

        return self._expansion_service.expand_from_chunks(
            query=query,
            chunks=selected_chunks,
            max_expansion_terms=max_expansion_terms,
        )

    def _candidate_fetch_k(self, top_k_feedback: int, source_ids: list[str] | None) -> int:
        if source_ids is None:
            return top_k_feedback
        return min(max(top_k_feedback * 5, top_k_feedback), 100)

    def _result_chunk_id(self, result: object) -> str:
        chunk_id = getattr(result, "doc_id", None) or getattr(result, "chunk_id", None)
        if not chunk_id:
            raise ValueError("retriever returned a result without a document identifier")
        return str(chunk_id)
