"""Services for the Query Feedback module."""

from __future__ import annotations

from src.query_feedback.expansion import QueryExpansionService
from src.query_feedback.schemas import ExpandedSearchResult, ExpansionResult, SearchResultItem


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

    def search(
        self,
        query: str,
        top_k: int = 10,
        source_ids: list[str] | None = None,
        expansion_enabled: bool = True,
        top_k_feedback: int = 5,
        max_expansion_terms: int = 6,
    ) -> ExpandedSearchResult:
        if not query.strip():
            raise ValueError("query must not be empty")
        if top_k < 1:
            raise ValueError("top_k must be at least 1")
        if top_k_feedback < 1:
            raise ValueError("top_k_feedback must be at least 1")
        if max_expansion_terms < 0:
            raise ValueError("max_expansion_terms must be greater than or equal to 0")

        if expansion_enabled:
            expansion_result = self.expand_query(
                query=query,
                top_k_feedback=top_k_feedback,
                max_expansion_terms=max_expansion_terms,
                source_ids=source_ids,
            )
            method = expansion_result.method
            expanded_query = expansion_result.expanded_query
            expansion_terms = expansion_result.expansion_terms
            feedback_documents_used = expansion_result.feedback_documents_used
            strategy = "hybrid_expanded_vector"
        else:
            expansion_result = None
            method = "none"
            expanded_query = query
            expansion_terms = []
            feedback_documents_used = 0
            strategy = "hybrid"

        fetch_k = self._candidate_fetch_k(top_k, source_ids)
        if expansion_enabled:
            results = self._hybrid_retriever.search(
                query=query,
                vector_query=expanded_query,
                top_k=fetch_k,
            )
        else:
            results = self._hybrid_retriever.search(query=query, top_k=fetch_k)

        enriched_results = self._enrich_results(
            results=results,
            limit=top_k,
            source_ids=source_ids,
        )

        return ExpandedSearchResult(
            original_query=query,
            expanded_query=expanded_query,
            expansion_terms=expansion_terms,
            method=method,
            strategy=strategy,
            expansion_enabled=expansion_enabled,
            feedback_documents_used=feedback_documents_used,
            results=enriched_results,
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

    def _enrich_results(
        self,
        results: list[object],
        limit: int,
        source_ids: list[str] | None,
    ) -> list[SearchResultItem]:
        chunk_ids = [self._result_chunk_id(result) for result in results]
        chunks_by_id = self._chunk_repo.get_chunks(chunk_ids)
        scores_by_id = {
            self._result_chunk_id(result): float(getattr(result, "score", 0.0))
            for result in results
        }

        enriched_results: list[SearchResultItem] = []
        for chunk_id in chunk_ids:
            chunk = chunks_by_id.get(chunk_id)
            if chunk is None:
                continue
            source_id = str(getattr(chunk, "source_id", "") or "")
            if source_ids is not None and source_id not in source_ids:
                continue
            enriched_results.append(
                SearchResultItem(
                    chunk_id=chunk_id,
                    score=scores_by_id.get(chunk_id, 0.0),
                    source_id=source_id,
                    url=str(getattr(chunk, "url", "") or ""),
                    title=str(getattr(chunk, "title", "") or ""),
                    breadcrumb=str(getattr(chunk, "breadcrumb", "") or ""),
                    text=str(getattr(chunk, "text", "") or ""),
                )
            )
            if len(enriched_results) >= limit:
                break

        return enriched_results
