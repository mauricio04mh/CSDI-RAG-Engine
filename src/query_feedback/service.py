"""Services for the Query Feedback module."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from src.query_feedback.expansion import QueryExpansionService
from src.query_feedback.reranker import FeedbackReranker
from src.query_feedback.repositories.feedback_repository import normalize_query
from src.query_feedback.schemas import (
    ExpandedSearchResult,
    ExpansionResult,
    FeedbackAdjustedSearchResultItem,
    FeedbackMatch,
    FeedbackRecord,
    FeedbackRerankResult,
    SearchResultItem,
)


class QueryFeedbackService:
    """Orchestrates query expansion using the existing retrieval pipeline."""

    def __init__(
        self,
        hybrid_retriever,
        chunk_repo,
        expansion_service: QueryExpansionService | None = None,
        feedback_repository=None,
        embedding_model=None,
        query_prefix: str = "",
    ) -> None:
        self._hybrid_retriever = hybrid_retriever
        self._chunk_repo = chunk_repo
        self._expansion_service = expansion_service or QueryExpansionService()
        self._feedback_repository = feedback_repository
        self._embedding_model = embedding_model
        self._query_prefix = query_prefix
        self._feedback_reranker = FeedbackReranker()

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

    def search_with_feedback(
        self,
        query: str,
        top_k: int = 10,
        source_ids: list[str] | None = None,
        expansion_enabled: bool = True,
        top_k_feedback: int = 5,
        max_expansion_terms: int = 6,
        feedback_enabled: bool = True,
        semantic_feedback_enabled: bool = True,
        semantic_similarity_threshold: float = 0.92,
    ) -> FeedbackRerankResult:
        if not query.strip():
            raise ValueError("query must not be empty")
        if top_k < 1:
            raise ValueError("top_k must be at least 1")
        if semantic_similarity_threshold < 0.0 or semantic_similarity_threshold > 1.0:
            raise ValueError("semantic_similarity_threshold must be between 0.0 and 1.0")

        search_result = self.search(
            query=query,
            top_k=top_k,
            source_ids=source_ids,
            expansion_enabled=expansion_enabled,
            top_k_feedback=top_k_feedback,
            max_expansion_terms=max_expansion_terms,
        )

        exact_matches: dict[str, FeedbackMatch] = {}
        semantic_matches: dict[str, FeedbackMatch] = {}
        matched_feedback_queries: list[dict[str, str | float]] = []

        if feedback_enabled and self._feedback_repository is not None:
            exact_matches = self._build_exact_feedback_map(query)
            if semantic_feedback_enabled and self._embedding_model is not None:
                semantic_matches, matched_feedback_queries = self._build_semantic_feedback_map(
                    query=query,
                    exact_matches=exact_matches,
                    semantic_similarity_threshold=semantic_similarity_threshold,
                )

        feedback_by_chunk_id = dict(exact_matches)
        for chunk_id, match in semantic_matches.items():
            feedback_by_chunk_id.setdefault(chunk_id, match)

        reranked_results = self._feedback_reranker.rerank(
            results=search_result.results,
            feedback_by_chunk_id=feedback_by_chunk_id,
            top_k=top_k,
        )

        feedback_items_used = sum(1 for item in reranked_results if item.feedback_applied)
        return FeedbackRerankResult(
            original_query=search_result.original_query,
            expanded_query=search_result.expanded_query,
            expansion_terms=search_result.expansion_terms,
            method=search_result.method,
            strategy=search_result.strategy,
            expansion_enabled=search_result.expansion_enabled,
            feedback_enabled=feedback_enabled,
            semantic_feedback_enabled=semantic_feedback_enabled,
            semantic_similarity_threshold=semantic_similarity_threshold,
            feedback_applied=feedback_items_used > 0,
            feedback_items_used=feedback_items_used,
            matched_feedback_queries=matched_feedback_queries,
            feedback_documents_used=search_result.feedback_documents_used,
            results=reranked_results,
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

    def _build_exact_feedback_map(self, query: str) -> dict[str, FeedbackMatch]:
        if self._feedback_repository is None:
            return {}
        matches: dict[str, FeedbackMatch] = {}
        for record in self._feedback_repository.get_feedback_for_query(query):
            if record.chunk_id in matches:
                continue
            matches[record.chunk_id] = FeedbackMatch(
                chunk_id=record.chunk_id,
                relevance=record.relevance,
                source_query=record.query,
                normalized_source_query=record.normalized_query,
                query_similarity=1.0,
                match_type="exact",
            )
        return matches

    def _build_semantic_feedback_map(
        self,
        query: str,
        exact_matches: dict[str, FeedbackMatch],
        semantic_similarity_threshold: float,
    ) -> tuple[dict[str, FeedbackMatch], list[dict[str, str | float]]]:
        if self._feedback_repository is None or self._embedding_model is None:
            return {}, []

        normalized_query = normalize_query(query)
        all_feedback = self._feedback_repository.list_all_feedback()
        grouped_records: dict[str, list[FeedbackRecord]] = defaultdict(list)
        representatives: dict[str, FeedbackRecord] = {}

        for record in all_feedback:
            grouped_records[record.normalized_query].append(record)
            representatives.setdefault(record.normalized_query, record)

        current_query_embedding = self._query_embedding(query)
        semantic_matches: dict[str, FeedbackMatch] = {}
        matched_feedback_queries: list[dict[str, str | float]] = []

        for candidate_normalized_query, representative in representatives.items():
            if candidate_normalized_query == normalized_query:
                continue
            similarity = self._query_similarity(current_query_embedding, representative.query)
            if similarity < semantic_similarity_threshold:
                continue
            matched_feedback_queries.append({
                "query": representative.query,
                "normalized_query": representative.normalized_query,
                "similarity": similarity,
            })
            for record in grouped_records[candidate_normalized_query]:
                if record.chunk_id in exact_matches:
                    continue
                existing = semantic_matches.get(record.chunk_id)
                if existing is not None and similarity <= existing.query_similarity:
                    continue
                semantic_matches[record.chunk_id] = FeedbackMatch(
                    chunk_id=record.chunk_id,
                    relevance=record.relevance,
                    source_query=record.query,
                    normalized_source_query=record.normalized_query,
                    query_similarity=similarity,
                    match_type="semantic",
                )

        matched_feedback_queries.sort(
            key=lambda item: (
                -float(item["similarity"]),
                str(item["normalized_query"]),
            )
        )
        return semantic_matches, matched_feedback_queries

    def _query_similarity(
        self,
        current_query_embedding: Iterable[float],
        candidate_query: str,
    ) -> float:
        candidate_embedding = self._query_embedding(candidate_query)
        return self._dot_product(current_query_embedding, candidate_embedding)

    def _query_embedding(self, query: str) -> Iterable[float]:
        return self._embedding_model.encode_query(query, prefix=self._query_prefix)

    def _dot_product(
        self,
        left: Iterable[float],
        right: Iterable[float],
    ) -> float:
        return float(sum(float(a) * float(b) for a, b in zip(left, right, strict=False)))
