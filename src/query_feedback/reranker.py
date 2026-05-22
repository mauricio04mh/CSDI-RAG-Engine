from __future__ import annotations

from src.query_feedback.schemas import (
    FeedbackAdjustedSearchResultItem,
    FeedbackMatch,
    SearchResultItem,
)

FEEDBACK_MULTIPLIER = {
    3: 0.50,
    2: 0.25,
    1: 0.05,
    0: -0.40,
}


class FeedbackReranker:
    def rerank(
        self,
        results: list[SearchResultItem],
        feedback_by_chunk_id: dict[str, FeedbackMatch],
        top_k: int,
    ) -> list[FeedbackAdjustedSearchResultItem]:
        adjusted_results: list[FeedbackAdjustedSearchResultItem] = []

        for result in results:
            feedback = feedback_by_chunk_id.get(result.chunk_id)
            original_score = result.score
            if feedback is not None:
                multiplier = FEEDBACK_MULTIPLIER[feedback.relevance]
                adjusted_score = original_score * (1 + multiplier)
                feedback_boost = adjusted_score - original_score
                adjusted_results.append(
                    FeedbackAdjustedSearchResultItem(
                        chunk_id=result.chunk_id,
                        original_score=original_score,
                        adjusted_score=adjusted_score,
                        feedback_boost=feedback_boost,
                        feedback_applied=True,
                        feedback_relevance=feedback.relevance,
                        feedback_source_query=feedback.source_query,
                        feedback_query_similarity=feedback.query_similarity,
                        feedback_match_type=feedback.match_type,
                        source_id=result.source_id,
                        url=result.url,
                        title=result.title,
                        breadcrumb=result.breadcrumb,
                        text=result.text,
                    )
                )
                continue

            adjusted_results.append(
                FeedbackAdjustedSearchResultItem(
                    chunk_id=result.chunk_id,
                    original_score=original_score,
                    adjusted_score=original_score,
                    feedback_boost=0.0,
                    feedback_applied=False,
                    feedback_relevance=None,
                    feedback_source_query=None,
                    feedback_query_similarity=None,
                    feedback_match_type=None,
                    source_id=result.source_id,
                    url=result.url,
                    title=result.title,
                    breadcrumb=result.breadcrumb,
                    text=result.text,
                )
            )

        adjusted_results.sort(
            key=lambda item: (
                -item.adjusted_score,
                -item.original_score,
                item.chunk_id,
            )
        )
        return adjusted_results[:top_k]
