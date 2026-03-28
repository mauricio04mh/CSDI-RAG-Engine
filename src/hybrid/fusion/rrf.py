from __future__ import annotations

from collections import defaultdict


def reciprocal_rank_fusion(
    ranked_lists: list[list[str]],
    k: int = 60,
) -> list[tuple[str, float]]:
    """Fuse multiple ranked lists into one using Reciprocal Rank Fusion.

    Each document gets a score of 1 / (k + rank) per list it appears in.
    Scores are summed across lists and the result is sorted descending.

    Args:
        ranked_lists: Each inner list is an ordered sequence of doc_ids
                      (position 0 = best match).
        k: Smoothing constant. Higher k reduces the impact of top ranks.
           Default 60 is the standard value from the original RRF paper.

    Returns:
        List of (doc_id, score) tuples sorted by score descending.
    """
    scores: dict[str, float] = defaultdict(float)
    for ranked_list in ranked_lists:
        for rank, doc_id in enumerate(ranked_list, start=1):
            scores[doc_id] += 1.0 / (k + rank)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)
