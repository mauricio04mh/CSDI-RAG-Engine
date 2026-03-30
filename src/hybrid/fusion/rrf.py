from __future__ import annotations

from collections import defaultdict


def reciprocal_rank_fusion(
    ranked_lists: list[list[str]],
    k: int = 60,
    weights: list[float] | None = None,
) -> list[tuple[str, float]]:
    """Fuse multiple ranked lists into one using weighted Reciprocal Rank Fusion.

    Each document gets a score of weight * 1 / (k + rank) per list it appears in.
    Scores are summed across lists and the result is sorted descending.

    Args:
        ranked_lists: Each inner list is an ordered sequence of doc_ids
                      (position 0 = best match).
        k: Smoothing constant. Higher k reduces the impact of top ranks.
           Default 60 is the standard value from the original RRF paper.
        weights: Per-list multipliers. Defaults to 1.0 for each list.
                 Use higher weights to emphasize a retriever's signal.
                 e.g. weights=[0.3, 0.7] gives more weight to the second list.

    Returns:
        List of (doc_id, score) tuples sorted by score descending.
    """
    if weights is None:
        weights = [1.0] * len(ranked_lists)

    if len(weights) != len(ranked_lists):
        raise ValueError("weights must have the same length as ranked_lists")

    scores: dict[str, float] = defaultdict(float)
    for ranked_list, weight in zip(ranked_lists, weights):
        for rank, doc_id in enumerate(ranked_list, start=1):
            scores[doc_id] += weight * (1.0 / (k + rank))

    return sorted(scores.items(), key=lambda x: x[1], reverse=True)
