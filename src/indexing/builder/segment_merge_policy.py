from __future__ import annotations


class SegmentMergePolicy:
    """Selects the oldest segments once the active segment count exceeds a threshold."""

    def __init__(self, max_segments: int) -> None:
        if max_segments < 2:
            raise ValueError("INDEX_MAX_SEGMENTS_IN_MEMORY must be at least 2 to support merging.")
        self.max_segments = max_segments

    def select_candidates(self, segment_ids: list[str]) -> list[str]:
        """Return the oldest segment IDs that should be merged into one new segment."""
        ordered = sorted(segment_ids)
        if len(ordered) <= self.max_segments:
            return []

        batch_size = len(ordered) - self.max_segments + 1
        batch_size = max(2, batch_size)
        return ordered[:batch_size]
