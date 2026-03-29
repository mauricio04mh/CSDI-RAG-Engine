from __future__ import annotations

from src.indexing.builder.segment_builder import IndexSegment


class SegmentReader:
    """Reads persisted segments from the database via BM25Repository."""

    def __init__(self, repository) -> None:  # BM25Repository — avoid circular import
        self.repository = repository

    def list_segments(self) -> list[str]:
        """Return all segment IDs ordered by creation time."""
        return self.repository.list_segment_ids()

    def count_segments(self) -> int:
        """Return the current number of active segments."""
        return len(self.list_segments())

    def read(self, segment_id: str) -> IndexSegment:
        """Load a segment from the database."""
        return self.repository.read_segment(segment_id)
