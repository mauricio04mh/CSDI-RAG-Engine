from __future__ import annotations

import logging

from src.indexing.builder.segment_builder import IndexSegment

logger = logging.getLogger(__name__)


class SegmentWriter:
    """Writes immutable segments to the database via BM25Repository."""

    def __init__(self, repository) -> None:
        self.repository = repository

    def write(self, segment: IndexSegment) -> str:
        """Persist a segment and return its segment_id."""
        self.repository.write_segment(segment)
        return segment.segment_id

    def delete(self, segment_id: str) -> None:
        """Delete a persisted segment."""
        self.repository.delete_segment(segment_id)
