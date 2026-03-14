from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from src.indexing.structures.corpus_stats import CorpusStats
from src.indexing.structures.inverted_index import InvertedIndex


@dataclass(slots=True)
class IndexSegment:
    """Materialized segment payload ready for persistence."""

    segment_id: str
    dictionary: dict[str, int]
    postings: dict[str, list[dict[str, int | str]]]
    doc_lengths: dict[str, int]
    stats: dict[str, int | float]


class SegmentBuilder:
    """Converts active in-memory structures into a persistent segment."""

    def _build_segment_id(self) -> str:
        """Generate a unique immutable segment identifier."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        return f"segment_{timestamp}_{uuid4().hex[:8]}"

    def build(
        self,
        inverted_index: InvertedIndex,
        doc_lengths: dict[str, int],
        corpus_stats: CorpusStats,
    ) -> IndexSegment:
        """Build an immutable segment snapshot."""
        dictionary, postings = inverted_index.to_serializable()
        segment_id = self._build_segment_id()

        return IndexSegment(
            segment_id=segment_id,
            dictionary=dictionary,
            postings=postings,
            doc_lengths=dict(sorted(doc_lengths.items())),
            stats=corpus_stats.to_dict(),
        )

    def build_from_components(
        self,
        dictionary: dict[str, int],
        postings: dict[str, list[dict[str, int | str]]],
        doc_lengths: dict[str, int],
        stats: dict[str, int | float],
    ) -> IndexSegment:
        """Build an immutable segment from already materialized data."""
        return IndexSegment(
            segment_id=self._build_segment_id(),
            dictionary=dict(sorted(dictionary.items())),
            postings=dict(sorted(postings.items())),
            doc_lengths=dict(sorted(doc_lengths.items())),
            stats=dict(stats),
        )
