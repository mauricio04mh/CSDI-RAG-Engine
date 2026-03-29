from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass

from src.indexing.builder.segment_builder import IndexSegment, SegmentBuilder
from src.indexing.storage.segment_reader import SegmentReader
from src.indexing.storage.segment_writer import SegmentWriter

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MergeResult:
    """Metadata describing a completed merge operation."""

    merged_segment_id: str
    source_segment_ids: list[str]
    documents_merged: int
    terms_merged: int


class SegmentMerger:
    """Reads, combines and rewrites segments while preserving immutable outputs."""

    def __init__(
        self,
        segment_reader: SegmentReader,
        segment_writer: SegmentWriter,
        segment_builder: SegmentBuilder,
    ) -> None:
        self.segment_reader = segment_reader
        self.segment_writer = segment_writer
        self.segment_builder = segment_builder

    def merge(self, segment_ids: list[str]) -> MergeResult | None:
        """Merge multiple persisted segments into a single new segment atomically."""
        if len(segment_ids) < 2:
            return None

        ordered_ids = sorted(segment_ids)
        segments = [self.segment_reader.read(segment_id) for segment_id in ordered_ids]
        merged_segment = self._build_merged_segment(segments)

        # Atomic write + delete via repository
        self.segment_writer.repository.merge_and_replace(merged_segment, ordered_ids)

        result = MergeResult(
            merged_segment_id=merged_segment.segment_id,
            source_segment_ids=[s.segment_id for s in segments],
            documents_merged=int(merged_segment.stats["total_documents"]),
            terms_merged=int(merged_segment.stats["total_terms"]),
        )
        logger.info(
            "segment_merge_completed merged_segment_id=%s source_segments=%s",
            result.merged_segment_id,
            ",".join(result.source_segment_ids),
        )
        return result

    def _build_merged_segment(self, segments: list[IndexSegment]) -> IndexSegment:
        merged_postings: dict[str, list[dict]] = defaultdict(list)
        merged_doc_lengths: dict[str, int] = {}

        for segment in segments:
            self._merge_doc_lengths(merged_doc_lengths, segment)
            self._merge_postings(merged_postings, segment)

        dictionary = {t: len(p) for t, p in sorted(merged_postings.items())}
        postings = dict(sorted(merged_postings.items()))
        total_documents = len(merged_doc_lengths)
        total_terms = sum(merged_doc_lengths.values())
        stats = {
            "total_documents": total_documents,
            "total_terms": total_terms,
            "average_document_length": total_terms / total_documents if total_documents else 0.0,
        }
        return self.segment_builder.build_from_components(
            dictionary=dictionary, postings=postings, doc_lengths=merged_doc_lengths, stats=stats
        )

    def _merge_doc_lengths(self, merged: dict[str, int], segment: IndexSegment) -> None:
        for doc_id, length in segment.doc_lengths.items():
            if doc_id in merged:
                raise ValueError(f"Duplicate doc_id '{doc_id}' found while merging segments.")
            merged[doc_id] = int(length)

    def _merge_postings(self, merged: dict[str, list[dict]], segment: IndexSegment) -> None:
        for term, postings in segment.postings.items():
            merged[term].extend(
                {"doc_id": str(p["doc_id"]), "tf": int(p["tf"])} for p in postings
            )
