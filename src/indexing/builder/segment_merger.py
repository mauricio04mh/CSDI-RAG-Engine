from __future__ import annotations
import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

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

    def merge(self, segment_paths: list[Path]) -> MergeResult | None:
        """Merge multiple persisted segments into a single new segment."""
        if len(segment_paths) < 2:
            return None

        ordered_paths = sorted(segment_paths)
        segments = [self.segment_reader.read(path) for path in ordered_paths]
        merged_segment = self._build_merged_segment(segments)
        self.segment_writer.write(merged_segment)

        for path in ordered_paths:
            self.segment_writer.delete(path)

        result = MergeResult(
            merged_segment_id=merged_segment.segment_id,
            source_segment_ids=[segment.segment_id for segment in segments],
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
        """Combine segment payloads into a single compact representation."""
        merged_postings: dict[str, list[dict[str, int | str]]] = defaultdict(list)
        merged_doc_lengths: dict[str, int] = {}

        for segment in segments:
            self._merge_doc_lengths(merged_doc_lengths, segment)
            self._merge_postings(merged_postings, segment)

        dictionary = {term: len(postings) for term, postings in sorted(merged_postings.items())}
        postings = {term: postings_list for term, postings_list in sorted(merged_postings.items())}
        total_documents = len(merged_doc_lengths)
        total_terms = sum(merged_doc_lengths.values())
        stats = {
            "total_documents": total_documents,
            "total_terms": total_terms,
            "average_document_length": total_terms / total_documents if total_documents else 0.0,
        }

        return self.segment_builder.build_from_components(
            dictionary=dictionary,
            postings=postings,
            doc_lengths=merged_doc_lengths,
            stats=stats,
        )

    def _merge_doc_lengths(self, merged_doc_lengths: dict[str, int], segment: IndexSegment) -> None:
        """Merge document length maps and reject duplicated document identifiers."""
        for doc_id, doc_length in segment.doc_lengths.items():
            if doc_id in merged_doc_lengths:
                raise ValueError(f"Duplicate doc_id '{doc_id}' found while merging segments.")
            merged_doc_lengths[doc_id] = int(doc_length)

    def _merge_postings(
        self,
        merged_postings: dict[str, list[dict[str, int | str]]],
        segment: IndexSegment,
    ) -> None:
        """Append postings term by term while normalizing numeric values."""
        for term, postings in segment.postings.items():
            normalized_postings = [{"doc_id": str(posting["doc_id"]), "tf": int(posting["tf"])} for posting in postings]
            merged_postings[term].extend(normalized_postings)
