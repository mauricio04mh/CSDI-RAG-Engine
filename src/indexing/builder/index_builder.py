from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from src.database.repositories.bm25_repository import BM25Repository
from src.indexing.builder.segment_builder import IndexSegment, SegmentBuilder
from src.indexing.builder.segment_merge_policy import SegmentMergePolicy
from src.indexing.builder.segment_merger import MergeResult, SegmentMerger
from src.indexing.builder.state import ActiveIndexState
from src.indexing.builder.term_counter import count_terms
from src.indexing.config.settings import Settings
from src.indexing.storage.segment_reader import SegmentReader
from src.indexing.storage.segment_writer import SegmentWriter

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class IndexedDocumentResult:
    doc_id: str
    buffered_documents: int
    flushed: bool
    segment_id: str | None = None


@dataclass(slots=True)
class MergeExecutionResult:
    merges: list[MergeResult]

    @property
    def total_merges(self) -> int:
        return len(self.merges)


class IndexBuilder:
    """Coordinates in-memory indexing and periodic segment flushing."""

    def __init__(self, settings: Settings, engine: Engine) -> None:
        self.settings = settings
        self.active_state = ActiveIndexState()
        self.segment_builder = SegmentBuilder()
        self._bm25_repo = BM25Repository(engine)
        self.segment_writer = SegmentWriter(self._bm25_repo)
        self.segment_reader = SegmentReader(self._bm25_repo)
        self.segment_merge_policy = SegmentMergePolicy(settings.index_max_segments_in_memory)
        self.segment_merger = SegmentMerger(
            segment_reader=self.segment_reader,
            segment_writer=self.segment_writer,
            segment_builder=self.segment_builder,
        )
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._flush_thread: threading.Thread | None = None
        self._last_flush_time = time.monotonic()

        existing_segments = self.segment_reader.list_segments()
        logger.info("index_builder_initialized existing_segments=%s", len(existing_segments))

    def start(self) -> None:
        if self._flush_thread and self._flush_thread.is_alive():
            return
        self._stop_event.clear()
        self._flush_thread = threading.Thread(target=self._flush_worker, name="index-flush-worker", daemon=True)
        self._flush_thread.start()
        self.merge_segments()
        logger.info("flush_worker_started interval=%ss", self.settings.index_flush_interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=self.settings.index_flush_interval + 1)
        self.flush(force=True)
        logger.info("index_builder_stopped")

    def add_document(self, doc_id: str, tokens: list[str]) -> IndexedDocumentResult:
        if not tokens:
            raise ValueError("Document must contain at least one token.")
        term_frequencies = count_terms(tokens)
        document_length = len(tokens)

        with self._lock:
            self.active_state.record_document(
                doc_id=doc_id, term_frequencies=term_frequencies, document_length=document_length
            )
            logger.info(
                "document_indexed doc_id=%s terms=%s doc_length=%s buffered=%s",
                doc_id, len(term_frequencies), document_length, self.active_state.buffered_documents,
            )
            if self.active_state.buffered_documents >= self.settings.index_buffer_size:
                buffered = self.active_state.buffered_documents
                segment = self._flush_locked()
                return IndexedDocumentResult(doc_id=doc_id, buffered_documents=buffered, flushed=True, segment_id=segment.segment_id if segment else None)
            return IndexedDocumentResult(doc_id=doc_id, buffered_documents=self.active_state.buffered_documents, flushed=False)

    def flush(self, force: bool = False) -> str | None:
<<<<<<< HEAD
=======
        """Flush the in-memory segment to the database if thresholds require it."""
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
        with self._lock:
            segment = self._flush_locked(force=force)
            return segment.segment_id if segment else None

    def merge_segments(self) -> MergeExecutionResult:
        with self._lock:
            return self._merge_segments_locked()

    def _flush_locked(self, force: bool = False) -> IndexSegment | None:
        if not self.active_state.has_documents():
            return None
        if not force and self.active_state.buffered_documents < self.settings.index_buffer_size:
            return None

        segment = self.segment_builder.build(
            inverted_index=self.active_state.inverted_index,
            doc_lengths=self.active_state.doc_lengths,
            corpus_stats=self.active_state.corpus_stats,
        )
        self.segment_writer.write(segment)
        flushed = self.active_state.buffered_documents
        self.active_state = ActiveIndexState()
        self._last_flush_time = time.monotonic()
        merge_result = self._merge_segments_locked()
        logger.info("segment_flushed segment_id=%s documents=%s", segment.segment_id, flushed)
        if merge_result.total_merges:
            logger.info("post_flush_merges_completed merges=%s", merge_result.total_merges)
        return segment

    def _flush_worker(self) -> None:
        while not self._stop_event.wait(timeout=1):
            if time.monotonic() - self._last_flush_time < self.settings.index_flush_interval:
                continue
            with self._lock:
                if not self.active_state.has_documents():
                    self._last_flush_time = time.monotonic()
                    continue
                segment = self._flush_locked(force=True)
                if segment:
                    logger.info("interval_flush_completed segment_id=%s", segment.segment_id)

    def _merge_segments_locked(self) -> MergeExecutionResult:
        merge_results: list[MergeResult] = []
        while True:
            segment_ids = self.segment_reader.list_segments()
            candidates = self.segment_merge_policy.select_candidates(segment_ids)
            if not candidates:
                break
<<<<<<< HEAD
            logger.info("segment_merge_started candidates=%s", ",".join(candidates))
=======

            logger.info(
                "segment_merge_started candidate_count=%s candidates=%s",
                len(candidates),
                ",".join(candidates),
            )
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
            result = self.segment_merger.merge(candidates)
            if result is None:
                break
            merge_results.append(result)
        return MergeExecutionResult(merges=merge_results)
