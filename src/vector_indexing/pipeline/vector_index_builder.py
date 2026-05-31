from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path

import faiss as faiss_lib
import numpy as np
from sqlalchemy.engine import Engine

from src.database.repositories.vector_repository import VectorRepository
from src.vector_indexing.config.settings import VectorSettings
from src.vector_indexing.encoder.embedding_model import EmbeddingModel
from src.vector_indexing.index.faiss_index import FaissIndex
from src.vector_indexing.index.vector_store import VectorStore

logger = logging.getLogger(__name__)

# Fraction of available CPU cores reserved for background ingestion encoding.
# The remaining cores stay available for real-time search query encoding.
_INGEST_CPU_FRACTION = 0.5


def _encode_with_limited_threads(embedding_model, texts: list[str]):
    """Run embedding_model.encode() with a reduced PyTorch intra-op thread count.

    During ingestion we cap OpenMP/MKL threads to _INGEST_CPU_FRACTION of the
    available cores. This leaves dedicated CPU capacity for concurrent search
    query encoding so searches aren't starved by large ingestion batches.

    The original thread count is always restored in a finally block.
    """
    try:
        import torch
        total = os.cpu_count() or 1
        ingest_threads = max(1, int(total * _INGEST_CPU_FRACTION))
        old_threads = torch.get_num_threads()
        torch.set_num_threads(ingest_threads)
        try:
            return embedding_model.encode(texts)
        finally:
            torch.set_num_threads(old_threads)
    except Exception:
        # torch unavailable or set_num_threads failed — fall back gracefully
        return embedding_model.encode(texts)


@dataclass(slots=True)
class IndexedVectorDocument:
    doc_id: str
    buffered_documents: int
    indexed_documents: int
    persisted: bool


class VectorIndexBuilder:
    """Coordinates embedding generation, batching, FAISS indexing and DB persistence.

    PostgreSQL (via pgvector) is the source of truth. FAISS is rebuilt from the
    database on startup and used for fast ANN search at query time.
    """

    def __init__(
        self,
        settings: VectorSettings,
        engine: Engine,
        vector_repo: VectorRepository | None = None,
        embedding_model: EmbeddingModel | None = None,
    ) -> None:
        self.settings = settings
        self.embedding_model = embedding_model or EmbeddingModel(
            model_name=settings.embedding_model,
            expected_dimension=settings.vector_dimension,
        )
        self._vector_repo = vector_repo or VectorRepository(engine)
        self.vector_store = VectorStore()
        self.faiss_index = FaissIndex(
            dimension=settings.vector_dimension,
            index_type=settings.faiss_index_type,
            hnsw_m=settings.hnsw_m,
            ef_construction=settings.hnsw_ef_construction,
            ef_search=settings.hnsw_ef_search,
        )
        self._buffer_doc_ids: list[str] = []
        self._buffer_texts: list[str] = []    # raw texts; embedded in batch at flush
        self._lock = threading.RLock()
        # Serializes flush operations so at most one batch is encoding at a time.
        # Held during the slow encode step; _lock is released then, so searches
        # can read the FAISS index concurrently with an ongoing ingestion flush.
        self._encode_lock = threading.Lock()
        # Tracks doc_ids that have been drained from the buffer but not yet
        # committed to vector_store. Included in the duplicate check so a
        # concurrent add_document() cannot race into the gap between drain and commit.
        self._inflight_doc_ids: set[str] = set()

    def start(self) -> None:
        """Seed the in-memory FAISS index from the database."""
        stored = self._vector_repo.load_metadata()
        if stored is not None:
            mismatches = []
            if stored["embedding_model"] != self.settings.embedding_model:
                mismatches.append(
                    f"embedding_model: stored={stored['embedding_model']!r} "
                    f"!= configured={self.settings.embedding_model!r}"
                )
            if stored["vector_dimension"] != self.settings.vector_dimension:
                mismatches.append(
                    f"vector_dimension: stored={stored['vector_dimension']} "
                    f"!= configured={self.settings.vector_dimension}"
                )
            if mismatches:
                if self.settings.faiss_index_path:
                    _p = Path(self.settings.faiss_index_path)
                    if _p.exists():
                        _p.unlink()
                        logger.warning("stale_disk_index_removed path=%s", _p)
                raise RuntimeError(
                    "Vector index metadata mismatch — the database contains vectors "
                    "built with a different model. Clear the index first:\n"
                    "  TRUNCATE vector_documents, vector_index_metadata RESTART IDENTITY;\n"
                    "Mismatches:\n" + "\n".join(f"  {m}" for m in mismatches)
                )

        # Fast path: load FAISS binary from disk, only fetch doc_ids from DB.
        if self.settings.faiss_index_path:
            index_path = Path(self.settings.faiss_index_path)
            if index_path.exists():
                loaded = faiss_lib.read_index(str(index_path))
                doc_ids_only = self._vector_repo.load_all_doc_ids()
                if loaded.d != self.settings.vector_dimension:
                    logger.warning(
                        "disk_index_dimension_mismatch expected=%s got=%s — rebuilding from DB",
                        self.settings.vector_dimension, loaded.d,
                    )
                elif loaded.ntotal != len(doc_ids_only):
                    logger.warning(
                        "disk_index_count_mismatch faiss=%s db=%s — rebuilding from DB",
                        loaded.ntotal, len(doc_ids_only),
                    )
                else:
                    with self._lock:
                        self.faiss_index._index = loaded
                        self.faiss_index._configure_index()
                        self.vector_store.vector_ids_to_doc_ids.clear()
                        self.vector_store.doc_ids_to_vector_ids.clear()
                        self.vector_store.add_documents(doc_ids_only)
                    logger.info(
                        "vector_index_loaded_from_disk path=%s vectors=%s",
                        index_path, len(self.vector_store),
                    )
                    return

        doc_ids, vectors = self._vector_repo.load_all_documents()
        if not doc_ids:
            logger.info("vector_index_initialized empty=true")
            return

        with self._lock:
            # Reset in-place: replacing these objects would silently break VectorRetriever's references.
            self.vector_store.vector_ids_to_doc_ids.clear()
            self.vector_store.doc_ids_to_vector_ids.clear()
            self.faiss_index._index = self.faiss_index._create_index()
            self.faiss_index._configure_index()
            self._buffer_doc_ids = []
            self._buffer_texts = []

            batch_size = 1000
            for i in range(0, len(doc_ids), batch_size):
                batch_ids = doc_ids[i : i + batch_size]
                batch_vectors = vectors[i : i + batch_size]
                self.vector_store.add_documents(batch_ids)
                self.faiss_index.add(batch_vectors)

        logger.info("vector_index_loaded vectors=%s", len(self.vector_store))

    def flush(self) -> int:
        """Flush any remaining buffered vectors to FAISS and the database.

        Returns the number of documents that were flushed.
        """
        return self._flush(force=True)

    def stop(self) -> None:
        self._flush(force=True)

    def remove_documents(self, doc_ids: list[str]) -> int:
        """Bulk soft-delete documents. Tombstones them in memory so searches skip them immediately."""
        count = self._vector_repo.delete_by_doc_ids(doc_ids)
        if count > 0:
            with self._lock:
                for doc_id in doc_ids:
                    if doc_id in self.vector_store.doc_ids_to_vector_ids:
                        self.vector_store.mark_deleted(doc_id)
        return count

    def remove_document(self, doc_id: str) -> bool:
        """Soft-delete a document from the vector index.

        Marks it deleted in the DB and tombstones it in memory so searches skip it.
        The FAISS slot is reclaimed on the next restart.
        """
        deleted = self._vector_repo.delete_document(doc_id)
        if deleted:
            with self._lock:
                self.vector_store.mark_deleted(doc_id)
        return deleted

    def add_document(self, doc_id: str, text: str) -> IndexedVectorDocument:
        if not text.strip():
            raise ValueError("Document text must not be empty.")

        with self._lock:
            if (
                doc_id in self.vector_store.doc_ids_to_vector_ids
                or doc_id in self._buffer_doc_ids
                or doc_id in self._inflight_doc_ids
            ):
                raise ValueError(f"Document '{doc_id}' already exists in the vector index.")

            self._buffer_doc_ids.append(doc_id)
            self._buffer_texts.append(text)
            should_flush = len(self._buffer_doc_ids) >= self.settings.vector_batch_size

        # Flush outside _lock so the slow encode step does not block searches.
        if should_flush:
            self._flush(force=True)

        with self._lock:
            return IndexedVectorDocument(
                doc_id=doc_id,
                buffered_documents=len(self._buffer_doc_ids),
                indexed_documents=len(self.vector_store),
                persisted=should_flush,
            )

    def _flush(self, force: bool = False) -> int:
        """Drain the buffer, encode outside _lock, then commit to FAISS.

        _encode_lock serializes concurrent flush calls so only one batch
        encodes at a time. _lock is released before encoding, letting
        VectorRetriever.search() read the FAISS index without waiting for
        the (potentially slow) CPU encoding step to finish.

        Returns the number of documents flushed (0 when there was nothing to do).
        """
        with self._encode_lock:
            # Snapshot and drain the buffer atomically. Mark drained docs as
            # inflight so concurrent add_document() calls see them during the
            # gap between drain and vector_store commit (closes race condition).
            with self._lock:
                if not self._buffer_doc_ids:
                    return 0
                if not force and len(self._buffer_doc_ids) < self.settings.vector_batch_size:
                    return 0
                batch_doc_ids = self._buffer_doc_ids
                batch_texts = self._buffer_texts
                self._buffer_doc_ids = []
                self._buffer_texts = []
                self._inflight_doc_ids.update(batch_doc_ids)

            # Encode while holding _encode_lock but NOT _lock.
            # Searches can read the FAISS index freely during this step.
            # Limit PyTorch/OpenMP threads to half available CPUs so search
            # query encoding always has dedicated cores and is not starved.
            vectors = _encode_with_limited_threads(self.embedding_model, batch_texts)

            # Commit to the in-memory index under _lock, clearing inflight first.
            with self._lock:
                self._inflight_doc_ids.difference_update(batch_doc_ids)
                self.vector_store.add_documents(batch_doc_ids)
                self.faiss_index.add(vectors)

            # DB + disk persistence needs no in-memory lock.
            # On failure: soft-delete the in-memory entries so searches skip
            # them, and they can be re-indexed cleanly after a restart.
            try:
                self._persist(batch_doc_ids, vectors)
            except Exception:
                with self._lock:
                    for doc_id in batch_doc_ids:
                        self.vector_store.mark_deleted(doc_id)
                logger.error(
                    "vector_persist_failed count=%s — in-memory entries soft-deleted; "
                    "will be re-indexable after restart",
                    len(batch_doc_ids),
                )
                raise
            return len(batch_doc_ids)

    def _persist(self, doc_ids: list[str], vectors: np.ndarray) -> None:
        """Persist new vectors to the database and optionally to a FAISS binary file."""
        self._vector_repo.save_documents(doc_ids, vectors)
        metadata = {
            "embedding_model": self.settings.embedding_model,
            "vector_dimension": self.settings.vector_dimension,
            "faiss_index_type": self.settings.faiss_index_type,
            "hnsw_m": self.settings.hnsw_m,
            "hnsw_ef_construction": self.settings.hnsw_ef_construction,
            "hnsw_ef_search": self.settings.hnsw_ef_search,
            "vector_count": len(self.vector_store),
        }
        self._vector_repo.save_metadata(metadata)
        if self.settings.faiss_index_path:
            path = Path(self.settings.faiss_index_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            faiss_lib.write_index(self.faiss_index._index, str(path))
            logger.info("faiss_index_persisted_to_disk path=%s", path)
