from __future__ import annotations

import logging
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
        with self._lock:
            count = len(self._buffer_doc_ids)
            self._flush_locked(force=True)
            return count

    def stop(self) -> None:
        with self._lock:
            self._flush_locked(force=True)

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
            if doc_id in self.vector_store.doc_ids_to_vector_ids or doc_id in self._buffer_doc_ids:
                raise ValueError(f"Document '{doc_id}' already exists in the vector index.")

            # Buffer raw text; the whole batch is embedded at once in _flush_locked().
            self._buffer_doc_ids.append(doc_id)
            self._buffer_texts.append(text)

            persisted = False
            if len(self._buffer_doc_ids) >= self.settings.vector_batch_size:
                self._flush_locked(force=True)
                persisted = True

            return IndexedVectorDocument(
                doc_id=doc_id,
                buffered_documents=len(self._buffer_doc_ids),
                indexed_documents=len(self.vector_store),
                persisted=persisted,
            )

    def _flush_locked(self, force: bool = False) -> bool:
        """Embed the buffered texts in one batch, then persist to FAISS and the database."""
        if not self._buffer_doc_ids:
            return False
        if not force and len(self._buffer_doc_ids) < self.settings.vector_batch_size:
            return False

        vectors = self.embedding_model.encode(self._buffer_texts)
        self.vector_store.add_documents(self._buffer_doc_ids)
        self.faiss_index.add(vectors)
        self._persist(self._buffer_doc_ids, vectors)
        self._buffer_doc_ids = []
        self._buffer_texts = []
        return True

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
