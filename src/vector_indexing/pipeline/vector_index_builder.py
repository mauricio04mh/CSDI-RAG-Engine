from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

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
    """Result metadata after indexing a document."""

    doc_id: str
    buffered_documents: int
    indexed_documents: int
    persisted: bool


class VectorIndexBuilder:
    """Coordinates embedding generation, batching, FAISS indexing and DB persistence.

    PostgreSQL (via pgvector) is the source of truth. FAISS is rebuilt from the
    database on startup and used for fast ANN search at query time.
    """

    def __init__(self, settings: VectorSettings, engine: Engine) -> None:
        self.settings = settings
        self.embedding_model = EmbeddingModel(
            model_name=settings.embedding_model,
            expected_dimension=settings.vector_dimension,
        )
        self._vector_repo = VectorRepository(engine)
        self.vector_store = VectorStore()
        self.faiss_index = FaissIndex(
            dimension=settings.vector_dimension,
            index_type=settings.faiss_index_type,
            hnsw_m=settings.hnsw_m,
            ef_construction=settings.hnsw_ef_construction,
            ef_search=settings.hnsw_ef_search,
        )
        self._buffer_doc_ids: list[str] = []
        self._buffer_vectors: list[np.ndarray] = []
        self._lock = threading.RLock()

    def start(self) -> None:
        """Seed the in-memory FAISS index from the database."""
        doc_ids, vectors = self._vector_repo.load_all_documents()
        if not doc_ids:
            logger.info("vector_index_initialized empty=true")
            return

        self.vector_store = VectorStore()
        self.faiss_index = FaissIndex(
            dimension=self.settings.vector_dimension,
            index_type=self.settings.faiss_index_type,
            hnsw_m=self.settings.hnsw_m,
            ef_construction=self.settings.hnsw_ef_construction,
            ef_search=self.settings.hnsw_ef_search,
        )
        # Load in batches to avoid memory spikes on large corpora
        batch_size = 1000
        for i in range(0, len(doc_ids), batch_size):
            batch_ids = doc_ids[i : i + batch_size]
            batch_vectors = vectors[i : i + batch_size]
            self.vector_store.add_documents(batch_ids)
            self.faiss_index.add(batch_vectors)

        logger.info("vector_index_loaded vectors=%s", len(self.vector_store))

    def stop(self) -> None:
        """Flush and persist any buffered vectors before shutdown."""
        with self._lock:
            self._flush_locked(force=True)

    def add_document(self, doc_id: str, text: str) -> IndexedVectorDocument:
        """Generate an embedding for the document and enqueue it for indexing."""
        if not text.strip():
            raise ValueError("Document text must not be empty.")

        with self._lock:
            if doc_id in self.vector_store.doc_ids_to_vector_ids or doc_id in self._buffer_doc_ids:
                raise ValueError(f"Document '{doc_id}' already exists in the vector index.")

            vector = self.embedding_model.encode_one(text)
            self._buffer_doc_ids.append(doc_id)
            self._buffer_vectors.append(vector)

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
        """Flush buffered vectors into FAISS and persist to the database."""
        if not self._buffer_doc_ids:
            return False

        if not force and len(self._buffer_doc_ids) < self.settings.vector_batch_size:
            return False

        vectors = np.vstack(self._buffer_vectors).astype(np.float32)
        self.vector_store.add_documents(self._buffer_doc_ids)
        self.faiss_index.add(vectors)
        self._persist(self._buffer_doc_ids, vectors)
        self._buffer_doc_ids = []
        self._buffer_vectors = []
        return True

    def _persist(self, doc_ids: list[str], vectors: np.ndarray) -> None:
        """Persist new vectors to the database."""
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
