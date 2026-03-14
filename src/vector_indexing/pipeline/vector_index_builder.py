from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

import numpy as np

from src.vector_indexing.config.settings import VectorSettings
from src.vector_indexing.encoder.embedding_model import EmbeddingModel
from src.vector_indexing.index.faiss_index import FaissIndex
from src.vector_indexing.index.vector_store import VectorStore
from src.vector_indexing.storage.index_reader import IndexReader
from src.vector_indexing.storage.index_writer import IndexWriter

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class IndexedVectorDocument:
    """Result metadata after indexing a document."""

    doc_id: str
    buffered_documents: int
    indexed_documents: int
    persisted: bool


@dataclass(slots=True)
class VectorSearchResult:
    """Resolved search result ready for API serialization."""

    doc_id: str
    score: float


class VectorIndexBuilder:
    """Coordinates embedding generation, batching, indexing and persistence."""

    def __init__(self, settings: VectorSettings) -> None:
        self.settings = settings
        self.embedding_model = EmbeddingModel(
            model_name=settings.embedding_model,
            expected_dimension=settings.vector_dimension,
        )
        self.index_reader = IndexReader(settings.vector_index_path)
        self.index_writer = IndexWriter(settings.vector_index_path)
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
        """Load the persisted FAISS index if it exists."""
        persisted_index = self.index_reader.load()
        if persisted_index is None:
            logger.info("vector_index_initialized empty=true path=%s", self.settings.vector_index_path)
            return

        self.vector_store = persisted_index.vector_store
        self.faiss_index = FaissIndex(
            dimension=self.settings.vector_dimension,
            index_type=self.settings.faiss_index_type,
            hnsw_m=self.settings.hnsw_m,
            ef_construction=self.settings.hnsw_ef_construction,
            ef_search=self.settings.hnsw_ef_search,
            index=persisted_index.index,
        )
        logger.info("vector_index_loaded vectors=%s path=%s", len(self.vector_store), self.settings.vector_index_path)

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

    def search(self, query: str, top_k: int) -> list[VectorSearchResult]:
        """Search nearest documents for the provided query."""
        if not query.strip():
            raise ValueError("Search query must not be empty.")

        with self._lock:
            if self._buffer_doc_ids:
                self._flush_locked(force=True)

            if len(self.vector_store) == 0:
                return []

            query_vector = self.embedding_model.encode_one(query)
            search_k = min(top_k, len(self.vector_store))
            scores, vector_ids = self.faiss_index.search(query_vector, search_k)

            results: list[VectorSearchResult] = []
            for score, vector_id in zip(scores[0], vector_ids[0]):
                if int(vector_id) < 0:
                    continue
                doc_id = self.vector_store.get_doc_id(int(vector_id))
                if doc_id is None:
                    continue
                results.append(VectorSearchResult(doc_id=doc_id, score=float(score)))
            return results

    def _flush_locked(self, force: bool = False) -> bool:
        """Flush buffered vectors into FAISS and persist the index."""
        if not self._buffer_doc_ids:
            return False

        if not force and len(self._buffer_doc_ids) < self.settings.vector_batch_size:
            return False

        vectors = np.vstack(self._buffer_vectors).astype(np.float32)
        self.vector_store.add_documents(self._buffer_doc_ids)
        self.faiss_index.add(vectors)
        self._persist()
        self._buffer_doc_ids = []
        self._buffer_vectors = []
        return True

    def _persist(self) -> None:
        """Persist the FAISS index and vector-store metadata to disk."""
        metadata = {
            "embedding_model": self.settings.embedding_model,
            "vector_dimension": self.settings.vector_dimension,
            "faiss_index_type": self.settings.faiss_index_type,
            "hnsw_m": self.settings.hnsw_m,
            "hnsw_ef_construction": self.settings.hnsw_ef_construction,
            "hnsw_ef_search": self.settings.hnsw_ef_search,
            "vector_count": len(self.vector_store),
        }
        self.index_writer.write(self.faiss_index.index, self.vector_store, metadata)
