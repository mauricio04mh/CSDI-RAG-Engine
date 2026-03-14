from __future__ import annotations

import logging

import faiss
import numpy as np

logger = logging.getLogger(__name__)


class FaissIndex:
    """Owns the FAISS index used for dense retrieval.

    We use normalized embeddings with inner product so the returned score
    approximates cosine similarity and is easier to combine later in a hybrid
    retrieval stack.
    """

    SUPPORTED_TYPES = {"HNSW"}

    def __init__(
        self,
        dimension: int,
        index_type: str,
        hnsw_m: int,
        ef_construction: int,
        ef_search: int,
        index: faiss.Index | None = None,
    ) -> None:
        self.dimension = dimension
        self.index_type = index_type.upper()
        self.hnsw_m = hnsw_m
        self.ef_construction = ef_construction
        self.ef_search = ef_search
        self._index = index or self._create_index()
        self._configure_index()

    @property
    def index(self) -> faiss.Index:
        """Expose the underlying FAISS index object."""
        return self._index

    @property
    def size(self) -> int:
        """Return the number of vectors currently stored."""
        return int(self._index.ntotal)

    def add(self, vectors: np.ndarray) -> None:
        """Insert vectors into the FAISS index."""
        if vectors.size == 0:
            return
        self._index.add(self._ensure_float32(vectors))
        logger.info("faiss_vectors_added count=%s total=%s", len(vectors), self.size)

    def search(self, query_vector: np.ndarray, top_k: int) -> tuple[np.ndarray, np.ndarray]:
        """Search nearest neighbors for a single query vector."""
        query = self._ensure_float32(query_vector)
        if query.ndim == 1:
            query = query.reshape(1, -1)
        return self._index.search(query, top_k)

    def set_ef_search(self, ef_search: int) -> None:
        """Update HNSW efSearch at runtime."""
        self.ef_search = ef_search
        if hasattr(self._index, "hnsw"):
            self._index.hnsw.efSearch = ef_search

    def _create_index(self) -> faiss.Index:
        """Create the configured FAISS index implementation."""
        if self.index_type not in self.SUPPORTED_TYPES:
            raise ValueError(f"Unsupported FAISS index type '{self.index_type}'.")

        index = faiss.IndexHNSWFlat(self.dimension, self.hnsw_m, faiss.METRIC_INNER_PRODUCT)
        logger.info(
            "faiss_index_created type=%s dimension=%s hnsw_m=%s",
            self.index_type,
            self.dimension,
            self.hnsw_m,
        )
        return index

    def _configure_index(self) -> None:
        """Apply HNSW tuning parameters to the current FAISS index."""
        if hasattr(self._index, "hnsw"):
            self._index.hnsw.efConstruction = self.ef_construction
            self._index.hnsw.efSearch = self.ef_search

    def _ensure_float32(self, vectors: np.ndarray) -> np.ndarray:
        """Ensure vectors match FAISS input requirements."""
        vectors = np.asarray(vectors, dtype=np.float32)
        if vectors.shape[-1] != self.dimension:
            raise ValueError(f"Expected vectors with dimension {self.dimension}, got {vectors.shape[-1]}.")
        return vectors
