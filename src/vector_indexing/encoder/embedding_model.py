from __future__ import annotations

import logging
import threading

import numpy as np
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class EmbeddingModel:
    """Lazy-loading encoder for dense text embeddings.

    The model is loaded only when the first encode request arrives so the
    service startup stays light even when the model weights are large.
    """

    def __init__(self, model_name: str, expected_dimension: int) -> None:
        self.model_name = model_name
        self.expected_dimension = expected_dimension
        self._model: SentenceTransformer | None = None
        self._lock = threading.Lock()

    def encode(self, texts: list[str]) -> np.ndarray:
        """Encode a batch of texts into normalized float32 vectors."""
        if not texts:
            return np.empty((0, self.expected_dimension), dtype=np.float32)

        model = self._get_model()
        vectors = model.encode(
            texts,
            batch_size=len(texts),
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        vectors = np.asarray(vectors, dtype=np.float32)
        if vectors.ndim == 1:
            vectors = vectors.reshape(1, -1)

        if vectors.shape[1] != self.expected_dimension:
            raise ValueError(
                f"Embedding dimension mismatch. Expected {self.expected_dimension}, got {vectors.shape[1]}."
            )

        return vectors

    def encode_one(self, text: str) -> np.ndarray:
        """Encode a single text and return its vector."""
        return self.encode([text])[0]

    def _get_model(self) -> SentenceTransformer:
        """Load the sentence-transformers model once."""
        if self._model is not None:
            return self._model

        with self._lock:
            if self._model is None:
                logger.info("loading_embedding_model model=%s", self.model_name)
                self._model = SentenceTransformer(self.model_name)
        return self._model
