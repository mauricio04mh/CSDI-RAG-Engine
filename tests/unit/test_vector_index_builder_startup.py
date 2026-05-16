from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.vector_indexing.config.settings import VectorSettings
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder


def _make_settings(**overrides) -> VectorSettings:
    base = dict(
        embedding_model="test-model",
        vector_dimension=384,
        faiss_index_type="HNSW",
        hnsw_m=32,
        hnsw_ef_construction=200,
        hnsw_ef_search=50,
        vector_batch_size=128,
        query_prefix="",
        faiss_index_path="",
        log_level="INFO",
        env_path=__import__("pathlib").Path("/tmp/.env"),
        project_root=__import__("pathlib").Path("/tmp"),
    )
    base.update(overrides)
    return VectorSettings(**base)


def _make_builder(settings: VectorSettings, stored_metadata: dict | None) -> VectorIndexBuilder:
    with patch("src.vector_indexing.pipeline.vector_index_builder.EmbeddingModel"):
        builder = VectorIndexBuilder.__new__(VectorIndexBuilder)
        builder.settings = settings
        builder.embedding_model = MagicMock()
        builder._vector_repo = MagicMock()
        builder._vector_repo.load_metadata.return_value = stored_metadata
        builder._vector_repo.load_all_documents.return_value = ([], __import__("numpy").empty((0,), dtype="float32"))
        builder.vector_store = __import__("src.vector_indexing.index.vector_store", fromlist=["VectorStore"]).VectorStore()
        builder.faiss_index = MagicMock()
        builder._buffer_doc_ids = []
        builder._buffer_texts = []
        import threading
        builder._lock = threading.RLock()
        return builder


def test_start_passes_when_no_metadata_stored():
    settings = _make_settings()
    builder = _make_builder(settings, stored_metadata=None)
    builder.start()  # must not raise


def test_start_passes_when_metadata_matches():
    settings = _make_settings()
    stored = {"embedding_model": "test-model", "vector_dimension": 384}
    builder = _make_builder(settings, stored_metadata=stored)
    builder.start()  # must not raise


def test_start_raises_on_model_mismatch():
    settings = _make_settings(embedding_model="new-model")
    stored = {"embedding_model": "old-model", "vector_dimension": 384}
    builder = _make_builder(settings, stored_metadata=stored)
    with pytest.raises(RuntimeError, match="embedding_model"):
        builder.start()


def test_start_raises_on_dimension_mismatch():
    settings = _make_settings(vector_dimension=384)
    stored = {"embedding_model": "test-model", "vector_dimension": 768}
    builder = _make_builder(settings, stored_metadata=stored)
    with pytest.raises(RuntimeError, match="vector_dimension"):
        builder.start()


def test_start_error_message_contains_truncate_hint():
    settings = _make_settings(embedding_model="new-model")
    stored = {"embedding_model": "old-model", "vector_dimension": 384}
    builder = _make_builder(settings, stored_metadata=stored)
    with pytest.raises(RuntimeError, match="TRUNCATE"):
        builder.start()
