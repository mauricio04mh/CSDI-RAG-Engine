from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import faiss
import numpy as np
import pytest

from src.vector_indexing.config.settings import VectorSettings
from src.vector_indexing.index.faiss_index import FaissIndex
from src.vector_indexing.index.vector_store import VectorStore
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder


DIM = 4


def _make_settings(faiss_index_path: str = "") -> VectorSettings:
    return VectorSettings(
        embedding_model="test-model",
        vector_dimension=DIM,
        faiss_index_type="HNSW",
        hnsw_m=4,
        hnsw_ef_construction=40,
        hnsw_ef_search=10,
        vector_batch_size=128,
        query_prefix="",
        faiss_index_path=faiss_index_path,
        log_level="INFO",
        env_path=Path("/tmp/.env"),
        project_root=Path("/tmp"),
    )


def _make_builder(settings: VectorSettings, stored_metadata=None, db_doc_ids=None, db_vectors=None) -> VectorIndexBuilder:
    builder = VectorIndexBuilder.__new__(VectorIndexBuilder)
    builder.settings = settings
    builder.embedding_model = MagicMock()
    builder._vector_repo = MagicMock()
    builder._vector_repo.load_metadata.return_value = stored_metadata
    builder._vector_repo.load_all_doc_ids.return_value = db_doc_ids or []
    empty_vecs = np.empty((0,), dtype=np.float32) if db_vectors is None else db_vectors
    builder._vector_repo.load_all_documents.return_value = (db_doc_ids or [], empty_vecs)
    builder.vector_store = VectorStore()
    builder.faiss_index = FaissIndex(
        dimension=DIM, index_type="HNSW", hnsw_m=4,
        ef_construction=40, ef_search=10,
    )
    builder._buffer_doc_ids = []
    builder._buffer_texts = []
    builder._lock = threading.RLock()
    return builder


def _write_tiny_faiss_index(path: Path, dim: int, n_vecs: int) -> None:
    idx = faiss.IndexHNSWFlat(dim, 4, faiss.METRIC_INNER_PRODUCT)
    vecs = np.random.rand(n_vecs, dim).astype(np.float32)
    faiss.normalize_L2(vecs)
    idx.add(vecs)
    faiss.write_index(idx, str(path))


def test_start_loads_from_disk_when_counts_match(tmp_path):
    idx_path = tmp_path / "faiss.bin"
    _write_tiny_faiss_index(idx_path, DIM, 2)

    settings = _make_settings(faiss_index_path=str(idx_path))
    builder = _make_builder(settings, db_doc_ids=["doc_a", "doc_b"])

    builder.start()

    # Should have used disk path — load_all_documents NOT called
    builder._vector_repo.load_all_documents.assert_not_called()
    # VectorStore should have both docs
    assert len(builder.vector_store) == 2
    assert builder.vector_store.get_doc_id(0) == "doc_a"


def test_start_falls_back_to_db_on_count_mismatch(tmp_path):
    idx_path = tmp_path / "faiss.bin"
    _write_tiny_faiss_index(idx_path, DIM, 2)

    settings = _make_settings(faiss_index_path=str(idx_path))
    # DB has 3 docs but disk index has 2 → mismatch
    vecs = np.random.rand(3, DIM).astype(np.float32)
    builder = _make_builder(settings, db_doc_ids=["a", "b", "c"], db_vectors=vecs)

    builder.start()

    builder._vector_repo.load_all_documents.assert_called_once()


def test_start_falls_back_to_db_on_dimension_mismatch(tmp_path):
    idx_path = tmp_path / "faiss.bin"
    wrong_dim = DIM + 4
    idx = faiss.IndexHNSWFlat(wrong_dim, 4, faiss.METRIC_INNER_PRODUCT)
    faiss.write_index(idx, str(idx_path))

    settings = _make_settings(faiss_index_path=str(idx_path))
    builder = _make_builder(settings, db_doc_ids=[])

    builder.start()  # should not raise; should fall back to DB
    builder._vector_repo.load_all_documents.assert_called_once()


def test_persist_writes_disk_index(tmp_path):
    idx_path = tmp_path / "faiss.bin"
    settings = _make_settings(faiss_index_path=str(idx_path))
    builder = _make_builder(settings)

    vec = np.random.rand(DIM).astype(np.float32)
    builder._persist(["doc_a"], vec.reshape(1, -1))

    assert idx_path.exists()


def test_no_disk_io_when_path_empty():
    settings = _make_settings(faiss_index_path="")
    builder = _make_builder(settings, db_doc_ids=[])
    builder.start()
    # No disk files should be created; no exception
    builder._vector_repo.load_all_documents.assert_called_once()
