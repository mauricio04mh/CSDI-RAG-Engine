from __future__ import annotations

import threading
from unittest.mock import MagicMock

import numpy as np
import pytest

from src.vector_indexing.index.vector_store import VectorStore
from src.vector_retrieval.pipeline.vector_retriever import VectorRetriever


def _make_retriever(store: VectorStore) -> VectorRetriever:
    embedding_model = MagicMock()
    embedding_model.encode_one.return_value = np.zeros(4, dtype=np.float32)

    faiss_index = MagicMock()
    # FAISS returns scores and vector_ids for 2 docs at positions 0 and 1
    faiss_index.search.return_value = (
        np.array([[0.9, 0.8]], dtype=np.float32),
        np.array([[0, 1]], dtype=np.int64),
    )

    return VectorRetriever(
        embedding_model=embedding_model,
        faiss_index=faiss_index,
        vector_store=store,
        lock=threading.RLock(),
    )


def test_deleted_doc_excluded_from_results():
    store = VectorStore()
    store.add_documents(["doc_a", "doc_b"])
    store.mark_deleted("doc_a")

    retriever = _make_retriever(store)
    results = retriever.search("test query", top_k=5)

    doc_ids = [r.doc_id for r in results]
    assert "doc_a" not in doc_ids
    assert "doc_b" in doc_ids


def test_non_deleted_docs_all_returned():
    store = VectorStore()
    store.add_documents(["doc_a", "doc_b"])

    retriever = _make_retriever(store)
    results = retriever.search("test query", top_k=5)

    assert len(results) == 2
    assert {r.doc_id for r in results} == {"doc_a", "doc_b"}


def test_all_deleted_returns_empty():
    store = VectorStore()
    store.add_documents(["doc_a", "doc_b"])
    store.mark_deleted("doc_a")
    store.mark_deleted("doc_b")

    retriever = _make_retriever(store)
    results = retriever.search("test query", top_k=5)

    assert results == []
