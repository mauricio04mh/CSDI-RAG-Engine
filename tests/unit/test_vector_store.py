from __future__ import annotations

import pytest

from src.vector_indexing.index.vector_store import VectorStore


@pytest.fixture()
def store() -> VectorStore:
    return VectorStore()


def test_empty_store_has_zero_length(store):
    assert len(store) == 0


def test_add_single_document(store):
    ids = store.add_documents(["doc_a"])
    assert ids == [0]
    assert len(store) == 1


def test_vector_id_increments_sequentially(store):
    ids = store.add_documents(["doc_a", "doc_b", "doc_c"])
    assert ids == [0, 1, 2]


def test_get_doc_id_resolves_correctly(store):
    store.add_documents(["doc_a", "doc_b"])
    assert store.get_doc_id(0) == "doc_a"
    assert store.get_doc_id(1) == "doc_b"


def test_get_doc_id_returns_none_for_out_of_range(store):
    store.add_documents(["doc_a"])
    assert store.get_doc_id(5) is None
    assert store.get_doc_id(-1) is None


def test_duplicate_doc_id_raises(store):
    store.add_documents(["doc_a"])
    with pytest.raises(ValueError, match="already exists"):
        store.add_documents(["doc_a"])


def test_bidirectional_mapping_consistent(store):
    store.add_documents(["doc_a", "doc_b", "doc_c"])
    for vector_id, doc_id in enumerate(["doc_a", "doc_b", "doc_c"]):
        assert store.doc_ids_to_vector_ids[doc_id] == vector_id
        assert store.vector_ids_to_doc_ids[vector_id] == doc_id


def test_add_documents_in_batches(store):
    store.add_documents(["doc_a", "doc_b"])
    store.add_documents(["doc_c", "doc_d"])
    assert len(store) == 4
    assert store.get_doc_id(2) == "doc_c"
    assert store.get_doc_id(3) == "doc_d"
