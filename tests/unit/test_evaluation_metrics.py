from __future__ import annotations

from math import log2

import pytest

from src.evaluation.metrics import (
    dcg_at_k,
    f1_at_k,
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
    reciprocal_rank,
)


def test_precision_at_k_uses_binary_relevance_threshold():
    retrieved_ids = ["doc-1", "doc-2", "doc-3"]
    qrels = {"doc-1": 3, "doc-2": 1, "doc-3": 2}

    assert precision_at_k(retrieved_ids, qrels, k=3) == pytest.approx(2 / 3)


def test_recall_at_k_uses_binary_relevance_threshold():
    retrieved_ids = ["doc-1", "doc-2"]
    qrels = {"doc-1": 3, "doc-2": 1, "doc-3": 2, "doc-4": 2}

    assert recall_at_k(retrieved_ids, qrels, k=2) == pytest.approx(1 / 3)


def test_f1_at_k_combines_precision_and_recall():
    retrieved_ids = ["doc-1", "doc-2", "doc-3"]
    qrels = {"doc-1": 3, "doc-2": 1, "doc-3": 0, "doc-4": 2}

    assert f1_at_k(retrieved_ids, qrels, k=3) == pytest.approx(0.4)


def test_reciprocal_rank_first_relevant_at_rank_2():
    retrieved_ids = ["doc-1", "doc-2", "doc-3"]
    qrels = {"doc-1": 1, "doc-2": 2, "doc-3": 3}

    assert reciprocal_rank(retrieved_ids, qrels) == pytest.approx(0.5)


def test_reciprocal_rank_returns_zero_when_no_relevant_document_is_retrieved():
    retrieved_ids = ["doc-1", "doc-2"]
    qrels = {"doc-1": 1, "doc-2": 0, "doc-3": 3}

    assert reciprocal_rank(retrieved_ids, qrels) == 0.0


def test_dcg_at_k_uses_graded_relevance_values():
    retrieved_ids = ["doc-1", "doc-2", "doc-3"]
    qrels = {"doc-1": 3, "doc-2": 2, "doc-3": 0}

    expected = 7 / log2(2) + 3 / log2(3) + 0 / log2(4)
    assert dcg_at_k(retrieved_ids, qrels, k=3) == pytest.approx(expected)


def test_ndcg_at_k_returns_one_for_ideal_ranking():
    retrieved_ids = ["doc-1", "doc-2", "doc-3"]
    qrels = {"doc-1": 3, "doc-2": 2, "doc-3": 1}

    assert ndcg_at_k(retrieved_ids, qrels, k=3) == pytest.approx(1.0)


def test_ndcg_at_k_returns_zero_when_there_are_no_relevant_judgments():
    assert ndcg_at_k(["doc-1", "doc-2"], {}, k=2) == 0.0


def test_duplicate_retrieved_document_ids_are_ignored():
    retrieved_ids = ["doc-1", "doc-1"]
    qrels = {"doc-1": 3, "doc-2": 2}

    assert precision_at_k(retrieved_ids, qrels, k=2) == pytest.approx(0.5)
