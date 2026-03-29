from __future__ import annotations

import pytest

from src.hybrid.fusion.rrf import reciprocal_rank_fusion


def test_empty_input_returns_empty():
    assert reciprocal_rank_fusion([]) == []


def test_single_list_preserves_order():
    result = reciprocal_rank_fusion([["a", "b", "c"]])
    doc_ids = [doc_id for doc_id, _ in result]
    assert doc_ids == ["a", "b", "c"]


def test_scores_decrease_with_rank():
    result = reciprocal_rank_fusion([["a", "b", "c"]])
    scores = [score for _, score in result]
    assert scores == sorted(scores, reverse=True)


def test_document_in_both_lists_scores_higher():
    # "overlap" appears in both lists, "only_bm25" only in the first
    result = reciprocal_rank_fusion([
        ["overlap", "only_bm25"],
        ["overlap", "only_vec"],
    ])
    scores = dict(result)
    assert scores["overlap"] > scores["only_bm25"]
    assert scores["overlap"] > scores["only_vec"]


def test_rrf_formula():
    k = 60
    # doc "a" at rank 1 in list 0 → score = 1/(60+1)
    result = reciprocal_rank_fusion([["a"]], k=k)
    _, score = result[0]
    assert abs(score - 1.0 / (k + 1)) < 1e-12


def test_scores_sum_across_lists():
    k = 60
    # "a" at rank 1 in both lists → score = 2 * 1/(k+1)
    result = reciprocal_rank_fusion([["a"], ["a"]], k=k)
    _, score = result[0]
    assert abs(score - 2.0 / (k + 1)) < 1e-12


def test_empty_sublists_are_ignored():
    result = reciprocal_rank_fusion([[], ["a", "b"], []])
    doc_ids = [doc_id for doc_id, _ in result]
    assert doc_ids == ["a", "b"]


def test_result_is_sorted_descending():
    result = reciprocal_rank_fusion([["z", "y", "x"], ["x", "y", "z"]])
    scores = [score for _, score in result]
    assert scores == sorted(scores, reverse=True)


def test_all_unique_documents():
    result = reciprocal_rank_fusion([["a", "b"], ["c", "d"]])
    assert len(result) == 4


def test_custom_k_parameter():
    k = 10
    result = reciprocal_rank_fusion([["a"]], k=k)
    _, score = result[0]
    assert abs(score - 1.0 / (k + 1)) < 1e-12
