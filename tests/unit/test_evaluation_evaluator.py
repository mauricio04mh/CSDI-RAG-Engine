from __future__ import annotations

import pytest

from src.evaluation.evaluator import evaluate_query, evaluate_strategy


def test_evaluate_query_returns_all_expected_metrics():
    retrieved_ids = ["doc-1", "doc-2", "doc-3"]
    qrels = {"doc-1": 3, "doc-2": 1, "doc-3": 2}

    result = evaluate_query(retrieved_ids, qrels, k=3)

    assert set(result) == {
        "precision_at_k",
        "recall_at_k",
        "f1_at_k",
        "reciprocal_rank",
        "ndcg_at_k",
    }
    assert result["precision_at_k"] == pytest.approx(2 / 3)
    assert result["recall_at_k"] == pytest.approx(1.0)
    assert result["f1_at_k"] == pytest.approx(0.8)
    assert result["reciprocal_rank"] == pytest.approx(1.0)
    assert 0.0 <= result["ndcg_at_k"] <= 1.0


def test_evaluate_strategy_returns_per_query_metrics():
    rankings = {
        "q1": ["doc-1", "doc-2"],
        "q2": ["doc-3", "doc-4"],
    }
    qrels = {
        "q1": {"doc-1": 2, "doc-2": 0},
        "q2": {"doc-3": 1, "doc-4": 3},
    }

    result = evaluate_strategy(rankings, qrels, k=2)

    assert result["k"] == 2
    assert result["evaluated_queries"] == 2
    assert set(result["per_query"]) == {"q1", "q2"}
    assert result["per_query"]["q1"]["precision_at_k"] == pytest.approx(0.5)
    assert result["per_query"]["q2"]["reciprocal_rank"] == pytest.approx(0.5)


def test_evaluate_strategy_computes_averages_correctly():
    rankings = {
        "q1": ["doc-1", "doc-2"],
        "q2": ["doc-3", "doc-4"],
    }
    qrels = {
        "q1": {"doc-1": 2, "doc-2": 0},
        "q2": {"doc-3": 0, "doc-4": 3},
    }

    result = evaluate_strategy(rankings, qrels, k=2)

    assert result["averages"]["precision_at_k"] == pytest.approx(0.5)
    assert result["averages"]["recall_at_k"] == pytest.approx(1.0)
    assert result["averages"]["f1_at_k"] == pytest.approx(2 / 3)
    assert result["averages"]["reciprocal_rank"] == pytest.approx(0.75)


def test_evaluate_strategy_skips_queries_without_qrels():
    rankings = {
        "q1": ["doc-1"],
        "q-missing": ["doc-2"],
    }
    qrels = {"q1": {"doc-1": 2}}

    result = evaluate_strategy(rankings, qrels, k=1)

    assert result["evaluated_queries"] == 1
    assert set(result["per_query"]) == {"q1"}


def test_evaluate_strategy_returns_zero_averages_when_no_queries_are_evaluated():
    result = evaluate_strategy({"q1": ["doc-1"]}, {}, k=5)

    assert result["evaluated_queries"] == 0
    assert result["per_query"] == {}
    assert result["averages"] == {
        "precision_at_k": 0.0,
        "recall_at_k": 0.0,
        "f1_at_k": 0.0,
        "reciprocal_rank": 0.0,
        "ndcg_at_k": 0.0,
    }
