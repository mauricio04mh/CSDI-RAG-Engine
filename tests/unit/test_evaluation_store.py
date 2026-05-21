from __future__ import annotations

import json
from pathlib import Path

from src.evaluation.store import EvaluationStore


def test_add_query_generates_next_query_id(tmp_path: Path):
    store = _make_store(tmp_path)
    _write_json(
        store.queries_path,
        [
            {"id": "q1", "query": "First query", "source_ids": None},
            {"id": "q3", "query": "Third query", "source_ids": ["python_docs"]},
        ],
    )

    created = store.add_query("New query", ["python_docs"])

    assert created.id == "q4"
    assert created.query == "New query"
    assert created.source_ids == ["python_docs"]


def test_add_query_initializes_qrels_for_new_query(tmp_path: Path):
    store = _make_store(tmp_path)
    _write_json(store.queries_path, [])
    _write_json(store.qrels_path, {})

    created = store.add_query("How do decorators work?")

    assert store.load_qrels() == {created.id: {}}


def test_update_judgment_persists_relevance(tmp_path: Path):
    store = _make_store(tmp_path)
    _write_json(store.queries_path, [{"id": "q1", "query": "Query", "source_ids": None}])
    _write_json(store.qrels_path, {"q1": {}})

    store.update_judgment("q1", "python_docs:chunk-1", 3)

    assert store.load_qrels()["q1"]["python_docs:chunk-1"] == 3


def test_rankings_update_persists_ordered_ids(tmp_path: Path):
    store = _make_store(tmp_path)

    store.update_rankings_for_query(
        "q1",
        {
            "bm25": ["doc-1", "doc-2"],
            "hybrid": ["doc-2", "doc-1"],
        },
    )

    assert store.load_rankings() == {
        "bm25": {"q1": ["doc-1", "doc-2"]},
        "hybrid": {"q1": ["doc-2", "doc-1"]},
    }


def test_report_save_and_load(tmp_path: Path):
    store = _make_store(tmp_path)
    report = {
        "k": 5,
        "strategies": {
            "bm25": {
                "averages": {"precision_at_k": 1.0},
                "evaluated_queries": 1,
                "per_query": {},
                "k": 5,
            }
        },
    }

    store.save_report(report)

    assert store.load_report() == report


def test_summary_returns_counts(tmp_path: Path):
    store = _make_store(tmp_path)
    _write_json(
        store.queries_path,
        [
            {"id": "q1", "query": "First", "source_ids": None},
            {"id": "q2", "query": "Second", "source_ids": None},
        ],
    )
    _write_json(
        store.qrels_path,
        {
            "q1": {"doc-1": 3, "doc-2": 1},
            "q2": {},
        },
    )
    store.save_report(
        {
            "k": 5,
            "strategies": {
                "bm25": {
                    "averages": {"precision_at_k": 0.5},
                    "evaluated_queries": 1,
                    "per_query": {},
                    "k": 5,
                }
            },
        }
    )

    assert store.get_summary() == {
        "queries_count": 2,
        "judged_queries_count": 1,
        "total_judgments": 2,
        "available_strategies": ["bm25", "vector", "hybrid"],
        "latest_report_exists": True,
        "latest_averages": {"bm25": {"precision_at_k": 0.5}},
    }


def _make_store(tmp_path: Path) -> EvaluationStore:
    return EvaluationStore(
        queries_path=tmp_path / "datasets" / "queries.json",
        qrels_path=tmp_path / "datasets" / "qrels.json",
        rankings_path=tmp_path / "datasets" / "rankings.generated.json",
        report_path=tmp_path / "results" / "evaluation_report.json",
    )


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
