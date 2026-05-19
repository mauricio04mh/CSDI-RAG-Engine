from __future__ import annotations

import json
from pathlib import Path

from src.evaluation.audit_qrels import build_qrels_audit


def test_relevance_zero_counts_as_judged(tmp_path: Path):
    _write_json(
        tmp_path / "queries.json",
        [{"id": "q1", "query": "Query", "source_ids": None}],
    )
    _write_json(
        tmp_path / "rankings.json",
        {"bm25": {"q1": ["doc-1"]}, "vector": {"q1": []}, "hybrid": {"q1": []}},
    )
    _write_json(tmp_path / "qrels.json", {"q1": {"doc-1": 0}})

    report = build_qrels_audit(
        queries_path=tmp_path / "queries.json",
        rankings_path=tmp_path / "rankings.json",
        qrels_path=tmp_path / "qrels.json",
    )

    assert report["judged_pairs"] == 1
    assert report["missing_pairs"] == 0
    assert report["missing_by_query"]["q1"]["bm25"] == []


def test_missing_chunks_are_detected_and_grouped_by_strategy(tmp_path: Path):
    _write_json(
        tmp_path / "queries.json",
        [{"id": "q1", "query": "Query", "source_ids": None}],
    )
    _write_json(
        tmp_path / "rankings.json",
        {
            "bm25": {"q1": ["doc-1", "doc-2"]},
            "vector": {"q1": ["doc-3"]},
            "hybrid": {"q1": []},
        },
    )
    _write_json(tmp_path / "qrels.json", {"q1": {"doc-1": 3}})

    report = build_qrels_audit(
        queries_path=tmp_path / "queries.json",
        rankings_path=tmp_path / "rankings.json",
        qrels_path=tmp_path / "qrels.json",
    )

    assert report["total_ranked_positions"] == 3
    assert report["judged_pairs"] == 1
    assert report["missing_pairs"] == 2
    assert report["missing_by_query"]["q1"]["bm25"] == ["doc-2"]
    assert report["missing_by_query"]["q1"]["vector"] == ["doc-3"]
    assert report["missing_by_query"]["q1"]["hybrid"] == []


def test_duplicate_chunk_ids_across_strategies_count_as_one_unique_pair(tmp_path: Path):
    _write_json(
        tmp_path / "queries.json",
        [{"id": "q1", "query": "Query", "source_ids": None}],
    )
    _write_json(
        tmp_path / "rankings.json",
        {
            "bm25": {"q1": ["doc-1", "doc-2"]},
            "vector": {"q1": ["doc-1"]},
            "hybrid": {"q1": ["doc-2"]},
        },
    )
    _write_json(tmp_path / "qrels.json", {"q1": {"doc-1": 2}})

    report = build_qrels_audit(
        queries_path=tmp_path / "queries.json",
        rankings_path=tmp_path / "rankings.json",
        qrels_path=tmp_path / "qrels.json",
    )

    assert report["total_ranked_positions"] == 4
    assert report["unique_ranked_pairs"] == 2
    assert report["judged_pairs"] == 1
    assert report["missing_pairs"] == 1
    assert report["missing_by_query"]["q1"]["bm25"] == ["doc-2"]
    assert report["missing_by_query"]["q1"]["vector"] == []
    assert report["missing_by_query"]["q1"]["hybrid"] == ["doc-2"]


def _write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
