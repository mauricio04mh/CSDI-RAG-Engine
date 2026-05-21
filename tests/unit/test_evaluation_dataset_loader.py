from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.evaluation.dataset_loader import load_qrels, load_queries, load_rankings


def test_load_queries_loads_valid_queries(tmp_path: Path):
    path = _write_json(
        tmp_path,
        "queries.json",
        [
            {
                "id": "q1",
                "query": "What is Python?",
                "source_ids": ["python_docs"],
            }
        ],
    )

    queries = load_queries(path)

    assert len(queries) == 1
    assert queries[0].id == "q1"
    assert queries[0].query == "What is Python?"
    assert queries[0].source_ids == ["python_docs"]


def test_load_queries_accepts_missing_source_ids_as_none(tmp_path: Path):
    path = _write_json(
        tmp_path,
        "queries.json",
        [{"id": "q1", "query": "What is Python?"}],
    )

    queries = load_queries(path)

    assert queries[0].source_ids is None


def test_load_queries_rejects_non_list_json(tmp_path: Path):
    path = _write_json(tmp_path, "queries.json", {"id": "q1"})

    with pytest.raises(ValueError, match="queries JSON must be a list"):
        load_queries(path)


@pytest.mark.parametrize(
    "query_data",
    [
        {"id": "", "query": "What is Python?"},
        {"id": "q1", "query": ""},
    ],
)
def test_load_queries_rejects_empty_id_or_empty_query(
    tmp_path: Path,
    query_data: dict[str, str],
):
    path = _write_json(tmp_path, "queries.json", [query_data])

    with pytest.raises(ValueError, match="non-empty string"):
        load_queries(path)


@pytest.mark.parametrize(
    "source_ids",
    [
        "python_docs",
        ["python_docs", 1],
    ],
)
def test_load_queries_rejects_source_ids_when_not_list_of_strings(
    tmp_path: Path,
    source_ids: object,
):
    path = _write_json(
        tmp_path,
        "queries.json",
        [{"id": "q1", "query": "What is Python?", "source_ids": source_ids}],
    )

    with pytest.raises(ValueError, match="source_ids must be a list of strings"):
        load_queries(path)


def test_load_qrels_loads_valid_qrels(tmp_path: Path):
    path = _write_json(
        tmp_path,
        "qrels.json",
        {"q1": {"doc-1": 3, "doc-2": 2.5}},
    )

    assert load_qrels(path) == {"q1": {"doc-1": 3, "doc-2": 2.5}}


def test_load_qrels_rejects_non_dict_json(tmp_path: Path):
    path = _write_json(tmp_path, "qrels.json", [{"q1": {"doc-1": 3}}])

    with pytest.raises(ValueError, match="qrels JSON must be an object"):
        load_qrels(path)


def test_load_qrels_rejects_negative_relevance(tmp_path: Path):
    path = _write_json(tmp_path, "qrels.json", {"q1": {"doc-1": -1}})

    with pytest.raises(ValueError, match="non-negative number"):
        load_qrels(path)


def test_load_qrels_rejects_non_numeric_relevance(tmp_path: Path):
    path = _write_json(tmp_path, "qrels.json", {"q1": {"doc-1": "high"}})

    with pytest.raises(ValueError, match="non-negative number"):
        load_qrels(path)


@pytest.mark.parametrize(
    "qrels",
    [
        {"": {"doc-1": 3}},
        {"q1": {"": 3}},
    ],
)
def test_load_qrels_rejects_empty_query_ids_or_empty_document_ids(
    tmp_path: Path,
    qrels: dict[str, dict[str, int]],
):
    path = _write_json(tmp_path, "qrels.json", qrels)

    with pytest.raises(ValueError, match="non-empty strings"):
        load_qrels(path)


def test_load_rankings_loads_valid_rankings(tmp_path: Path):
    rankings = {
        "bm25": {"q1": ["doc-1", "doc-2"], "q2": ["doc-3"]},
        "vector": {"q1": ["doc-2", "doc-1"]},
    }
    path = _write_json(tmp_path, "rankings.json", rankings)

    assert load_rankings(path) == rankings


def test_load_rankings_rejects_non_dict_json(tmp_path: Path):
    path = _write_json(tmp_path, "rankings.json", ["bm25"])

    with pytest.raises(ValueError, match="rankings JSON must be an object"):
        load_rankings(path)


def test_load_rankings_rejects_empty_strategy_names(tmp_path: Path):
    path = _write_json(tmp_path, "rankings.json", {"": {"q1": ["doc-1"]}})

    with pytest.raises(ValueError, match="strategy names must be non-empty strings"):
        load_rankings(path)


def test_load_rankings_rejects_strategy_values_that_are_not_dicts(tmp_path: Path):
    path = _write_json(tmp_path, "rankings.json", {"bm25": ["doc-1"]})

    with pytest.raises(ValueError, match="strategy 'bm25' must be an object"):
        load_rankings(path)


def test_load_rankings_rejects_empty_query_ids(tmp_path: Path):
    path = _write_json(tmp_path, "rankings.json", {"bm25": {"": ["doc-1"]}})

    with pytest.raises(ValueError, match="query IDs .* must be non-empty strings"):
        load_rankings(path)


@pytest.mark.parametrize(
    "ranking",
    [
        "doc-1",
        ["doc-1", ""],
        ["doc-1", 2],
    ],
)
def test_load_rankings_rejects_rankings_that_are_not_lists_of_non_empty_strings(
    tmp_path: Path,
    ranking: object,
):
    path = _write_json(tmp_path, "rankings.json", {"bm25": {"q1": ranking}})

    with pytest.raises(ValueError, match="list of non-empty strings"):
        load_rankings(path)


def _write_json(tmp_path: Path, filename: str, data: object) -> Path:
    path = tmp_path / filename
    path.write_text(json.dumps(data), encoding="utf-8")
    return path
