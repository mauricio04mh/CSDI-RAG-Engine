"""File-loading helpers for offline evaluation datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.evaluation.schemas import EvaluationQuery


def load_queries(path: str | Path) -> list[EvaluationQuery]:
    """Load and validate evaluation queries from a JSON file."""
    data = _load_json(path)
    if not isinstance(data, list):
        raise ValueError("queries JSON must be a list of objects")

    return [_parse_query(query_data, index) for index, query_data in enumerate(data)]


def load_qrels(path: str | Path) -> dict[str, dict[str, int | float]]:
    """Load and validate relevance judgments from a JSON file."""
    data = _load_json(path)
    if not isinstance(data, dict):
        raise ValueError("qrels JSON must be an object")

    qrels: dict[str, dict[str, int | float]] = {}
    for query_id, judgments in data.items():
        if not _is_non_empty_string(query_id):
            raise ValueError("qrel query IDs must be non-empty strings")
        if not isinstance(judgments, dict):
            raise ValueError(f"qrels for query '{query_id}' must be an object")

        qrels[query_id] = _parse_query_judgments(query_id, judgments)

    return qrels


def _load_json(path: str | Path) -> Any:
    with Path(path).open(encoding="utf-8") as file:
        return json.load(file)


def _parse_query(data: Any, index: int) -> EvaluationQuery:
    if not isinstance(data, dict):
        raise ValueError(f"query at index {index} must be an object")

    query_id = data.get("id")
    query_text = data.get("query")
    source_ids = data.get("source_ids")

    if not _is_non_empty_string(query_id):
        raise ValueError(f"query at index {index} must have a non-empty string id")
    if not _is_non_empty_string(query_text):
        raise ValueError(f"query '{query_id}' must have a non-empty string query")
    if source_ids is not None and not _is_string_list(source_ids):
        raise ValueError(f"query '{query_id}' source_ids must be a list of strings")

    return EvaluationQuery(id=query_id, query=query_text, source_ids=source_ids)


def _parse_query_judgments(
    query_id: str,
    judgments: dict[Any, Any],
) -> dict[str, int | float]:
    parsed: dict[str, int | float] = {}

    for document_id, relevance in judgments.items():
        if not _is_non_empty_string(document_id):
            raise ValueError(
                f"qrel document IDs for query '{query_id}' must be non-empty strings"
            )
        if not _is_numeric_relevance(relevance):
            raise ValueError(
                f"qrel relevance for query '{query_id}', document '{document_id}' "
                "must be a non-negative number"
            )

        parsed[document_id] = relevance

    return parsed


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _is_numeric_relevance(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and value >= 0
    )
