"""Collect precomputed rankings from the running backend via HTTP."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx

from src.evaluation.dataset_loader import load_queries

RankingsByStrategy = dict[str, dict[str, list[str]]]

STRATEGIES = {
    "bm25": ("/api/v1/search/bm25", "doc_id"),
    "vector": ("/api/v1/vector/search", "doc_id"),
    "hybrid": ("/api/v1/search", "chunk_id"),
}


def collect_rankings(
    queries_path: str | Path,
    output_path: str | Path,
    base_url: str = "http://localhost:8888",
    top_k: int = 10,
) -> RankingsByStrategy:
    """Collect rankings for all evaluation queries and write them as JSON."""
    queries = load_queries(queries_path)
    rankings: RankingsByStrategy = {strategy_name: {} for strategy_name in STRATEGIES}

    with httpx.Client(timeout=60) as client:
        for query in queries:
            payload: dict[str, Any] = {"query": query.query, "top_k": top_k}
            if query.source_ids is not None:
                payload["source_ids"] = query.source_ids

            for strategy_name, (endpoint, id_field) in STRATEGIES.items():
                results = _request_results(
                    client=client,
                    base_url=base_url,
                    endpoint=endpoint,
                    payload=payload,
                    query_id=query.id,
                    strategy_name=strategy_name,
                )
                rankings[strategy_name][query.id] = _extract_ids(
                    results=results,
                    id_field=id_field,
                    query_id=query.id,
                    strategy_name=strategy_name,
                )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rankings, indent=2) + "\n", encoding="utf-8")

    return rankings


def _request_results(
    client: httpx.Client,
    base_url: str,
    endpoint: str,
    payload: Mapping[str, Any],
    query_id: str,
    strategy_name: str,
) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}{endpoint}"
    try:
        response = client.post(url, json=dict(payload))
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"{strategy_name} request failed for query '{query_id}': {exc}"
        ) from exc

    if response.status_code != 200:
        raise RuntimeError(
            f"{strategy_name} request failed for query '{query_id}' "
            f"with status {response.status_code}: {response.text}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"{strategy_name} response for query '{query_id}' is not valid JSON"
        ) from exc

    if not isinstance(data, dict) or not isinstance(data.get("results"), list):
        raise RuntimeError(
            f"{strategy_name} response for query '{query_id}' "
            "must be an object with a results list"
        )

    results = data["results"]
    if not all(isinstance(result, dict) for result in results):
        raise RuntimeError(
            f"{strategy_name} response results for query '{query_id}' "
            "must be objects"
        )

    return results


def _extract_ids(
    results: list[dict[str, Any]],
    id_field: str,
    query_id: str,
    strategy_name: str,
) -> list[str]:
    extracted_ids: list[str] = []

    for index, result in enumerate(results):
        result_id = result.get(id_field)
        if not isinstance(result_id, str) or not result_id.strip():
            raise RuntimeError(
                f"{strategy_name} result at index {index} for query '{query_id}' "
                f"must include non-empty string field '{id_field}'"
            )

        extracted_ids.append(result_id)

    return extracted_ids


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect backend rankings for IR evaluation.")
    parser.add_argument("--queries", required=True, help="Path to evaluation queries JSON.")
    parser.add_argument("--output", required=True, help="Path for generated rankings JSON.")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8888",
        help="Base URL for the running backend.",
    )
    parser.add_argument("--top-k", type=int, default=10, help="Number of results to request.")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for backend ranking collection."""
    args = _parse_args()
    collect_rankings(
        queries_path=args.queries,
        output_path=args.output,
        base_url=args.base_url,
        top_k=args.top_k,
    )


if __name__ == "__main__":
    main()
