"""Refresh the persisted base evaluation rankings via the local evaluation API."""

from __future__ import annotations

import argparse
from pathlib import Path

import httpx

from src.evaluation.dataset_loader import load_queries
from src.evaluation.store import DEFAULT_QUERIES_PATH

DEFAULT_STRATEGIES = ["bm25", "vector", "hybrid"]


def refresh_base_dataset(
    queries_path: str | Path = DEFAULT_QUERIES_PATH,
    base_url: str = "http://localhost:8888",
    top_k: int = 10,
) -> dict[str, dict[str, int]]:
    """Regenerate persisted evaluation rankings for every stored query."""
    queries = load_queries(queries_path)
    summary: dict[str, dict[str, int]] = {}

    with httpx.Client(timeout=60) as client:
        for query in queries:
            response = _post_rankings(
                client=client,
                base_url=base_url,
                query_id=query.id,
                top_k=top_k,
            )
            rankings = _parse_rankings_response(response, query.id)
            counts = {
                strategy: len(rankings.get(strategy, []))
                for strategy in DEFAULT_STRATEGIES
            }
            summary[query.id] = counts
            print(
                f"{query.id}: "
                f"bm25 {counts['bm25']}, "
                f"vector {counts['vector']}, "
                f"hybrid {counts['hybrid']}"
            )

    return summary


def _post_rankings(
    client: httpx.Client,
    base_url: str,
    query_id: str,
    top_k: int,
) -> httpx.Response:
    url = f"{base_url.rstrip('/')}/api/v1/evaluation/queries/{query_id}/rankings"
    payload = {"top_k": top_k, "strategies": DEFAULT_STRATEGIES}

    try:
        response = client.post(url, json=payload)
    except httpx.HTTPError as exc:
        raise RuntimeError(
            "Failed to refresh evaluation rankings. "
            f"Could not reach backend at {base_url}: {exc}"
        ) from exc

    if response.status_code != 200:
        raise RuntimeError(
            "Failed to refresh evaluation rankings for "
            f"query '{query_id}' with status {response.status_code}: {response.text}"
        )

    return response


def _parse_rankings_response(
    response: httpx.Response,
    query_id: str,
) -> dict[str, list[object]]:
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Evaluation rankings response for query '{query_id}' is not valid JSON"
        ) from exc

    rankings = data.get("rankings") if isinstance(data, dict) else None
    if not isinstance(rankings, dict):
        raise RuntimeError(
            f"Evaluation rankings response for query '{query_id}' must include a rankings object"
        )

    for strategy, items in rankings.items():
        if not isinstance(strategy, str) or not isinstance(items, list):
            raise RuntimeError(
                f"Evaluation rankings response for query '{query_id}' has invalid rankings structure"
            )

    return rankings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh the base evaluation dataset rankings through the local backend.",
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8888",
        help="Base URL for the running backend.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of results to request per strategy.",
    )
    parser.add_argument(
        "--queries",
        default=str(DEFAULT_QUERIES_PATH),
        help="Path to queries.json.",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for refreshing the base evaluation dataset."""
    args = _parse_args()
    refresh_base_dataset(
        queries_path=args.queries,
        base_url=args.base_url,
        top_k=args.top_k,
    )


if __name__ == "__main__":
    main()
