"""Audit missing relevance judgments for the current evaluation rankings."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.evaluation.dataset_loader import load_qrels, load_rankings, load_queries


def build_qrels_audit(
    queries_path: str | Path,
    rankings_path: str | Path,
    qrels_path: str | Path,
) -> dict:
    """Build an audit report for judged and missing query-chunk pairs."""
    queries = load_queries(queries_path)
    rankings = load_rankings(rankings_path)
    qrels = load_qrels(qrels_path)

    query_ids = [query.id for query in queries]
    strategies = list(rankings.keys())
    missing_by_query = {
        query_id: {strategy: [] for strategy in strategies}
        for query_id in query_ids
    }

    total_ranked_positions = 0
    unique_pairs: set[tuple[str, str]] = set()
    judged_pairs: set[tuple[str, str]] = set()
    missing_pairs: set[tuple[str, str]] = set()

    for strategy, rankings_by_query in rankings.items():
        for query_id in query_ids:
            ranked_chunk_ids = rankings_by_query.get(query_id, [])
            query_judgments = qrels.get(query_id, {})
            missing_for_strategy = missing_by_query[query_id][strategy]

            for chunk_id in ranked_chunk_ids:
                total_ranked_positions += 1
                pair = (query_id, chunk_id)
                unique_pairs.add(pair)

                if chunk_id in query_judgments:
                    judged_pairs.add(pair)
                    continue

                missing_pairs.add(pair)
                missing_for_strategy.append(chunk_id)

    return {
        "total_queries": len(query_ids),
        "total_ranked_positions": total_ranked_positions,
        "unique_ranked_pairs": len(unique_pairs),
        "judged_pairs": len(judged_pairs),
        "missing_pairs": len(missing_pairs),
        "missing_by_query": missing_by_query,
    }


def run_qrels_audit(
    queries_path: str | Path,
    rankings_path: str | Path,
    qrels_path: str | Path,
    output_path: str | Path,
) -> dict:
    """Run the qrels audit and persist the resulting JSON report."""
    report = build_qrels_audit(
        queries_path=queries_path,
        rankings_path=rankings_path,
        qrels_path=qrels_path,
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit missing relevance judgments for generated rankings.",
    )
    parser.add_argument("--rankings", required=True, help="Path to rankings.generated.json.")
    parser.add_argument("--qrels", required=True, help="Path to qrels.json.")
    parser.add_argument("--queries", required=True, help="Path to queries.json.")
    parser.add_argument("--output", required=True, help="Path for qrels_audit_report.json.")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for qrels audit."""
    args = _parse_args()
    run_qrels_audit(
        queries_path=args.queries,
        rankings_path=args.rankings,
        qrels_path=args.qrels,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
