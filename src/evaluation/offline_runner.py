"""Offline runner for precomputed retrieval evaluation reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.evaluation.dataset_loader import load_qrels, load_rankings
from src.evaluation.evaluator import evaluate_all_strategies


def run_offline_evaluation(
    rankings_path: str | Path,
    qrels_path: str | Path,
    output_path: str | Path,
    k: int = 5,
) -> dict:
    """Load rankings and qrels, evaluate all strategies, and write a JSON report."""
    rankings = load_rankings(rankings_path)
    qrels = load_qrels(qrels_path)
    report = evaluate_all_strategies(rankings, qrels, k)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )

    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline IR evaluation.")
    parser.add_argument("--rankings", required=True, help="Path to rankings JSON.")
    parser.add_argument("--qrels", required=True, help="Path to qrels JSON.")
    parser.add_argument("--output", required=True, help="Path for the report JSON.")
    parser.add_argument("--k", type=int, default=5, help="Evaluation cutoff.")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for offline evaluation."""
    args = _parse_args()
    run_offline_evaluation(
        rankings_path=args.rankings,
        qrels_path=args.qrels,
        output_path=args.output,
        k=args.k,
    )


if __name__ == "__main__":
    main()
