"""Export missing query-chunk judgments with chunk metadata for manual review."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.database.config import build_engine
from src.database.repositories.chunk_repository import ChunkRepository
from src.evaluation.dataset_loader import load_queries


def export_pending_judgments(
    audit_path: str | Path,
    queries_path: str | Path,
    output_path: str | Path,
    database_url: str | None = None,
) -> list[dict[str, object | None]]:
    """Export missing judgments into a review-friendly JSON file."""
    audit = _load_json(audit_path)
    missing_pairs = _collect_missing_pairs(audit)
    queries = load_queries(queries_path)
    query_text_by_id = {query.id: query.query for query in queries}

    chunk_repo = ChunkRepository(build_engine(database_url))
    chunks = chunk_repo.get_chunks(sorted({chunk_id for _, chunk_id in missing_pairs}))

    export_rows: list[dict[str, object | None]] = []
    for query_id, chunk_id in missing_pairs:
        chunk = chunks.get(chunk_id)
        export_rows.append(
            {
                "query_id": query_id,
                "query_text": query_text_by_id.get(query_id, ""),
                "chunk_id": chunk_id,
                "source_id": getattr(chunk, "source_id", None),
                "title": getattr(chunk, "title", None),
                "url": getattr(chunk, "url", None),
                "breadcrumb": getattr(chunk, "breadcrumb", None),
                "text": getattr(chunk, "text", None),
                "current_relevance": None,
                "suggested_relevance": None,
            }
        )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(export_rows, indent=2) + "\n", encoding="utf-8")
    return export_rows


def _collect_missing_pairs(audit: dict) -> list[tuple[str, str]]:
    missing_by_query = audit.get("missing_by_query")
    if not isinstance(missing_by_query, dict):
        raise ValueError("audit report must contain a missing_by_query object")

    pairs: set[tuple[str, str]] = set()
    for query_id, by_strategy in missing_by_query.items():
        if not isinstance(query_id, str) or not isinstance(by_strategy, dict):
            raise ValueError("audit report has invalid missing_by_query structure")
        for chunk_ids in by_strategy.values():
            if not isinstance(chunk_ids, list):
                raise ValueError("audit report has invalid missing chunk list")
            for chunk_id in chunk_ids:
                if not isinstance(chunk_id, str) or not chunk_id.strip():
                    raise ValueError("audit report contains an invalid chunk_id")
                pairs.add((query_id, chunk_id))

    return sorted(pairs)


def _load_json(path: str | Path) -> dict:
    with Path(path).open(encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError("audit report must be a JSON object")
    return data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export pending evaluation judgments with chunk metadata.",
    )
    parser.add_argument("--audit", required=True, help="Path to qrels_audit_report.json.")
    parser.add_argument("--queries", required=True, help="Path to queries.json.")
    parser.add_argument("--output", required=True, help="Path for pending_judgments.json.")
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional database URL override for chunk metadata lookup.",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for pending judgments export."""
    args = _parse_args()
    export_pending_judgments(
        audit_path=args.audit,
        queries_path=args.queries,
        output_path=args.output,
        database_url=args.database_url,
    )


if __name__ == "__main__":
    main()
