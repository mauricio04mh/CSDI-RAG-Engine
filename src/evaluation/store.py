"""JSON-backed persistence for the evaluation workflow."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.evaluation.dataset_loader import load_qrels as load_qrels_file
from src.evaluation.dataset_loader import load_rankings as load_rankings_file
from src.evaluation.dataset_loader import load_queries as load_queries_file
from src.evaluation.schemas import EvaluationQuery

EVALUATION_DIR = Path(__file__).resolve().parent
DATASETS_DIR = EVALUATION_DIR / "datasets"
RESULTS_DIR = EVALUATION_DIR / "results"

DEFAULT_QUERIES_PATH = DATASETS_DIR / "queries.json"
DEFAULT_QRELS_PATH = DATASETS_DIR / "qrels.json"
DEFAULT_RANKINGS_PATH = DATASETS_DIR / "rankings.generated.json"
DEFAULT_REPORT_PATH = RESULTS_DIR / "evaluation_report.json"
AVAILABLE_STRATEGIES = ["bm25", "vector", "hybrid"]


@dataclass(slots=True)
class EvaluationStore:
    """JSON-backed store for evaluation datasets, qrels, rankings, and reports."""

    queries_path: Path = DEFAULT_QUERIES_PATH
    qrels_path: Path = DEFAULT_QRELS_PATH
    rankings_path: Path = DEFAULT_RANKINGS_PATH
    report_path: Path = DEFAULT_REPORT_PATH

    def load_queries(self) -> list[EvaluationQuery]:
        if not self.queries_path.exists():
            return []
        return load_queries_file(self.queries_path)

    def save_queries(self, queries: list[EvaluationQuery]) -> None:
        _write_json(self.queries_path, [asdict(query) for query in queries])

    def add_query(
        self,
        query: str,
        source_ids: list[str] | None = None,
    ) -> EvaluationQuery:
        normalized_query = query.strip()
        if not normalized_query:
            raise ValueError("query must be a non-empty string")

        queries = self.load_queries()
        created = EvaluationQuery(
            id=_next_query_id(queries),
            query=normalized_query,
            source_ids=source_ids,
        )
        queries.append(created)
        self.save_queries(queries)

        qrels = self.load_qrels()
        qrels.setdefault(created.id, {})
        self.save_qrels(qrels)

        return created

    def get_query(self, query_id: str) -> EvaluationQuery | None:
        return next((query for query in self.load_queries() if query.id == query_id), None)

    def load_qrels(self) -> dict[str, dict[str, int | float]]:
        if not self.qrels_path.exists():
            return {}
        return load_qrels_file(self.qrels_path)

    def save_qrels(self, qrels: dict[str, dict[str, int | float]]) -> None:
        _write_json(self.qrels_path, qrels)

    def update_judgment(
        self,
        query_id: str,
        chunk_id: str,
        relevance: int,
    ) -> dict[str, int | float]:
        if self.get_query(query_id) is None:
            raise KeyError(f"query '{query_id}' was not found")
        if relevance not in {0, 1, 2, 3}:
            raise ValueError("relevance must be 0, 1, 2, or 3")
        if not chunk_id.strip():
            raise ValueError("chunk_id must be a non-empty string")

        qrels = self.load_qrels()
        qrels.setdefault(query_id, {})[chunk_id] = relevance
        self.save_qrels(qrels)
        return qrels[query_id]

    def get_judgments(self, query_id: str) -> dict[str, int | float]:
        return self.load_qrels().get(query_id, {})

    def load_rankings(self) -> dict[str, dict[str, list[str]]]:
        if not self.rankings_path.exists():
            return {}
        return load_rankings_file(self.rankings_path)

    def save_rankings(self, rankings: dict[str, dict[str, list[str]]]) -> None:
        _write_json(self.rankings_path, rankings)

    def update_rankings_for_query(
        self,
        query_id: str,
        rankings_by_strategy: dict[str, list[str]],
    ) -> dict[str, dict[str, list[str]]]:
        rankings = self.load_rankings()
        for strategy, ordered_ids in rankings_by_strategy.items():
            rankings.setdefault(strategy, {})[query_id] = ordered_ids
        self.save_rankings(rankings)
        return rankings

    def load_report(self) -> dict[str, Any] | None:
        if not self.report_path.exists():
            return None
        return _read_json(self.report_path)

    def save_report(self, report: dict[str, Any]) -> None:
        _write_json(self.report_path, report)

    def get_summary(self) -> dict[str, Any]:
        queries = self.load_queries()
        qrels = self.load_qrels()
        report = self.load_report()
        latest_averages: dict[str, dict[str, float]] = {}

        if report:
            strategies = report.get("strategies", {})
            if isinstance(strategies, dict):
                latest_averages = {
                    strategy: payload.get("averages", {})
                    for strategy, payload in strategies.items()
                    if isinstance(payload, dict)
                }

        return {
            "queries_count": len(queries),
            "judged_queries_count": sum(1 for query_id in qrels if qrels[query_id]),
            "total_judgments": sum(len(judgments) for judgments in qrels.values()),
            "available_strategies": AVAILABLE_STRATEGIES,
            "latest_report_exists": report is not None,
            "latest_averages": latest_averages,
        }


DEFAULT_STORE = EvaluationStore()


def load_queries() -> list[EvaluationQuery]:
    return DEFAULT_STORE.load_queries()


def save_queries(queries: list[EvaluationQuery]) -> None:
    DEFAULT_STORE.save_queries(queries)


def add_query(query: str, source_ids: list[str] | None = None) -> EvaluationQuery:
    return DEFAULT_STORE.add_query(query, source_ids)


def get_query(query_id: str) -> EvaluationQuery | None:
    return DEFAULT_STORE.get_query(query_id)


def load_qrels() -> dict[str, dict[str, int | float]]:
    return DEFAULT_STORE.load_qrels()


def save_qrels(qrels: dict[str, dict[str, int | float]]) -> None:
    DEFAULT_STORE.save_qrels(qrels)


def update_judgment(query_id: str, chunk_id: str, relevance: int) -> dict[str, int | float]:
    return DEFAULT_STORE.update_judgment(query_id, chunk_id, relevance)


def get_judgments(query_id: str) -> dict[str, int | float]:
    return DEFAULT_STORE.get_judgments(query_id)


def load_rankings() -> dict[str, dict[str, list[str]]]:
    return DEFAULT_STORE.load_rankings()


def save_rankings(rankings: dict[str, dict[str, list[str]]]) -> None:
    DEFAULT_STORE.save_rankings(rankings)


def update_rankings_for_query(
    query_id: str,
    rankings_by_strategy: dict[str, list[str]],
) -> dict[str, dict[str, list[str]]]:
    return DEFAULT_STORE.update_rankings_for_query(query_id, rankings_by_strategy)


def load_report() -> dict[str, Any] | None:
    return DEFAULT_STORE.load_report()


def save_report(report: dict[str, Any]) -> None:
    DEFAULT_STORE.save_report(report)


def get_summary() -> dict[str, Any]:
    return DEFAULT_STORE.get_summary()


def _next_query_id(queries: list[EvaluationQuery]) -> str:
    max_id = 0
    for query in queries:
        if query.id.startswith("q") and query.id[1:].isdigit():
            max_id = max(max_id, int(query.id[1:]))
    return f"q{max_id + 1}"


def _read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
