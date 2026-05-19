from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.database.repositories.chunk_repository import ChunkRepository
from src.evaluation import store
from src.evaluation.evaluator import evaluate_all_strategies
from src.evaluation.schemas import EvaluationQuery

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/evaluation", tags=["evaluation"])

VALID_STRATEGIES = {"bm25", "vector", "hybrid"}


class EvaluationQueryCreateRequest(BaseModel):
    query: str = Field(..., min_length=1)
    source_ids: list[str] | None = None


class EvaluationQueryResponse(BaseModel):
    id: str
    query: str
    source_ids: list[str] | None = None


class RankingRunRequest(BaseModel):
    top_k: int = Field(default=10, ge=1, le=100)
    strategies: list[str] = Field(default_factory=lambda: ["bm25", "vector", "hybrid"])


class RankingResultResponse(BaseModel):
    chunk_id: str
    score: float | None = None
    source_id: str | None = None
    url: str | None = None
    title: str | None = None
    breadcrumb: str | None = None
    text: str | None = None
    current_relevance: int | float | None = None


class RankingsResponse(BaseModel):
    query: EvaluationQueryResponse
    top_k: int
    rankings: dict[str, list[RankingResultResponse]]


class JudgmentUpdateRequest(BaseModel):
    relevance: int = Field(..., ge=0, le=3)
    notes: str | None = None


class JudgmentResponse(BaseModel):
    query_id: str
    chunk_id: str
    relevance: int


class EvaluationRunRequest(BaseModel):
    k: int = Field(default=10, ge=1, le=100)


class EvaluationSummaryResponse(BaseModel):
    queries_count: int
    judged_queries_count: int
    total_judgments: int
    available_strategies: list[str]
    latest_report_exists: bool
    latest_averages: dict[str, dict[str, float]]


@router.get("/queries", response_model=list[EvaluationQueryResponse])
def list_queries() -> list[EvaluationQueryResponse]:
    return [_query_response(query) for query in store.load_queries()]


@router.post("/queries", response_model=EvaluationQueryResponse)
def create_query(payload: EvaluationQueryCreateRequest) -> EvaluationQueryResponse:
    if not payload.query.strip():
        raise HTTPException(status_code=400, detail="query must be a non-empty string")
    try:
        created = store.add_query(payload.query, payload.source_ids)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _query_response(created)


@router.post("/queries/{query_id}/rankings", response_model=RankingsResponse)
def run_rankings(
    query_id: str,
    payload: RankingRunRequest,
    request: Request,
) -> RankingsResponse:
    query = _get_query_or_404(query_id)
    strategies = _validate_strategies(payload.strategies)
    judgments = store.get_judgments(query_id)
    response_rankings: dict[str, list[RankingResultResponse]] = {}
    persisted_rankings: dict[str, list[str]] = {}

    for strategy in strategies:
        results = _run_strategy(strategy, query, payload.top_k, request)
        enriched = _enrich_results(
            results=results,
            chunk_repo=request.app.state.chunk_repo,
            judgments=judgments,
            source_ids=query.source_ids,
        )
        response_rankings[strategy] = enriched
        persisted_rankings[strategy] = [item.chunk_id for item in enriched]

    store.update_rankings_for_query(query_id, persisted_rankings)

    return RankingsResponse(
        query=_query_response(query),
        top_k=payload.top_k,
        rankings=response_rankings,
    )


@router.get("/queries/{query_id}/rankings", response_model=RankingsResponse)
def get_rankings(query_id: str, request: Request) -> RankingsResponse:
    query = _get_query_or_404(query_id)
    persisted = store.load_rankings()
    judgments = store.get_judgments(query_id)
    response_rankings: dict[str, list[RankingResultResponse]] = {}

    for strategy, rankings_by_query in persisted.items():
        chunk_ids = rankings_by_query.get(query_id)
        if chunk_ids is None:
            continue
        response_rankings[strategy] = _enrich_ids(
            chunk_ids=chunk_ids,
            chunk_repo=request.app.state.chunk_repo,
            judgments=judgments,
            source_ids=query.source_ids,
        )

    top_k = max((len(items) for items in response_rankings.values()), default=0)
    return RankingsResponse(
        query=_query_response(query),
        top_k=top_k,
        rankings=response_rankings,
    )


@router.put("/queries/{query_id}/judgments/{chunk_id:path}", response_model=JudgmentResponse)
def update_judgment(
    query_id: str,
    chunk_id: str,
    payload: JudgmentUpdateRequest,
) -> JudgmentResponse:
    _get_query_or_404(query_id)
    try:
        store.update_judgment(query_id, chunk_id, payload.relevance)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JudgmentResponse(
        query_id=query_id,
        chunk_id=chunk_id,
        relevance=payload.relevance,
    )


@router.get("/queries/{query_id}/judgments")
def get_judgments(query_id: str) -> dict[str, Any]:
    _get_query_or_404(query_id)
    return {
        "query_id": query_id,
        "judgments": store.get_judgments(query_id),
    }


@router.post("/run")
def run_evaluation(payload: EvaluationRunRequest) -> dict[str, Any]:
    rankings = store.load_rankings()
    qrels = store.load_qrels()
    report = evaluate_all_strategies(rankings, qrels, payload.k)
    store.save_report(report)
    return report


@router.get("/report")
def get_report() -> dict[str, Any]:
    report = store.load_report()
    if report is None:
        raise HTTPException(status_code=404, detail="Evaluation report not found")
    return report


@router.get("/summary", response_model=EvaluationSummaryResponse)
def get_summary() -> EvaluationSummaryResponse:
    return EvaluationSummaryResponse(**store.get_summary())


def _query_response(query: EvaluationQuery) -> EvaluationQueryResponse:
    return EvaluationQueryResponse(
        id=query.id,
        query=query.query,
        source_ids=query.source_ids,
    )


def _get_query_or_404(query_id: str) -> EvaluationQuery:
    query = store.get_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Evaluation query '{query_id}' not found")
    return query


def _validate_strategies(strategies: list[str]) -> list[str]:
    if not strategies:
        raise HTTPException(status_code=400, detail="strategies must not be empty")
    invalid = sorted(set(strategies) - VALID_STRATEGIES)
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported evaluation strategies: {', '.join(invalid)}",
        )
    return strategies


def _run_strategy(
    strategy: str,
    query: EvaluationQuery,
    top_k: int,
    request: Request,
) -> list[Any]:
    retriever = {
        "bm25": request.app.state.bm25_retriever,
        "vector": request.app.state.vector_retriever,
        "hybrid": request.app.state.hybrid_retriever,
    }[strategy]
    try:
        return retriever.search(query=query.query, top_k=top_k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("evaluation_ranking_failed strategy=%s query_id=%s", strategy, query.id)
        raise HTTPException(
            status_code=500,
            detail=f"Evaluation ranking failed for strategy '{strategy}'",
        ) from exc


def _enrich_results(
    results: list[Any],
    chunk_repo: ChunkRepository,
    judgments: dict[str, int | float],
    source_ids: list[str] | None,
) -> list[RankingResultResponse]:
    chunk_ids = [_result_chunk_id(result) for result in results]
    chunks = chunk_repo.get_chunks(chunk_ids)
    enriched: list[RankingResultResponse] = []

    for result in results:
        chunk_id = _result_chunk_id(result)
        item = _build_ranking_item(
            chunk_id=chunk_id,
            score=_result_score(result),
            chunk=chunks.get(chunk_id),
            relevance=judgments.get(chunk_id),
        )
        if _matches_source_filter(item, source_ids):
            enriched.append(item)

    return enriched


def _enrich_ids(
    chunk_ids: list[str],
    chunk_repo: ChunkRepository,
    judgments: dict[str, int | float],
    source_ids: list[str] | None,
) -> list[RankingResultResponse]:
    chunks = chunk_repo.get_chunks(chunk_ids)
    enriched: list[RankingResultResponse] = []

    for chunk_id in chunk_ids:
        item = _build_ranking_item(
            chunk_id=chunk_id,
            score=None,
            chunk=chunks.get(chunk_id),
            relevance=judgments.get(chunk_id),
        )
        if _matches_source_filter(item, source_ids):
            enriched.append(item)

    return enriched


def _result_chunk_id(result: Any) -> str:
    chunk_id = getattr(result, "chunk_id", None) or getattr(result, "doc_id", None)
    if isinstance(result, dict):
        chunk_id = result.get("chunk_id") or result.get("doc_id")
    if not isinstance(chunk_id, str) or not chunk_id.strip():
        raise HTTPException(status_code=500, detail="Retriever result is missing a chunk identifier")
    return chunk_id


def _result_score(result: Any) -> float | None:
    score = result.get("score") if isinstance(result, dict) else getattr(result, "score", None)
    return float(score) if score is not None else None


def _build_ranking_item(
    chunk_id: str,
    score: float | None,
    chunk: Any | None,
    relevance: int | float | None,
) -> RankingResultResponse:
    return RankingResultResponse(
        chunk_id=chunk_id,
        score=score,
        source_id=getattr(chunk, "source_id", None),
        url=getattr(chunk, "url", None),
        title=getattr(chunk, "title", None),
        breadcrumb=getattr(chunk, "breadcrumb", None),
        text=getattr(chunk, "text", None),
        current_relevance=relevance,
    )


def _matches_source_filter(
    item: RankingResultResponse,
    source_ids: list[str] | None,
) -> bool:
    if source_ids is None:
        return True
    if item.source_id is None:
        return True
    return item.source_id in set(source_ids)
