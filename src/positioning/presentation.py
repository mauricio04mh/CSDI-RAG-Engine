from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class PositionedResult:
    chunk_id: str
    relevance_score: float
    freshness_score: float
    display_priority: float
    rank: int
    source_type: str
    retrieval_method: str


def build_positioned_results(query: str, hits: list, chunks: dict[str, object]) -> dict[str, PositionedResult]:
    """Build presentation-oriented ranking fields for already retrieved chunks."""
    _ = query
    hit_scores = [float(getattr(hit, "score", 0.0)) for hit in hits]
    min_score = min(hit_scores) if hit_scores else 0.0
    max_score = max(hit_scores) if hit_scores else 0.0

    candidates: list[tuple[object, object, PositionedResult]] = []
    for hit in hits:
        chunk = chunks.get(hit.doc_id)
        if chunk is None:
            continue
        source_type = _source_type_for_chunk(chunk)
        retrieval_method = _retrieval_method_for_hit(hit, source_type)
        relevance_score = _normalize_score(float(getattr(hit, "score", 0.0)), min_score, max_score)
        freshness = compute_freshness_score(_reference_date(chunk))
        display_priority = compute_display_priority(
            relevance_score=relevance_score,
            freshness_score=freshness,
            source_type=source_type,
            retrieval_method=retrieval_method,
        )
        candidates.append(
            (
                hit,
                chunk,
                PositionedResult(
                    chunk_id=hit.doc_id,
                    relevance_score=relevance_score,
                    freshness_score=freshness,
                    display_priority=display_priority,
                    rank=0,
                    source_type=source_type,
                    retrieval_method=retrieval_method,
                ),
            )
        )

    candidates.sort(key=lambda item: item[2].display_priority, reverse=True)
    positioned: dict[str, PositionedResult] = {}
    for rank, (_, _, result) in enumerate(candidates, start=1):
        result.rank = rank
        positioned[result.chunk_id] = result
    return positioned


def compute_freshness_score(reference_date: datetime | str | None) -> float:
    date_value = _coerce_datetime(reference_date)
    if date_value is None:
        return 0.5

    now = datetime.now(timezone.utc)
    age_days = max((now - date_value).days, 0)
    if age_days <= 7:
        return 1.0
    if age_days <= 30:
        return 0.8
    if age_days <= 90:
        return 0.6
    if age_days <= 180:
        return 0.4
    if age_days <= 365:
        return 0.2
    return 0.1


def compute_display_priority(
    *,
    relevance_score: float,
    freshness_score: float,
    source_type: str,
    retrieval_method: str,
) -> float:
    source_score = {
        "corpus": 1.0,
        "web_cache": 0.2,
    }.get(source_type, 0.4)
    method_score = {
        "hybrid": 1.0,
        "vector": 0.85,
        "bm25": 0.8,
        "web_cache": 0.75,
    }.get(retrieval_method, 0.6)
    return round(
        relevance_score * 0.65
        + freshness_score * 0.05
        + source_score * 0.20
        + method_score * 0.10,
        6,
    )


def _normalize_score(score: float, min_score: float, max_score: float) -> float:
    if max_score <= min_score:
        return 1.0 if score > 0 else 0.0
    return (score - min_score) / (max_score - min_score)


def _source_type_for_chunk(chunk: object) -> str:
    source_id = str(getattr(chunk, "source_id", ""))
    breadcrumb = str(getattr(chunk, "breadcrumb", ""))
    if source_id.startswith("web:") or breadcrumb == "web-search":
        return "web_cache"
    return "corpus"


def _retrieval_method_for_hit(hit: object, source_type: str) -> str:
    if source_type == "web_cache":
        return "web_cache"
    found_by = getattr(hit, "found_by", frozenset())
    if "bm25" in found_by and "vector" in found_by:
        return "hybrid"
    if "bm25" in found_by:
        return "bm25"
    if "vector" in found_by:
        return "vector"
    return "hybrid"


def _reference_date(chunk: object) -> datetime | str | None:
    return (
        getattr(chunk, "document_updated_at", None)
        or getattr(chunk, "published_at", None)
        or getattr(chunk, "created_at", None)
    )


def _coerce_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
