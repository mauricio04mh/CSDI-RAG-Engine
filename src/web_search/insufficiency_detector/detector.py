from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from src.web_search.insufficiency_detector.config.settings import InsufficiencyDetectorSettings
from src.web_search.insufficiency_detector.reasons import InsufficiencyReason
from src.web_search.insufficiency_detector.schemas import (
    InsufficiencyDecision,
    InsufficiencyMetrics,
    RetrievedChunk,
)

# Unicode word tokens without underscores so Spanish accents and "ñ" are kept.
_WORD_RE = re.compile(r"[^\W_]+")

_STOPWORDS: frozenset[str] = frozenset(
    {
        # English (small, high-impact set)
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "do",
        "does",
        "did",
        "this",
        "that",
        "these",
        "those",
        "it",
        "its",
        "as",
        "if",
        "then",
        "than",
        "when",
        "where",
        "how",
        "what",
        "which",
        "who",
        "why",
        # Spanish (small, high-impact set)
        "el",
        "la",
        "los",
        "las",
        "y",
        "o",
        "pero",
        "en",
        "de",
        "del",
        "al",
        "con",
        "por",
        "para",
        "es",
        "son",
        "ser",
        "como",
        "que",
        "qué",
        "cual",
        "cuál",
        "cuando",
        "cuándo",
        "donde",
        "dónde",
        "quien",
        "quién",
        "esto",
        "esta",
        "estos",
        "estas",
        "un",
        "una",
        "unos",
        "unas",
    }
)


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def simple_tokenize(text: str) -> list[str]:
    tokens = _WORD_RE.findall(text.lower())
    return [t for t in tokens if t not in _STOPWORDS and len(t) > 1]


def _safe_get(mapping: Mapping[str, Any] | None, key: str, default: Any = None) -> Any:
    if not mapping:
        return default
    return mapping.get(key, default)


@dataclass(slots=True)
class InsufficiencyDetector:
    settings: InsufficiencyDetectorSettings
    tokenize: Callable[[str], list[str]] = simple_tokenize

    def evaluate(
        self,
        query: str,
        results: list[RetrievedChunk],
        retrieval_context: Mapping[str, Any] | None = None,
    ) -> InsufficiencyDecision:
        num_results = len(results)
        if num_results == 0:
            metrics = InsufficiencyMetrics(
                num_results=0,
                unique_urls=0,
                top_score=0.0,
                top_score_norm=0.0,
                quantity_score=0.0,
                coverage_score=0.0,
                diversity_score=0.0,
                answerability_score=0.0,
                local_confidence=0.0,
                relevant_results=0,
            )
            return InsufficiencyDecision(
                needs_web_search=True,
                sufficiency_confidence=0.0,
                reasons=[InsufficiencyReason.NO_RESULTS],
                metrics=metrics,
            )

        top_score = _max_non_none((r.score for r in results), default=0.0)
        top_score_norm = self._normalize_top_score(top_score, retrieval_context)
        quantity_score = min(1.0, num_results / float(self.settings.expected_results))

        coverage_score, relevant_results = self._coverage_metrics(query, results)
        unique_urls = _unique_url_count(results)
        diversity_score = unique_urls / float(num_results) if num_results else 0.0
        answerability_score = self._answerability_score(coverage_score, relevant_results)

        local_confidence = _clamp01(
            self.settings.w_top * top_score_norm
            + self.settings.w_quantity * quantity_score
            + self.settings.w_coverage * coverage_score
            + self.settings.w_diversity * diversity_score
            + self.settings.w_answerability * answerability_score
        )

        reasons: list[InsufficiencyReason] = []
        if num_results < self.settings.min_results:
            reasons.append(InsufficiencyReason.LOW_NUM_RESULTS)
        if top_score_norm < self.settings.min_top_score:
            reasons.append(InsufficiencyReason.LOW_TOP_SCORE)
        if coverage_score < self.settings.min_coverage_score:
            reasons.append(InsufficiencyReason.LOW_COVERAGE)
        if diversity_score < self.settings.min_source_diversity:
            reasons.append(InsufficiencyReason.LOW_SOURCE_DIVERSITY)
        if answerability_score < self.settings.min_answerability_score:
            reasons.append(InsufficiencyReason.LOW_ANSWERABILITY)

        needs_web_search = local_confidence < self.settings.confidence_threshold
        if needs_web_search:
            reasons.append(InsufficiencyReason.LOW_CONFIDENCE)

        metrics = InsufficiencyMetrics(
            num_results=num_results,
            unique_urls=unique_urls,
            top_score=float(top_score),
            top_score_norm=float(top_score_norm),
            quantity_score=float(quantity_score),
            coverage_score=float(coverage_score),
            diversity_score=float(diversity_score),
            answerability_score=float(answerability_score),
            local_confidence=float(local_confidence),
            relevant_results=relevant_results,
        )

        return InsufficiencyDecision(
            needs_web_search=needs_web_search,
            sufficiency_confidence=float(local_confidence),
            reasons=_dedupe_preserve_order(reasons),
            metrics=metrics,
        )

    def _coverage_metrics(self, query: str, results: list[RetrievedChunk]) -> tuple[float, int]:
        query_terms = set(self.tokenize(query))
        if not query_terms:
            return 0.0, 0

        top_n = min(len(results), self.settings.coverage_top_n)
        ordered_results = sorted(
            results,
            key=lambda item: item.score if item.score is not None else float("-inf"),
            reverse=True,
        )
        coverages: list[float] = []
        relevant = 0
        for item in ordered_results[:top_n]:
            chunk_terms = set(self.tokenize(item.text))
            if not chunk_terms:
                coverages.append(0.0)
                continue
            overlap = len(query_terms.intersection(chunk_terms)) / float(len(query_terms))
            coverages.append(overlap)
            if overlap >= self.settings.relevant_overlap_threshold:
                relevant += 1

        coverage_score = sum(coverages) / float(len(coverages)) if coverages else 0.0
        return _clamp01(coverage_score), relevant

    def _answerability_score(self, coverage_score: float, relevant_results: int) -> float:
        if self.settings.min_relevant_results <= 0:
            relevant_ratio = 1.0
        else:
            relevant_ratio = min(1.0, relevant_results / float(self.settings.min_relevant_results))
        return _clamp01(0.6 * relevant_ratio + 0.4 * coverage_score)

    def _normalize_top_score(self, top_score: float, retrieval_context: Mapping[str, Any] | None) -> float:
        fusion = _safe_get(retrieval_context, "fusion")
        if isinstance(fusion, Mapping) and str(fusion.get("method", "")).lower() == "rrf":
            rrf_k = int(fusion.get("rrf_k", 60))
            weights = fusion.get("weights")
            weight_sum = 1.0
            if isinstance(weights, Mapping):
                try:
                    weight_sum = float(sum(float(v) for v in weights.values()))
                except Exception:
                    weight_sum = 1.0
            max_rrf = weight_sum / float(rrf_k + 1) if rrf_k >= 0 else 0.0
            if max_rrf > 0:
                return _clamp01(float(top_score) / max_rrf)
        return _clamp01(float(top_score))


def _max_non_none(values: Iterable[float | None], default: float) -> float:
    found = False
    best = float(default)
    for v in values:
        if v is None:
            continue
        value = float(v)
        if not found or value > best:
            best = value
            found = True
    return best if found else float(default)


def _unique_url_count(results: list[RetrievedChunk]) -> int:
    urls = [r.url for r in results if r.url]
    if urls:
        return len(set(urls))
    source_ids = [r.source_id for r in results if r.source_id]
    return len(set(source_ids)) if source_ids else 0


def _dedupe_preserve_order(values: list[InsufficiencyReason]) -> list[InsufficiencyReason]:
    seen: set[InsufficiencyReason] = set()
    out: list[InsufficiencyReason] = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out
