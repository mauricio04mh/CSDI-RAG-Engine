from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable

from src.bm25.text.tokenizer import tokenize
from src.query_feedback.schemas import ExpansionResult

_TERM_RE = re.compile(r"[a-z0-9]+")


class QueryExpansionService:
    """Build deterministic pseudo-relevance feedback expansions from ranked chunks."""

    def expand_from_chunks(
        self,
        query: str,
        chunks: list[object],
        max_expansion_terms: int = 6,
    ) -> ExpansionResult:
        stripped_query = query.strip()
        if not stripped_query:
            raise ValueError("query must not be empty")

        if max_expansion_terms <= 0:
            return ExpansionResult(
                original_query=query,
                expanded_query=query,
                expansion_terms=[],
                method="pseudo_relevance_feedback",
                feedback_documents_used=len(chunks),
            )

        query_tokens = set(tokenize(stripped_query))
        candidates = self._extract_candidate_terms(chunks, query_tokens)
        ordered_terms = [
            term
            for term, _score in sorted(
                candidates.items(),
                key=lambda item: (-item[1], item[0]),
            )[:max_expansion_terms]
        ]

        return ExpansionResult(
            original_query=query,
            expanded_query=self._build_expanded_query(query, ordered_terms),
            expansion_terms=ordered_terms,
            method="pseudo_relevance_feedback",
            feedback_documents_used=len(chunks),
        )

    def _extract_candidate_terms(
        self,
        chunks: list[object],
        query_tokens: set[str],
    ) -> dict[str, float]:
        scores: dict[str, float] = defaultdict(float)

        for rank, chunk in enumerate(chunks, start=1):
            rank_weight = 1 / rank
            field_values = (
                (3.0, getattr(chunk, "title", None)),
                (2.0, getattr(chunk, "breadcrumb", None)),
                (1.0, getattr(chunk, "text", None)),
            )

            for field_weight, value in field_values:
                for term in self._unique_terms(value or ""):
                    candidate_tokens = self._candidate_tokens(term)
                    if not candidate_tokens or candidate_tokens & query_tokens:
                        continue
                    scores[term] += field_weight * rank_weight

        return dict(scores)

    def _candidate_tokens(self, term: str) -> set[str]:
        return set(tokenize(term))

    def _build_expanded_query(self, query: str, expansion_terms: list[str]) -> str:
        if not expansion_terms:
            return query
        return f"{query} {' '.join(expansion_terms)}"

    def _unique_terms(self, text: str) -> Iterable[str]:
        seen: set[str] = set()
        for raw_term in _TERM_RE.findall(text.lower()):
            if raw_term in seen:
                continue
            seen.add(raw_term)
            if len(raw_term) < 3 or raw_term.isdigit():
                continue
            if not self._candidate_tokens(raw_term):
                continue
            yield raw_term
