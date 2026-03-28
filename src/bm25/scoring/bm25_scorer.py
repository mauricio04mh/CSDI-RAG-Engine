from __future__ import annotations

import math


class BM25Scorer:
    """Computes BM25 scores for lexical candidate documents."""

    def __init__(self, k1: float, b: float) -> None:
        self.k1 = k1
        self.b = b

    def score(
        self,
        term_frequency: int,
        document_length: int,
        avg_document_length: float,
        document_frequency: int,
        total_documents: int,
    ) -> float:
        """Compute the BM25 contribution of one query term for one document."""
        if term_frequency <= 0 or document_frequency <= 0 or total_documents <= 0:
            return 0.0
        if avg_document_length <= 0:
            avg_document_length = 1.0

        idf = math.log(((total_documents - document_frequency + 0.5) / (document_frequency + 0.5)) + 1.0)
        denominator = term_frequency + self.k1 * (1 - self.b + self.b * (document_length / avg_document_length))
        return idf * ((term_frequency * (self.k1 + 1)) / denominator)
