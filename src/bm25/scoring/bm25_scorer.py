from __future__ import annotations

import math


class BM25Scorer:
    """Computes the BM25 relevance score for a single term-document pair."""

    def __init__(self, k1: float = 1.5, b: float = 0.75) -> None:
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
        if avg_document_length == 0 or total_documents == 0:
            return 0.0
        idf = math.log(((total_documents - document_frequency + 0.5) / (document_frequency + 0.5)) + 1.0)
        tf_norm = (term_frequency * (self.k1 + 1)) / (
            term_frequency + self.k1 * (1 - self.b + self.b * (document_length / avg_document_length))
        )
        return idf * tf_norm
