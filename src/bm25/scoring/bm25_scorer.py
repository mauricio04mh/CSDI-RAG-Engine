from __future__ import annotations

import math


class BM25Scorer:
<<<<<<< HEAD
    """Computes the BM25 relevance score for a single term-document pair."""

    def __init__(self, k1: float = 1.5, b: float = 0.75) -> None:
=======
    """Computes BM25 scores for lexical candidate documents."""

    def __init__(self, k1: float, b: float) -> None:
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
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
<<<<<<< HEAD
        if avg_document_length == 0 or total_documents == 0:
            return 0.0
        idf = math.log(((total_documents - document_frequency + 0.5) / (document_frequency + 0.5)) + 1.0)
        tf_norm = (term_frequency * (self.k1 + 1)) / (
            term_frequency + self.k1 * (1 - self.b + self.b * (document_length / avg_document_length))
        )
        return idf * tf_norm
=======
        """Compute the BM25 contribution of one query term for one document."""
        if term_frequency <= 0 or document_frequency <= 0 or total_documents <= 0:
            return 0.0
        if avg_document_length <= 0:
            avg_document_length = 1.0

        idf = math.log(((total_documents - document_frequency + 0.5) / (document_frequency + 0.5)) + 1.0)
        denominator = term_frequency + self.k1 * (1 - self.b + self.b * (document_length / avg_document_length))
        return idf * ((term_frequency * (self.k1 + 1)) / denominator)
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
