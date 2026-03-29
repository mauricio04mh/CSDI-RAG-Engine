from __future__ import annotations

import logging
import re
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from src.bm25.config.settings import BM25Settings
from src.bm25.scoring.bm25_scorer import BM25Scorer
from src.bm25.structures.bm25_index import BM25Index
from src.bm25.structures.postings_list import PostingsList
from src.database.repositories.bm25_repository import BM25Repository

logger = logging.getLogger(__name__)
TOKEN_PATTERN = re.compile(r"\w+")


@dataclass(slots=True)
class BM25Result:
    doc_id: str
    score: float


class BM25Retriever:
    """Loads the inverted index from the database and ranks candidates with BM25."""

    def __init__(self, settings: BM25Settings, engine: Engine) -> None:
        self.settings = settings
        self._bm25_repo = BM25Repository(engine)
        self.scorer = BM25Scorer(k1=settings.bm25_k1, b=settings.bm25_b)
        self._lock = threading.RLock()
        self._index = BM25Index(
            dictionary={}, postings={}, doc_lengths={}, total_documents=0, avg_document_length=0.0
        )

    def start(self) -> None:
        """Load the current inverted index from the database into memory."""
        with self._lock:
            raw_postings, dictionary, doc_lengths, total_documents, avg_document_length = (
                self._bm25_repo.load_full_index()
            )
            self._index = BM25Index(
                dictionary=dictionary,
                postings={
                    term: PostingsList.from_serialized(payload)
                    for term, payload in raw_postings.items()
                },
                doc_lengths=doc_lengths,
                total_documents=total_documents,
                avg_document_length=avg_document_length,
            )

    def reload(self) -> None:
        """Reload the lexical index from the database."""
        self.start()

    def search(self, query: str, top_k: int) -> list[BM25Result]:
        """Tokenize the query, score candidate documents and return the top results."""
        query_tokens = self._tokenize(query)
        if not query_tokens:
            return []

        with self._lock:
            query_term_counts = Counter(query_tokens)
            candidate_scores: dict[str, float] = defaultdict(float)

            for term, query_frequency in query_term_counts.items():
                postings_list = self._index.postings.get(term)
                if postings_list is None:
                    continue

                document_frequency = self._index.dictionary.get(term, len(postings_list))
                for posting in postings_list:
                    doc_length = self._index.doc_lengths.get(posting.doc_id)
                    if doc_length is None:
                        continue
                    score = self.scorer.score(
                        term_frequency=posting.tf,
                        document_length=doc_length,
                        avg_document_length=self._index.avg_document_length,
                        document_frequency=document_frequency,
                        total_documents=self._index.total_documents,
                    )
                    candidate_scores[posting.doc_id] += score * query_frequency

            ranked_results = sorted(
                (BM25Result(doc_id=doc_id, score=score) for doc_id, score in candidate_scores.items()),
                key=lambda item: item.score,
                reverse=True,
            )
            return ranked_results[:top_k]

    def _tokenize(self, query: str) -> list[str]:
        """Tokenize the query into lowercase lexical terms."""
        return [token.lower() for token in TOKEN_PATTERN.findall(query)]
