from __future__ import annotations

import math

import pytest

from src.bm25.scoring.bm25_scorer import BM25Scorer


@pytest.fixture()
def scorer() -> BM25Scorer:
    return BM25Scorer(k1=1.5, b=0.75)


# ---------------------------------------------------------------------------
# edge cases that must return 0.0
# ---------------------------------------------------------------------------

def test_zero_term_frequency_returns_zero(scorer):
    assert scorer.score(term_frequency=0, document_length=100, avg_document_length=100,
                        document_frequency=5, total_documents=100) == 0.0


def test_zero_document_frequency_returns_zero(scorer):
    assert scorer.score(term_frequency=3, document_length=100, avg_document_length=100,
                        document_frequency=0, total_documents=100) == 0.0


def test_zero_total_documents_returns_zero(scorer):
    assert scorer.score(term_frequency=3, document_length=100, avg_document_length=100,
                        document_frequency=5, total_documents=0) == 0.0


def test_zero_avg_doc_length_uses_fallback(scorer):
    # avg_document_length=0 → clamped to 1.0 internally, must not crash
    result = scorer.score(term_frequency=2, document_length=10, avg_document_length=0,
                          document_frequency=5, total_documents=100)
    assert result > 0.0


# ---------------------------------------------------------------------------
# positive scores
# ---------------------------------------------------------------------------

def test_positive_score_for_valid_inputs(scorer):
    result = scorer.score(term_frequency=3, document_length=100, avg_document_length=100,
                          document_frequency=5, total_documents=1000)
    assert result > 0.0


def test_higher_tf_gives_higher_score(scorer):
    base = dict(document_length=100, avg_document_length=100,
                document_frequency=5, total_documents=1000)
    low = scorer.score(term_frequency=1, **base)
    high = scorer.score(term_frequency=10, **base)
    assert high > low


def test_rarer_term_gives_higher_idf(scorer):
    base = dict(term_frequency=3, document_length=100,
                avg_document_length=100, total_documents=1000)
    common = scorer.score(document_frequency=500, **base)
    rare = scorer.score(document_frequency=5, **base)
    assert rare > common


def test_longer_doc_penalized_when_b_positive(scorer):
    base = dict(term_frequency=3, avg_document_length=100,
                document_frequency=5, total_documents=1000)
    short = scorer.score(document_length=50, **base)
    long_ = scorer.score(document_length=500, **base)
    assert short > long_


def test_idf_formula_is_correct(scorer):
    # idf = log((N - df + 0.5) / (df + 0.5) + 1)
    N, df = 1000, 10
    expected_idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
    # with tf=1, doc_length=avg_length the TF norm simplifies nicely
    tf = 1
    avg = doc_len = 100
    denominator = tf + scorer.k1 * (1 - scorer.b + scorer.b * (doc_len / avg))
    expected_tf_norm = (tf * (scorer.k1 + 1)) / denominator
    expected = expected_idf * expected_tf_norm
    result = scorer.score(term_frequency=tf, document_length=doc_len,
                          avg_document_length=avg, document_frequency=df,
                          total_documents=N)
    assert abs(result - expected) < 1e-10
