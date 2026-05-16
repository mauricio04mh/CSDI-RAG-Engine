from __future__ import annotations

import dataclasses
from pathlib import Path

from src.web_search.insufficiency_detector.config.settings import InsufficiencyDetectorSettings
from src.web_search.insufficiency_detector.detector import InsufficiencyDetector, simple_tokenize
from src.web_search.insufficiency_detector.reasons import InsufficiencyReason
from src.web_search.insufficiency_detector.schemas import RetrievedChunk


def _settings(**overrides) -> InsufficiencyDetectorSettings:
    base = InsufficiencyDetectorSettings(
        min_results=5,
        expected_results=10,
        min_top_score=0.35,
        relevant_overlap_threshold=0.20,
        min_relevant_results=2,
        min_coverage_score=0.20,
        min_answerability_score=0.40,
        min_source_diversity=0.30,
        confidence_threshold=0.55,
        coverage_top_n=5,
        w_top=0.25,
        w_quantity=0.15,
        w_coverage=0.25,
        w_diversity=0.20,
        w_answerability=0.15,
        env_path=Path("."),
        project_root=Path("."),
    )
    base_dict = {f.name: getattr(base, f.name) for f in dataclasses.fields(InsufficiencyDetectorSettings)}
    return InsufficiencyDetectorSettings(**{**base_dict, **overrides})


def test_insufficiency_no_results() -> None:
    detector = InsufficiencyDetector(settings=_settings())
    decision = detector.evaluate(query="python decorators", results=[])
    assert decision.needs_web_search is True
    assert decision.sufficiency_confidence == 0.0
    assert decision.reasons == [InsufficiencyReason.NO_RESULTS]
    assert decision.metrics.num_results == 0


def test_insufficiency_rrf_normalization_can_reach_one() -> None:
    # If the top doc appears at rank 1 in both lists and weights sum to 1.0,
    # the maximum RRF score is 1 / (k + 1). With k=60, that's 1/61.
    detector = InsufficiencyDetector(settings=_settings(confidence_threshold=0.7))
    top_rrf = 1.0 / 61.0
    results = [
        RetrievedChunk(
            chunk_id="c1",
            text="Python decorator examples and patterns.",
            score=top_rrf,
            url="https://example.com/a",
            source_id="s1",
        ),
        RetrievedChunk(
            chunk_id="c2",
            text="A decorator in Python wraps a function.",
            score=top_rrf / 2.0,
            url="https://example.com/b",
            source_id="s1",
        ),
    ]
    decision = detector.evaluate(
        query="python decorator",
        results=results,
        retrieval_context={
            "fusion": {"method": "rrf", "rrf_k": 60, "weights": {"bm25": 0.3, "vector": 0.7}}
        },
    )
    assert decision.metrics.top_score_norm == 1.0
    assert decision.needs_web_search is False


def test_insufficiency_low_diversity_reason_and_low_confidence() -> None:
    detector = InsufficiencyDetector(settings=_settings(confidence_threshold=0.8, min_source_diversity=0.6))
    results = [
        RetrievedChunk(
            chunk_id=f"c{i}",
            text="Python decorator examples and patterns.",
            score=0.9,
            url="https://example.com/same",
            source_id="s1",
        )
        for i in range(5)
    ]
    decision = detector.evaluate(query="python decorator", results=results)
    assert InsufficiencyReason.LOW_SOURCE_DIVERSITY in decision.reasons
    assert decision.needs_web_search is True


def test_insufficiency_low_coverage_triggers_web_search() -> None:
    detector = InsufficiencyDetector(settings=_settings(confidence_threshold=0.6))
    results = [
        RetrievedChunk(
            chunk_id="c1",
            text="This chunk is about HTTP caching headers.",
            score=0.9,
            url="https://example.com/a",
            source_id="s1",
        ),
        RetrievedChunk(
            chunk_id="c2",
            text="Another chunk about DNS resolution and networking.",
            score=0.8,
            url="https://example.com/b",
            source_id="s1",
        ),
    ]
    decision = detector.evaluate(query="python decorator", results=results)
    assert decision.needs_web_search is True
    assert InsufficiencyReason.LOW_COVERAGE in decision.reasons
    assert InsufficiencyReason.LOW_ANSWERABILITY in decision.reasons
    assert InsufficiencyReason.LOW_CONFIDENCE in decision.reasons


def test_insufficiency_is_independent_of_input_order_for_top_score_and_coverage() -> None:
    detector = InsufficiencyDetector(settings=_settings(coverage_top_n=1))
    results = [
        RetrievedChunk(
            chunk_id="c1",
            text="This chunk is about HTTP caching headers.",
            score=0.1,
            url="https://example.com/a",
            source_id="s1",
        ),
        RetrievedChunk(
            chunk_id="c2",
            text="Python decorator examples and patterns.",
            score=0.9,
            url="https://example.com/b",
            source_id="s2",
        ),
    ]
    decision = detector.evaluate(query="python decorator", results=results)
    assert decision.metrics.top_score == 0.9
    assert decision.metrics.coverage_score > 0.0
    assert decision.metrics.relevant_results == 1


def test_simple_tokenize_keeps_spanish_accents_and_enye() -> None:
    tokens = simple_tokenize("niñez corazón pingüino mañana año")
    assert tokens == ["niñez", "corazón", "pingüino", "mañana", "año"]
