from __future__ import annotations

from enum import StrEnum


class InsufficiencyReason(StrEnum):
    NO_RESULTS = "no_results"
    LOW_NUM_RESULTS = "low_num_results"
    LOW_TOP_SCORE = "low_top_score"
    LOW_COVERAGE = "low_coverage"
    LOW_SOURCE_DIVERSITY = "low_source_diversity"
    LOW_ANSWERABILITY = "low_answerability"
    LOW_CONFIDENCE = "low_confidence"

