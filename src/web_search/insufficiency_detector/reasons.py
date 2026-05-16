from __future__ import annotations

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - Python < 3.11
    from enum import Enum

    class StrEnum(str, Enum):
        """Compatibility shim for Python versions without enum.StrEnum."""


class InsufficiencyReason(StrEnum):
    NO_RESULTS = "no_results"
    LOW_NUM_RESULTS = "low_num_results"
    LOW_TOP_SCORE = "low_top_score"
    LOW_COVERAGE = "low_coverage"
    LOW_SOURCE_DIVERSITY = "low_source_diversity"
    LOW_ANSWERABILITY = "low_answerability"
    LOW_CONFIDENCE = "low_confidence"
