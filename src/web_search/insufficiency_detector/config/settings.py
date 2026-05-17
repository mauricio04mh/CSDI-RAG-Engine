from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import dotenv_values, load_dotenv
except ModuleNotFoundError:  # pragma: no cover
    def dotenv_values(path: Path) -> dict[str, str]:
        values: dict[str, str] = {}
        try:
            raw = Path(path).read_text(encoding="utf-8")
        except FileNotFoundError:
            return values
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
        return values

    def load_dotenv(*, dotenv_path: Path, override: bool = False) -> bool:
        loaded = False
        for key, value in dotenv_values(Path(dotenv_path)).items():
            if override or os.getenv(key) is None:
                os.environ[key] = value
                loaded = True
        return loaded

ENV_DEFAULTS: dict[str, str] = {
    # Count-based sufficiency
    "INSUFF_MIN_RESULTS": "5",
    "INSUFF_EXPECTED_RESULTS": "10",
    # Score-based sufficiency (applied to normalized top score)
    "INSUFF_MIN_TOP_SCORE": "0.35",
    # Coverage / answerability
    "INSUFF_RELEVANT_OVERLAP_THRESHOLD": "0.20",
    "INSUFF_MIN_RELEVANT_RESULTS": "2",
    "INSUFF_MIN_COVERAGE_SCORE": "0.20",
    "INSUFF_MIN_ANSWERABILITY_SCORE": "0.40",
    # Diversity
    "INSUFF_MIN_SOURCE_DIVERSITY": "0.30",
    # Confidence decision
    "INSUFF_CONFIDENCE_THRESHOLD": "0.65",
    # Feature computation limits
    "INSUFF_COVERAGE_TOP_N": "5",
    # Metric weights (must sum to 1.0)
    # W_TOP reduced: retrieval score != answer quality (high scores for topic-matched but non-answering docs)
    # W_COVERAGE/W_ANSWERABILITY raised: better proxies for whether chunks actually answer the query
    "INSUFF_W_TOP": "0.10",
    "INSUFF_W_QUANTITY": "0.15",
    "INSUFF_W_COVERAGE": "0.35",
    "INSUFF_W_DIVERSITY": "0.15",
    "INSUFF_W_ANSWERABILITY": "0.25",
}


@dataclass(slots=True, frozen=True)
class InsufficiencyDetectorSettings:
    min_results: int
    expected_results: int
    min_top_score: float
    relevant_overlap_threshold: float
    min_relevant_results: int
    min_coverage_score: float
    min_answerability_score: float
    min_source_diversity: float
    confidence_threshold: float
    coverage_top_n: int
    w_top: float
    w_quantity: float
    w_coverage: float
    w_diversity: float
    w_answerability: float
    env_path: Path
    project_root: Path

    def weight_sum(self) -> float:
        return self.w_top + self.w_quantity + self.w_coverage + self.w_diversity + self.w_answerability


def _ensure_env_file(env_path: Path) -> None:
    env_path.parent.mkdir(parents=True, exist_ok=True)
    if not env_path.exists():
        env_path.write_text("", encoding="utf-8")
    current_values = dotenv_values(env_path)
    missing_lines = [f"{k}={v}" for k, v in ENV_DEFAULTS.items() if current_values.get(k) is None]
    if missing_lines:
        with env_path.open("a", encoding="utf-8") as f:
            if env_path.stat().st_size > 0:
                f.write("\n")
            f.write("\n".join(missing_lines) + "\n")


def _parse_positive_int(name: str, raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")
    return value


def _parse_fraction(name: str, raw: str) -> float:
    value = float(raw)
    if not (0.0 <= value <= 1.0):
        raise ValueError(f"{name} must be between 0.0 and 1.0.")
    return value


def load_settings() -> InsufficiencyDetectorSettings:
    project_root = Path(__file__).resolve().parents[4]
    env_path = project_root / ".env"
    _ensure_env_file(env_path)
    load_dotenv(dotenv_path=env_path, override=False)

    settings = InsufficiencyDetectorSettings(
        min_results=_parse_positive_int("INSUFF_MIN_RESULTS", os.getenv("INSUFF_MIN_RESULTS", ENV_DEFAULTS["INSUFF_MIN_RESULTS"])),
        expected_results=_parse_positive_int("INSUFF_EXPECTED_RESULTS", os.getenv("INSUFF_EXPECTED_RESULTS", ENV_DEFAULTS["INSUFF_EXPECTED_RESULTS"])),
        min_top_score=_parse_fraction("INSUFF_MIN_TOP_SCORE", os.getenv("INSUFF_MIN_TOP_SCORE", ENV_DEFAULTS["INSUFF_MIN_TOP_SCORE"])),
        relevant_overlap_threshold=_parse_fraction(
            "INSUFF_RELEVANT_OVERLAP_THRESHOLD",
            os.getenv("INSUFF_RELEVANT_OVERLAP_THRESHOLD", ENV_DEFAULTS["INSUFF_RELEVANT_OVERLAP_THRESHOLD"]),
        ),
        min_relevant_results=_parse_positive_int(
            "INSUFF_MIN_RELEVANT_RESULTS",
            os.getenv("INSUFF_MIN_RELEVANT_RESULTS", ENV_DEFAULTS["INSUFF_MIN_RELEVANT_RESULTS"]),
        ),
        min_coverage_score=_parse_fraction(
            "INSUFF_MIN_COVERAGE_SCORE",
            os.getenv("INSUFF_MIN_COVERAGE_SCORE", ENV_DEFAULTS["INSUFF_MIN_COVERAGE_SCORE"]),
        ),
        min_answerability_score=_parse_fraction(
            "INSUFF_MIN_ANSWERABILITY_SCORE",
            os.getenv("INSUFF_MIN_ANSWERABILITY_SCORE", ENV_DEFAULTS["INSUFF_MIN_ANSWERABILITY_SCORE"]),
        ),
        min_source_diversity=_parse_fraction(
            "INSUFF_MIN_SOURCE_DIVERSITY",
            os.getenv("INSUFF_MIN_SOURCE_DIVERSITY", ENV_DEFAULTS["INSUFF_MIN_SOURCE_DIVERSITY"]),
        ),
        confidence_threshold=_parse_fraction(
            "INSUFF_CONFIDENCE_THRESHOLD",
            os.getenv("INSUFF_CONFIDENCE_THRESHOLD", ENV_DEFAULTS["INSUFF_CONFIDENCE_THRESHOLD"]),
        ),
        coverage_top_n=_parse_positive_int(
            "INSUFF_COVERAGE_TOP_N",
            os.getenv("INSUFF_COVERAGE_TOP_N", ENV_DEFAULTS["INSUFF_COVERAGE_TOP_N"]),
        ),
        w_top=_parse_fraction("INSUFF_W_TOP", os.getenv("INSUFF_W_TOP", ENV_DEFAULTS["INSUFF_W_TOP"])),
        w_quantity=_parse_fraction("INSUFF_W_QUANTITY", os.getenv("INSUFF_W_QUANTITY", ENV_DEFAULTS["INSUFF_W_QUANTITY"])),
        w_coverage=_parse_fraction("INSUFF_W_COVERAGE", os.getenv("INSUFF_W_COVERAGE", ENV_DEFAULTS["INSUFF_W_COVERAGE"])),
        w_diversity=_parse_fraction("INSUFF_W_DIVERSITY", os.getenv("INSUFF_W_DIVERSITY", ENV_DEFAULTS["INSUFF_W_DIVERSITY"])),
        w_answerability=_parse_fraction("INSUFF_W_ANSWERABILITY", os.getenv("INSUFF_W_ANSWERABILITY", ENV_DEFAULTS["INSUFF_W_ANSWERABILITY"])),
        env_path=env_path,
        project_root=project_root,
    )

    if abs(settings.weight_sum() - 1.0) > 1e-6:
        raise ValueError("Insufficiency detector weights must sum to 1.0.")

    return settings
