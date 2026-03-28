from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values, load_dotenv

ENV_DEFAULTS: dict[str, str] = {
    "BM25_K1": "1.5",
    "BM25_B": "0.75",
}


@dataclass(slots=True)
class BM25Settings:
    """Validated BM25 retrieval settings."""

    bm25_k1: float
    bm25_b: float
    env_path: Path
    project_root: Path


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


def _parse_positive_float(name: str, raw: str) -> float:
    value = float(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")
    return value


def load_settings() -> BM25Settings:
    project_root = Path(__file__).resolve().parents[3]
    env_path = project_root / ".env"
    _ensure_env_file(env_path)
    load_dotenv(dotenv_path=env_path, override=False)

    return BM25Settings(
        bm25_k1=_parse_positive_float("BM25_K1", os.getenv("BM25_K1", ENV_DEFAULTS["BM25_K1"])),
        bm25_b=_parse_positive_float("BM25_B", os.getenv("BM25_B", ENV_DEFAULTS["BM25_B"])),
        env_path=env_path,
        project_root=project_root,
    )
