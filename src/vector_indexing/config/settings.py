from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values, load_dotenv

ENV_DEFAULTS: dict[str, str] = {
    "EMBEDDING_MODEL": "sentence-transformers/all-MiniLM-L6-v2",
    "VECTOR_DIMENSION": "384",
    "FAISS_INDEX_TYPE": "HNSW",
    "HNSW_M": "32",
    "HNSW_EF_CONSTRUCTION": "200",
    "HNSW_EF_SEARCH": "50",
<<<<<<< HEAD
=======
    # Number of documents buffered before vectors are inserted into FAISS.
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
    "VECTOR_BATCH_SIZE": "128",
    "LOG_LEVEL": "INFO",
}


@dataclass(slots=True)
class VectorSettings:
    """Validated vector indexing settings."""

    embedding_model: str
    vector_dimension: int
    faiss_index_type: str
    hnsw_m: int
    hnsw_ef_construction: int
    hnsw_ef_search: int
    vector_batch_size: int
    log_level: str
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


def _parse_positive_int(name: str, raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")
    return value


def load_settings() -> VectorSettings:
    project_root = Path(__file__).resolve().parents[3]
    env_path = project_root / ".env"
    _ensure_env_file(env_path)
    load_dotenv(dotenv_path=env_path, override=False)

    return VectorSettings(
        embedding_model=os.getenv("EMBEDDING_MODEL", ENV_DEFAULTS["EMBEDDING_MODEL"]),
        vector_dimension=_parse_positive_int("VECTOR_DIMENSION", os.getenv("VECTOR_DIMENSION", ENV_DEFAULTS["VECTOR_DIMENSION"])),
        faiss_index_type=os.getenv("FAISS_INDEX_TYPE", ENV_DEFAULTS["FAISS_INDEX_TYPE"]).upper(),
        hnsw_m=_parse_positive_int("HNSW_M", os.getenv("HNSW_M", ENV_DEFAULTS["HNSW_M"])),
<<<<<<< HEAD
        hnsw_ef_construction=_parse_positive_int("HNSW_EF_CONSTRUCTION", os.getenv("HNSW_EF_CONSTRUCTION", ENV_DEFAULTS["HNSW_EF_CONSTRUCTION"])),
        hnsw_ef_search=_parse_positive_int("HNSW_EF_SEARCH", os.getenv("HNSW_EF_SEARCH", ENV_DEFAULTS["HNSW_EF_SEARCH"])),
        vector_batch_size=_parse_positive_int("VECTOR_BATCH_SIZE", os.getenv("VECTOR_BATCH_SIZE", ENV_DEFAULTS["VECTOR_BATCH_SIZE"])),
=======
        hnsw_ef_construction=_parse_positive_int(
            "HNSW_EF_CONSTRUCTION",
            os.getenv("HNSW_EF_CONSTRUCTION", ENV_DEFAULTS["HNSW_EF_CONSTRUCTION"]),
        ),
        hnsw_ef_search=_parse_positive_int(
            "HNSW_EF_SEARCH",
            os.getenv("HNSW_EF_SEARCH", ENV_DEFAULTS["HNSW_EF_SEARCH"]),
        ),
        vector_batch_size=_parse_positive_int(
            "VECTOR_BATCH_SIZE",
            os.getenv("VECTOR_BATCH_SIZE", ENV_DEFAULTS["VECTOR_BATCH_SIZE"]),
        ),
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
        log_level=os.getenv("LOG_LEVEL", ENV_DEFAULTS["LOG_LEVEL"]).upper(),
        env_path=env_path,
        project_root=project_root,
    )
