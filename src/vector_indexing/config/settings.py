from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values, load_dotenv

ENV_DEFAULTS: dict[str, str] = {
    # Sentence-transformers model used to generate dense document and query embeddings.
    "EMBEDDING_MODEL": "sentence-transformers/all-MiniLM-L6-v2",
    # Expected output dimension for the embedding model and FAISS index.
    "VECTOR_DIMENSION": "384",
    # FAISS index implementation to build for dense retrieval.
    "FAISS_INDEX_TYPE": "HNSW",
    # HNSW graph out-degree controlling recall and memory usage.
    "HNSW_M": "32",
    # HNSW construction-time exploration parameter controlling indexing quality.
    "HNSW_EF_CONSTRUCTION": "200",
    # HNSW search-time exploration parameter controlling recall and latency.
    "HNSW_EF_SEARCH": "50",
    # Directory where the FAISS index and metadata are persisted.
    "VECTOR_INDEX_PATH": "./vector_index",
    # Number of documents buffered before vectors are inserted into FAISS.
    "VECTOR_BATCH_SIZE": "128",
    # Logging verbosity used by the vector indexing service.
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
    vector_index_path: str
    vector_batch_size: int
    log_level: str
    env_path: Path
    project_root: Path


def _ensure_env_file(env_path: Path) -> None:
    """Create the project `.env` file and backfill missing keys."""
    env_path.parent.mkdir(parents=True, exist_ok=True)
    if not env_path.exists():
        env_path.write_text("", encoding="utf-8")

    current_values = dotenv_values(env_path)
    missing_lines = [f"{key}={value}" for key, value in ENV_DEFAULTS.items() if current_values.get(key) is None]
    if missing_lines:
        with env_path.open("a", encoding="utf-8") as file_obj:
            if env_path.stat().st_size > 0:
                file_obj.write("\n")
            file_obj.write("\n".join(missing_lines))
            file_obj.write("\n")


def _parse_positive_int(name: str, raw_value: str) -> int:
    """Parse a strictly positive integer setting."""
    value = int(raw_value)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")
    return value


def load_settings() -> VectorSettings:
    """Load and validate settings from the root `.env` file."""
    project_root = Path(__file__).resolve().parents[3]
    env_path = project_root / ".env"
    _ensure_env_file(env_path)
    load_dotenv(dotenv_path=env_path, override=False)

    raw_index_path = os.getenv("VECTOR_INDEX_PATH", ENV_DEFAULTS["VECTOR_INDEX_PATH"])
    index_path = Path(raw_index_path)
    if not index_path.is_absolute():
        index_path = project_root / index_path

    settings = VectorSettings(
        embedding_model=os.getenv("EMBEDDING_MODEL", ENV_DEFAULTS["EMBEDDING_MODEL"]),
        vector_dimension=_parse_positive_int(
            "VECTOR_DIMENSION",
            os.getenv("VECTOR_DIMENSION", ENV_DEFAULTS["VECTOR_DIMENSION"]),
        ),
        faiss_index_type=os.getenv("FAISS_INDEX_TYPE", ENV_DEFAULTS["FAISS_INDEX_TYPE"]).upper(),
        hnsw_m=_parse_positive_int("HNSW_M", os.getenv("HNSW_M", ENV_DEFAULTS["HNSW_M"])),
        hnsw_ef_construction=_parse_positive_int(
            "HNSW_EF_CONSTRUCTION",
            os.getenv("HNSW_EF_CONSTRUCTION", ENV_DEFAULTS["HNSW_EF_CONSTRUCTION"]),
        ),
        hnsw_ef_search=_parse_positive_int(
            "HNSW_EF_SEARCH",
            os.getenv("HNSW_EF_SEARCH", ENV_DEFAULTS["HNSW_EF_SEARCH"]),
        ),
        vector_index_path=str(index_path),
        vector_batch_size=_parse_positive_int(
            "VECTOR_BATCH_SIZE",
            os.getenv("VECTOR_BATCH_SIZE", ENV_DEFAULTS["VECTOR_BATCH_SIZE"]),
        ),
        log_level=os.getenv("LOG_LEVEL", ENV_DEFAULTS["LOG_LEVEL"]).upper(),
        env_path=env_path,
        project_root=project_root,
    )

    Path(settings.vector_index_path).mkdir(parents=True, exist_ok=True)
    return settings
