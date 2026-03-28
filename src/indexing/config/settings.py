from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values, load_dotenv

ENV_DEFAULTS: dict[str, str] = {
    "INDEX_BUFFER_SIZE": "10000",
<<<<<<< HEAD
    "INDEX_MAX_SEGMENTS_IN_MEMORY": "5",
=======
    # Maximum number of active segments allowed before automatic merging starts.
    "INDEX_MAX_SEGMENTS_IN_MEMORY": "5",
    # Maximum number of seconds between automatic flushes of pending in-memory documents.
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
    "INDEX_FLUSH_INTERVAL": "30",
    "LOG_LEVEL": "INFO",
}


@dataclass(slots=True)
class Settings:
    """Validated settings used across the indexing service."""

    index_buffer_size: int
    index_max_segments_in_memory: int
    index_flush_interval: int
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


<<<<<<< HEAD
def _parse_positive_int(name: str, raw: str) -> int:
    value = int(raw)
=======
def _parse_positive_int(name: str, raw_value: str) -> int:
    """Parse a strictly positive integer setting."""
    value = int(raw_value)
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")
    return value


def load_settings() -> Settings:
    project_root = Path(__file__).resolve().parents[3]
    env_path = project_root / ".env"
    _ensure_env_file(env_path)
    load_dotenv(dotenv_path=env_path, override=False)

    settings = Settings(
<<<<<<< HEAD
        index_buffer_size=_parse_positive_int("INDEX_BUFFER_SIZE", os.getenv("INDEX_BUFFER_SIZE", ENV_DEFAULTS["INDEX_BUFFER_SIZE"])),
        index_max_segments_in_memory=_parse_positive_int("INDEX_MAX_SEGMENTS_IN_MEMORY", os.getenv("INDEX_MAX_SEGMENTS_IN_MEMORY", ENV_DEFAULTS["INDEX_MAX_SEGMENTS_IN_MEMORY"])),
        index_flush_interval=_parse_positive_int("INDEX_FLUSH_INTERVAL", os.getenv("INDEX_FLUSH_INTERVAL", ENV_DEFAULTS["INDEX_FLUSH_INTERVAL"])),
=======
        index_buffer_size=_parse_positive_int(
            "INDEX_BUFFER_SIZE",
            os.getenv("INDEX_BUFFER_SIZE", ENV_DEFAULTS["INDEX_BUFFER_SIZE"]),
        ),
        index_max_segments_in_memory=_parse_positive_int(
            "INDEX_MAX_SEGMENTS_IN_MEMORY",
            os.getenv("INDEX_MAX_SEGMENTS_IN_MEMORY", ENV_DEFAULTS["INDEX_MAX_SEGMENTS_IN_MEMORY"]),
        ),
        index_flush_interval=_parse_positive_int(
            "INDEX_FLUSH_INTERVAL",
            os.getenv("INDEX_FLUSH_INTERVAL", ENV_DEFAULTS["INDEX_FLUSH_INTERVAL"]),
        ),
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
        log_level=os.getenv("LOG_LEVEL", ENV_DEFAULTS["LOG_LEVEL"]).upper(),
        env_path=env_path,
        project_root=project_root,
    )
<<<<<<< HEAD
=======

>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
    logging.getLogger(__name__).debug("settings_loaded env_path=%s", env_path)
    return settings
