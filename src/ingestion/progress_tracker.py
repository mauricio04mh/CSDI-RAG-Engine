from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

IngestPhase = Literal["idle", "crawling", "indexing", "completed", "error"]

_HISTORY_PATH = Path("ingest_history.json")


@dataclass
class IngestionProgress:
    source_id: str
    phase: IngestPhase = "idle"
    pages_total: int = 0
    pages_scraped: int = 0
    chunks_indexed: int = 0
    progress_pct: float = 0.0
    started_at: str | None = None
    finished_at: str | None = None
    last_ingest_at: str | None = None
    error: str | None = None


class IngestionTracker:
    """Thread-safe in-memory tracker for active ingestions + persistent last_ingest_at."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: dict[str, IngestionProgress] = {}
        self._history: dict[str, str] = self._load_history()

    # ── public API ────────────────────────────────────────────────────────────

    def start(self, source_id: str, pages_total: int = 0) -> None:
        with self._lock:
            self._active[source_id] = IngestionProgress(
                source_id=source_id,
                phase="crawling",
                pages_total=pages_total,
                started_at=_now(),
            )

    def set_pages_total(self, source_id: str, pages_total: int) -> None:
        with self._lock:
            prog = self._active.get(source_id)
            if prog:
                prog.pages_total = pages_total
                prog.phase = "indexing"
                prog.progress_pct = 0.0

    def page_done(self, source_id: str, chunks_this_page: int) -> None:
        with self._lock:
            prog = self._active.get(source_id)
            if prog:
                prog.pages_scraped += 1
                prog.chunks_indexed += chunks_this_page
                if prog.pages_total > 0:
                    prog.progress_pct = round(
                        prog.pages_scraped / prog.pages_total * 100, 1
                    )

    def complete(self, source_id: str) -> None:
        now = _now()
        with self._lock:
            prog = self._active.get(source_id)
            if prog:
                prog.phase = "completed"
                prog.progress_pct = 100.0
                prog.finished_at = now
                prog.last_ingest_at = now
            self._history[source_id] = now
        self._save_history()

    def fail(self, source_id: str, error: str) -> None:
        with self._lock:
            prog = self._active.get(source_id)
            if prog:
                prog.phase = "error"
                prog.finished_at = _now()
                prog.error = error

    def get(self, source_id: str) -> IngestionProgress | None:
        with self._lock:
            return self._active.get(source_id)

    def seed_from_db(self, db_dates: dict[str, str]) -> None:
        """Backfill last_ingest_at from DB for sources with no JSON history entry."""
        with self._lock:
            for source_id, last_at in db_dates.items():
                if source_id not in self._history:
                    self._history[source_id] = last_at
        logger.debug("ingest_history_seeded_from_db count=%s", len(db_dates))

    def last_ingest_at(self, source_id: str) -> str | None:
        with self._lock:
            active = self._active.get(source_id)
            if active and active.last_ingest_at:
                return active.last_ingest_at
            return self._history.get(source_id)

    def to_dict(self, source_id: str) -> dict:
        with self._lock:
            prog = self._active.get(source_id)
            if prog:
                return asdict(prog)
            return {
                "source_id": source_id,
                "phase": "idle",
                "pages_total": 0,
                "pages_scraped": 0,
                "chunks_indexed": 0,
                "progress_pct": 0.0,
                "started_at": None,
                "finished_at": None,
                "last_ingest_at": self._history.get(source_id),
                "error": None,
            }

    # ── persistence ───────────────────────────────────────────────────────────

    def _load_history(self) -> dict[str, str]:
        try:
            return json.loads(_HISTORY_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except Exception as exc:
            logger.warning("ingest_history_load_failed reason=%s", exc)
            return {}

    def _save_history(self) -> None:
        try:
            with self._lock:
                data = dict(self._history)
            _HISTORY_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("ingest_history_save_failed reason=%s", exc)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
