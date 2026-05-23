from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_USER_SOURCES_PATH = Path("user_sources.json")


@dataclass
class UserSource:
    source_id: str
    name: str
    base_url: str
    source_kind: str  # 'url_manual' | 'upload_file'
    added_at: str


class UserSourceRepository:
    """Tracks user-added sources (URL manual and uploaded files) persisted to user_sources.json."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sources: dict[str, UserSource] = self._load()

    def register(self, source_id: str, name: str, base_url: str, source_kind: str) -> None:
        with self._lock:
            self._sources[source_id] = UserSource(
                source_id=source_id,
                name=name,
                base_url=base_url,
                source_kind=source_kind,
                added_at=datetime.now(timezone.utc).isoformat(),
            )
        self._save()

    def list_sources(self) -> list[UserSource]:
        with self._lock:
            return list(self._sources.values())

    def _load(self) -> dict[str, UserSource]:
        try:
            data = json.loads(_USER_SOURCES_PATH.read_text(encoding="utf-8"))
            return {entry["source_id"]: UserSource(**entry) for entry in data}
        except FileNotFoundError:
            return {}
        except Exception as exc:
            logger.warning("user_sources_load_failed reason=%s", exc)
            return {}

    def _save(self) -> None:
        try:
            with self._lock:
                data = [asdict(s) for s in self._sources.values()]
            _USER_SOURCES_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("user_sources_save_failed reason=%s", exc)
