from __future__ import annotations

from typing import Protocol

from src.web_search.schemas import WebSearchDocument, WebSearchHit


class DocumentFetcher(Protocol):
    def fetch(self, hit: WebSearchHit) -> WebSearchDocument | None:
        ...
