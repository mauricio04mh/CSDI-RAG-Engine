from __future__ import annotations

from typing import Protocol

from src.web_search.schemas import WebSearchHit


class SearchProvider(Protocol):
    def search(self, query: str, top_k: int) -> list[WebSearchHit]:
        ...
