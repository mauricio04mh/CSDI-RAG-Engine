from __future__ import annotations

import logging
from urllib.parse import parse_qs, unquote, urlparse

import httpx
from bs4 import BeautifulSoup

from src.web_search.schemas import WebSearchHit

logger = logging.getLogger(__name__)


class DuckDuckGoSearchProvider:
    """DuckDuckGo HTML search provider (no API key required)."""

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        endpoint: str = "https://duckduckgo.com/html/",
        user_agent: str = "CSDI-RAG-Engine/0.1",
    ) -> None:
        self._timeout = timeout
        self._endpoint = endpoint
        self._headers = {"User-Agent": user_agent}

    def search(self, query: str, top_k: int) -> list[WebSearchHit]:
        try:
            with httpx.Client(timeout=self._timeout, follow_redirects=True, headers=self._headers) as client:
                response = client.get(self._endpoint, params={"q": query})
            response.raise_for_status()
        except Exception as exc:
            logger.warning("duckduckgo_search_failed query=%s reason=%s", query, exc)
            return []

        soup = BeautifulSoup(response.text, "lxml")
        result_nodes = soup.select(".result")

        hits: list[WebSearchHit] = []
        for node in result_nodes:
            if len(hits) >= top_k:
                break

            anchor = node.select_one("a.result__a")
            if anchor is None:
                continue

            title = anchor.get_text(" ", strip=True)
            url = _normalize_duckduckgo_link(anchor.get("href", ""))
            snippet_node = node.select_one(".result__snippet")
            snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""

            if not title or not url:
                continue

            hits.append(
                WebSearchHit(
                    title=title,
                    url=url,
                    snippet=snippet,
                    provider="duckduckgo",
                    metadata={},
                )
            )

        logger.info("duckduckgo_search_completed query=%s hits=%s", query, len(hits))
        return hits


def _normalize_duckduckgo_link(raw_url: str) -> str:
    """Resolve DuckDuckGo redirect links to the original URL when possible."""
    if not raw_url:
        return ""
    parsed = urlparse(raw_url)
    is_ddg_redirect = parsed.path.startswith("/l/") and (
        parsed.netloc == "" or parsed.netloc.endswith("duckduckgo.com")
    )
    if is_ddg_redirect:
        uddg = parse_qs(parsed.query).get("uddg")
        if uddg:
            return unquote(uddg[0])
    return raw_url
