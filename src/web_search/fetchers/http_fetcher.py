from __future__ import annotations

import logging
import re

import httpx

from src.scraper.scraper import Scraper
from src.sources_config.schemas.source_config import ScraperConfig
from src.web_search.schemas import WebSearchDocument, WebSearchHit

logger = logging.getLogger(__name__)
_WHITESPACE = re.compile(r"\s+")
_WEB_SEARCH_SCRAPER_CONFIG = ScraperConfig(
    main_content_selectors=["main", "article", "[role='main']", "body"],
    title_selectors=["h1", "title"],
    breadcrumb_selectors=[],
    code_block_selectors=["pre code", "pre"],
    exclude_selectors=["script", "style", "noscript"],
)


class HttpDocumentFetcher:
    def __init__(
        self,
        *,
        timeout: float = 10.0,
        max_chars: int = 10000,
        user_agent: str = "CSDI-RAG-Engine/0.1",
    ) -> None:
        self._timeout = timeout
        self._max_chars = max_chars
        self._headers = {"User-Agent": user_agent}
        self._scraper = Scraper()

    def fetch(self, hit: WebSearchHit) -> WebSearchDocument | None:
        try:
            with httpx.Client(timeout=self._timeout, follow_redirects=True, headers=self._headers) as client:
                response = client.get(hit.url)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("web_search_fetch_failed url=%s reason=%s", hit.url, exc)
            return None

        content_type = response.headers.get("content-type", "")
        if "html" in content_type:
            scraped = self._scraper.parse(
                url=hit.url,
                html=response.text,
                config=_WEB_SEARCH_SCRAPER_CONFIG,
                source_id="web-search",
            )
            if scraped is None:
                logger.warning("web_search_parse_failed url=%s", hit.url)
                return None
            title = scraped.title or hit.title
            text = scraped.content
            breadcrumb = scraped.breadcrumb
        else:
            title = hit.title
            text = response.text
            breadcrumb = ""

        cleaned = _WHITESPACE.sub(" ", text).strip()
        if not cleaned:
            return None

        return WebSearchDocument(
            url=hit.url,
            title=title,
            text=cleaned[: self._max_chars],
            metadata={
                "provider": hit.provider,
                "status_code": response.status_code,
                "content_type": content_type,
                "breadcrumb": breadcrumb,
            },
        )
