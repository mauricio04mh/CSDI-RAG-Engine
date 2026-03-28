from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import httpx

from src.sources_config.schemas.source_config import SourceConfig

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": "CSDI-RAG-Engine/1.0 (documentation crawler)",
}


@dataclass(slots=True)
class CrawledPage:
    """Raw result of fetching one URL."""

    url: str
    html: str
    status_code: int


@dataclass
class CrawlResult:
    """All pages collected for a source."""

    source_id: str
    pages: list[CrawledPage] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.pages)


class Crawler:
    """BFS crawler that discovers and fetches pages within a source's boundaries.

    Follows links starting from seed_urls up to max_depth levels.
    Respects allowed_domains, allowed_path_prefixes, and blocked_path_patterns.
    """

    def __init__(self, timeout: float = 15.0) -> None:
        self._timeout = timeout

    def crawl(self, source: SourceConfig) -> CrawlResult:
        result = CrawlResult(source_id=source.source_id)
        visited: set[str] = set()

        # queue entries: (url, current_depth)
        queue: deque[tuple[str, int]] = deque(
            (url, 0) for url in source.seed_urls
        )

        with httpx.Client(
            headers=_DEFAULT_HEADERS,
            timeout=self._timeout,
            follow_redirects=True,
        ) as client:
            while queue:
                url, depth = queue.popleft()
                url = self._normalize(url)

                if url in visited:
                    continue
                if not self._is_allowed(url, source):
                    continue

                visited.add(url)
                page = self._fetch(client, url)
                if page is None:
                    continue

                result.pages.append(page)
                logger.info(
                    "crawled url=%s depth=%s total=%s", url, depth, result.total
                )

                if depth < source.max_depth:
                    for link in self._extract_links(page.html, url):
                        if link not in visited:
                            queue.append((link, depth + 1))

        logger.info(
            "crawl_finished source=%s pages=%s", source.source_id, result.total
        )
        return result

    def _fetch(self, client: httpx.Client, url: str) -> CrawledPage | None:
        try:
            response = client.get(url)
            if response.status_code != 200:
                logger.warning("fetch_failed url=%s status=%s", url, response.status_code)
                return None
            return CrawledPage(
                url=url,
                html=response.text,
                status_code=response.status_code,
            )
        except httpx.HTTPError as exc:
            logger.warning("fetch_error url=%s error=%s", url, exc)
            return None

    def _extract_links(self, html: str, base_url: str) -> list[str]:
        """Extract all href links from raw HTML without a full parse."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            absolute = urljoin(base_url, href)
            # Drop fragments and query strings for deduplication
            parsed = urlparse(absolute)
            clean = parsed._replace(fragment="", query="").geturl()
            links.append(clean)
        return links

    def _is_allowed(self, url: str, source: SourceConfig) -> bool:
        parsed = urlparse(url)

        if parsed.scheme not in ("http", "https"):
            return False

        if parsed.netloc not in source.allowed_domains:
            return False

        path = parsed.path
        if source.allowed_path_prefixes:
            if not any(path.startswith(p) for p in source.allowed_path_prefixes):
                return False

        if any(pattern in path for pattern in source.blocked_path_patterns):
            return False

        return True

    def _normalize(self, url: str) -> str:
        parsed = urlparse(url)
        return parsed._replace(fragment="", query="").geturl()
