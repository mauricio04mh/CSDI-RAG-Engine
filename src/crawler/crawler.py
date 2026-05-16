from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field, replace
from time import monotonic, sleep
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx

from src.sources_config.schemas.source_config import SourceConfig

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": "CSDI-RAG-Engine/1.0 (documentation crawler)",
}


@dataclass(slots=True)
class RobotsPolicy:
    parser: RobotFileParser
    crawl_delay: float | None = None


@dataclass(slots=True)
class CrawledPage:
    """Raw result of fetching one URL."""

    url: str
    html: str
    status_code: int
    content_type: str = "text/html"
    depth: int = 0
    discovered_from_url: str | None = None


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
        self._sleep = sleep
        self._clock = monotonic

    def crawl(self, source: SourceConfig) -> CrawlResult:
        result = CrawlResult(source_id=source.source_id)
        visited: set[str] = set()
        robots_cache: dict[str, RobotsPolicy] = {}
        next_request_at: dict[str, float] = {}

        # queue entries: (url, current_depth)
        queue: deque[tuple[str, int, str | None]] = deque(
            (url, 0, None) for url in source.seed_urls
        )

        with httpx.Client(
            headers={"User-Agent": source.user_agent or _DEFAULT_HEADERS["User-Agent"]},
            timeout=source.request_timeout_seconds or self._timeout,
            follow_redirects=True,
        ) as client:
            while queue and result.total < source.max_pages:
                url, depth, discovered_from_url = queue.popleft()
                url = self._normalize(url, source)

                if url in visited:
                    continue
                if not self._is_allowed(url, source):
                    continue
                if not self._can_fetch_by_robots(client, url, source, robots_cache):
                    continue

                visited.add(url)
                self._wait_for_next_request(url, source, robots_cache, next_request_at)
                page = self._fetch(client, url, source)
                if page is None:
                    continue
                page = replace(
                    page,
                    url=url,
                    depth=depth,
                    discovered_from_url=discovered_from_url,
                )

                result.pages.append(page)
                logger.info(
                    "crawled url=%s depth=%s total=%s", url, depth, result.total
                )

                if depth < source.max_depth:
                    for link in self._extract_links(page.html, url, source):
                        if link not in visited:
                            queue.append((link, depth + 1, url))

        logger.info(
            "crawl_finished source=%s pages=%s", source.source_id, result.total
        )
        return result

    def _fetch(self, client: httpx.Client, url: str, source: SourceConfig) -> CrawledPage | None:
        attempts = max(source.max_retries, 0) + 1
        for attempt in range(1, attempts + 1):
            try:
                response = client.get(url)
            except httpx.HTTPError as exc:
                if attempt >= attempts:
                    logger.warning("fetch_error url=%s error=%s", url, exc)
                    return None
                logger.info("fetch_retry url=%s attempt=%s error=%s", url, attempt, exc)
                continue

            if response.status_code != 200:
                if response.status_code >= 500 and attempt < attempts:
                    logger.info(
                        "fetch_retry_status url=%s attempt=%s status=%s",
                        url,
                        attempt,
                        response.status_code,
                    )
                    continue
                logger.warning("fetch_failed url=%s status=%s", url, response.status_code)
                return None

            content_type = (
                response.headers.get("content-type", "text/html").split(";")[0].strip().lower()
                or "text/html"
            )
            if (
                source.allowed_content_types
                and not any(self._content_type_matches(content_type, allowed) for allowed in source.allowed_content_types)
            ):
                logger.info(
                    "fetch_skipped_content_type url=%s content_type=%s allowed=%s",
                    url,
                    content_type,
                    source.allowed_content_types,
                )
                return None

            return CrawledPage(
                url=url,
                html=response.text,
                status_code=response.status_code,
                content_type=content_type,
            )

        return None

    def _extract_links(self, html: str, base_url: str, source: SourceConfig) -> list[str]:
        """Extract all href links from raw HTML without a full parse."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            absolute = urljoin(base_url, href)
            clean = self._normalize(absolute, source)
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

        path_lower = path.lower()
        if any(path_lower.endswith(ext.lower()) for ext in source.blocked_extensions):
            return False

        if any(pattern in path for pattern in source.blocked_path_patterns):
            return False

        return True

    def _normalize(self, url: str, source: SourceConfig) -> str:
        parsed = urlparse(url)
        filtered_query = parsed.query
        if source.strip_query_strings:
            filtered_query = self._filter_query(parsed.query, source.allowed_query_params)
        elif source.allowed_query_params:
            filtered_query = self._filter_query(parsed.query, source.allowed_query_params)

        return parsed._replace(fragment="", query=filtered_query).geturl()

    def _filter_query(self, query: str, allowed_query_params: list[str]) -> str:
        if not query or not allowed_query_params:
            return ""
        allowed = set(allowed_query_params)
        pairs = [(key, value) for key, value in parse_qsl(query, keep_blank_values=True) if key in allowed]
        return urlencode(pairs, doseq=True)

    def _content_type_matches(self, content_type: str, allowed: str) -> bool:
        allowed = allowed.lower()
        if allowed.endswith("/*"):
            return content_type.startswith(allowed[:-1])
        return content_type == allowed

    def _can_fetch_by_robots(
        self,
        client: httpx.Client,
        url: str,
        source: SourceConfig,
        robots_cache: dict[str, RobotsPolicy],
    ) -> bool:
        if not source.respect_robots:
            return True

        policy = self._get_robots_policy(client, url, source, robots_cache)
        allowed = policy.parser.can_fetch(source.user_agent, url)
        if not allowed:
            logger.info("robots_blocked url=%s user_agent=%s", url, source.user_agent)
        return allowed

    def _get_robots_policy(
        self,
        client: httpx.Client,
        url: str,
        source: SourceConfig,
        robots_cache: dict[str, RobotsPolicy],
    ) -> RobotsPolicy:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        cached = robots_cache.get(domain)
        if cached is not None:
            return cached

        robots_url = f"{parsed.scheme}://{domain}/robots.txt"
        parser = RobotFileParser()
        parser.set_url(robots_url)
        crawl_delay: float | None = None

        try:
            response = client.get(robots_url)
            if response.status_code == 200:
                parser.parse(response.text.splitlines())
                robots_delay = parser.crawl_delay(source.user_agent)
                if robots_delay is None:
                    robots_delay = parser.crawl_delay("*")
                if robots_delay is not None:
                    crawl_delay = float(robots_delay)
            else:
                parser.parse([])
                logger.info("robots_unavailable url=%s status=%s", robots_url, response.status_code)
        except httpx.HTTPError as exc:
            parser.parse([])
            logger.warning("robots_fetch_error url=%s error=%s", robots_url, exc)

        policy = RobotsPolicy(parser=parser, crawl_delay=crawl_delay)
        robots_cache[domain] = policy
        return policy

    def _wait_for_next_request(
        self,
        url: str,
        source: SourceConfig,
        robots_cache: dict[str, RobotsPolicy],
        next_request_at: dict[str, float],
    ) -> None:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        delay = max(float(source.crawl_delay_seconds), 0.0)
        if source.respect_robots:
            policy = robots_cache.get(domain)
            if policy is not None and policy.crawl_delay is not None:
                delay = max(delay, policy.crawl_delay)

        if delay <= 0:
            return

        now = self._clock()
        ready_at = next_request_at.get(domain, now)
        wait_seconds = ready_at - now
        if wait_seconds > 0:
            self._sleep(wait_seconds)
            now = self._clock()
        next_request_at[domain] = now + delay
