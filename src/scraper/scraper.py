from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from bs4 import BeautifulSoup

from src.sources_config.schemas.source_config import ScraperConfig

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ScrapedDocument:
    """Structured content extracted from one HTML page."""

    url: str
    title: str
    content: str
    breadcrumb: str
    code_blocks: list[str]
    source_id: str
    published_at: datetime | None = None
    document_updated_at: datetime | None = None


class Scraper:
    """Extracts structured content from HTML using per-source CSS selectors.

    Uses the ScraperConfig to know which elements to target and which to ignore.
    Falls back gracefully when a selector matches nothing.
    """

    def parse(
        self,
        url: str,
        html: str,
        config: ScraperConfig,
        source_id: str,
        response_headers: dict[str, str] | None = None,
    ) -> ScrapedDocument | None:
        soup = BeautifulSoup(html, "lxml")

        self._remove_excluded(soup, config.exclude_selectors)

        title = self._extract_first_text(soup, config.title_selectors)
        if not title:
            logger.warning("no_title_found url=%s", url)
            return None

        content = self._extract_first_text(soup, config.main_content_selectors)
        if not content:
            logger.warning("no_content_found url=%s", url)
            return None

        breadcrumb = self._extract_breadcrumb(soup, config.breadcrumb_selectors)
        code_blocks = self._extract_code_blocks(soup, config.code_block_selectors)
        published_at, document_updated_at = self._extract_dates(soup, response_headers or {})

        logger.info(
            "scraped url=%s title=%r content_len=%s code_blocks=%s",
            url, title[:50], len(content), len(code_blocks),
        )

        return ScrapedDocument(
            url=url,
            title=title.strip(),
            content=content.strip(),
            breadcrumb=breadcrumb,
            code_blocks=code_blocks,
            source_id=source_id,
            published_at=published_at,
            document_updated_at=document_updated_at,
        )

    def _remove_excluded(self, soup: BeautifulSoup, selectors: list[str]) -> None:
        for selector in selectors:
            for tag in soup.select(selector):
                tag.decompose()

    def _extract_first_text(self, soup: BeautifulSoup, selectors: list[str]) -> str:
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(separator=" ", strip=True)
        return ""

    def _extract_breadcrumb(self, soup: BeautifulSoup, selectors: list[str]) -> str:
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                parts = [el.get_text(strip=True) for el in elements if el.get_text(strip=True)]
                return " > ".join(parts)
        return ""

    def _extract_code_blocks(self, soup: BeautifulSoup, selectors: list[str]) -> list[str]:
        seen: set[str] = set()
        blocks: list[str] = []
        for selector in selectors:
            for element in soup.select(selector):
                code = element.get_text(strip=True)
                if code and code not in seen:
                    seen.add(code)
                    blocks.append(code)
        return blocks

    def _extract_dates(
        self,
        soup: BeautifulSoup,
        response_headers: dict[str, str],
    ) -> tuple[datetime | None, datetime | None]:
        published_candidates = [
            self._meta_content(soup, 'meta[property="article:published_time"]'),
            self._meta_content(soup, 'meta[name="article:published_time"]'),
            self._meta_content(soup, 'meta[name="date"]'),
            self._meta_content(soup, 'meta[name="dc.date"]'),
            self._meta_content(soup, 'meta[name="DC.date"]'),
            self._json_ld_date(soup, "datePublished"),
        ]
        updated_candidates = [
            self._meta_content(soup, 'meta[property="article:modified_time"]'),
            self._meta_content(soup, 'meta[property="og:updated_time"]'),
            self._meta_content(soup, 'meta[name="last-modified"]'),
            self._meta_content(soup, 'meta[name="modified"]'),
            self._json_ld_date(soup, "dateModified"),
            self._time_datetime(soup, ["updated", "modified"]),
            response_headers.get("last-modified") or response_headers.get("Last-Modified"),
        ]
        return (
            _first_parsed_datetime(published_candidates),
            _first_parsed_datetime(updated_candidates),
        )

    def _meta_content(self, soup: BeautifulSoup, selector: str) -> str | None:
        element = soup.select_one(selector)
        if not element:
            return None
        value = element.get("content")
        return str(value).strip() if value else None

    def _json_ld_date(self, soup: BeautifulSoup, key: str) -> str | None:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            value = _find_json_key(payload, key)
            if value:
                return value
        return None

    def _time_datetime(self, soup: BeautifulSoup, keywords: list[str]) -> str | None:
        for element in soup.select("time[datetime]"):
            marker = " ".join(
                str(element.get(attr, ""))
                for attr in ("class", "id", "itemprop", "property")
            ).lower()
            if any(keyword in marker for keyword in keywords):
                value = element.get("datetime")
                return str(value).strip() if value else None
        return None


def _find_json_key(payload, key: str) -> str | None:
    if isinstance(payload, dict):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        for nested in payload.values():
            found = _find_json_key(nested, key)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_json_key(item, key)
            if found:
                return found
    return None


def _first_parsed_datetime(values: list[str | None]) -> datetime | None:
    for value in values:
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
