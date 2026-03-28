from __future__ import annotations

import logging
from dataclasses import dataclass, field

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
