from __future__ import annotations

import bs4

import pytest

from src.scraper.scraper import Scraper
from src.sources_config.schemas import ScraperConfig


def make_config() -> ScraperConfig:
    return ScraperConfig(
        main_content_selectors=["article", "main"],
        title_selectors=["h1", "title"],
        breadcrumb_selectors=["nav.breadcrumb a"],
        code_block_selectors=["pre code", "code"],
        exclude_selectors=["script", ".ads"],
    )


@pytest.fixture(autouse=True)
def patch_scraper_parser(monkeypatch):
    original_soup = bs4.BeautifulSoup

    def parse_with_html_parser(markup: str, _features: str):
        return original_soup(markup, "html.parser")

    monkeypatch.setattr("src.scraper.scraper.BeautifulSoup", parse_with_html_parser)


def test_parse_extracts_structured_document_and_deduplicates_code_blocks() -> None:
    scraper = Scraper()
    config = make_config()

    document = scraper.parse(
        url="https://example.com/docs/page",
        source_id="docs",
        config=config,
        html="""
            <html>
                <head><title>Fallback title</title><script>noise</script></head>
                <body>
                    <nav class="breadcrumb">
                        <a href="/">Home</a>
                        <a href="/docs">Docs</a>
                    </nav>
                    <div class="ads">ignore me</div>
                    <article>
                        <h1>Decorators</h1>
                        <p>Useful text for developers.</p>
                        <pre><code>print("hello")</code></pre>
                        <code>print("hello")</code>
                        <code>sum([1, 2])</code>
                    </article>
                </body>
            </html>
        """,
    )

    assert document is not None
    assert document.title == "Decorators"
    assert document.content == 'Decorators Useful text for developers. print("hello") print("hello") sum([1, 2])'
    assert document.breadcrumb == "Home > Docs"
    assert document.code_blocks == ['print("hello")', "sum([1, 2])"]


def test_parse_returns_none_when_title_is_missing() -> None:
    scraper = Scraper()

    document = scraper.parse(
        url="https://example.com/docs/page",
        source_id="docs",
        config=make_config(),
        html="""
            <html>
                <body>
                    <article><p>Content exists</p></article>
                </body>
            </html>
        """,
    )

    assert document is None


def test_parse_returns_none_when_main_content_is_missing() -> None:
    scraper = Scraper()

    document = scraper.parse(
        url="https://example.com/docs/page",
        source_id="docs",
        config=make_config(),
        html="""
            <html>
                <body>
                    <h1>Decorators</h1>
                    <div>But no matching content selector</div>
                </body>
            </html>
        """,
    )

    assert document is None
