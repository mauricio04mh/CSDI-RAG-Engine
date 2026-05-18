from __future__ import annotations

import sys
from types import SimpleNamespace

import bs4

try:
    import httpx
except ModuleNotFoundError:
    class _HTTPError(Exception):
        pass

    class _Client:
        def __init__(self, *args, **kwargs) -> None:
            pass

    httpx = SimpleNamespace(HTTPError=_HTTPError, Client=_Client)
    sys.modules["httpx"] = httpx

from src.crawler.crawler import Crawler, CrawledPage
from src.sources_config.schemas import ScraperConfig, SourceConfig


def make_source(
    *,
    seed_urls: list[str] | None = None,
    allowed_domains: list[str] | None = None,
    allowed_path_prefixes: list[str] | None = None,
    blocked_path_patterns: list[str] | None = None,
    max_depth: int = 1,
    respect_robots: bool = False,
    crawl_delay_seconds: float = 0.0,
    max_pages: int = 100,
    max_retries: int = 2,
    strip_query_strings: bool = True,
    allowed_query_params: list[str] | None = None,
    blocked_extensions: list[str] | None = None,
    allowed_content_types: list[str] | None = None,
) -> SourceConfig:
    return SourceConfig(
        source_id="docs",
        name="Docs",
        base_url="https://example.com",
        allowed_domains=allowed_domains or ["example.com"],
        seed_urls=seed_urls or ["https://example.com/docs/start"],
        allowed_path_prefixes=allowed_path_prefixes or ["/docs/"],
        blocked_path_patterns=blocked_path_patterns or ["/search.html"],
        max_depth=max_depth,
        technology=["python"],
        respect_robots=respect_robots,
        crawl_delay_seconds=crawl_delay_seconds,
        max_pages=max_pages,
        max_retries=max_retries,
        strip_query_strings=strip_query_strings,
        allowed_query_params=allowed_query_params or [],
        blocked_extensions=blocked_extensions or [],
        allowed_content_types=allowed_content_types or ["text/html"],
        scraper=ScraperConfig(),
    )


class _DummyClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_crawl_discovers_links_within_boundaries_and_depth(monkeypatch) -> None:
    source = make_source(seed_urls=["https://example.com/docs/start?utm=1#intro"], max_depth=1)
    crawler = Crawler()
    monkeypatch.setattr("src.crawler.crawler.httpx.Client", _DummyClient)
    original_soup = bs4.BeautifulSoup

    def parse_with_html_parser(markup: str, _features: str):
        return original_soup(markup, "html.parser")

    monkeypatch.setattr("bs4.BeautifulSoup", parse_with_html_parser)

    start_url = "https://example.com/docs/start"
    page_a_url = "https://example.com/docs/page-a"
    page_b_url = "https://example.com/docs/page-b"
    pages = {
        start_url: CrawledPage(
            url=start_url,
            status_code=200,
            html="""
                <a href="/docs/page-a?x=1#frag">A</a>
                <a href="/docs/page-a">A again</a>
                <a href="/docs/page-b">B</a>
                <a href="/docs/search.html">Blocked</a>
                <a href="/blog/post">Outside prefix</a>
                <a href="https://other.com/docs/foreign">Other domain</a>
                <a href="mailto:test@example.com">Mail</a>
            """,
            content_type="text/html",
            depth=0,
        ),
        page_a_url: CrawledPage(
            url=page_a_url,
            status_code=200,
            html='<a href="/docs/deeper">Too deep</a>',
            content_type="text/html",
        ),
        page_b_url: CrawledPage(
            url=page_b_url,
            status_code=200,
            html="<p>done</p>",
            content_type="text/html",
        ),
    }
    fetched_urls: list[str] = []

    def fake_fetch(_client, url: str, _source: SourceConfig) -> CrawledPage | None:
        fetched_urls.append(url)
        return pages.get(url)

    monkeypatch.setattr(crawler, "_fetch", fake_fetch)

    result = crawler.crawl(source)

    assert fetched_urls == [start_url, page_a_url, page_b_url]
    assert [page.url for page in result.pages] == [start_url, page_a_url, page_b_url]
    assert result.pages[0].depth == 0
    assert result.pages[0].discovered_from_url is None


def test_extract_links_normalizes_query_and_fragment() -> None:
    crawler = Crawler()
    source = make_source()
    original_soup = bs4.BeautifulSoup

    def parse_with_html_parser(markup: str, _features: str):
        return original_soup(markup, "html.parser")

    from unittest.mock import patch

    with patch("bs4.BeautifulSoup", parse_with_html_parser):
        links = crawler._extract_links(
            """
            <a href="/docs/a?ref=1#top">one</a>
            <a href="https://example.com/docs/b?lang=en">two</a>
            """,
            "https://example.com/docs/start",
            source,
        )

    assert links == [
        "https://example.com/docs/a",
        "https://example.com/docs/b",
    ]


def test_fetch_returns_none_for_non_200_and_http_error() -> None:
    crawler = Crawler()
    source = make_source(max_retries=0)

    non_200_client = SimpleNamespace(
        get=lambda _url: SimpleNamespace(status_code=404, text="missing", headers={})
    )
    assert crawler._fetch(non_200_client, "https://example.com/missing", source) is None

    class _FailingClient:
        def get(self, _url):
            raise httpx.HTTPError("boom")

    assert crawler._fetch(_FailingClient(), "https://example.com/error", source) is None


def test_normalize_keeps_only_allowed_query_params() -> None:
    crawler = Crawler()
    source = make_source(
        strip_query_strings=True,
        allowed_query_params=["lang"],
    )

    normalized = crawler._normalize(
        "https://example.com/docs/page?lang=en&utm_source=test#intro",
        source,
    )

    assert normalized == "https://example.com/docs/page?lang=en"


def test_is_allowed_blocks_blocked_extensions() -> None:
    crawler = Crawler()
    source = make_source(blocked_extensions=[".pdf", ".zip"])

    assert not crawler._is_allowed("https://example.com/docs/file.pdf", source)
    assert crawler._is_allowed("https://example.com/docs/file.html", source)


def test_fetch_retries_server_errors_and_honors_allowed_content_types() -> None:
    crawler = Crawler()
    source = make_source(max_retries=1, allowed_content_types=["text/html"])
    calls: list[str] = []

    responses = [
        SimpleNamespace(status_code=503, text="retry", headers={"content-type": "text/html"}),
        SimpleNamespace(status_code=200, text="<html>ok</html>", headers={"content-type": "text/html; charset=utf-8"}),
    ]

    class _ClientWithRetry:
        def get(self, _url):
            calls.append(_url)
            return responses.pop(0)

    page = crawler._fetch(_ClientWithRetry(), "https://example.com/docs/page", source)

    assert page is not None
    assert page.content_type == "text/html"
    assert calls == ["https://example.com/docs/page", "https://example.com/docs/page"]


def test_fetch_skips_disallowed_content_type() -> None:
    crawler = Crawler()
    source = make_source(allowed_content_types=["text/html"])
    client = SimpleNamespace(
        get=lambda _url: SimpleNamespace(
            status_code=200,
            text="binary",
            headers={"content-type": "application/pdf"},
        )
    )

    assert crawler._fetch(client, "https://example.com/docs/file.pdf", source) is None


def test_wait_for_next_request_respects_delay() -> None:
    crawler = Crawler()
    source = make_source(crawl_delay_seconds=1.0)
    sleeps: list[float] = []
    times = iter([0.0, 0.0, 1.0])
    crawler._sleep = lambda seconds: sleeps.append(seconds)
    crawler._clock = lambda: next(times)

    next_request_at: dict[str, float] = {}
    crawler._wait_for_next_request("https://example.com/docs/one", source, {}, next_request_at)
    crawler._wait_for_next_request("https://example.com/docs/two", source, {}, next_request_at)

    assert sleeps == [1.0]
    assert next_request_at["example.com"] == 2.0


def test_crawl_respects_robots_and_max_pages(monkeypatch) -> None:
    source = make_source(
        seed_urls=["https://example.com/docs/start"],
        max_depth=1,
        max_pages=2,
        respect_robots=True,
    )
    crawler = Crawler()
    monkeypatch.setattr("src.crawler.crawler.httpx.Client", _DummyClient)
    original_soup = bs4.BeautifulSoup

    def parse_with_html_parser(markup: str, _features: str):
        return original_soup(markup, "html.parser")

    monkeypatch.setattr("bs4.BeautifulSoup", parse_with_html_parser)

    start_url = "https://example.com/docs/start"
    page_a_url = "https://example.com/docs/page-a"
    page_b_url = "https://example.com/docs/page-b"
    pages = {
        start_url: CrawledPage(
            url=start_url,
            status_code=200,
            html="""
                <a href="/docs/page-a">A</a>
                <a href="/docs/page-b">B</a>
            """,
            content_type="text/html",
        ),
        page_a_url: CrawledPage(
            url=page_a_url,
            status_code=200,
            html="<p>a</p>",
            content_type="text/html",
        ),
        page_b_url: CrawledPage(
            url=page_b_url,
            status_code=200,
            html="<p>b</p>",
            content_type="text/html",
        ),
    }
    fetched_urls: list[str] = []

    def fake_fetch(_client, url: str, _source: SourceConfig) -> CrawledPage | None:
        fetched_urls.append(url)
        return pages.get(url)

    def fake_can_fetch(_client, url: str, _source: SourceConfig, _cache) -> bool:
        return url != page_b_url

    monkeypatch.setattr(crawler, "_fetch", fake_fetch)
    monkeypatch.setattr(crawler, "_can_fetch_by_robots", fake_can_fetch)

    result = crawler.crawl(source)

    assert fetched_urls == [start_url, page_a_url]
    assert [page.url for page in result.pages] == [start_url, page_a_url]
    assert result.total == 2


def test_get_robots_policy_uses_user_agent_crawl_delay() -> None:
    crawler = Crawler()
    source = make_source(respect_robots=True)
    client = SimpleNamespace(
        get=lambda _url: SimpleNamespace(
            status_code=200,
            text="User-agent: *\nDisallow: /private\nCrawl-delay: 4\n",
            headers={"content-type": "text/plain"},
        )
    )
    cache: dict[str, object] = {}

    policy = crawler._get_robots_policy(client, "https://example.com/docs/page", source, cache)

    assert policy.parser.can_fetch(source.user_agent, "https://example.com/docs/public")
    assert not policy.parser.can_fetch(source.user_agent, "https://example.com/private")
    assert policy.crawl_delay == 4.0
