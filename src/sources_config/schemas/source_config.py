from dataclasses import dataclass, field

_DEFAULT_USER_AGENT = "CSDI-RAG-Engine/1.0 (documentation crawler)"


@dataclass
class ScraperConfig:
    main_content_selectors: list[str] = field(default_factory=list)
    title_selectors: list[str] = field(default_factory=list)
    breadcrumb_selectors: list[str] = field(default_factory=list)
    code_block_selectors: list[str] = field(default_factory=list)
    exclude_selectors: list[str] = field(default_factory=list)


@dataclass
class SourceConfig:
    source_id: str
    name: str
    base_url: str
    allowed_domains: list[str]
    seed_urls: list[str]
    allowed_path_prefixes: list[str]
    blocked_path_patterns: list[str]
    max_depth: int
    use_browser_fallback: bool
    technology: list[str]
    respect_robots: bool = True
    user_agent: str = _DEFAULT_USER_AGENT
    crawl_delay_seconds: float = 1.0
    max_pages: int = 100
    max_retries: int = 2
    request_timeout_seconds: float = 15.0
    allowed_content_types: list[str] = field(default_factory=lambda: ["text/html"])
    blocked_extensions: list[str] = field(default_factory=list)
    strip_query_strings: bool = True
    allowed_query_params: list[str] = field(default_factory=list)
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
