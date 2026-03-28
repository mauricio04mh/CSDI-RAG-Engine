from dataclasses import dataclass, field


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
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
