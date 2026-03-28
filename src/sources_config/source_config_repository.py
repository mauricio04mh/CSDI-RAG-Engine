import json
from pathlib import Path

from .schemas import ScraperConfig, SourceConfig


def _load_sources() -> list[SourceConfig]:
    data_path = Path(__file__).with_name("data") / "sources_data.json"
    raw_sources = json.loads(data_path.read_text(encoding="utf-8"))
    return [_build_source_config(raw_source) for raw_source in raw_sources]


def _build_source_config(raw_source: dict) -> SourceConfig:
    scraper_raw = raw_source.get("scraper", {})
    return SourceConfig(
        **{
            **raw_source,
            "scraper": ScraperConfig(**scraper_raw),
        }
    )


class SourceConfigRepository:
    def __init__(self):
        self._sources = {source.source_id: source for source in _load_sources()}

    def list_sources(self) -> list[SourceConfig]:
        return list(self._sources.values())

    def get_source(self, source_id: str) -> SourceConfig:
        source = self._sources.get(source_id)
        if source is None:
            raise ValueError(f"Source '{source_id}' not found")
        return source

    def exists(self, source_id: str) -> bool:
        return source_id in self._sources

    def get_seed_urls(self, source_id: str) -> list[str]:
        return self.get_source(source_id).seed_urls

    def get_allowed_domains(self, source_id: str) -> list[str]:
        return self.get_source(source_id).allowed_domains

    def get_max_depth(self, source_id: str) -> int:
        return self.get_source(source_id).max_depth

    def use_browser_fallback(self, source_id: str) -> bool:
        return self.get_source(source_id).use_browser_fallback

    def get_scraper_config(self, source_id: str) -> ScraperConfig:
        return self.get_source(source_id).scraper
