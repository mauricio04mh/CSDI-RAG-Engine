from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class SourceDocumentInput:
    source_id: str
    url: str
    normalized_url: str
    title: str
    breadcrumb: str
    text_content: str
    raw_html: str | None = None
    code_blocks: list[str] = field(default_factory=list)
    content_type: str = "text/html"
    http_status: int = 200
    fetch_method: str = "http"
    crawl_depth: int | None = None
    discovered_from_url: str | None = None
    fetched_at: datetime | None = None
    last_seen_at: datetime | None = None

    @property
    def document_id(self) -> str:
        return build_document_id(self.source_id, self.normalized_url)

    @property
    def content_hash(self) -> str:
        return build_content_hash(
            title=self.title,
            breadcrumb=self.breadcrumb,
            text_content=self.text_content,
            code_blocks=self.code_blocks,
        )


def build_document_id(source_id: str, normalized_url: str) -> str:
    digest = hashlib.sha1(normalized_url.encode("utf-8")).hexdigest()[:12]
    return f"{source_id}:{digest}"


def build_content_hash(
    *,
    title: str,
    breadcrumb: str,
    text_content: str,
    code_blocks: list[str],
) -> str:
    payload = json.dumps(
        {
            "title": title,
            "breadcrumb": breadcrumb,
            "text_content": text_content,
            "code_blocks": code_blocks,
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
