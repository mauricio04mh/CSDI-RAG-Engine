from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.orchestrator.api.routes import router


@dataclass(slots=True)
class FakeSourceDocument:
    document_id: str
    source_id: str
    url: str
    title: str
    breadcrumb: str
    text_content: str
    content_type: str = "text/html"
    http_status: int = 200
    crawl_depth: int | None = 0
    fetched_at: datetime | None = None
    last_seen_at: datetime | None = None
    published_at: datetime | None = None
    document_updated_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    is_active: bool = True


class FakeSourceDocumentRepository:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        fetched_at = datetime(2026, 5, 23, 14, 0, tzinfo=timezone.utc)
        self.documents = [
            FakeSourceDocument(
                document_id="python_docs:abc123",
                source_id="python_docs",
                url="https://docs.python.org/3/tutorial/index.html",
                title="The Python Tutorial",
                breadcrumb="Python Docs / Tutorial",
                text_content="Python is an easy to learn, powerful programming language.\n\nIt has efficient structures.",
                fetched_at=fetched_at,
                last_seen_at=fetched_at,
                created_at=fetched_at,
                updated_at=fetched_at,
            )
        ]

    def list_documents(
        self,
        *,
        page: int,
        page_size: int,
        source_id: str | None = None,
        active_only: bool = True,
    ):
        self.calls.append({
            "page": page,
            "page_size": page_size,
            "source_id": source_id,
            "active_only": active_only,
        })
        return self.documents, 153


def _build_client(repo: FakeSourceDocumentRepository) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.source_document_repo = repo
    return TestClient(app)


def test_list_scraped_documents_returns_pagination_and_items() -> None:
    repo = FakeSourceDocumentRepository()
    client = _build_client(repo)

    response = client.get(
        "/api/v1/ingest/documents",
        params={
            "page": 2,
            "page_size": 20,
            "source_id": "python_docs",
            "active_only": "true",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["page"] == 2
    assert payload["page_size"] == 20
    assert payload["total"] == 153
    assert payload["total_pages"] == 8
    assert repo.calls == [{
        "page": 2,
        "page_size": 20,
        "source_id": "python_docs",
        "active_only": True,
    }]
    assert payload["items"] == [
        {
            "document_id": "python_docs:abc123",
            "source_id": "python_docs",
            "url": "https://docs.python.org/3/tutorial/index.html",
            "title": "The Python Tutorial",
            "breadcrumb": "Python Docs / Tutorial",
            "content_type": "text/html",
            "http_status": 200,
            "crawl_depth": 0,
            "fetched_at": "2026-05-23T14:00:00+00:00",
            "last_seen_at": "2026-05-23T14:00:00+00:00",
            "published_at": None,
            "document_updated_at": None,
            "created_at": "2026-05-23T14:00:00+00:00",
            "updated_at": "2026-05-23T14:00:00+00:00",
            "is_active": True,
            "text_preview": (
                "Python is an easy to learn, powerful programming language. "
                "It has efficient structures."
            ),
        }
    ]


def test_list_scraped_documents_uses_default_pagination() -> None:
    repo = FakeSourceDocumentRepository()
    client = _build_client(repo)

    response = client.get("/api/v1/ingest/documents")

    assert response.status_code == 200
    assert repo.calls == [{
        "page": 1,
        "page_size": 20,
        "source_id": None,
        "active_only": True,
    }]


def test_list_scraped_documents_rejects_invalid_page_size() -> None:
    repo = FakeSourceDocumentRepository()
    client = _build_client(repo)

    response = client.get("/api/v1/ingest/documents?page_size=101")

    assert response.status_code == 422
