from __future__ import annotations

import pytest
from fastapi import FastAPI
import httpx

from src.query_feedback.api.routes import router


@pytest.mark.anyio
async def test_query_feedback_health_endpoint_returns_expected_payload():
    app = FastAPI()
    app.include_router(router)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.get("/api/v1/query-feedback/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "module": "query-feedback"}
