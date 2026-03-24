
"""FastAPI entry point for the GenBI intelligence service."""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import FastAPI
from pydantic import BaseModel, Field, field_validator

logger = structlog.get_logger(__name__)

app = FastAPI(
    title="GenBI Intelligence Service",
    version="0.1.0",
)


class QueryRequest(BaseModel):
    """Request payload for the stub query endpoint."""

    query: str = Field(..., min_length=1, max_length=500)
    request_id: str = Field(..., min_length=1)

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        """Normalize and validate the incoming query string."""
        normalized = value.strip()
        if not normalized:
            raise ValueError("query must not be empty")
        return normalized

    @field_validator("request_id")
    @classmethod
    def validate_request_id(cls, value: str) -> str:
        """Normalize and validate the request identifier."""
        normalized = value.strip()
        if not normalized:
            raise ValueError("request_id must not be empty")
        return normalized


class QueryResponse(BaseModel):
    """Response payload returned by the query endpoint."""

    sql: str | None
    results: list[dict[str, Any]]
    row_count: int
    explanation: str
    chart_hint: str | None
    metadata: dict[str, Any]
    error: str | None


@app.get("/health")
async def health() -> dict[str, str]:
    """Return service readiness for Phase 0."""
    return {
        "status": "ok",
        "service": "intelligence",
        "database": "not_loaded",
        "vector_store": "not_loaded",
        "embedding_model": "not_loaded",
    }


@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest) -> QueryResponse:
    """Return a stubbed response confirming gateway-to-service flow."""
    logger.info(
        "received query request",
        request_id=request.request_id,
        query=request.query,
    )

    response = QueryResponse(
        sql=None,
        results=[],
        row_count=0,
        explanation=(
            "Phase 0 stub response from the intelligence service. "
            f"Received query: '{request.query}'. "
            "This confirms the Go to Python request flow is working."
        ),
        chart_hint=None,
        metadata={
            "request_id": request.request_id,
            "service": "intelligence",
            "phase": "phase_0",
            "stub": True,
        },
        error=None,
    )

    logger.info(
        "returning stub query response",
        request_id=request.request_id,
        row_count=response.row_count,
        stub=response.metadata["stub"],
    )

    return response

