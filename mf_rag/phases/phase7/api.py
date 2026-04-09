from __future__ import annotations

from pathlib import Path
from time import perf_counter

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from mf_rag.ingestion.groww_client import SELECTED_GROWW_FUND_URLS
from mf_rag.phases.phase9 import MetricsCollector, freshness_status
from mf_rag.phases.phase8 import TelemetryLogger, enforce_query_policy
from mf_rag.phases.phase7.service import ChatOrchestrator


class ChatRequest(BaseModel):
    query: str = Field(min_length=1)


class CompareRequest(BaseModel):
    scheme_ids: list[str] = Field(min_length=2)


def create_app(orchestrator: ChatOrchestrator) -> FastAPI:
    app = FastAPI(title="Mutual Fund RAG Chatbot API", version="0.1.0")
    telemetry = TelemetryLogger(log_file=Path("data/telemetry/events.jsonl"))
    metrics = MetricsCollector(metrics_file=Path("data/metrics/chat_metrics.jsonl"))
    frontend_dir = Path("frontend")
    if frontend_dir.exists():
        app.mount("/frontend", StaticFiles(directory=str(frontend_dir)), name="frontend")

    @app.get("/")
    def root() -> FileResponse:
        index_file = frontend_dir / "index.html"
        if not index_file.exists():
            raise HTTPException(status_code=404, detail="Frontend not found")
        return FileResponse(index_file)

    @app.post("/chat")
    def post_chat(req: ChatRequest) -> dict:
        trace = telemetry.new_trace()
        start = perf_counter()
        allowed, refusal = enforce_query_policy(req.query)
        telemetry.log_event(trace.trace_id, "chat_request", {"query": req.query, "allowed": allowed})
        if not allowed:
            elapsed_ms = (perf_counter() - start) * 1000.0
            metrics.record(
                {
                    "trace_id": trace.trace_id,
                    "grounded": False,
                    "reason": "policy_refusal",
                    "retrieval_latency_ms": elapsed_ms,
                    "as_of": None,
                }
            )
            return {
                "answer": refusal,
                "grounded": False,
                "reason": "policy_refusal",
                "trace_id": trace.trace_id,
                "citations": {},
                "as_of": None,
            }

        response = orchestrator.chat(req.query)
        elapsed_ms = (perf_counter() - start) * 1000.0
        response["trace_id"] = trace.trace_id
        freshness = freshness_status(response.get("as_of"))
        response["freshness"] = freshness
        telemetry.log_event(
            trace.trace_id,
            "chat_response",
            {"reason": response.get("reason"), "grounded": response.get("grounded", False)},
        )
        metrics.record(
            {
                "trace_id": trace.trace_id,
                "grounded": response.get("grounded", False),
                "reason": response.get("reason"),
                "retrieval_latency_ms": elapsed_ms,
                "as_of": response.get("as_of"),
            }
        )
        return response

    @app.get("/scheme/{scheme_id}")
    def get_scheme(scheme_id: str) -> dict:
        row = orchestrator.get_scheme(scheme_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Scheme not found")
        return row

    @app.post("/compare")
    def post_compare(req: CompareRequest) -> dict:
        return orchestrator.compare(req.scheme_ids)

    @app.get("/metrics")
    def get_metrics() -> dict:
        snapshot = metrics.snapshot()
        return {
            "total_requests": snapshot.total_requests,
            "grounded_rate": snapshot.grounded_rate,
            "refusal_rate": snapshot.refusal_rate,
            "low_confidence_rate": snapshot.low_confidence_rate,
            "avg_retrieval_latency_ms": snapshot.avg_retrieval_latency_ms,
        }

    @app.get("/funds")
    def get_funds() -> dict:
        # Curated list requested for targeted chatbot testing.
        return {"fund_urls": SELECTED_GROWW_FUND_URLS}

    return app
