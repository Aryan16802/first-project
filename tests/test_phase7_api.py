import json
from pathlib import Path

from fastapi.testclient import TestClient

from mf_rag.phase4_runner import run_phase4
from mf_rag.phases.phase5 import RetrievalPipeline
from mf_rag.phases.phase7.api import create_app
from mf_rag.phases.phase7.service import ChatOrchestrator
from mf_rag.storage.structured_store import StructuredStore
from mf_rag.storage.vector_store import InMemoryVectorStore, VectorDocument


class FakeLLM:
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        return "Expense ratio is 1.62. Source: https://groww.in/x. As of 2026-04-08T07:00:00+00:00."


def _build_client(tmp_path: Path) -> TestClient:
    curated = tmp_path / "curated.jsonl"
    db_path = tmp_path / "truth.db"
    row = {
        "scheme_id": "INF0001",
        "scheme_name": "Axis ELSS Tax Saver Fund",
        "amc_name": "Axis Mutual Fund",
        "category": "Equity",
        "subcategory": "ELSS",
        "expense_ratio": 1.62,
        "exit_load": "Nil",
        "min_sip": 500,
        "lock_in_period_days": 1095,
        "riskometer": "Very High",
        "benchmark": "NIFTY 500 TRI",
        "nav_value": 87.13,
        "nav_date": "2026-04-07",
        "aum_value": 35600000000,
        "aum_date": "2026-03-31",
        "fund_managers": ["Manager A"],
        "portfolio_holdings": [],
        "source_url": "https://groww.in/mutual-funds/axis-elss-tax-saver-fund",
        "source_timestamp": "2026-04-08T07:00:00+00:00",
        "ingestion_run_id": "ingest_1",
        "version": "v1",
        "quality_flags": [],
    }
    curated.write_text(json.dumps(row) + "\n", encoding="utf-8")
    run_phase4(curated, db_path)
    store = StructuredStore(db_path)
    vector = InMemoryVectorStore()
    vector.upsert(
        [VectorDocument(doc_id="d1", text="Axis ELSS summary", metadata={"scheme_id": "INF0001", "version": "v1"})]
    )
    retrieval = RetrievalPipeline(structured_store=store, vector_store=vector)
    orchestrator = ChatOrchestrator(retrieval=retrieval, llm_client=FakeLLM(), store=store)
    return TestClient(create_app(orchestrator))


def test_chat_endpoint_returns_grounded_answer(tmp_path: Path) -> None:
    client = _build_client(tmp_path)
    res = client.post("/chat", json={"query": "What is expense ratio of INF0001?"})
    assert res.status_code == 200
    body = res.json()
    assert body["grounded"] is True
    assert "1.62" in body["answer"]
    assert "trace_id" in body
    assert "freshness" in body


def test_scheme_endpoint_returns_scheme_details(tmp_path: Path) -> None:
    client = _build_client(tmp_path)
    res = client.get("/scheme/INF0001")
    assert res.status_code == 200
    body = res.json()
    assert body["scheme_id"] == "INF0001"


def test_compare_endpoint_returns_multiple_schemes(tmp_path: Path) -> None:
    client = _build_client(tmp_path)
    res = client.post("/compare", json={"scheme_ids": ["INF0001", "INF9999"]})
    assert res.status_code == 200
    body = res.json()
    assert len(body["schemes"]) == 1


def test_chat_endpoint_refuses_pii_request(tmp_path: Path) -> None:
    client = _build_client(tmp_path)
    res = client.post("/chat", json={"query": "My PAN is ABCDE1234F. Tell me NAV."})
    assert res.status_code == 200
    body = res.json()
    assert body["grounded"] is False
    assert body["reason"] == "policy_refusal"


def test_metrics_endpoint_returns_aggregates(tmp_path: Path) -> None:
    client = _build_client(tmp_path)
    client.post("/chat", json={"query": "What is expense ratio of INF0001?"})
    client.post("/chat", json={"query": "My PAN is ABCDE1234F. Tell me NAV."})
    res = client.get("/metrics")
    assert res.status_code == 200
    body = res.json()
    assert body["total_requests"] >= 2
    assert 0 <= body["grounded_rate"] <= 1


def test_funds_endpoint_returns_selected_urls(tmp_path: Path) -> None:
    client = _build_client(tmp_path)
    res = client.get("/funds")
    assert res.status_code == 200
    body = res.json()
    assert len(body["fund_urls"]) == 10
