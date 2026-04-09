import json
from pathlib import Path

from mf_rag.phase4_runner import run_phase4
from mf_rag.phases.phase5 import RetrievalPipeline
from mf_rag.storage.structured_store import StructuredStore
from mf_rag.storage.vector_store import InMemoryVectorStore, VectorDocument


def _seed_store(tmp_path: Path) -> tuple[StructuredStore, InMemoryVectorStore]:
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
        "portfolio_holdings": [
            {"security_name": "HDFC Bank", "sector": "Financials", "weight": 7.1, "as_of_date": "2026-03-31"}
        ],
        "source_url": "https://groww.in/mutual-funds/axis-elss-tax-saver-fund",
        "source_timestamp": "2026-04-08T07:00:00+00:00",
        "ingestion_run_id": "ingest_1",
        "version": "v1",
        "quality_flags": [],
    }
    curated.write_text(json.dumps(row) + "\n", encoding="utf-8")
    run_phase4(curated, db_path)

    vector = InMemoryVectorStore()
    vector.upsert(
        [
            VectorDocument(
                doc_id="summary_1",
                text="Axis ELSS is an equity-linked savings scheme.",
                metadata={"scheme_id": "INF0001", "version": "v1"},
            )
        ]
    )
    return StructuredStore(db_path), vector


def test_phase5_returns_grounded_context_when_available(tmp_path: Path) -> None:
    structured, vector = _seed_store(tmp_path)
    pipeline = RetrievalPipeline(structured_store=structured, vector_store=vector)

    result = pipeline.run("What is the expense ratio of INF0001?")
    assert result.answerable is True
    assert result.reason == "grounded_context_ready"
    assert result.context_packet["factual_context"]["expense_ratio"] == 1.62
    assert result.context_packet["citations"]["source_url"].startswith("https://groww.in")


def test_phase5_refuses_pii_query(tmp_path: Path) -> None:
    structured, vector = _seed_store(tmp_path)
    pipeline = RetrievalPipeline(structured_store=structured, vector_store=vector)

    result = pipeline.run("My PAN is ABCDE1234F, what fund should I buy?")
    assert result.answerable is False
    assert result.reason == "pii_detected"
    assert result.fallback_message is not None


def test_phase5_falls_back_without_verified_context(tmp_path: Path) -> None:
    structured, vector = _seed_store(tmp_path)
    pipeline = RetrievalPipeline(structured_store=structured, vector_store=vector)

    result = pipeline.run("What is the NAV of INF9999?")
    assert result.answerable is False
    assert result.reason in {"missing_verified_context", "low_confidence_context"}
    assert result.fallback_message is not None


def test_phase5_matches_scheme_by_partial_name_tokens(tmp_path: Path) -> None:
    structured, vector = _seed_store(tmp_path)
    pipeline = RetrievalPipeline(structured_store=structured, vector_store=vector)

    result = pipeline.run("What is nav of axis elss tax saver fund")
    # NAV may still be low confidence in synthetic data, but scheme should be found.
    assert result.reason in {"grounded_context_ready", "low_confidence_context"}


def test_phase5_supports_aum_and_holdings_intent(tmp_path: Path) -> None:
    structured, vector = _seed_store(tmp_path)
    pipeline = RetrievalPipeline(structured_store=structured, vector_store=vector)
    result = pipeline.run("What are AUM and holdings of INF0001?")
    assert result.reason in {"grounded_context_ready", "low_confidence_context"}
