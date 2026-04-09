import json
from pathlib import Path

from mf_rag.phase4_runner import run_phase4
from mf_rag.storage.cache_layer import VersionedCache
from mf_rag.storage.structured_store import StructuredStore
from mf_rag.storage.vector_store import InMemoryVectorStore, VectorDocument


def test_structured_store_upsert_and_retrieve(tmp_path: Path) -> None:
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

    assert run_phase4(curated, db_path) == 1
    store = StructuredStore(db_path)
    facts = store.get_latest_scheme_facts("INF0001")
    assert facts is not None
    assert facts["scheme_name"] == "Axis ELSS Tax Saver Fund"
    assert facts["riskometer"] == "Very High"


def test_vector_store_query_with_metadata_filter() -> None:
    store = InMemoryVectorStore()
    store.upsert(
        [
            VectorDocument(
                doc_id="d1",
                text="Axis ELSS summary",
                metadata={"scheme_id": "INF0001", "version": "v1"},
            ),
            VectorDocument(
                doc_id="d2",
                text="Debt fund note",
                metadata={"scheme_id": "INF9999", "version": "v1"},
            ),
        ]
    )
    results = store.query("axis", top_k=5, metadata_filter={"scheme_id": "INF0001"})
    assert len(results) == 1
    assert results[0].doc_id == "d1"


def test_versioned_cache_keys_prevent_stale_reads() -> None:
    cache = VersionedCache()
    key_v1 = cache.build_key("chat", "Axis ELSS nav", "v1")
    key_v2 = cache.build_key("chat", "Axis ELSS nav", "v2")
    cache.set(key_v1, {"answer": "87.13"})
    cache.set(key_v2, {"answer": "88.01"})

    assert cache.get(key_v1)["answer"] == "87.13"
    assert cache.get(key_v2)["answer"] == "88.01"
    assert key_v1 != key_v2
