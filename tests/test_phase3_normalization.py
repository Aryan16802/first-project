import json
from pathlib import Path

from mf_rag.phase3_runner import run_phase3
from mf_rag.processing.normalize import normalize_scheme_record


def test_normalization_enforces_elss_lock_in_and_riskometer() -> None:
    raw = {
        "scheme_id": "INF100",
        "scheme_name": "Test ELSS",
        "amc_name": "Test AMC",
        "category": "Equity",
        "subcategory": "ELSS",
        "expense_ratio": "1.20%",
        "exit_load": "Nil",
        "min_sip": "500",
        "lock_in_period_days": None,
        "riskometer": "very high",
        "benchmark": "NIFTY 500 TRI",
        "nav_value": "23.10",
        "nav_date": "2026-04-08",
        "aum_value": "1000000",
        "aum_date": "2026-03-31",
        "fund_managers": ["Manager A"],
        "portfolio_holdings": [],
        "source_url": "https://groww.in/test",
        "source_timestamp": "2026-04-08T07:00:00+00:00",
    }
    rec = normalize_scheme_record(raw, ingestion_run_id="ingest_1", version="v1")
    assert rec.lock_in_period_days == 1095
    assert rec.riskometer == "Very High"
    assert "derived_elss_lock_in" in rec.quality_flags


def test_normalization_adds_stale_and_missing_flags() -> None:
    raw = {
        "scheme_id": "INF101",
        "scheme_name": "Test Debt",
        "amc_name": "Test AMC",
        "category": "Debt",
        "subcategory": "Short Duration",
        "expense_ratio": "",
        "exit_load": "Nil",
        "min_sip": "",
        "lock_in_period_days": 0,
        "riskometer": "unknown-risk",
        "benchmark": "CRISIL Short Term Bond Index",
        "nav_value": "12.20",
        "nav_date": "2024-01-01",
        "aum_value": "2000000",
        "aum_date": "2024-01-01",
        "fund_managers": ["Manager B"],
        "portfolio_holdings": [],
        "source_url": "https://groww.in/test2",
        "source_timestamp": "2026-04-08T07:00:00+00:00",
    }
    rec = normalize_scheme_record(raw, ingestion_run_id="ingest_2", version="v1")
    assert "missing_field:expense_ratio" in rec.quality_flags
    assert "missing_field:min_sip" in rec.quality_flags
    assert "stale_field:nav_date" in rec.quality_flags
    assert "stale_field:aum_date" in rec.quality_flags
    assert "anomaly_flag:riskometer_unrecognized" in rec.quality_flags


def test_phase3_runner_writes_curated_jsonl(tmp_path: Path) -> None:
    raw_payload = {
        "schemes": [
            {
                "scheme_id": "INF0001",
                "scheme_name": "Axis ELSS Tax Saver Fund",
                "amc_name": "Axis Mutual Fund",
                "category": "Equity",
                "subcategory": "ELSS",
                "expense_ratio": "1.62",
                "exit_load": "Nil",
                "min_sip": "500",
                "lock_in_period_days": 1095,
                "riskometer": "Very High",
                "benchmark": "NIFTY 500 TRI",
                "nav_value": "87.13",
                "nav_date": "2026-04-07",
                "aum_value": "35600000000",
                "aum_date": "2026-03-31",
                "fund_managers": ["Manager A", "Manager B"],
                "portfolio_holdings": [],
                "source_url": "https://groww.in/mutual-funds/axis-elss-tax-saver-fund",
                "source_timestamp": "2026-04-08T07:00:00+00:00",
            }
        ]
    }
    raw_file = tmp_path / "raw.json"
    curated = tmp_path / "curated" / "schemes.jsonl"
    raw_file.write_text(json.dumps(raw_payload), encoding="utf-8")

    count = run_phase3(raw_file, curated, ingestion_run_id="ingest_x", version="v1")
    assert count == 1
    lines = curated.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["ingestion_run_id"] == "ingest_x"
