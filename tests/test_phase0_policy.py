from decimal import Decimal

from mf_rag.models import CanonicalSchemeRecord
from mf_rag.policy import classify_scope


def test_scope_allows_mutual_fund_queries() -> None:
    decision = classify_scope("What is the NAV and expense ratio of Axis ELSS scheme?")
    assert decision.allowed is True
    assert decision.reason == "in_scope"


def test_scope_blocks_pii_queries() -> None:
    decision = classify_scope("My PAN is ABCDE1234F, suggest tax fund")
    assert decision.allowed is False
    assert decision.reason == "pii_detected"


def test_canonical_model_validates_ranges() -> None:
    record = CanonicalSchemeRecord(
        scheme_id="INF0001",
        scheme_name="Axis ELSS Tax Saver Fund",
        amc_name="Axis Mutual Fund",
        category="Equity",
        subcategory="ELSS",
        expense_ratio=Decimal("1.62"),
        exit_load="Nil",
        min_sip=Decimal("500"),
        lock_in_period_days=1095,
        riskometer="Very High",
        benchmark="NIFTY 500 TRI",
        nav_value=Decimal("87.13"),
        nav_date="2026-04-07",
        aum_value=Decimal("35600000000"),
        aum_date="2026-03-31",
        fund_managers=["Manager A"],
        portfolio_holdings=[],
        source_url="https://groww.in/example",
        source_timestamp="2026-04-08T07:00:00+00:00",
        ingestion_run_id="ingest_abc123",
        version="v1",
    )
    assert record.scheme_id == "INF0001"
