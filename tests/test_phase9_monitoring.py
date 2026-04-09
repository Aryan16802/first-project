from datetime import datetime, timedelta, timezone
from pathlib import Path

from mf_rag.phases.phase9 import MetricsCollector, compute_data_lag_hours, freshness_status


def test_metrics_collector_snapshot_computes_rates(tmp_path: Path) -> None:
    collector = MetricsCollector(tmp_path / "metrics" / "chat_metrics.jsonl")
    collector.record({"grounded": True, "reason": "ok", "retrieval_latency_ms": 100})
    collector.record({"grounded": False, "reason": "policy_refusal", "retrieval_latency_ms": 50})
    collector.record({"grounded": False, "reason": "low_confidence_context", "retrieval_latency_ms": 150})

    snap = collector.snapshot()
    assert snap.total_requests == 3
    assert round(snap.grounded_rate, 3) == round(1 / 3, 3)
    assert round(snap.refusal_rate, 3) == round(1 / 3, 3)
    assert round(snap.low_confidence_rate, 3) == round(1 / 3, 3)
    assert snap.avg_retrieval_latency_ms == 100.0


def test_freshness_helpers_report_stale_or_fresh() -> None:
    now = datetime.now(tz=timezone.utc)
    recent = (now - timedelta(hours=2)).isoformat()
    old = (now - timedelta(hours=72)).isoformat()

    assert compute_data_lag_hours(recent, now=now) is not None
    assert freshness_status(recent, threshold_hours=48)["status"] == "fresh"
    assert freshness_status(old, threshold_hours=48)["status"] == "stale"
