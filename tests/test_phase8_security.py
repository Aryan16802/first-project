import json
from pathlib import Path

from mf_rag.phases.phase8 import TelemetryLogger, enforce_query_policy, mask_sensitive_text, sanitize_telemetry


def test_policy_refuses_pii_query() -> None:
    allowed, refusal = enforce_query_policy("My PAN is ABCDE1234F. Tell me best scheme.")
    assert allowed is False
    assert refusal is not None


def test_mask_sensitive_text_redacts_patterns() -> None:
    text = "Contact me at test@example.com and phone 9876543210"
    masked = mask_sensitive_text(text)
    assert "[REDACTED]" in masked
    assert "test@example.com" not in masked


def test_sanitize_telemetry_masks_nested_payload() -> None:
    payload = {"query": "PAN ABCDE1234F", "meta": {"email": "u@example.com"}}
    safe = sanitize_telemetry(payload)
    assert "[REDACTED]" in safe["query"]
    assert "[REDACTED]" in safe["meta"]["email"]


def test_telemetry_logger_writes_jsonl(tmp_path: Path) -> None:
    logger = TelemetryLogger(tmp_path / "telemetry" / "events.jsonl")
    trace = logger.new_trace()
    logger.log_event(trace.trace_id, "chat_request", {"query": "my email is u@example.com"})
    rows = (tmp_path / "telemetry" / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(rows) == 1
    event = json.loads(rows[0])
    assert event["trace_id"] == trace.trace_id
    assert "[REDACTED]" in event["payload"]["query"]
