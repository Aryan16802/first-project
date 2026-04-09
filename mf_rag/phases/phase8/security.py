from __future__ import annotations

import re
from typing import Any

from mf_rag.policy import classify_scope, refusal_message


HIGH_RISK_PATTERNS = [
    re.compile(r"\b\d{12}\b"),  # aadhaar-like
    re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b"),  # PAN-like
    re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b"),
    re.compile(r"\b\d{10}\b"),
]


def enforce_query_policy(query: str) -> tuple[bool, str | None]:
    decision = classify_scope(query)
    if decision.allowed:
        return True, None
    return False, refusal_message()


def mask_sensitive_text(text: str) -> str:
    masked = text
    for pattern in HIGH_RISK_PATTERNS:
        masked = pattern.sub("[REDACTED]", masked)
    return masked


def sanitize_telemetry(payload: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for k, v in payload.items():
        if isinstance(v, str):
            safe[k] = mask_sensitive_text(v)
        elif isinstance(v, dict):
            safe[k] = sanitize_telemetry(v)
        elif isinstance(v, list):
            safe[k] = [mask_sensitive_text(x) if isinstance(x, str) else x for x in v]
        else:
            safe[k] = v
    return safe
