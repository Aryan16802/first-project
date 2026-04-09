from __future__ import annotations

from dataclasses import dataclass
import re


PII_PATTERNS = [
    re.compile(r"\b(?:aadhaar|aadhar|pan|account number|ifsc|passport|dob)\b", re.IGNORECASE),
    re.compile(r"\b\d{10}\b"),
    re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b"),
]

DOMAIN_KEYWORDS = {
    "mutual fund",
    "scheme",
    "nav",
    "aum",
    "expense ratio",
    "exit load",
    "sip",
    "riskometer",
    "benchmark",
    "holding",
    "fund manager",
    "elss",
}


@dataclass
class ScopeDecision:
    allowed: bool
    reason: str


def classify_scope(query: str) -> ScopeDecision:
    text = query.strip().lower()
    if not text:
        return ScopeDecision(False, "empty_query")

    for pattern in PII_PATTERNS:
        if pattern.search(text):
            return ScopeDecision(False, "pii_detected")

    if any(keyword in text for keyword in DOMAIN_KEYWORDS):
        return ScopeDecision(True, "in_scope")

    return ScopeDecision(False, "out_of_scope")


def refusal_message() -> str:
    return (
        "I cannot help with personal information or out-of-scope queries. "
        "I can answer mutual-fund scheme questions from the approved indexed data."
    )
