from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from mf_rag.policy import classify_scope, refusal_message
from mf_rag.storage.structured_store import StructuredStore
from mf_rag.storage.vector_store import InMemoryVectorStore


@dataclass
class RetrievalResult:
    answerable: bool
    reason: str
    context_packet: dict[str, Any]
    fallback_message: str | None = None


INTENT_TO_FIELDS: dict[str, set[str]] = {
    "nav": {"nav_value", "nav_date"},
    "nav_min_sip": {"nav_value", "nav_date", "min_sip"},
    "expense_ratio": {"expense_ratio"},
    "aum": {"aum_value", "aum_date"},
    "holdings": {"portfolio_holdings"},
    "aum_holdings": {"aum_value", "aum_date", "portfolio_holdings"},
    "riskometer": {"riskometer"},
    "benchmark": {"benchmark"},
    "min_sip": {"min_sip"},
    "exit_load": {"exit_load"},
    "lock_in": {"lock_in_period_days"},
    "fund_manager": {"fund_managers"},
}


def _detect_intent(query: str) -> str:
    text = query.lower()
    if ("aum" in text or "fund size" in text) and ("holding" in text or "portfolio" in text):
        return "aum_holdings"
    if ("nav" in text) and ("minimum sip" in text or "min sip" in text or "sip" in text):
        return "nav_min_sip"
    if "expense ratio" in text:
        return "expense_ratio"
    if "exit load" in text:
        return "exit_load"
    if "minimum sip" in text or "min sip" in text or "sip" in text:
        return "min_sip"
    if "lock-in" in text or "lock in" in text or "elss" in text:
        return "lock_in"
    if "riskometer" in text or "risk" in text:
        return "riskometer"
    if "benchmark" in text:
        return "benchmark"
    if "aum" in text or "fund size" in text:
        return "aum"
    if "holding" in text or "portfolio" in text:
        return "holdings"
    if "manager" in text:
        return "fund_manager"
    return "nav"


def _extract_scheme_id(query: str) -> str | None:
    m = re.search(r"\bINF[A-Z0-9]+\b", query.upper())
    return m.group(0) if m else None


def _normalize_text(text: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", text.lower()).strip()


def _best_scheme_by_token_overlap(query: str, schemes: list[dict[str, Any]]) -> dict[str, Any] | None:
    query_tokens = {t for t in _normalize_text(query).split() if len(t) > 1}
    if not query_tokens:
        return None

    best = None
    best_score = 0
    for scheme in schemes:
        name_tokens = {t for t in _normalize_text(str(scheme.get("scheme_name", ""))).split() if len(t) > 1}
        if not name_tokens:
            continue
        score = len(query_tokens & name_tokens)
        if score > best_score:
            best = scheme
            best_score = score
    if best_score >= 2:
        return best
    return None


class RetrievalPipeline:
    def __init__(self, structured_store: StructuredStore, vector_store: InMemoryVectorStore) -> None:
        self.structured_store = structured_store
        self.vector_store = vector_store

    def run(self, query: str, min_confidence: float = 0.8) -> RetrievalResult:
        scope = classify_scope(query)
        if not scope.allowed:
            return RetrievalResult(
                answerable=False,
                reason=scope.reason,
                context_packet={},
                fallback_message=refusal_message(),
            )

        intent = _detect_intent(query)
        required_fields = INTENT_TO_FIELDS.get(intent, {"nav_value", "nav_date"})
        scheme_id = _extract_scheme_id(query)

        facts = None
        if scheme_id:
            facts = self.structured_store.get_latest_scheme_facts(scheme_id)
        if not facts:
            candidates = self.structured_store.find_scheme_candidates(query_text=query)
            if candidates:
                scheme_id = candidates[0]["scheme_id"]
                facts = self.structured_store.get_latest_scheme_facts(scheme_id)
        if not facts:
            best = _best_scheme_by_token_overlap(query, self.structured_store.list_schemes())
            if best:
                scheme_id = best["scheme_id"]
                facts = self.structured_store.get_latest_scheme_facts(scheme_id)

        if not facts or not scheme_id:
            return RetrievalResult(
                answerable=False,
                reason="missing_verified_context",
                context_packet={},
                fallback_message="I do not have enough verified data in the indexed knowledge base to answer this.",
            )

        managers = self.structured_store.get_scheme_managers(scheme_id, facts["ingestion_run_id"])
        holdings = self.structured_store.get_scheme_holdings(scheme_id, facts["ingestion_run_id"], limit=10)
        docs = self.vector_store.query(
            text=query,
            top_k=3,
            metadata_filter={"scheme_id": scheme_id, "version": facts["version"]},
        )

        factual_context = dict(facts)
        factual_context["fund_managers"] = managers
        factual_context["portfolio_holdings"] = holdings
        missing_count = sum(1 for field in required_fields if factual_context.get(field) in (None, "", []))
        confidence = 1.0 - (missing_count / max(1, len(required_fields)))

        if confidence < min_confidence:
            return RetrievalResult(
                answerable=False,
                reason="low_confidence_context",
                context_packet={
                    "scheme_id": scheme_id,
                    "intent": intent,
                    "confidence": confidence,
                    "citations": {
                        "source_url": facts["source_url"],
                        "source_timestamp": facts["source_timestamp"],
                        "ingestion_run_id": facts["ingestion_run_id"],
                        "version": facts["version"],
                    },
                },
                fallback_message="I do not have enough verified data in the indexed knowledge base to answer this.",
            )

        return RetrievalResult(
            answerable=True,
            reason="grounded_context_ready",
            context_packet={
                "intent": intent,
                "required_fields": sorted(required_fields),
                "confidence": confidence,
                "factual_context": factual_context,
                "semantic_context": [{"doc_id": d.doc_id, "text": d.text, "metadata": d.metadata} for d in docs],
                "citations": {
                    "source_url": facts["source_url"],
                    "source_timestamp": facts["source_timestamp"],
                    "ingestion_run_id": facts["ingestion_run_id"],
                    "version": facts["version"],
                },
            },
        )
