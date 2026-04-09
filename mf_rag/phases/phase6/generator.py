from __future__ import annotations

import json
from typing import Any, Protocol

from mf_rag.phases.phase6.verifier import verify_answer_grounding


FALLBACK = "I do not have enough verified data in the indexed knowledge base to answer this."


class LLMClient(Protocol):
    def generate(self, system_prompt: str, user_prompt: str) -> str: ...


def _build_prompts(query: str, context_packet: dict[str, Any]) -> tuple[str, str]:
    system_prompt = (
        "You are a mutual-fund factual assistant. Use only retrieved context. "
        "Never infer or fabricate. If context is insufficient, return fallback exactly. "
        "Always include source URL and as-of timestamp."
    )
    user_prompt = (
        f"User query: {query}\n\n"
        f"Retrieved context JSON:\n{json.dumps(context_packet, ensure_ascii=True)}\n\n"
        "Return a concise factual answer using only this context."
    )
    return system_prompt, user_prompt


def _build_deterministic_answer(context_packet: dict[str, Any]) -> str | None:
    intent = context_packet.get("intent")
    facts = context_packet.get("factual_context", {})
    scheme = facts.get("scheme_name", "this scheme")

    if intent == "aum_holdings":
        aum_value = facts.get("aum_value")
        aum_date = facts.get("aum_date")
        holdings = facts.get("portfolio_holdings") or []
        if aum_value in (None, "") or aum_date in (None, "") or not holdings:
            return None
        top = holdings[:5]
        hold_text = ", ".join(f"{h.get('security_name')} ({h.get('weight')}%)" for h in top)
        return (
            f"AUM of {scheme} is {aum_value} as of {aum_date}. "
            f"Top holdings: {hold_text}."
        )

    if intent == "holdings":
        holdings = facts.get("portfolio_holdings") or []
        if not holdings:
            return None
        top = holdings[:5]
        hold_text = ", ".join(f"{h.get('security_name')} ({h.get('weight')}%)" for h in top)
        return f"Top holdings of {scheme}: {hold_text}."

    return None


def generate_grounded_answer(query: str, context_packet: dict[str, Any], llm_client: LLMClient) -> dict[str, Any]:
    if not context_packet:
        return {"answer": FALLBACK, "grounded": False, "reason": "empty_context"}

    deterministic = _build_deterministic_answer(context_packet)
    if deterministic:
        return {
            "answer": deterministic,
            "grounded": True,
            "reason": "ok",
            "citations": context_packet.get("citations", {}),
        }

    system_prompt, user_prompt = _build_prompts(query, context_packet)
    answer = llm_client.generate(system_prompt, user_prompt)

    required_fields = context_packet.get("required_fields", [])
    factual_context = context_packet.get("factual_context", {})
    verification = verify_answer_grounding(answer, factual_context, required_fields)
    if not verification.grounded:
        return {
            "answer": FALLBACK,
            "grounded": False,
            "reason": "ungrounded_generation_blocked",
            "ungrounded_claims": verification.ungrounded_claims,
        }

    citations = context_packet.get("citations", {})
    return {
        "answer": answer,
        "grounded": True,
        "reason": "ok",
        "citations": citations,
    }
