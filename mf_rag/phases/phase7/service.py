from __future__ import annotations

from typing import Any, Protocol

from mf_rag.phases.phase5 import RetrievalPipeline
from mf_rag.phases.phase6 import FALLBACK, generate_grounded_answer
from mf_rag.storage.structured_store import StructuredStore


class LLMClient(Protocol):
    def generate(self, system_prompt: str, user_prompt: str) -> str: ...


class ChatOrchestrator:
    def __init__(self, retrieval: RetrievalPipeline, llm_client: LLMClient, store: StructuredStore) -> None:
        self.retrieval = retrieval
        self.llm_client = llm_client
        self.store = store

    def chat(self, query: str) -> dict[str, Any]:
        retrieval_result = self.retrieval.run(query)
        if not retrieval_result.answerable:
            citations = retrieval_result.context_packet.get("citations", {})
            return {
                "answer": retrieval_result.fallback_message or FALLBACK,
                "grounded": False,
                "reason": retrieval_result.reason,
                "citations": citations,
                "as_of": citations.get("source_timestamp"),
            }

        generation = generate_grounded_answer(
            query=query,
            context_packet=retrieval_result.context_packet,
            llm_client=self.llm_client,
        )
        citations = generation.get("citations", {})
        return {
            "answer": generation["answer"],
            "grounded": generation["grounded"],
            "reason": generation["reason"],
            "citations": citations,
            "as_of": citations.get("source_timestamp"),
        }

    def get_scheme(self, scheme_id: str) -> dict[str, Any] | None:
        facts = self.store.get_latest_scheme_facts(scheme_id)
        if not facts:
            return None
        managers = self.store.get_scheme_managers(scheme_id, facts["ingestion_run_id"])
        out = dict(facts)
        out["fund_managers"] = managers
        return out

    def compare(self, scheme_ids: list[str]) -> dict[str, Any]:
        rows = []
        for sid in scheme_ids:
            facts = self.get_scheme(sid)
            if facts:
                rows.append(facts)
        return {"schemes": rows}
