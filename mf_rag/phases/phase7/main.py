from __future__ import annotations

from pathlib import Path

from mf_rag.phases.phase5 import RetrievalPipeline
from mf_rag.phases.phase6 import GroqClient, load_groq_config
from mf_rag.phases.phase7.api import create_app
from mf_rag.phases.phase7.service import ChatOrchestrator
from mf_rag.storage.structured_store import StructuredStore
from mf_rag.storage.vector_store import InMemoryVectorStore


def build_app(data_dir: Path | None = None):
    root = data_dir or Path("data")
    store = StructuredStore(root / "truth.db")
    store.init_schema()
    vector = InMemoryVectorStore()
    retrieval = RetrievalPipeline(structured_store=store, vector_store=vector)
    llm = GroqClient(load_groq_config())
    orchestrator = ChatOrchestrator(retrieval=retrieval, llm_client=llm, store=store)
    return create_app(orchestrator)


app = build_app()
