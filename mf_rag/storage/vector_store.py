from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class VectorDocument:
    doc_id: str
    text: str
    metadata: dict[str, Any]


class InMemoryVectorStore:
    """
    Placeholder semantic context layer.
    Stores text + metadata and supports metadata filtering.
    """

    def __init__(self) -> None:
        self._docs: list[VectorDocument] = []

    def upsert(self, docs: list[VectorDocument]) -> None:
        by_id = {d.doc_id: d for d in self._docs}
        for doc in docs:
            by_id[doc.doc_id] = doc
        self._docs = list(by_id.values())

    def query(self, text: str, top_k: int = 5, metadata_filter: dict[str, Any] | None = None) -> list[VectorDocument]:
        metadata_filter = metadata_filter or {}
        filtered: list[VectorDocument] = []
        for doc in self._docs:
            if all(doc.metadata.get(k) == v for k, v in metadata_filter.items()):
                filtered.append(doc)
        # Phase 4 behavior: deterministic selection by insertion order.
        return filtered[:top_k]
