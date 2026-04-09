from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CacheEntry:
    key: str
    value: Any


class VersionedCache:
    """
    Phase 4 cache layer with version-aware keys to avoid stale answers.
    """

    def __init__(self) -> None:
        self._values: dict[str, Any] = {}

    @staticmethod
    def build_key(namespace: str, query: str, data_version: str) -> str:
        return f"{namespace}:{data_version}:{query.strip().lower()}"

    def get(self, key: str) -> Any | None:
        return self._values.get(key)

    def set(self, key: str, value: Any) -> None:
        self._values[key] = value
