from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any


@dataclass
class MetricsSnapshot:
    total_requests: int
    grounded_rate: float
    refusal_rate: float
    low_confidence_rate: float
    avg_retrieval_latency_ms: float


class MetricsCollector:
    def __init__(self, metrics_file: Path) -> None:
        self.metrics_file = metrics_file
        self.metrics_file.parent.mkdir(parents=True, exist_ok=True)

    def record(self, event: dict[str, Any]) -> None:
        row = {"timestamp": datetime.now(tz=timezone.utc).isoformat(), **event}
        with self.metrics_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")

    def _load(self) -> list[dict[str, Any]]:
        if not self.metrics_file.exists():
            return []
        rows: list[dict[str, Any]] = []
        for line in self.metrics_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def snapshot(self) -> MetricsSnapshot:
        rows = self._load()
        if not rows:
            return MetricsSnapshot(0, 0.0, 0.0, 0.0, 0.0)

        total = len(rows)
        grounded = sum(1 for r in rows if r.get("grounded") is True)
        refusals = sum(1 for r in rows if r.get("reason") in {"policy_refusal", "out_of_scope", "pii_detected"})
        low_conf = sum(1 for r in rows if r.get("reason") == "low_confidence_context")
        latencies = [float(r.get("retrieval_latency_ms", 0.0)) for r in rows]

        return MetricsSnapshot(
            total_requests=total,
            grounded_rate=grounded / total,
            refusal_rate=refusals / total,
            low_confidence_rate=low_conf / total,
            avg_retrieval_latency_ms=sum(latencies) / total,
        )
