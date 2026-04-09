from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
from uuid import uuid4

from mf_rag.phases.phase8.security import sanitize_telemetry


@dataclass
class TraceContext:
    trace_id: str
    created_at: str


class TelemetryLogger:
    def __init__(self, log_file: Path) -> None:
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def new_trace() -> TraceContext:
        return TraceContext(trace_id=f"trc_{uuid4().hex[:12]}", created_at=datetime.now(tz=timezone.utc).isoformat())

    def log_event(self, trace_id: str, event_name: str, payload: dict) -> None:
        row = {
            "trace_id": trace_id,
            "event_name": event_name,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "payload": sanitize_telemetry(payload),
        }
        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")
