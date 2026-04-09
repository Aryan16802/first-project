from __future__ import annotations

import json
from pathlib import Path

from mf_rag.processing.normalize import normalize_scheme_record


def run_phase3(raw_file: Path, curated_file: Path, ingestion_run_id: str, version: str) -> int:
    payload = json.loads(raw_file.read_text(encoding="utf-8"))
    schemes = payload.get("schemes", [])
    curated_file.parent.mkdir(parents=True, exist_ok=True)

    rows: list[str] = []
    for raw in schemes:
        normalized = normalize_scheme_record(raw=raw, ingestion_run_id=ingestion_run_id, version=version)
        rows.append(normalized.model_dump_json())

    curated_file.write_text("\n".join(rows) + ("\n" if rows else ""), encoding="utf-8")
    return len(rows)
