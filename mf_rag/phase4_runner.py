from __future__ import annotations

import json
from pathlib import Path

from mf_rag.storage.structured_store import StructuredStore


def run_phase4(curated_jsonl: Path, db_path: Path) -> int:
    store = StructuredStore(db_path=db_path)
    store.init_schema()

    count = 0
    for line in curated_jsonl.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        store.upsert_curated_record(row)
        count += 1
    return count
