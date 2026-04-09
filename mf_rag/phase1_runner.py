from __future__ import annotations

import os
from pathlib import Path

from mf_rag.ingestion.groww_client import GrowwClient
from mf_rag.ingestion.pipeline import IngestionMetadataTracker, Phase1IngestionPipeline, RawLandingStore


def run_phase1(data_root: Path, use_live: bool = False) -> str:
    sample = Path(__file__).parent / "sample_data" / "groww_schemes.json"
    client = GrowwClient(sample_file=sample, use_live=use_live)
    pipeline = Phase1IngestionPipeline(
        client=client,
        landing_store=RawLandingStore(data_root / "raw"),
        tracker=IngestionMetadataTracker(data_root / "metadata" / "ingestion_runs.jsonl"),
    )
    result = pipeline.run()
    return result.run_id


if __name__ == "__main__":
    root = Path("data")
    use_live = os.getenv("MF_USE_LIVE_GROWW", "false").strip().lower() in {"1", "true", "yes"}
    run_id = run_phase1(root, use_live=use_live)
    print(f"Phase 1 ingestion completed with run_id={run_id}")
