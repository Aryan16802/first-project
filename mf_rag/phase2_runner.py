from __future__ import annotations

from pathlib import Path

from mf_rag.ingestion.groww_client import GrowwClient
from mf_rag.ingestion.pipeline import IngestionMetadataTracker, Phase1IngestionPipeline, RawLandingStore
from mf_rag.orchestration.events import EventBus
from mf_rag.orchestration.freshness_engine import FreshnessStateStore, Phase2FreshnessEngine


def build_phase2_engine(data_root: Path) -> Phase2FreshnessEngine:
    sample = Path(__file__).parent / "sample_data" / "groww_schemes.json"
    client = GrowwClient(sample_file=sample)
    ingestion = Phase1IngestionPipeline(
        client=client,
        landing_store=RawLandingStore(data_root / "raw"),
        tracker=IngestionMetadataTracker(data_root / "metadata" / "ingestion_runs.jsonl"),
    )
    return Phase2FreshnessEngine(
        run_ingestion=ingestion.run,
        event_bus=EventBus(),
        state_store=FreshnessStateStore(data_root / "state" / "freshness_state.json"),
    )


if __name__ == "__main__":
    root = Path("data")
    engine = build_phase2_engine(root)
    state = engine.scheduler_tick()
    print(f"Phase 2 tick complete. active_run_id={state.active_run_id}")
