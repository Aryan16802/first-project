import json
from pathlib import Path

from mf_rag.ingestion.groww_client import GrowwClient
from mf_rag.ingestion.pipeline import IngestionMetadataTracker, Phase1IngestionPipeline, RawLandingStore
from mf_rag.orchestration.events import EventBus
from mf_rag.orchestration.freshness_engine import FreshnessState, FreshnessStateStore, Phase2FreshnessEngine


def test_phase2_success_publishes_dataset_updated(tmp_path: Path) -> None:
    sample = Path("mf_rag/sample_data/groww_schemes.json")
    ingestion = Phase1IngestionPipeline(
        client=GrowwClient(sample_file=sample),
        landing_store=RawLandingStore(tmp_path / "raw"),
        tracker=IngestionMetadataTracker(tmp_path / "metadata" / "ingestion_runs.jsonl"),
    )
    bus = EventBus()
    store = FreshnessStateStore(tmp_path / "state" / "freshness_state.json")
    engine = Phase2FreshnessEngine(ingestion.run, bus, store)

    state = engine.scheduler_tick()

    assert state.active_run_id is not None
    assert state.last_good_run_id == state.active_run_id
    assert state.freshness_warning is None
    assert len(bus.events) == 1
    assert bus.events[0].name == "dataset_updated"


def test_phase2_failure_keeps_last_good_snapshot(tmp_path: Path) -> None:
    bus = EventBus()
    store = FreshnessStateStore(tmp_path / "state" / "freshness_state.json")
    store.save(FreshnessState(active_run_id="ingest_good_1", last_good_run_id="ingest_good_1", freshness_warning=None))

    def fail_ingestion() -> None:
        raise RuntimeError("network timeout")

    engine = Phase2FreshnessEngine(fail_ingestion, bus, store)
    state = engine.scheduler_tick()

    assert state.active_run_id == "ingest_good_1"
    assert state.last_good_run_id == "ingest_good_1"
    assert state.freshness_warning is not None
    assert bus.events[-1].name == "dataset_update_failed"

    state_data = json.loads((tmp_path / "state" / "freshness_state.json").read_text(encoding="utf-8"))
    assert state_data["active_run_id"] == "ingest_good_1"
