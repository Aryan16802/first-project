from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Callable

from mf_rag.ingestion.pipeline import IngestionRunResult
from mf_rag.orchestration.events import EventBus


@dataclass
class FreshnessState:
    active_run_id: str | None
    last_good_run_id: str | None
    freshness_warning: str | None


class FreshnessStateStore:
    def __init__(self, state_file: Path) -> None:
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> FreshnessState:
        if not self.state_file.exists():
            return FreshnessState(active_run_id=None, last_good_run_id=None, freshness_warning=None)
        data = json.loads(self.state_file.read_text(encoding="utf-8"))
        return FreshnessState(
            active_run_id=data.get("active_run_id"),
            last_good_run_id=data.get("last_good_run_id"),
            freshness_warning=data.get("freshness_warning"),
        )

    def save(self, state: FreshnessState) -> None:
        self.state_file.write_text(
            json.dumps(
                {
                    "active_run_id": state.active_run_id,
                    "last_good_run_id": state.last_good_run_id,
                    "freshness_warning": state.freshness_warning,
                },
                indent=2,
            ),
            encoding="utf-8",
        )


class Phase2FreshnessEngine:
    """
    Scheduler-triggered orchestration for ingestion.
    On success, emits dataset_updated.
    On failure, keeps last good snapshot active and emits dataset_update_failed.
    """

    def __init__(
        self,
        run_ingestion: Callable[[], IngestionRunResult],
        event_bus: EventBus,
        state_store: FreshnessStateStore,
    ) -> None:
        self.run_ingestion = run_ingestion
        self.event_bus = event_bus
        self.state_store = state_store

    def scheduler_tick(self) -> FreshnessState:
        state = self.state_store.load()
        try:
            result = self.run_ingestion()
            state.active_run_id = result.run_id
            state.last_good_run_id = result.run_id
            state.freshness_warning = None
            self.state_store.save(state)
            self.event_bus.publish(
                "dataset_updated",
                {"run_id": result.run_id, "records": result.records, "status": result.status},
            )
            return state
        except Exception as exc:  # noqa: BLE001
            state.active_run_id = state.last_good_run_id
            state.freshness_warning = "latest_ingestion_failed_using_last_good_snapshot"
            self.state_store.save(state)
            self.event_bus.publish(
                "dataset_update_failed",
                {
                    "error": str(exc),
                    "active_run_id": state.active_run_id,
                    "last_good_run_id": state.last_good_run_id,
                },
            )
            return state
