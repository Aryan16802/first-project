from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from mf_rag.ingestion.groww_client import GrowwClient


@dataclass
class IngestionRunResult:
    run_id: str
    started_at: str
    ended_at: str
    status: str
    records: int
    checksum: str
    raw_file: str


class RawLandingStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def write_snapshot(self, run_id: str, payload: dict) -> tuple[Path, str]:
        run_dir = self.root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        raw_file = run_dir / "scheme_master.json"
        text = json.dumps(payload, indent=2, sort_keys=True)
        raw_file.write_text(text, encoding="utf-8")
        checksum = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return raw_file, checksum


class IngestionMetadataTracker:
    def __init__(self, metadata_file: Path) -> None:
        self.metadata_file = metadata_file
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)

    def append(self, run: IngestionRunResult) -> None:
        with self.metadata_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(run)) + "\n")


class Phase1IngestionPipeline:
    def __init__(
        self,
        client: GrowwClient,
        landing_store: RawLandingStore,
        tracker: IngestionMetadataTracker,
    ) -> None:
        self.client = client
        self.landing_store = landing_store
        self.tracker = tracker

    def run(self) -> IngestionRunResult:
        run_id = f"ingest_{uuid4().hex[:12]}"
        started_at = datetime.now(tz=timezone.utc).isoformat()
        payload = {"schemes": self.client.fetch_scheme_master()}
        raw_file, checksum = self.landing_store.write_snapshot(run_id, payload)
        ended_at = datetime.now(tz=timezone.utc).isoformat()
        result = IngestionRunResult(
            run_id=run_id,
            started_at=started_at,
            ended_at=ended_at,
            status="success",
            records=len(payload["schemes"]),
            checksum=checksum,
            raw_file=str(raw_file),
        )
        self.tracker.append(result)
        return result
