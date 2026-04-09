import json
from pathlib import Path

from mf_rag.phase1_runner import run_phase1


def test_phase1_ingestion_writes_raw_and_metadata(tmp_path: Path) -> None:
    run_id = run_phase1(tmp_path)

    raw_file = tmp_path / "raw" / run_id / "scheme_master.json"
    metadata_file = tmp_path / "metadata" / "ingestion_runs.jsonl"

    assert raw_file.exists()
    assert metadata_file.exists()

    payload = json.loads(raw_file.read_text(encoding="utf-8"))
    assert "schemes" in payload
    assert len(payload["schemes"]) >= 1

    lines = metadata_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    run_row = json.loads(lines[0])
    assert run_row["run_id"] == run_id
    assert run_row["status"] == "success"
    assert run_row["records"] >= 1
