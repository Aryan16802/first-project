from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def compute_data_lag_hours(as_of_iso: str | None, now: datetime | None = None) -> float | None:
    if not as_of_iso:
        return None
    reference = now or datetime.now(tz=timezone.utc)
    as_of_dt = datetime.fromisoformat(as_of_iso.replace("Z", "+00:00"))
    delta = reference - as_of_dt
    return max(0.0, delta.total_seconds() / 3600.0)


def freshness_status(as_of_iso: str | None, threshold_hours: float = 48.0) -> dict[str, Any]:
    lag = compute_data_lag_hours(as_of_iso)
    if lag is None:
        return {"status": "unknown", "lag_hours": None}
    return {
        "status": "stale" if lag > threshold_hours else "fresh",
        "lag_hours": lag,
    }
