from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class Event:
    name: str
    payload: dict[str, Any]
    timestamp: str


class EventBus:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def publish(self, name: str, payload: dict[str, Any]) -> Event:
        event = Event(
            name=name,
            payload=payload,
            timestamp=datetime.now(tz=timezone.utc).isoformat(),
        )
        self.events.append(event)
        return event
