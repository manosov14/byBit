from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List


DEFAULT_STATE_PATH = Path("logs") / "state.json"


@dataclass(slots=True)
class PersistentState:
    armed: bool = False
    consec_losses: int = 0
    symbols: List[str] = field(default_factory=list)
    last_events: Dict[str, str] = field(default_factory=dict)
    last_notified_at: Dict[str, int] = field(default_factory=dict)
    extra: Dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, object]) -> "PersistentState":
        data = dict(payload or {})
        known_keys = {"armed", "consec_losses", "symbols", "last_events", "last_notified_at"}
        extra = {k: v for k, v in data.items() if k not in known_keys}
        return cls(
            armed=bool(data.get("armed", False)),
            consec_losses=int(data.get("consec_losses", 0) or 0),
            symbols=list(data.get("symbols", []) or []),
            last_events=dict(data.get("last_events", {}) or {}),
            last_notified_at=dict(data.get("last_notified_at", {}) or {}),
            extra=extra,
        )

    def to_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        extra = payload.pop("extra", {})
        payload.update(extra)
        return payload


class StateStore:
    def __init__(self, path: str | Path = DEFAULT_STATE_PATH):
        self.path = Path(path)

    def load(self) -> PersistentState:
        try:
            with self.path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            return PersistentState()
        return PersistentState.from_dict(raw)

    def save(self, state: PersistentState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(state.to_dict(), f, ensure_ascii=False)

    def update(self, mutate) -> PersistentState:
        state = self.load()
        mutate(state)
        self.save(state)
        return state

