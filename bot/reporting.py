from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Sequence

from core.strategy import BreakoutInfo, PlannedTrade


def _is_number(value: str) -> bool:
    try:
        float(str(value).replace("%", "").replace("$", ""))
        return True
    except Exception:
        return False


def render_table(headers: Sequence[object], rows: Iterable[Sequence[object]], align_right: bool = True) -> str:
    headers_text = [str(h) for h in headers]
    rows_text = [["" if c is None else str(c) for c in row] for row in rows]
    widths = [len(h) for h in headers_text]
    for row in rows_text:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt(row: Sequence[str]) -> str:
        formatted: List[str] = []
        for idx, cell in enumerate(row):
            if align_right and _is_number(cell):
                formatted.append(cell.rjust(widths[idx]))
            else:
                formatted.append(cell.ljust(widths[idx]))
        return " | ".join(formatted)

    separator = "-+-".join("-" * w for w in widths)
    lines = [fmt(headers_text), separator]
    lines.extend(fmt(row) for row in rows_text)
    return "```\n" + "\n".join(lines) + "\n```"


def make_event_id(symbol: str, breakout: BreakoutInfo) -> str:
    dt = datetime.fromtimestamp(int(breakout.ts) / 1000, tz=timezone.utc).date().isoformat()
    return f"{symbol}|{dt}|{breakout.side}|{round(breakout.level, 4)}|{breakout.level_source}"


def format_signal_message(symbol: str, trade: PlannedTrade, breakout: BreakoutInfo) -> str:
    meta = trade.meta or {}
    break_pct = float(meta.get("break_pct", breakout.break_pct) * 100)
    close_pct = float(meta.get("close_back_pct", breakout.close_back_pct) * 100)
    vol_ratio = meta.get("volume_ratio")
    vol_text = f"{vol_ratio:.2f}" if isinstance(vol_ratio, (int, float)) else "—"
    level_source = meta.get("level_source", breakout.level_source)
    return (
        f"[{symbol}] {trade.side.upper()} @ {trade.entry:.4f} "
        f"SL={trade.sl:.4f} TP={trade.tp:.4f} "
        f"break={break_pct:.2f}% close={close_pct:.2f}% vol={vol_text} "
        f"level={level_source}"
    )

