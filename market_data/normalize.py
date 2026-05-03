from __future__ import annotations

import pandas as pd

BAR_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume"]


def normalize_bars_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize provider OHLCV output to Finbot's canonical daily bar schema."""

    if frame.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)

    if isinstance(frame.index, pd.MultiIndex):
        frame = frame.reset_index()
    else:
        frame = frame.reset_index().rename(columns={"index": "timestamp"})

    date_column = "timestamp" if "timestamp" in frame.columns else "date"
    frame["date"] = pd.to_datetime(frame[date_column]).dt.date
    return frame[BAR_COLUMNS]

