from __future__ import annotations

from datetime import date
from datetime import datetime
import json
from pathlib import Path
import tempfile
from typing import Any

import pandas as pd

from market_data.http import MassiveClient

TICKER_COLUMNS = [
    "ticker",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "type",
    "active",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "last_updated_utc",
]

LISTED_PRIMARY_EXCHANGES = {"XNYS", "XNAS", "ARCX", "XASE", "BATS"}


def normalize_tickers(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize Massive ticker reference rows."""

    if not rows:
        return pd.DataFrame(columns=TICKER_COLUMNS)

    frame = pd.json_normalize(rows)
    for column in TICKER_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    return frame[TICKER_COLUMNS].sort_values("ticker").reset_index(drop=True)


def fetch_ticker_universe(
    client: MassiveClient,
    as_of: date | None = None,
    market: str = "stocks",
    active: bool = True,
    limit: int = 1000,
    calls_per_minute: float = 0,
) -> pd.DataFrame:
    """Fetch the Massive ticker universe for downstream per-symbol jobs."""

    params: dict[str, Any] = {
        "market": market,
        "active": str(active).lower(),
        "limit": limit,
        "sort": "ticker",
        "order": "asc",
    }
    if as_of is not None:
        params["date"] = as_of.isoformat()

    rows = client.get_paginated(
        "/v3/reference/tickers",
        params=params,
        calls_per_minute=calls_per_minute,
    )
    return normalize_tickers(rows)


def filter_common_stocks(tickers: pd.DataFrame) -> pd.DataFrame:
    """Filter to active US common stocks on listed primary exchanges."""

    if tickers.empty:
        return pd.DataFrame(columns=tickers.columns)

    return (
        tickers[
            (tickers["type"] == "CS")
            & (tickers["active"] == True)
            & (tickers["market"] == "stocks")
            & (tickers["locale"] == "us")
            & (tickers["primary_exchange"].isin(LISTED_PRIMARY_EXCHANGES))
        ]
        .copy()
        .sort_values("ticker")
        .reset_index(drop=True)
    )


def write_ticker_universe(
    tickers: pd.DataFrame,
    output_dir: str | Path,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Atomically write ticker universe parquet and sidecar metadata."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "tickers.parquet"
    metadata_path = output_dir / "tickers.metadata.json"

    collected_at_utc = datetime.utcnow()
    snapshot_metadata = {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "rows": int(len(tickers)),
        "tickers": int(tickers["ticker"].nunique()) if "ticker" in tickers.columns else 0,
        "parquet_file": output_path.name,
        **(metadata or {}),
    }

    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".parquet", delete=False) as temp_parquet:
        temp_parquet_path = Path(temp_parquet.name)
    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".json", mode="w", encoding="utf-8", delete=False) as temp_metadata:
        temp_metadata_path = Path(temp_metadata.name)
        json.dump(snapshot_metadata, temp_metadata, indent=2)

    try:
        tickers.to_parquet(temp_parquet_path, index=False)
        temp_parquet_path.replace(output_path)
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_parquet_path.unlink(missing_ok=True)
        temp_metadata_path.unlink(missing_ok=True)

    return output_path


def read_ticker_universe(output_dir: str | Path) -> pd.DataFrame:
    """Read ticker universe parquet from output_dir/tickers.parquet."""

    return pd.read_parquet(Path(output_dir) / "tickers.parquet")
