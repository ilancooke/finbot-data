from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
import tempfile
from typing import Any

import pandas as pd


def write_daily_snapshot(
    data_date: date,
    bars: pd.DataFrame,
    output_dir: str | Path,
    metadata_extra: dict[str, Any] | None = None,
) -> Path:
    """Write a daily bars parquet snapshot and sidecar metadata."""

    return write_historical_snapshot(
        bars=bars,
        output_dir=output_dir,
        metadata={
            "data_date": data_date.isoformat(),
            **(metadata_extra or {}),
        },
    )


def write_historical_snapshot(
    bars: pd.DataFrame,
    output_dir: str | Path,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Atomically write a historical bars parquet snapshot and sidecar metadata."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "historical.parquet"
    metadata_path = output_dir / "historical.metadata.json"

    if "date" in bars.columns and not bars.empty:
        date_min = pd.to_datetime(bars["date"]).min().date().isoformat()
        date_max = pd.to_datetime(bars["date"]).max().date().isoformat()
    else:
        date_min = None
        date_max = None

    collected_at_utc = datetime.utcnow()
    snapshot_metadata = {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "rows": int(len(bars)),
        "symbols": int(bars["symbol"].nunique()) if "symbol" in bars.columns else 0,
        "data_min_date": date_min,
        "data_max_date": date_max,
        "parquet_file": output_path.name,
        **(metadata or {}),
    }

    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".parquet", delete=False) as temp_parquet:
        temp_parquet_path = Path(temp_parquet.name)
    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".json", mode="w", encoding="utf-8", delete=False) as temp_metadata:
        temp_metadata_path = Path(temp_metadata.name)
        json.dump(snapshot_metadata, temp_metadata, indent=2)

    try:
        bars.to_parquet(temp_parquet_path, index=False)
        temp_parquet_path.replace(output_path)
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_parquet_path.unlink(missing_ok=True)
        temp_metadata_path.unlink(missing_ok=True)

    return output_path
