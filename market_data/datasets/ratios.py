from __future__ import annotations

from datetime import datetime
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Any, Callable

import pandas as pd

from market_data.config import get_env, resolve_finbot_data_path
from market_data.providers.massive import RATIO_COLUMNS, download_ratios
from market_data.universe import read_ticker_universe

logger = logging.getLogger(__name__)

DEFAULT_RATIOS_DIR = Path("data/ratios")
DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_CALLS_PER_MINUTE = 0
DEFAULT_LIMIT = 50_000
RATIOS_PARQUET_FILE = "ratios.parquet"
RATIOS_METADATA_FILE = "ratios.metadata.json"

RatiosDownloader = Callable[[str, int, float], pd.DataFrame]


def resolve_reference_dir(reference_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        reference_dir,
        env_key="FINBOT_REFERENCE_DIR",
        default_path=DEFAULT_REFERENCE_DIR,
        data_root_subpath="reference",
    )


def resolve_ratios_dir(ratios_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        ratios_dir,
        env_key="FINBOT_RATIOS_DIR",
        default_path=DEFAULT_RATIOS_DIR,
        data_root_subpath="ratios",
    )


def default_calls_per_minute() -> float:
    return float(os.getenv("MASSIVE_RATIOS_CALLS_PER_MINUTE", str(DEFAULT_CALLS_PER_MINUTE)))


def get_massive_api_key() -> str:
    api_key = get_env("MASSIVE_API_KEY") or get_env("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("Missing MASSIVE_API_KEY or POLYGON_API_KEY for ratios download")
    return api_key


def normalize_ratios_frame(ratios: pd.DataFrame) -> pd.DataFrame:
    """Normalize ratios rows to the durable schema."""

    if ratios.empty:
        return pd.DataFrame(columns=RATIO_COLUMNS)

    frame = ratios.copy()
    for column in RATIO_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    return (
        frame[RATIO_COLUMNS]
        .drop_duplicates(subset=["ticker", "date"], keep="last")
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )


def read_ratios(output_dir: str | Path) -> pd.DataFrame:
    """Read ratios parquet from output_dir/ratios.parquet."""

    return normalize_ratios_frame(pd.read_parquet(Path(output_dir) / RATIOS_PARQUET_FILE))


def write_ratios_snapshot(
    ratios: pd.DataFrame,
    output_dir: str | Path,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Atomically write ratios parquet and sidecar metadata."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / RATIOS_PARQUET_FILE
    metadata_path = output_dir / RATIOS_METADATA_FILE

    normalized = normalize_ratios_frame(ratios)
    if "date" in normalized.columns and not normalized.empty:
        date_min = pd.to_datetime(normalized["date"]).min().date().isoformat()
        date_max = pd.to_datetime(normalized["date"]).max().date().isoformat()
    else:
        date_min = None
        date_max = None

    collected_at_utc = datetime.utcnow()
    snapshot_metadata = {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "rows": int(len(normalized)),
        "tickers": int(normalized["ticker"].nunique()) if "ticker" in normalized.columns else 0,
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
        normalized.to_parquet(temp_parquet_path, index=False)
        temp_parquet_path.replace(output_path)
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_parquet_path.unlink(missing_ok=True)
        temp_metadata_path.unlink(missing_ok=True)

    return output_path


def _ticker_list(tickers: pd.DataFrame) -> list[str]:
    if "ticker" not in tickers.columns:
        raise ValueError("Ticker universe must include a ticker column")
    return sorted(tickers["ticker"].dropna().astype(str).str.upper().unique().tolist())


def download_ratios_snapshot(
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    calls_per_minute: float = DEFAULT_CALLS_PER_MINUTE,
    limit: int = DEFAULT_LIMIT,
    downloader: RatiosDownloader | None = None,
) -> Path:
    """Download a replace-style latest Massive ratios snapshot."""

    resolved_input_dir = resolve_reference_dir(input_dir)
    resolved_output_dir = resolve_ratios_dir(output_dir)
    universe = read_ticker_universe(resolved_input_dir)
    target_tickers = _ticker_list(universe)
    target_ticker_set = set(target_tickers)

    if target_tickers:
        api_key = get_massive_api_key()
        fetch = downloader or (lambda key, page_limit, cpm: download_ratios(api_key=key, limit=page_limit, calls_per_minute=cpm))
        raw_ratios = normalize_ratios_frame(fetch(api_key, limit, calls_per_minute))
        filtered_ratios = raw_ratios[raw_ratios["ticker"].isin(target_ticker_set)].copy().reset_index(drop=True)
    else:
        raw_ratios = pd.DataFrame(columns=RATIO_COLUMNS)
        filtered_ratios = pd.DataFrame(columns=RATIO_COLUMNS)

    metadata = {
        "provider": "massive",
        "dataset": "ratios",
        "mode": "replace",
        "input_file": "tickers.parquet",
        "input_tickers": len(target_tickers),
        "raw_rows": int(len(raw_ratios)),
        "output_rows": int(len(filtered_ratios)),
        "calls_per_minute": calls_per_minute,
        "limit": limit,
    }
    output_path = write_ratios_snapshot(filtered_ratios, resolved_output_dir, metadata=metadata)
    logger.info(
        "Wrote Massive ratios snapshot rows=%d raw_rows=%d input_tickers=%d output=%s",
        len(filtered_ratios),
        len(raw_ratios),
        len(target_tickers),
        output_path,
    )
    return output_path
