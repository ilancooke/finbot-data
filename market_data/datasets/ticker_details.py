from __future__ import annotations

from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Callable, Any

import pandas as pd

from market_data.config import get_env
from market_data.http import MassiveClient
from market_data.providers.massive import TICKER_DETAIL_COLUMNS, download_ticker_details
from market_data.universe import read_ticker_universe

logger = logging.getLogger(__name__)

DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_CALLS_PER_MINUTE = 5
DETAILS_PARQUET_FILE = "ticker_details.parquet"
DETAILS_METADATA_FILE = "ticker_details.metadata.json"

TickerDetailsDownloader = Callable[[str, str, date | None], pd.DataFrame]


def resolve_reference_dir(reference_dir: str | Path | None) -> Path:
    if reference_dir is not None:
        return Path(reference_dir)
    return Path(os.getenv("FINBOT_REFERENCE_DIR", str(DEFAULT_REFERENCE_DIR)))


def default_calls_per_minute() -> float:
    return float(os.getenv("MASSIVE_CALLS_PER_MINUTE", str(DEFAULT_CALLS_PER_MINUTE)))


def get_massive_api_key() -> str:
    api_key = get_env("MASSIVE_API_KEY") or get_env("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("Missing MASSIVE_API_KEY or POLYGON_API_KEY for ticker details download")
    return api_key


def normalize_ticker_details_frame(details: pd.DataFrame) -> pd.DataFrame:
    """Normalize cached/fetched ticker details to the durable schema."""

    if details.empty:
        return pd.DataFrame(columns=TICKER_DETAIL_COLUMNS)

    frame = details.copy()
    for column in TICKER_DETAIL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    return frame[TICKER_DETAIL_COLUMNS].sort_values("ticker").reset_index(drop=True)


def read_ticker_details(output_dir: str | Path) -> pd.DataFrame:
    """Read ticker details parquet from output_dir/ticker_details.parquet."""

    return normalize_ticker_details_frame(pd.read_parquet(Path(output_dir) / DETAILS_PARQUET_FILE))


def write_ticker_details_snapshot(
    details: pd.DataFrame,
    output_dir: str | Path,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Atomically write ticker details parquet and sidecar metadata."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / DETAILS_PARQUET_FILE
    metadata_path = output_dir / DETAILS_METADATA_FILE

    normalized = normalize_ticker_details_frame(details)
    collected_at_utc = datetime.utcnow()
    snapshot_metadata = {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "rows": int(len(normalized)),
        "tickers": int(normalized["ticker"].nunique()) if "ticker" in normalized.columns else 0,
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


def _read_cached_details(output_dir: Path, target_tickers: set[str]) -> pd.DataFrame:
    details_path = output_dir / DETAILS_PARQUET_FILE
    if not details_path.exists():
        return pd.DataFrame(columns=TICKER_DETAIL_COLUMNS)

    cached = read_ticker_details(output_dir)
    return cached[cached["ticker"].isin(target_tickers)].copy().reset_index(drop=True)


def _failure_metadata(ticker: str, exc: Exception) -> dict[str, str]:
    return {
        "ticker": ticker,
        "error_type": type(exc).__name__,
        "message": str(exc)[:500],
    }


def download_ticker_details_snapshot(
    details_date: date | None = None,
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    calls_per_minute: float = DEFAULT_CALLS_PER_MINUTE,
    refresh_all: bool = False,
    limit: int | None = None,
    downloader: TickerDetailsDownloader | None = None,
) -> Path:
    """Download Massive ticker details for the current ticker universe."""

    resolved_input_dir = resolve_reference_dir(input_dir)
    resolved_output_dir = resolve_reference_dir(output_dir) if output_dir is not None else resolved_input_dir
    universe = read_ticker_universe(resolved_input_dir)
    target_tickers = _ticker_list(universe)
    target_ticker_set = set(target_tickers)

    cached = pd.DataFrame(columns=TICKER_DETAIL_COLUMNS)
    if not refresh_all:
        cached = _read_cached_details(resolved_output_dir, target_ticker_set)

    cached_tickers = set(cached["ticker"].tolist()) if not cached.empty else set()
    fetch_candidates = [ticker for ticker in target_tickers if ticker not in cached_tickers]
    fetch_tickers = fetch_candidates[:limit] if limit is not None else fetch_candidates

    frames: list[pd.DataFrame] = []
    missing_tickers: list[str] = []
    failed_tickers: list[dict[str, str]] = []

    api_key = get_massive_api_key() if fetch_tickers else ""
    fetch = downloader or (lambda ticker, key, as_of: download_ticker_details(ticker=ticker, api_key=key, as_of=as_of))

    for idx, ticker in enumerate(fetch_tickers, start=1):
        try:
            details = normalize_ticker_details_frame(fetch(ticker, api_key, details_date))
        except Exception as exc:
            failed_tickers.append(_failure_metadata(ticker, exc))
            logger.exception("Massive ticker details failed ticker=%s (%d/%d)", ticker, idx, len(fetch_tickers))
            details = pd.DataFrame(columns=TICKER_DETAIL_COLUMNS)

        if details.empty:
            missing_tickers.append(ticker)
            logger.info("No Massive ticker details for ticker=%s (%d/%d)", ticker, idx, len(fetch_tickers))
        else:
            frames.append(details)
            logger.info("Downloaded Massive ticker details ticker=%s (%d/%d)", ticker, idx, len(fetch_tickers))

        if idx < len(fetch_tickers):
            MassiveClient.sleep_for_rate_limit(calls_per_minute)

    merge_frames = []
    if not cached.empty:
        merge_frames.append(cached)
    merge_frames.extend(frames)

    if merge_frames:
        all_details = (
            pd.concat(merge_frames, ignore_index=True)
            .drop_duplicates(subset=["ticker"], keep="last")
            .sort_values("ticker")
            .reset_index(drop=True)
        )
    else:
        all_details = pd.DataFrame(columns=TICKER_DETAIL_COLUMNS)

    mode = "replace" if refresh_all else "cache-merge"
    metadata = {
        "provider": "massive",
        "dataset": "ticker_details",
        "mode": mode,
        "details_date": details_date.isoformat() if details_date else "latest",
        "input_file": "tickers.parquet",
        "input_tickers": len(target_tickers),
        "cached_tickers": len(cached_tickers),
        "fetch_candidates": len(fetch_candidates),
        "requested_tickers": len(fetch_tickers),
        "pending_tickers": max(len(fetch_candidates) - len(fetch_tickers), 0),
        "fetched_tickers": int(sum(len(frame) for frame in frames)),
        "missing_tickers": missing_tickers,
        "failed_tickers": failed_tickers,
        "calls_per_minute": calls_per_minute,
    }
    output_path = write_ticker_details_snapshot(all_details, resolved_output_dir, metadata=metadata)
    logger.info(
        "Wrote Massive ticker details snapshot rows=%d cached=%d requested=%d output=%s",
        len(all_details),
        len(cached_tickers),
        len(fetch_tickers),
        output_path,
    )
    return output_path
