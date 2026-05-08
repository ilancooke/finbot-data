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
from market_data.http import MassiveClient
from market_data.providers.massive import RELATED_TICKER_COLUMNS, download_related_tickers
from market_data.universe import read_ticker_universe

logger = logging.getLogger(__name__)

DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_CALLS_PER_MINUTE = 5
RELATED_PARQUET_FILE = "related_tickers.parquet"
RELATED_METADATA_FILE = "related_tickers.metadata.json"

RelatedTickersDownloader = Callable[[str, str], pd.DataFrame]


def resolve_reference_dir(reference_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        reference_dir,
        env_key="FINBOT_REFERENCE_DIR",
        default_path=DEFAULT_REFERENCE_DIR,
        data_root_subpath="reference",
    )


def default_calls_per_minute() -> float:
    return float(os.getenv("MASSIVE_CALLS_PER_MINUTE", str(DEFAULT_CALLS_PER_MINUTE)))


def get_massive_api_key() -> str:
    api_key = get_env("MASSIVE_API_KEY") or get_env("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("Missing MASSIVE_API_KEY or POLYGON_API_KEY for related tickers download")
    return api_key


def normalize_related_tickers_frame(related: pd.DataFrame) -> pd.DataFrame:
    """Normalize related ticker rows to the durable schema."""

    if related.empty:
        return pd.DataFrame(columns=RELATED_TICKER_COLUMNS)

    frame = related.copy()
    for column in RELATED_TICKER_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame["related_ticker"] = frame["related_ticker"].astype(str).str.upper()
    return (
        frame[RELATED_TICKER_COLUMNS]
        .drop_duplicates(subset=["ticker", "related_ticker"], keep="first")
        .sort_values(["ticker", "result_order", "related_ticker"])
        .reset_index(drop=True)
    )


def read_related_tickers(output_dir: str | Path) -> pd.DataFrame:
    """Read related tickers parquet from output_dir/related_tickers.parquet."""

    return normalize_related_tickers_frame(pd.read_parquet(Path(output_dir) / RELATED_PARQUET_FILE))


def write_related_tickers_snapshot(
    related: pd.DataFrame,
    output_dir: str | Path,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Atomically write related tickers parquet and sidecar metadata."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / RELATED_PARQUET_FILE
    metadata_path = output_dir / RELATED_METADATA_FILE

    normalized = normalize_related_tickers_frame(related)
    collected_at_utc = datetime.utcnow()
    snapshot_metadata = {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "rows": int(len(normalized)),
        "tickers": int(normalized["ticker"].nunique()) if "ticker" in normalized.columns else 0,
        "related_tickers": int(normalized["related_ticker"].nunique()) if "related_ticker" in normalized.columns else 0,
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


def _failure_metadata(ticker: str, exc: Exception) -> dict[str, str]:
    return {
        "ticker": ticker,
        "error_type": type(exc).__name__,
        "message": str(exc)[:500],
    }


def download_related_tickers_snapshot(
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    calls_per_minute: float = DEFAULT_CALLS_PER_MINUTE,
    limit: int | None = None,
    downloader: RelatedTickersDownloader | None = None,
) -> Path:
    """Download a replace-style Massive related tickers snapshot."""

    resolved_input_dir = resolve_reference_dir(input_dir)
    resolved_output_dir = resolve_reference_dir(output_dir) if output_dir is not None else resolved_input_dir
    universe = read_ticker_universe(resolved_input_dir)
    target_tickers = _ticker_list(universe)
    requested_tickers = target_tickers[:limit] if limit is not None else target_tickers

    frames: list[pd.DataFrame] = []
    empty_tickers: list[str] = []
    failed_tickers: list[dict[str, str]] = []

    api_key = get_massive_api_key() if requested_tickers else ""
    fetch = downloader or (lambda ticker, key: download_related_tickers(ticker=ticker, api_key=key))

    for idx, ticker in enumerate(requested_tickers, start=1):
        try:
            related = normalize_related_tickers_frame(fetch(ticker, api_key))
        except Exception as exc:
            failed_tickers.append(_failure_metadata(ticker, exc))
            logger.exception("Massive related tickers failed ticker=%s (%d/%d)", ticker, idx, len(requested_tickers))
            if idx < len(requested_tickers):
                MassiveClient.sleep_for_rate_limit(calls_per_minute)
            continue

        if related.empty:
            empty_tickers.append(ticker)
            logger.info("No Massive related tickers for ticker=%s (%d/%d)", ticker, idx, len(requested_tickers))
        else:
            frames.append(related)
            logger.info("Downloaded Massive related tickers ticker=%s rows=%d (%d/%d)", ticker, len(related), idx, len(requested_tickers))

        if idx < len(requested_tickers):
            MassiveClient.sleep_for_rate_limit(calls_per_minute)

    if frames:
        all_related = normalize_related_tickers_frame(pd.concat(frames, ignore_index=True))
    else:
        all_related = pd.DataFrame(columns=RELATED_TICKER_COLUMNS)

    metadata = {
        "provider": "massive",
        "dataset": "related_tickers",
        "mode": "replace",
        "input_file": "tickers.parquet",
        "input_tickers": len(target_tickers),
        "requested_tickers": len(requested_tickers),
        "partial": limit is not None,
        "pending_tickers": max(len(target_tickers) - len(requested_tickers), 0),
        "empty_tickers": empty_tickers,
        "failed_tickers": failed_tickers,
        "calls_per_minute": calls_per_minute,
    }
    output_path = write_related_tickers_snapshot(all_related, resolved_output_dir, metadata=metadata)
    logger.info(
        "Wrote Massive related tickers snapshot rows=%d requested=%d output=%s",
        len(all_related),
        len(requested_tickers),
        output_path,
    )
    return output_path
