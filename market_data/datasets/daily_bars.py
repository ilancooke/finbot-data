from __future__ import annotations

from datetime import date, timedelta
import logging
import os
from pathlib import Path
import time
from typing import Callable

import pandas as pd

from market_data.config import get_env
from market_data.normalize import BAR_COLUMNS
from market_data.providers.massive import download_grouped_daily_bars
from market_data.storage import write_daily_snapshot, write_historical_snapshot

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path("data/daily_bars")
DEFAULT_HISTORY_YEARS = 2
DEFAULT_CALLS_PER_MINUTE = 5
TRUTHY_VALUES = {"1", "true", "yes"}


def network_disabled() -> bool:
    return os.getenv("FINBOT_INGEST_DISABLE_NETWORK", "").lower() in TRUTHY_VALUES


def resolve_output_dir(output_dir: str | Path | None) -> Path:
    if output_dir is not None:
        return Path(output_dir)
    return Path(os.getenv("FINBOT_RAW_BARS_DIR", str(DEFAULT_OUTPUT_DIR)))


def default_years() -> int:
    return int(os.getenv("FINBOT_HISTORY_YEARS", str(DEFAULT_HISTORY_YEARS)))


def default_calls_per_minute() -> float:
    return float(os.getenv("MASSIVE_CALLS_PER_MINUTE", str(DEFAULT_CALLS_PER_MINUTE)))


def get_massive_api_key() -> str:
    api_key = get_env("MASSIVE_API_KEY") or get_env("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("Missing MASSIVE_API_KEY or POLYGON_API_KEY for market data ingest")
    return api_key


def window_start_date(end_date: date, years: int) -> date:
    return (pd.Timestamp(end_date) - pd.DateOffset(years=years)).date() + timedelta(days=1)


def days_window_start_date(end_date: date, days: int) -> date:
    return end_date - timedelta(days=days - 1)


def business_dates(start_date: date, end_date: date) -> list[date]:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    return [timestamp.date() for timestamp in pd.bdate_range(start=start_date, end=end_date)]


def sleep_after_request(calls_per_minute: float) -> None:
    if calls_per_minute <= 0:
        return
    time.sleep(60.0 / calls_per_minute)


def base_metadata(mode: str) -> dict[str, object]:
    return {
        "provider": "massive",
        "adjusted": True,
        "mode": mode,
    }


def download_single_date(data_date: date, output_dir: str | Path | None = None) -> Path:
    """Download adjusted daily bars for one market date and write a fresh snapshot."""

    resolved_output_dir = resolve_output_dir(output_dir)
    if network_disabled():
        logger.warning("Network ingest disabled via FINBOT_INGEST_DISABLE_NETWORK; writing empty snapshot")
        output_path = write_daily_snapshot(
            data_date,
            pd.DataFrame(columns=BAR_COLUMNS),
            resolved_output_dir,
            metadata_extra=base_metadata("single-date"),
        )
        logger.info("Daily ingest completed with network disabled. Output=%s", output_path)
        return output_path

    bars = download_grouped_daily_bars(data_date=data_date, api_key=get_massive_api_key())

    output_path = write_daily_snapshot(
        data_date,
        bars,
        resolved_output_dir,
        metadata_extra=base_metadata("single-date"),
    )
    logger.info(
        "Downloaded Massive grouped daily bars for date=%s symbols=%d rows=%d output=%s",
        data_date.isoformat(),
        int(bars["symbol"].nunique()) if "symbol" in bars.columns else 0,
        len(bars),
        output_path,
    )
    return output_path


def download_history(
    end_date: date,
    years: int | None,
    days: int | None = None,
    output_dir: str | Path | None = None,
    calls_per_minute: float = DEFAULT_CALLS_PER_MINUTE,
    downloader: Callable[[date, str], pd.DataFrame] | None = None,
) -> Path:
    """Download a rolling adjusted daily history window and replace the snapshot."""

    resolved_output_dir = resolve_output_dir(output_dir)
    if days is not None:
        start_date = days_window_start_date(end_date, days)
        window_metadata = {"history_days": days}
    elif years is not None:
        start_date = window_start_date(end_date, years)
        window_metadata = {"history_years": years}
    else:
        raise ValueError("Either years or days is required")
    market_dates = business_dates(start_date, end_date)
    metadata = {
        **base_metadata("replace"),
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        **window_metadata,
        "market_dates_requested": len(market_dates),
        "calls_per_minute": calls_per_minute,
        "empty_market_dates": [],
    }

    if network_disabled():
        logger.warning("Network ingest disabled via FINBOT_INGEST_DISABLE_NETWORK; writing empty history snapshot")
        return write_historical_snapshot(
            pd.DataFrame(columns=BAR_COLUMNS),
            resolved_output_dir,
            metadata=metadata,
        )

    api_key = get_massive_api_key()
    fetch = downloader or (lambda data_date, key: download_grouped_daily_bars(data_date=data_date, api_key=key))
    frames: list[pd.DataFrame] = []
    empty_dates: list[str] = []

    for idx, data_date in enumerate(market_dates, start=1):
        bars = fetch(data_date, api_key)
        if bars.empty:
            empty_dates.append(data_date.isoformat())
            logger.info("No Massive grouped daily bars for date=%s (%d/%d)", data_date.isoformat(), idx, len(market_dates))
        else:
            frames.append(bars)
            logger.info(
                "Downloaded Massive grouped daily bars date=%s rows=%d (%d/%d)",
                data_date.isoformat(),
                len(bars),
                idx,
                len(market_dates),
            )
        if idx < len(market_dates):
            sleep_after_request(calls_per_minute)

    if frames:
        all_bars = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["symbol", "date"], keep="last")
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )
    else:
        all_bars = pd.DataFrame(columns=BAR_COLUMNS)

    metadata["empty_market_dates"] = empty_dates
    output_path = write_historical_snapshot(all_bars, resolved_output_dir, metadata=metadata)
    logger.info(
        "Replaced Massive daily history snapshot start=%s end=%s rows=%d symbols=%d output=%s",
        start_date.isoformat(),
        end_date.isoformat(),
        len(all_bars),
        int(all_bars["symbol"].nunique()) if "symbol" in all_bars.columns else 0,
        output_path,
    )
    return output_path
