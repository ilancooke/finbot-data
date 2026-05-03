"""Download adjusted daily US stock bars from Massive."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import logging
import os
from pathlib import Path
import sys
import time
from typing import Callable, Sequence

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from finbot_data.config import get_env
from finbot_data.normalize import BAR_COLUMNS
from finbot_data.providers.massive import download_grouped_daily_bars
from finbot_data.storage import write_daily_snapshot, write_historical_snapshot

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path("data/daily_bars")
DEFAULT_HISTORY_YEARS = 2
DEFAULT_CALLS_PER_MINUTE = 5
TRUTHY_VALUES = {"1", "true", "yes"}


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging once for CLI execution."""

    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _network_disabled() -> bool:
    return os.getenv("FINBOT_INGEST_DISABLE_NETWORK", "").lower() in TRUTHY_VALUES


def _resolve_output_dir(output_dir: str | Path | None) -> Path:
    if output_dir is not None:
        return Path(output_dir)
    return Path(os.getenv("FINBOT_RAW_BARS_DIR", str(DEFAULT_OUTPUT_DIR)))


def _default_years() -> int:
    return int(os.getenv("FINBOT_HISTORY_YEARS", str(DEFAULT_HISTORY_YEARS)))


def _default_calls_per_minute() -> float:
    return float(os.getenv("MASSIVE_CALLS_PER_MINUTE", str(DEFAULT_CALLS_PER_MINUTE)))


def _get_massive_api_key() -> str:
    api_key = get_env("MASSIVE_API_KEY") or get_env("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("Missing MASSIVE_API_KEY or POLYGON_API_KEY for market data ingest")
    return api_key


def _window_start_date(end_date: date, years: int) -> date:
    return (pd.Timestamp(end_date) - pd.DateOffset(years=years)).date() + timedelta(days=1)


def _days_window_start_date(end_date: date, days: int) -> date:
    return end_date - timedelta(days=days - 1)


def _business_dates(start_date: date, end_date: date) -> list[date]:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    return [timestamp.date() for timestamp in pd.bdate_range(start=start_date, end=end_date)]


def _sleep_after_request(calls_per_minute: float) -> None:
    if calls_per_minute <= 0:
        return
    time.sleep(60.0 / calls_per_minute)


def _base_metadata(mode: str) -> dict[str, object]:
    return {
        "provider": "massive",
        "adjusted": True,
        "mode": mode,
    }


def download_single_date(data_date: date, output_dir: str | Path | None = None) -> Path:
    """Download adjusted daily bars for one market date and write a fresh snapshot."""

    resolved_output_dir = _resolve_output_dir(output_dir)
    if _network_disabled():
        logger.warning("Network ingest disabled via FINBOT_INGEST_DISABLE_NETWORK; writing empty snapshot")
        output_path = write_daily_snapshot(
            data_date,
            pd.DataFrame(columns=BAR_COLUMNS),
            resolved_output_dir,
            metadata_extra=_base_metadata("single-date"),
        )
        logger.info("Daily ingest completed with network disabled. Output=%s", output_path)
        return output_path

    bars = download_grouped_daily_bars(data_date=data_date, api_key=_get_massive_api_key())

    output_path = write_daily_snapshot(
        data_date,
        bars,
        resolved_output_dir,
        metadata_extra=_base_metadata("single-date"),
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

    resolved_output_dir = _resolve_output_dir(output_dir)
    if days is not None:
        start_date = _days_window_start_date(end_date, days)
        window_metadata = {"history_days": days}
    elif years is not None:
        start_date = _window_start_date(end_date, years)
        window_metadata = {"history_years": years}
    else:
        raise ValueError("Either years or days is required")
    market_dates = _business_dates(start_date, end_date)
    metadata = {
        **_base_metadata("replace"),
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        **window_metadata,
        "market_dates_requested": len(market_dates),
        "calls_per_minute": calls_per_minute,
        "empty_market_dates": [],
    }

    if _network_disabled():
        logger.warning("Network ingest disabled via FINBOT_INGEST_DISABLE_NETWORK; writing empty history snapshot")
        return write_historical_snapshot(
            pd.DataFrame(columns=BAR_COLUMNS),
            resolved_output_dir,
            metadata=metadata,
        )

    api_key = _get_massive_api_key()
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
            _sleep_after_request(calls_per_minute)

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


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date '{value}'. Expected YYYY-MM-DD") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download adjusted daily US stock bars from Massive")
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--date",
        type=_parse_date,
        help="Single market date in YYYY-MM-DD format for smoke/debug runs",
    )
    date_group.add_argument(
        "--end-date",
        type=_parse_date,
        help="Inclusive end date in YYYY-MM-DD format for a rolling history replace",
    )
    window_group = parser.add_mutually_exclusive_group()
    window_group.add_argument(
        "--years",
        type=int,
        default=None,
        help=f"History window in years for --end-date mode. Default: {DEFAULT_HISTORY_YEARS}",
    )
    window_group.add_argument(
        "--days",
        type=int,
        default=None,
        help="History window in calendar days for --end-date mode",
    )
    parser.add_argument(
        "--calls-per-minute",
        type=float,
        default=_default_calls_per_minute(),
        help=f"Massive REST pacing for --end-date mode. Use 0 to disable. Default: {DEFAULT_CALLS_PER_MINUTE}",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=f"Output directory for historical.parquet and metadata. Default: {DEFAULT_OUTPUT_DIR}",
    )
    args = parser.parse_args(argv)
    if args.date and (args.days is not None or args.years is not None):
        parser.error("--days and --years can only be used with --end-date")
    if args.end_date and args.days is None and args.years is None:
        args.years = _default_years()
    if args.years is not None and args.years <= 0:
        parser.error("--years must be greater than 0")
    if args.days is not None and args.days <= 0:
        parser.error("--days must be greater than 0")
    if args.calls_per_minute < 0:
        parser.error("--calls-per-minute must be 0 or greater")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)

    target_date = args.date or args.end_date
    logger.info("Massive daily download started target_date=%s", target_date.isoformat())
    started = time.perf_counter()
    try:
        if args.date:
            download_single_date(args.date, output_dir=args.output_dir)
        else:
            download_history(
                end_date=args.end_date,
                years=args.years,
                days=args.days,
                output_dir=args.output_dir,
                calls_per_minute=args.calls_per_minute,
            )
        logger.info(
            "Massive daily download succeeded target_date=%s total_time=%.3fs",
            target_date.isoformat(),
            time.perf_counter() - started,
        )
        return 0
    except Exception:
        logger.exception("Massive daily download failed target_date=%s", target_date.isoformat())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
