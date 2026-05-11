"""Download adjusted daily US stock bars from Massive."""

from __future__ import annotations

import argparse
from datetime import date
import logging
from pathlib import Path
import sys
import time
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_data.datasets.daily_bars import (
    DEFAULT_CALLS_PER_MINUTE,
    DEFAULT_HISTORY_YEARS,
    DEFAULT_OUTPUT_DIR,
    default_calls_per_minute,
    default_years,
    download_history,
    download_single_date,
)

logger = logging.getLogger(__name__)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging once for CLI execution."""

    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


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
        default=default_calls_per_minute(),
        help=f"Massive REST pacing for --end-date mode. Use 0 to disable. Default: {DEFAULT_CALLS_PER_MINUTE}",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=f"Output directory for historical.parquet and metadata. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Keep all symbols returned by Massive grouped bars instead of filtering to reference/tickers.parquet.",
    )
    parser.add_argument(
        "--ticker-universe-dir",
        default=None,
        help="Directory containing tickers.parquet. Defaults to FINBOT_REFERENCE_DIR or FINBOT_DATA_ROOT/reference.",
    )
    args = parser.parse_args(argv)
    if args.date and (args.days is not None or args.years is not None):
        parser.error("--days and --years can only be used with --end-date")
    if args.end_date and args.days is None and args.years is None:
        args.years = default_years()
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
            download_single_date(
                args.date,
                output_dir=args.output_dir,
                filter_to_ticker_universe=not args.all_symbols,
                ticker_universe_dir=args.ticker_universe_dir,
            )
        else:
            download_history(
                end_date=args.end_date,
                years=args.years,
                days=args.days,
                output_dir=args.output_dir,
                calls_per_minute=args.calls_per_minute,
                filter_to_ticker_universe=not args.all_symbols,
                ticker_universe_dir=args.ticker_universe_dir,
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
