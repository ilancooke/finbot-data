"""Download historical Massive financial statements for the filtered ticker universe."""

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

from market_data.datasets.financials import (
    DEFAULT_CALLS_PER_MINUTE,
    DEFAULT_FINANCIALS_DIR,
    DEFAULT_LIMIT,
    DEFAULT_REFERENCE_DIR,
    DEFAULT_TICKER_BATCH_SIZE,
    STATEMENTS,
    default_calls_per_minute,
    default_years,
    download_financials_history,
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
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM-DD date, got {value!r}") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download historical Massive financial statements for the filtered ticker universe")
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=date.today(),
        help="Inclusive period_end upper bound. Default: today",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=default_years(),
        help="History window in years. Default: FINBOT_FINANCIALS_HISTORY_YEARS, FINBOT_HISTORY_YEARS, or 2",
    )
    parser.add_argument(
        "--statement",
        dest="statements",
        action="append",
        choices=STATEMENTS,
        help="Statement to download. Repeat for multiple statements. Default: all",
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help=f"Directory containing tickers.parquet. Default: {DEFAULT_REFERENCE_DIR}",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=f"Output directory for financial statement parquet files. Default: {DEFAULT_FINANCIALS_DIR}",
    )
    parser.add_argument(
        "--calls-per-minute",
        type=float,
        default=default_calls_per_minute(),
        help=f"Massive REST pacing between pages and ticker batches. Use 0 to disable. Default: {DEFAULT_CALLS_PER_MINUTE}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Massive result limit per request. Maximum documented value: {DEFAULT_LIMIT}",
    )
    parser.add_argument(
        "--ticker-batch-size",
        type=int,
        default=DEFAULT_TICKER_BATCH_SIZE,
        help=f"Ticker count per tickers.any_of request. Default: {DEFAULT_TICKER_BATCH_SIZE}",
    )
    parser.add_argument(
        "--ticker-limit",
        type=int,
        default=None,
        help="Optional ticker count for smoke tests. Default: all tickers in the universe",
    )
    args = parser.parse_args(argv)
    if args.years <= 0:
        parser.error("--years must be greater than 0")
    if args.calls_per_minute < 0:
        parser.error("--calls-per-minute must be 0 or greater")
    if args.limit <= 0:
        parser.error("--limit must be greater than 0")
    if args.limit > DEFAULT_LIMIT:
        parser.error(f"--limit must be {DEFAULT_LIMIT} or less")
    if args.ticker_batch_size <= 0:
        parser.error("--ticker-batch-size must be greater than 0")
    if args.ticker_limit is not None and args.ticker_limit <= 0:
        parser.error("--ticker-limit must be greater than 0")
    if args.end_date > date.today():
        parser.error("--end-date cannot be in the future")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        download_financials_history(
            end_date=args.end_date,
            years=args.years,
            statements=args.statements or STATEMENTS,
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            calls_per_minute=args.calls_per_minute,
            limit=args.limit,
            ticker_batch_size=args.ticker_batch_size,
            ticker_limit=args.ticker_limit,
        )
        logger.info("Financials download succeeded total_time=%.3fs", time.perf_counter() - started)
        return 0
    except Exception:
        logger.exception("Financials download failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
