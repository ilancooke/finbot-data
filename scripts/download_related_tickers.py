"""Download Massive related tickers for the filtered ticker universe."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
import time
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_data.datasets.related_tickers import (
    DEFAULT_CALLS_PER_MINUTE,
    DEFAULT_REFERENCE_DIR,
    default_calls_per_minute,
    download_related_tickers_snapshot,
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


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Massive related tickers for the filtered ticker universe")
    parser.add_argument(
        "--input-dir",
        default=None,
        help=f"Directory containing tickers.parquet. Default: {DEFAULT_REFERENCE_DIR}",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for related_tickers.parquet. Default: same as --input-dir",
    )
    parser.add_argument(
        "--calls-per-minute",
        type=float,
        default=default_calls_per_minute(),
        help=f"Massive REST pacing between ticker requests. Use 0 to disable. Default: {DEFAULT_CALLS_PER_MINUTE}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of source tickers to fetch in this run, useful for smoke tests",
    )
    args = parser.parse_args(argv)
    if args.calls_per_minute < 0:
        parser.error("--calls-per-minute must be 0 or greater")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be greater than 0")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        download_related_tickers_snapshot(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            calls_per_minute=args.calls_per_minute,
            limit=args.limit,
        )
        logger.info("Related tickers download succeeded total_time=%.3fs", time.perf_counter() - started)
        return 0
    except Exception:
        logger.exception("Related tickers download failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
