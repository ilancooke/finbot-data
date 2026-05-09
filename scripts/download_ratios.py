"""Download latest Massive financial ratios for the filtered ticker universe."""

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

from market_data.datasets.ratios import (
    DEFAULT_CALLS_PER_MINUTE,
    DEFAULT_LIMIT,
    DEFAULT_RATIOS_DIR,
    DEFAULT_REFERENCE_DIR,
    default_calls_per_minute,
    download_ratios_snapshot,
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
    parser = argparse.ArgumentParser(description="Download latest Massive financial ratios for the filtered ticker universe")
    parser.add_argument(
        "--input-dir",
        default=None,
        help=f"Directory containing tickers.parquet. Default: {DEFAULT_REFERENCE_DIR}",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=f"Output directory for ratios.parquet. Default: {DEFAULT_RATIOS_DIR}",
    )
    parser.add_argument(
        "--calls-per-minute",
        type=float,
        default=default_calls_per_minute(),
        help=f"Massive REST pacing between paginated requests. Use 0 to disable. Default: {DEFAULT_CALLS_PER_MINUTE}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Massive result limit per request. Maximum documented value: {DEFAULT_LIMIT}",
    )
    args = parser.parse_args(argv)
    if args.calls_per_minute < 0:
        parser.error("--calls-per-minute must be 0 or greater")
    if args.limit <= 0:
        parser.error("--limit must be greater than 0")
    if args.limit > DEFAULT_LIMIT:
        parser.error(f"--limit must be {DEFAULT_LIMIT} or less")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        download_ratios_snapshot(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            calls_per_minute=args.calls_per_minute,
            limit=args.limit,
        )
        logger.info("Ratios download succeeded total_time=%.3fs", time.perf_counter() - started)
        return 0
    except Exception:
        logger.exception("Ratios download failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
