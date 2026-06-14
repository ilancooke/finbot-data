"""Download the full ticker universe and write the filtered Finbot universe."""

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

from market_data.datasets.ticker_universe import DEFAULT_REFERENCE_DIR, download_and_write_ticker_universe

logger = logging.getLogger(__name__)


def configure_logging(level: int = logging.INFO) -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download ticker universe datasets")
    parser.add_argument(
        "--output-dir",
        default=None,
        help=f"Output directory for filtered tickers.parquet. Default: {DEFAULT_REFERENCE_DIR}",
    )
    parser.add_argument(
        "--raw-output-dir",
        default=None,
        help="Output directory for raw Sharadar ticker JSONL. Default: FINBOT_DATA_ROOT/raw/nasdaq_data_link/sharadar/tickers or data/raw/nasdaq_data_link/sharadar/tickers",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        paths = download_and_write_ticker_universe(output_dir=args.output_dir, raw_output_dir=args.raw_output_dir)
        logger.info("Ticker universe download succeeded paths=%s total_time=%.3fs", paths, time.perf_counter() - started)
        return 0
    except Exception:
        logger.exception("Ticker universe download failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
