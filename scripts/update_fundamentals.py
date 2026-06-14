"""Fetch SF1 rows updated since a date and merge them into fundamentals."""

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

from market_data.datasets.fundamentals import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REFERENCE_DIR,
    update_sf1,
)

logger = logging.getLogger(__name__)


def configure_logging(level: int = logging.INFO) -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date '{value}'. Expected YYYY-MM-DD") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update SF1 fundamentals")
    parser.add_argument("--lastupdated-gte", type=_parse_date, required=True, help="Inclusive lastupdated lower bound in YYYY-MM-DD format.")
    parser.add_argument("--reference-dir", default=None, help=f"Directory containing tickers.parquet. Default: {DEFAULT_REFERENCE_DIR}")
    parser.add_argument("--output-dir", default=None, help=f"Output directory for sf1.parquet. Default: {DEFAULT_OUTPUT_DIR}")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        output_path = update_sf1(
            lastupdated_gte=args.lastupdated_gte,
            reference_dir=args.reference_dir,
            output_dir=args.output_dir,
        )
        logger.info("Updated SF1 fundamentals output=%s total_time=%.3fs", output_path, time.perf_counter() - started)
        return 0
    except Exception:
        logger.exception("Updating SF1 fundamentals failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
