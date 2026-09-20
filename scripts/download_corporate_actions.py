"""Download and normalize Alpaca US corporate actions from 2016 onward."""

from __future__ import annotations

import argparse
from datetime import date
import json
import logging
from pathlib import Path
import sys
import time
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_data.datasets.corporate_actions import (
    DEFAULT_OVERLAP_DAYS,
    DEFAULT_SYMBOL_BATCH_SIZE,
    download_and_write_corporate_actions,
)

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Alpaca corporate actions")
    parser.add_argument("--snapshot-date", type=_parse_date, default=None)
    parser.add_argument("--reference-dir", default=None)
    parser.add_argument("--raw-dir", default=None)
    parser.add_argument("--overlap-days", type=int, default=DEFAULT_OVERLAP_DAYS)
    parser.add_argument("--symbol-batch-size", type=int, default=DEFAULT_SYMBOL_BATCH_SIZE)
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        outputs = download_and_write_corporate_actions(
            snapshot_date=args.snapshot_date,
            reference_dir=args.reference_dir,
            raw_dir=args.raw_dir,
            overlap_days=args.overlap_days,
            symbol_batch_size=args.symbol_batch_size,
            full_refresh=args.full_refresh,
        )
        metadata = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
        logger.info(
            "Corporate actions completed rows=%s matched=%s unmatched=%s split_refresh=%s elapsed_seconds=%.1f",
            metadata["row_count"],
            metadata["matched_event_count"],
            metadata["unmatched_event_count"],
            metadata["split_history_refresh_event_count"],
            time.perf_counter() - started,
        )
        return 0
    except Exception:
        logger.exception("Corporate actions download failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
