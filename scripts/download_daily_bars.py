"""Pilot, backfill, or incrementally update Alpaca daily bars."""
from __future__ import annotations
import argparse
from datetime import date
import json
import logging
from pathlib import Path
import sys
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0, str(PROJECT_ROOT))
from market_data.datasets.daily_bars import DEFAULT_SYMBOL_BATCH_SIZE, DEFAULT_UPDATE_OVERLAP_DAYS, run_daily_bars

logger = logging.getLogger(__name__)
def _date(value: str) -> date:
    try: return date.fromisoformat(value)
    except ValueError as exc: raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc

def _args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Alpaca raw and split-adjusted daily bars")
    parser.add_argument("mode", choices=("pilot", "backfill", "update"))
    parser.add_argument("--snapshot-date", type=_date); parser.add_argument("--start-date", type=_date); parser.add_argument("--end-date", type=_date)
    parser.add_argument("--output-dir"); parser.add_argument("--reference-dir"); parser.add_argument("--raw-dir")
    parser.add_argument("--symbol-batch-size", type=int, default=DEFAULT_SYMBOL_BATCH_SIZE)
    parser.add_argument("--update-overlap-days", type=int, default=DEFAULT_UPDATE_OVERLAP_DAYS)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)

def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    args = _args(argv)
    try:
        outputs = run_daily_bars(mode=args.mode, snapshot_date=args.snapshot_date, start_date=args.start_date, end_date=args.end_date,
            output_dir=args.output_dir, reference_dir=args.reference_dir, raw_dir=args.raw_dir,
            symbol_batch_size=args.symbol_batch_size, update_overlap_days=args.update_overlap_days, force=args.force)
        metadata = json.loads(outputs["metadata"].read_text())
        logger.info("Daily bars completed mode=%s rows=%s symbols=%s quarantine=%s requests=%s elapsed_seconds=%s peak_memory_mb=%s pilot_passed=%s",
            args.mode, metadata["row_count"], metadata["symbol_count"], outputs["quarantine"].name,
            metadata["request_count"], metadata["elapsed_seconds"], metadata["peak_memory_mb"], metadata.get("pilot_passed"))
        return 0
    except Exception:
        logger.exception("Daily bars job failed mode=%s", args.mode); return 1

if __name__ == "__main__": raise SystemExit(main())
