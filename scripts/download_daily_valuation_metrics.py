"""Download/export Sharadar DAILY and write the filtered daily valuation metrics dataset."""

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

from market_data.datasets.daily_valuation_metrics import (
    DEFAULT_CSV_CHUNK_ROWS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RAW_EXPORT_DIR,
    DEFAULT_REFERENCE_DIR,
    build_daily_valuation_metrics_from_files,
    request_bulk_daily_valuation_metric_files,
)

logger = logging.getLogger(__name__)


def configure_logging(level: int = logging.INFO) -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download/build daily valuation metrics")
    parser.add_argument(
        "--input-file",
        action="append",
        default=None,
        help="Existing CSV, zipped CSV, or parquet export file. Repeat for multiple files. If omitted, bulk files are downloaded.",
    )
    parser.add_argument(
        "--bulk-download",
        action="store_true",
        help="Request a Nasdaq Data Link DAILY table export and download it before converting. This is the default when no --input-file is supplied.",
    )
    parser.add_argument("--reference-dir", default=None, help=f"Directory containing tickers.parquet. Default: {DEFAULT_REFERENCE_DIR}")
    parser.add_argument("--raw-export-dir", default=None, help=f"Directory for downloaded DAILY export files. Default: {DEFAULT_RAW_EXPORT_DIR}")
    parser.add_argument("--output-dir", default=None, help=f"Output directory for daily_valuation_metrics.parquet. Default: {DEFAULT_OUTPUT_DIR}")
    parser.add_argument("--chunk-rows", type=int, default=DEFAULT_CSV_CHUNK_ROWS, help=f"Rows per conversion chunk. Default: {DEFAULT_CSV_CHUNK_ROWS}")
    parser.add_argument("--poll-seconds", type=float, default=60.0, help="Seconds to wait between export status checks. Default: 60.")
    parser.add_argument("--max-polls", type=int, default=30, help="Maximum export status checks after the first request. Default: 30.")
    args = parser.parse_args(argv)
    if args.chunk_rows <= 0:
        parser.error("--chunk-rows must be greater than 0")
    if args.poll_seconds < 0:
        parser.error("--poll-seconds must be 0 or greater")
    if args.max_polls < 0:
        parser.error("--max-polls must be 0 or greater")
    if not args.input_file:
        args.bulk_download = True
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        input_files = [Path(path) for path in args.input_file or []]
        if args.bulk_download:
            input_files.extend(
                request_bulk_daily_valuation_metric_files(
                    raw_export_dir=args.raw_export_dir,
                    poll_seconds=args.poll_seconds,
                    max_polls=args.max_polls,
                )
            )
        output_path = build_daily_valuation_metrics_from_files(
            input_files,
            reference_dir=args.reference_dir,
            output_dir=args.output_dir,
            chunk_rows=args.chunk_rows,
        )
        logger.info("Built daily valuation metrics output=%s total_time=%.3fs", output_path, time.perf_counter() - started)
        return 0
    except Exception:
        logger.exception("Building daily valuation metrics failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
