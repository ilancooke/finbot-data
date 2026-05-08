"""Download the filtered Massive common-stock universe."""

from __future__ import annotations

import argparse
from datetime import date
import logging
import os
from pathlib import Path
import sys
import time
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_data.config import get_env, resolve_finbot_data_path
from market_data.http import MassiveClient
from market_data.universe import (
    LISTED_PRIMARY_EXCHANGES,
    fetch_ticker_universe,
    filter_common_stocks,
    write_ticker_universe,
)

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path("data/reference")
DEFAULT_CALLS_PER_MINUTE = 5


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging once for CLI execution."""

    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _resolve_output_dir(output_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        output_dir,
        env_key="FINBOT_REFERENCE_DIR",
        default_path=DEFAULT_OUTPUT_DIR,
        data_root_subpath="reference",
    )


def _get_massive_api_key() -> str:
    api_key = get_env("MASSIVE_API_KEY") or get_env("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("Missing MASSIVE_API_KEY or POLYGON_API_KEY for common-stock universe download")
    return api_key


def _default_calls_per_minute() -> float:
    return float(os.getenv("MASSIVE_CALLS_PER_MINUTE", str(DEFAULT_CALLS_PER_MINUTE)))


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date '{value}'. Expected YYYY-MM-DD") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download filtered Massive common-stock universe")
    parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="Optional point-in-time universe date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--calls-per-minute",
        type=float,
        default=_default_calls_per_minute(),
        help=f"Massive REST pacing between paginated requests. Use 0 to disable. Default: {DEFAULT_CALLS_PER_MINUTE}",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=f"Output directory for tickers.parquet. Default: {DEFAULT_OUTPUT_DIR}",
    )
    args = parser.parse_args(argv)
    if args.calls_per_minute < 0:
        parser.error("--calls-per-minute must be 0 or greater")
    return args


def download_tickers(
    universe_date: date | None = None,
    output_dir: str | Path | None = None,
    calls_per_minute: float = DEFAULT_CALLS_PER_MINUTE,
) -> Path:
    client = MassiveClient(api_key=_get_massive_api_key())
    tickers = fetch_ticker_universe(
        client,
        as_of=universe_date,
        active=True,
        calls_per_minute=calls_per_minute,
    )
    common_stocks = filter_common_stocks(tickers)
    output_path = write_ticker_universe(
        common_stocks,
        _resolve_output_dir(output_dir),
        metadata={
            "provider": "massive",
            "dataset": "ticker_universe",
            "mode": "replace",
            "universe_date": universe_date.isoformat() if universe_date else "latest",
            "input_rows": int(len(tickers)),
            "output_rows": int(len(common_stocks)),
            "calls_per_minute": calls_per_minute,
            "filter": {
                "type": "CS",
                "active": True,
                "market": "stocks",
                "locale": "us",
                "primary_exchanges": sorted(LISTED_PRIMARY_EXCHANGES),
            },
        },
    )
    logger.info(
        "Downloaded Massive common stock universe date=%s input_rows=%d output_rows=%d calls_per_minute=%.3g output=%s",
        universe_date.isoformat() if universe_date else "latest",
        len(tickers),
        len(common_stocks),
        calls_per_minute,
        output_path,
    )
    return output_path


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        download_tickers(
            universe_date=args.date,
            output_dir=args.output_dir,
            calls_per_minute=args.calls_per_minute,
        )
        logger.info("Common-stock universe download succeeded total_time=%.3fs", time.perf_counter() - started)
        return 0
    except Exception:
        logger.exception("Common-stock universe download failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
