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
    filter_symbol_ticker_universe,
    read_known_universe_symbols,
    write_ticker_universe,
)

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path("data/reference")
DEFAULT_CALLS_PER_MINUTE = 0
DEFAULT_UNIVERSE = "sp500"
KNOWN_UNIVERSE_FILES = {
    "sp500": "sp500constituents.csv",
}
KNOWN_UNIVERSE_NAMES = {
    "sp500": "S&P 500",
}


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


def _workspace_data_root() -> Path:
    return PROJECT_ROOT.parents[1] / "data"


def _resolve_known_universe_file(universe: str, known_universe_file: str | Path | None) -> Path | None:
    if universe == "all_common_stocks":
        return None

    if known_universe_file is not None:
        return Path(known_universe_file)

    env_value = get_env("FINBOT_KNOWN_UNIVERSE_FILE")
    if env_value:
        return Path(env_value)

    file_name = KNOWN_UNIVERSE_FILES[universe]
    data_root = get_env("FINBOT_DATA_ROOT")
    candidates = []
    if data_root:
        candidates.append(Path(data_root) / file_name)
    candidates.extend(
        [
            Path.cwd() / "data" / file_name,
            _workspace_data_root() / file_name,
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate

    searched = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(
        f"Could not find known universe file for {universe}. "
        f"Set FINBOT_KNOWN_UNIVERSE_FILE or pass --known-universe-file. Searched: {searched}"
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
    parser.add_argument(
        "--universe",
        choices=["sp500", "all_common_stocks"],
        default=os.getenv("FINBOT_TICKER_UNIVERSE", DEFAULT_UNIVERSE),
        help=f"Known universe strategy. Default: {DEFAULT_UNIVERSE}",
    )
    parser.add_argument(
        "--known-universe-file",
        default=None,
        help="CSV file containing a Symbol or Ticker column for the selected known universe.",
    )
    args = parser.parse_args(argv)
    if args.calls_per_minute < 0:
        parser.error("--calls-per-minute must be 0 or greater")
    return args


def download_tickers(
    universe_date: date | None = None,
    output_dir: str | Path | None = None,
    calls_per_minute: float = DEFAULT_CALLS_PER_MINUTE,
    universe: str = DEFAULT_UNIVERSE,
    known_universe_file: str | Path | None = None,
) -> Path:
    client = MassiveClient(api_key=_get_massive_api_key())
    tickers = fetch_ticker_universe(
        client,
        as_of=universe_date,
        active=True,
        calls_per_minute=calls_per_minute,
    )
    common_stocks = filter_common_stocks(tickers)
    resolved_known_universe_file = _resolve_known_universe_file(universe, known_universe_file)
    known_universe_symbols = (
        read_known_universe_symbols(resolved_known_universe_file)
        if resolved_known_universe_file is not None
        else []
    )
    output_tickers = (
        filter_symbol_ticker_universe(common_stocks, known_universe_symbols)
        if known_universe_symbols
        else common_stocks
    )
    missing_known_tickers = sorted(set(known_universe_symbols) - set(output_tickers["ticker"].tolist()))
    output_path = write_ticker_universe(
        output_tickers,
        _resolve_output_dir(output_dir),
        metadata={
            "provider": "massive",
            "dataset": "ticker_universe",
            "mode": "replace",
            "universe_strategy": universe,
            "universe_name": KNOWN_UNIVERSE_NAMES.get(universe, "All active US common stocks"),
            "known_universe_file": str(resolved_known_universe_file) if resolved_known_universe_file else None,
            "known_universe_symbols": len(known_universe_symbols),
            "universe_date": universe_date.isoformat() if universe_date else "latest",
            "input_rows": int(len(tickers)),
            "common_stock_rows": int(len(common_stocks)),
            "output_rows": int(len(output_tickers)),
            "calls_per_minute": calls_per_minute,
            "filter": {
                "type": "CS",
                "active": True,
                "market": "stocks",
                "locale": "us",
                "primary_exchanges": sorted(LISTED_PRIMARY_EXCHANGES),
                "known_universe": universe,
                "missing_known_tickers": missing_known_tickers,
            },
        },
    )
    logger.info(
        "Downloaded Massive common stock universe strategy=%s date=%s input_rows=%d common_stock_rows=%d output_rows=%d calls_per_minute=%.3g output=%s",
        universe,
        universe_date.isoformat() if universe_date else "latest",
        len(tickers),
        len(common_stocks),
        len(output_tickers),
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
            universe=args.universe,
            known_universe_file=args.known_universe_file,
        )
        logger.info("Common-stock universe download succeeded total_time=%.3fs", time.perf_counter() - started)
        return 0
    except Exception:
        logger.exception("Common-stock universe download failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
