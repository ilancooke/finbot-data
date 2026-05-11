from __future__ import annotations

import csv
from io import StringIO
import logging
from pathlib import Path

import requests

from market_data.universe import read_known_universe_symbols

logger = logging.getLogger(__name__)

SP500_CSV_URL = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"


def load_sp500_symbols(
    symbols_file: str | Path | None = None,
    symbols_url: str = SP500_CSV_URL,
) -> list[str]:
    """Load the current S&P 500 symbol universe from a local file or CSV URL."""

    if symbols_file:
        path = Path(symbols_file)
        if path.exists():
            symbols = read_known_universe_symbols(path)
            logger.info("Loaded %d symbols from %s", len(symbols), path)
            return symbols

    response = requests.get(symbols_url, timeout=30)
    response.raise_for_status()

    rows = csv.DictReader(StringIO(response.text))
    symbols = sorted(
        {
            (row.get("Symbol") or row.get("symbol") or "").strip().upper()
            for row in rows
            if row.get("Symbol") or row.get("symbol")
        }
    )
    if not symbols:
        raise RuntimeError("No S&P 500 symbols parsed from source")

    logger.info("Loaded %d S&P 500 symbols", len(symbols))
    return symbols
