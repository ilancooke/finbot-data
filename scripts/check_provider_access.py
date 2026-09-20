"""Perform a small, read-only credential and provider-access check."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_data.config import get_alpaca_credentials, get_massive_api_key
from market_data.providers.alpaca import AlpacaClient
from market_data.providers.massive import MassiveClient

logger = logging.getLogger(__name__)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check read-only access to a configured data provider")
    parser.add_argument("provider", choices=("massive", "alpaca"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    args = _parse_args(argv)
    try:
        if args.provider == "massive":
            client = MassiveClient(get_massive_api_key())
            payload = client.get_json(
                "/v3/reference/tickers",
                params={"market": "stocks", "active": "true", "limit": 1},
            )
        else:
            api_key, api_secret = get_alpaca_credentials()
            client = AlpacaClient(api_key, api_secret)
            payload = client.get_json(
                "/v2/stocks/AAPL/bars",
                params={
                    "timeframe": "1Day",
                    "start": "2016-01-04",
                    "end": "2016-01-06",
                    "limit": 1,
                    "feed": "sip",
                },
            )
        result_type = "object" if isinstance(payload, dict) else "array"
        logger.info("Provider access succeeded provider=%s response_type=%s", args.provider, result_type)
        return 0
    except Exception:
        logger.exception("Provider access failed provider=%s", args.provider)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
