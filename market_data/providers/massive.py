from __future__ import annotations

from datetime import date
import logging
from typing import Any

import pandas as pd

from market_data.http import MASSIVE_BASE_URL, MassiveClient, MassiveHttpError
from market_data.normalize import BAR_COLUMNS

logger = logging.getLogger(__name__)

MassiveApiError = MassiveHttpError


def normalize_grouped_daily_response(payload: dict[str, Any], data_date: date) -> pd.DataFrame:
    """Normalize Massive grouped daily bars to the canonical daily bar schema."""

    rows = []
    for result in payload.get("results") or []:
        symbol = result.get("T")
        if not symbol:
            continue
        rows.append(
            {
                "date": pd.to_datetime(result.get("t"), unit="ms").date() if result.get("t") else data_date,
                "symbol": str(symbol).upper(),
                "open": result.get("o"),
                "high": result.get("h"),
                "low": result.get("l"),
                "close": result.get("c"),
                "volume": result.get("v"),
            }
        )

    if not rows:
        return pd.DataFrame(columns=BAR_COLUMNS)

    return (
        pd.DataFrame(rows, columns=BAR_COLUMNS)
        .drop_duplicates(subset=["symbol", "date"], keep="last")
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )


def download_grouped_daily_bars(
    data_date: date,
    api_key: str,
    base_url: str = MASSIVE_BASE_URL,
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Download adjusted grouped daily OHLCV bars for all US stocks."""

    client = client or MassiveClient(api_key=api_key, base_url=base_url)
    payload = client.get_json(
        f"/v2/aggs/grouped/locale/us/market/stocks/{data_date.isoformat()}",
        params={"adjusted": "true"},
    )
    status = payload.get("status")
    if status not in {"OK", "DELAYED"}:
        raise RuntimeError(f"Massive grouped daily request failed with status={status!r}")

    bars = normalize_grouped_daily_response(payload, data_date)
    logger.info("Fetched Massive grouped daily bars date=%s rows=%d", data_date.isoformat(), len(bars))
    return bars
