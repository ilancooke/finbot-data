from __future__ import annotations

from datetime import datetime
import logging
from typing import Sequence

import pandas as pd

from market_data.normalize import BAR_COLUMNS, normalize_bars_frame

logger = logging.getLogger(__name__)


def download_adjusted_daily_bars(
    symbols: Sequence[str],
    start_dt: datetime,
    end_dt: datetime,
    api_key: str,
    api_secret: str,
) -> pd.DataFrame:
    """Download adjusted daily OHLCV bars from Alpaca's IEX feed."""

    from alpaca.data.enums import Adjustment, DataFeed
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    client = StockHistoricalDataClient(api_key, api_secret)
    daily_frames: list[pd.DataFrame] = []

    for idx, symbol in enumerate(symbols, start=1):
        request = StockBarsRequest(
            symbol_or_symbols=symbol,
            start=start_dt,
            end=end_dt,
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            adjustment=Adjustment.ALL,
            feed=DataFeed.IEX,
            limit=10_000,
        )
        bars = client.get_stock_bars(request).df
        normalized = normalize_bars_frame(bars)
        if not normalized.empty:
            daily_frames.append(normalized)
        logger.info("Fetched bars symbol=%s (%d/%d) rows=%d", symbol, idx, len(symbols), len(normalized))

    if not daily_frames:
        return pd.DataFrame(columns=BAR_COLUMNS)

    return (
        pd.concat(daily_frames, ignore_index=True)
        .drop_duplicates(subset=["symbol", "date"])
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )
