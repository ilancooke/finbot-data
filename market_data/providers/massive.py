from __future__ import annotations

from datetime import date
import logging
from typing import Any

import pandas as pd

from market_data.http import MASSIVE_BASE_URL, MassiveClient, MassiveHttpError
from market_data.normalize import BAR_COLUMNS

logger = logging.getLogger(__name__)

MassiveApiError = MassiveHttpError

TICKER_DETAIL_COLUMNS = [
    "ticker",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "type",
    "active",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "sic_code",
    "sic_description",
    "description",
    "homepage_url",
    "market_cap",
    "total_employees",
    "list_date",
    "delisted_utc",
    "ticker_root",
    "ticker_suffix",
    "phone_number",
    "share_class_shares_outstanding",
    "weighted_shares_outstanding",
    "round_lot",
]

RELATED_TICKER_COLUMNS = ["ticker", "related_ticker", "result_order"]

RATIO_COLUMNS = [
    "ticker",
    "cik",
    "date",
    "price",
    "average_volume",
    "market_cap",
    "earnings_per_share",
    "price_to_earnings",
    "price_to_book",
    "price_to_sales",
    "price_to_cash_flow",
    "price_to_free_cash_flow",
    "dividend_yield",
    "return_on_assets",
    "return_on_equity",
    "debt_to_equity",
    "current",
    "quick",
    "cash",
    "ev_to_sales",
    "ev_to_ebitda",
    "enterprise_value",
    "free_cash_flow",
]


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


def normalize_ticker_details_response(payload: dict[str, Any], requested_ticker: str) -> pd.DataFrame:
    """Normalize Massive ticker overview response to native company detail fields."""

    result = payload.get("results") or {}
    if not result:
        return pd.DataFrame(columns=TICKER_DETAIL_COLUMNS)

    row = {column: result.get(column) for column in TICKER_DETAIL_COLUMNS}
    row["ticker"] = str(row.get("ticker") or requested_ticker).upper()
    return pd.DataFrame([row], columns=TICKER_DETAIL_COLUMNS)


def download_ticker_details(
    ticker: str,
    api_key: str,
    as_of: date | None = None,
    base_url: str = MASSIVE_BASE_URL,
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Download Massive reference details for one ticker."""

    normalized_ticker = ticker.upper()
    params = {"date": as_of.isoformat()} if as_of is not None else None
    client = client or MassiveClient(api_key=api_key, base_url=base_url)
    payload = client.get_json(f"/v3/reference/tickers/{normalized_ticker}", params=params)
    details = normalize_ticker_details_response(payload, normalized_ticker)
    logger.info("Fetched Massive ticker details ticker=%s rows=%d", normalized_ticker, len(details))
    return details


def normalize_related_tickers_response(payload: dict[str, Any], requested_ticker: str) -> pd.DataFrame:
    """Normalize Massive related ticker response to source/related ticker rows."""

    normalized_ticker = requested_ticker.upper()
    rows = []
    for idx, result in enumerate(payload.get("results") or [], start=1):
        related_ticker = result.get("ticker")
        if not related_ticker:
            continue
        rows.append(
            {
                "ticker": normalized_ticker,
                "related_ticker": str(related_ticker).upper(),
                "result_order": idx,
            }
        )

    if not rows:
        return pd.DataFrame(columns=RELATED_TICKER_COLUMNS)

    return (
        pd.DataFrame(rows, columns=RELATED_TICKER_COLUMNS)
        .drop_duplicates(subset=["ticker", "related_ticker"], keep="first")
        .sort_values(["ticker", "result_order", "related_ticker"])
        .reset_index(drop=True)
    )


def download_related_tickers(
    ticker: str,
    api_key: str,
    base_url: str = MASSIVE_BASE_URL,
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Download Massive related tickers for one ticker."""

    normalized_ticker = ticker.upper()
    client = client or MassiveClient(api_key=api_key, base_url=base_url)
    payload = client.get_json(f"/v1/related-companies/{normalized_ticker}")
    related = normalize_related_tickers_response(payload, normalized_ticker)
    logger.info("Fetched Massive related tickers ticker=%s rows=%d", normalized_ticker, len(related))
    return related


def normalize_ratios_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize Massive financial ratios rows to the durable ratios schema."""

    if not rows:
        return pd.DataFrame(columns=RATIO_COLUMNS)

    frame = pd.json_normalize(rows)
    for column in RATIO_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    return (
        frame[RATIO_COLUMNS]
        .drop_duplicates(subset=["ticker", "date"], keep="last")
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )


def normalize_ratios_response(payload: dict[str, Any]) -> pd.DataFrame:
    """Normalize a Massive financial ratios response."""

    return normalize_ratios_rows(payload.get("results") or [])


def download_ratios(
    api_key: str,
    base_url: str = MASSIVE_BASE_URL,
    client: MassiveClient | None = None,
    limit: int = 50_000,
    calls_per_minute: float = 0,
) -> pd.DataFrame:
    """Download latest Massive financial ratios rows."""

    client = client or MassiveClient(api_key=api_key, base_url=base_url)
    rows = client.get_paginated(
        "/stocks/financials/v1/ratios",
        params={"limit": limit, "sort": "ticker.asc"},
        calls_per_minute=calls_per_minute,
    )
    ratios = normalize_ratios_rows(rows)
    logger.info("Fetched Massive financial ratios rows=%d", len(ratios))
    return ratios
