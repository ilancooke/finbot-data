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

BALANCE_SHEET_COLUMNS = [
    "ticker",
    "cik",
    "period_end",
    "filing_date",
    "fiscal_year",
    "fiscal_quarter",
    "timeframe",
    "tickers",
    "accounts_payable",
    "accrued_and_other_current_liabilities",
    "accumulated_other_comprehensive_income",
    "additional_paid_in_capital",
    "cash_and_equivalents",
    "commitments_and_contingencies",
    "common_stock",
    "debt_current",
    "deferred_revenue_current",
    "goodwill",
    "intangible_assets_net",
    "inventories",
    "long_term_debt_and_capital_lease_obligations",
    "noncontrolling_interest",
    "other_assets",
    "other_current_assets",
    "other_equity",
    "other_noncurrent_liabilities",
    "preferred_stock",
    "property_plant_equipment_net",
    "receivables",
    "retained_earnings_deficit",
    "short_term_investments",
    "total_assets",
    "total_current_assets",
    "total_current_liabilities",
    "total_equity",
    "total_equity_attributable_to_parent",
    "total_liabilities",
    "total_liabilities_and_equity",
    "treasury_stock",
]

CASH_FLOW_STATEMENT_COLUMNS = [
    "ticker",
    "cik",
    "period_end",
    "filing_date",
    "fiscal_year",
    "fiscal_quarter",
    "timeframe",
    "tickers",
    "cash_from_operating_activities_continuing_operations",
    "change_in_cash_and_equivalents",
    "change_in_other_operating_assets_and_liabilities_net",
    "depreciation_depletion_and_amortization",
    "dividends",
    "effect_of_currency_exchange_rate",
    "income_loss_from_discontinued_operations",
    "long_term_debt_issuances_repayments",
    "net_cash_from_financing_activities",
    "net_cash_from_financing_activities_continuing_operations",
    "net_cash_from_financing_activities_discontinued_operations",
    "net_cash_from_investing_activities",
    "net_cash_from_investing_activities_continuing_operations",
    "net_cash_from_investing_activities_discontinued_operations",
    "net_cash_from_operating_activities",
    "net_cash_from_operating_activities_discontinued_operations",
    "net_income",
    "noncontrolling_interests",
    "other_cash_adjustments",
    "other_financing_activities",
    "other_investing_activities",
    "other_operating_activities",
    "purchase_of_property_plant_and_equipment",
    "sale_of_property_plant_and_equipment",
    "short_term_debt_issuances_repayments",
]

INCOME_STATEMENT_COLUMNS = [
    "ticker",
    "cik",
    "period_end",
    "filing_date",
    "fiscal_year",
    "fiscal_quarter",
    "timeframe",
    "tickers",
    "basic_earnings_per_share",
    "basic_shares_outstanding",
    "consolidated_net_income_loss",
    "cost_of_revenue",
    "depreciation_depletion_amortization",
    "diluted_earnings_per_share",
    "diluted_shares_outstanding",
    "discontinued_operations",
    "ebitda",
    "equity_in_affiliates",
    "extraordinary_items",
    "gross_profit",
    "income_before_income_taxes",
    "income_taxes",
    "interest_expense",
    "interest_income",
    "net_income_loss_attributable_common_shareholders",
    "noncontrolling_interest",
    "operating_income",
    "other_income_expense",
    "other_operating_expenses",
    "preferred_stock_dividends_declared",
    "research_development",
    "revenue",
    "selling_general_administrative",
    "total_operating_expenses",
    "total_other_income_expense",
]

FINANCIAL_STATEMENT_COLUMNS = {
    "balance_sheets": BALANCE_SHEET_COLUMNS,
    "cash_flow_statements": CASH_FLOW_STATEMENT_COLUMNS,
    "income_statements": INCOME_STATEMENT_COLUMNS,
}

FINANCIAL_STATEMENT_ENDPOINTS = {
    "balance_sheets": "/stocks/financials/v1/balance-sheets",
    "cash_flow_statements": "/stocks/financials/v1/cash-flow-statements",
    "income_statements": "/stocks/financials/v1/income-statements",
}


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


def _normalize_result_tickers(value: Any) -> list[str]:
    if isinstance(value, list):
        return sorted({str(ticker).upper() for ticker in value if ticker})
    if value:
        return [str(value).upper()]
    return []


def normalize_financial_statement_rows(
    rows: list[dict[str, Any]],
    statement: str,
    ticker_universe: set[str] | None = None,
) -> pd.DataFrame:
    """Normalize Massive financial statement rows to a durable, nullable schema."""

    columns = FINANCIAL_STATEMENT_COLUMNS[statement]
    if not rows:
        return pd.DataFrame(columns=columns)

    target_tickers = {ticker.upper() for ticker in ticker_universe} if ticker_universe is not None else None
    expanded_rows = []
    for row in rows:
        provider_tickers = _normalize_result_tickers(row.get("tickers"))
        matched_tickers = sorted(set(provider_tickers) & target_tickers) if target_tickers is not None else provider_tickers
        if not matched_tickers and target_tickers is None:
            matched_tickers = [None]

        for ticker in matched_tickers:
            expanded = {column: row.get(column) for column in columns}
            expanded["ticker"] = ticker
            expanded["tickers"] = provider_tickers
            expanded_rows.append(expanded)

    if not expanded_rows:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(expanded_rows)
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    frame["ticker"] = frame["ticker"].astype("string").str.upper()
    frame["period_end"] = pd.to_datetime(frame["period_end"], errors="coerce").dt.date
    frame["filing_date"] = pd.to_datetime(frame["filing_date"], errors="coerce").dt.date
    return (
        frame[columns]
        .drop_duplicates(subset=["ticker", "cik", "period_end", "timeframe"], keep="last")
        .sort_values(["ticker", "period_end", "timeframe"], na_position="last")
        .reset_index(drop=True)
    )


def normalize_financial_statement_response(
    payload: dict[str, Any],
    statement: str,
    ticker_universe: set[str] | None = None,
) -> pd.DataFrame:
    """Normalize a Massive financial statement response."""

    return normalize_financial_statement_rows(payload.get("results") or [], statement, ticker_universe=ticker_universe)


def download_financial_statement_rows(
    statement: str,
    api_key: str,
    start_date: date,
    end_date: date,
    tickers: list[str],
    base_url: str = MASSIVE_BASE_URL,
    client: MassiveClient | None = None,
    limit: int = 50_000,
    calls_per_minute: float = 0,
) -> list[dict[str, Any]]:
    """Download raw Massive financial statement rows for a ticker batch."""

    client = client or MassiveClient(api_key=api_key, base_url=base_url)
    endpoint = FINANCIAL_STATEMENT_ENDPOINTS[statement]
    params = {
        "period_end.gte": start_date.isoformat(),
        "period_end.lte": end_date.isoformat(),
        "limit": limit,
        "sort": "period_end.asc",
    }
    if tickers:
        params["tickers.any_of"] = ",".join(sorted({ticker.upper() for ticker in tickers}))

    rows = client.get_paginated(endpoint, params=params, calls_per_minute=calls_per_minute)
    logger.info("Fetched Massive %s rows=%d", statement, len(rows))
    return rows
