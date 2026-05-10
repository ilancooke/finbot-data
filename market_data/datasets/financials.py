from __future__ import annotations

from datetime import date, datetime, timedelta
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Any, Callable

import pandas as pd

from market_data.config import get_env, resolve_finbot_data_path
from market_data.http import MassiveClient
from market_data.providers.massive import (
    FINANCIAL_STATEMENT_COLUMNS,
    download_financial_statement_rows,
    normalize_financial_statement_rows,
)
from market_data.universe import read_ticker_universe

logger = logging.getLogger(__name__)

DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_FINANCIALS_DIR = Path("data/financials")
DEFAULT_HISTORY_YEARS = 2
DEFAULT_CALLS_PER_MINUTE = 0
DEFAULT_LIMIT = 50_000
DEFAULT_TICKER_BATCH_SIZE = 100
TRUTHY_VALUES = {"1", "true", "yes"}

STATEMENT_FILE_STEMS = {
    "balance_sheets": "balance_sheets",
    "cash_flow_statements": "cash_flow_statements",
    "income_statements": "income_statements",
}
STATEMENTS = tuple(STATEMENT_FILE_STEMS)

FinancialStatementDownloader = Callable[[str, str, date, date, list[str], int, float], list[dict[str, Any]]]


def network_disabled() -> bool:
    return os.getenv("FINBOT_INGEST_DISABLE_NETWORK", "").lower() in TRUTHY_VALUES


def resolve_reference_dir(reference_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        reference_dir,
        env_key="FINBOT_REFERENCE_DIR",
        default_path=DEFAULT_REFERENCE_DIR,
        data_root_subpath="reference",
    )


def resolve_financials_dir(financials_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        financials_dir,
        env_key="FINBOT_FINANCIALS_DIR",
        default_path=DEFAULT_FINANCIALS_DIR,
        data_root_subpath="financials",
    )


def default_years() -> int:
    return int(os.getenv("FINBOT_FINANCIALS_HISTORY_YEARS", os.getenv("FINBOT_HISTORY_YEARS", str(DEFAULT_HISTORY_YEARS))))


def default_calls_per_minute() -> float:
    return float(os.getenv("MASSIVE_FINANCIALS_CALLS_PER_MINUTE", str(DEFAULT_CALLS_PER_MINUTE)))


def get_massive_api_key() -> str:
    api_key = get_env("MASSIVE_API_KEY") or get_env("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("Missing MASSIVE_API_KEY or POLYGON_API_KEY for financials download")
    return api_key


def window_start_date(end_date: date, years: int) -> date:
    return (pd.Timestamp(end_date) - pd.DateOffset(years=years)).date() + timedelta(days=1)


def _ticker_list(tickers: pd.DataFrame) -> list[str]:
    if "ticker" not in tickers.columns:
        raise ValueError("Ticker universe must include a ticker column")
    return sorted(tickers["ticker"].dropna().astype(str).str.upper().unique().tolist())


def _ticker_batches(tickers: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")
    return [tickers[idx : idx + batch_size] for idx in range(0, len(tickers), batch_size)]


def _statement_paths(output_dir: Path, statement: str) -> tuple[Path, Path]:
    stem = STATEMENT_FILE_STEMS[statement]
    return output_dir / f"{stem}.parquet", output_dir / f"{stem}.metadata.json"


def normalize_financial_statement_frame(statement: str, financials: pd.DataFrame) -> pd.DataFrame:
    """Normalize financial statement rows to the durable statement schema."""

    columns = FINANCIAL_STATEMENT_COLUMNS[statement]
    if financials.empty:
        return pd.DataFrame(columns=columns)

    frame = financials.copy()
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


def read_financial_statement(output_dir: str | Path, statement: str) -> pd.DataFrame:
    """Read one financial statement parquet from output_dir."""

    parquet_path, _ = _statement_paths(Path(output_dir), statement)
    return normalize_financial_statement_frame(statement, pd.read_parquet(parquet_path))


def write_financial_statement_snapshot(
    statement: str,
    financials: pd.DataFrame,
    output_dir: str | Path,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Atomically write a financial statement parquet and sidecar metadata."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path, metadata_path = _statement_paths(output_dir, statement)

    normalized = normalize_financial_statement_frame(statement, financials)
    if "period_end" in normalized.columns and not normalized.empty:
        period_min = pd.to_datetime(normalized["period_end"], errors="coerce").min().date().isoformat()
        period_max = pd.to_datetime(normalized["period_end"], errors="coerce").max().date().isoformat()
    else:
        period_min = None
        period_max = None

    collected_at_utc = datetime.utcnow()
    snapshot_metadata = {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "rows": int(len(normalized)),
        "tickers": int(normalized["ticker"].nunique()) if "ticker" in normalized.columns else 0,
        "data_min_date": period_min,
        "data_max_date": period_max,
        "data_min_period_end": period_min,
        "data_max_period_end": period_max,
        "parquet_file": output_path.name,
        **(metadata or {}),
    }

    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".parquet", delete=False) as temp_parquet:
        temp_parquet_path = Path(temp_parquet.name)
    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".json", mode="w", encoding="utf-8", delete=False) as temp_metadata:
        temp_metadata_path = Path(temp_metadata.name)
        json.dump(snapshot_metadata, temp_metadata, indent=2)

    try:
        normalized.to_parquet(temp_parquet_path, index=False)
        temp_parquet_path.replace(output_path)
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_parquet_path.unlink(missing_ok=True)
        temp_metadata_path.unlink(missing_ok=True)

    return output_path


def download_financials_history(
    end_date: date,
    years: int,
    statements: list[str] | tuple[str, ...] = STATEMENTS,
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    calls_per_minute: float = DEFAULT_CALLS_PER_MINUTE,
    limit: int = DEFAULT_LIMIT,
    ticker_batch_size: int = DEFAULT_TICKER_BATCH_SIZE,
    ticker_limit: int | None = None,
    downloader: FinancialStatementDownloader | None = None,
) -> dict[str, Path]:
    """Download historical Massive financial statements for the ticker universe."""

    requested_statements = list(statements)
    unknown_statements = sorted(set(requested_statements) - set(STATEMENTS))
    if unknown_statements:
        raise ValueError(f"Unknown financial statements: {', '.join(unknown_statements)}")

    start_date = window_start_date(end_date, years)
    resolved_input_dir = resolve_reference_dir(input_dir)
    resolved_output_dir = resolve_financials_dir(output_dir)
    universe = read_ticker_universe(resolved_input_dir)
    target_tickers = _ticker_list(universe)
    requested_tickers = target_tickers[:ticker_limit] if ticker_limit is not None else target_tickers
    batches = _ticker_batches(requested_tickers, ticker_batch_size) if requested_tickers else []
    ticker_set = set(requested_tickers)
    fetch = downloader or download_financial_statement_rows
    output_paths: dict[str, Path] = {}

    if network_disabled():
        logger.warning("Network ingest disabled via FINBOT_INGEST_DISABLE_NETWORK; writing empty financials snapshots")

    api_key = get_massive_api_key() if requested_tickers and not network_disabled() else ""

    for statement in requested_statements:
        raw_rows: list[dict[str, Any]] = []
        if api_key:
            for idx, batch in enumerate(batches, start=1):
                if downloader is not None:
                    rows = fetch(statement, api_key, start_date, end_date, batch, limit, calls_per_minute)
                else:
                    rows = download_financial_statement_rows(
                        statement=statement,
                        api_key=api_key,
                        start_date=start_date,
                        end_date=end_date,
                        tickers=batch,
                        limit=limit,
                        calls_per_minute=calls_per_minute,
                    )
                raw_rows.extend(rows)
                logger.info(
                    "Downloaded Massive %s batch=%d/%d batch_tickers=%d raw_rows=%d",
                    statement,
                    idx,
                    len(batches),
                    len(batch),
                    len(rows),
                )
                if idx < len(batches):
                    MassiveClient.sleep_for_rate_limit(calls_per_minute)

        financials = normalize_financial_statement_rows(raw_rows, statement, ticker_universe=ticker_set)
        metadata = {
            "provider": "massive",
            "dataset": statement,
            "mode": "replace",
            "input_file": "tickers.parquet",
            "input_tickers": len(target_tickers),
            "requested_tickers": len(requested_tickers),
            "partial": ticker_limit is not None,
            "pending_tickers": max(len(target_tickers) - len(requested_tickers), 0),
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "history_years": years,
            "raw_rows": len(raw_rows),
            "output_rows": int(len(financials)),
            "calls_per_minute": calls_per_minute,
            "limit": limit,
            "ticker_batch_size": ticker_batch_size,
        }
        output_paths[statement] = write_financial_statement_snapshot(statement, financials, resolved_output_dir, metadata=metadata)
        logger.info("Wrote Massive %s history rows=%d output=%s", statement, len(financials), output_paths[statement])

    return output_paths
