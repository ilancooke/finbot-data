from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import tempfile
from typing import Any, Callable

import pandas as pd

from market_data.config import get_env, resolve_finbot_data_path
from market_data.providers.nasdaq_data_link import NasdaqDataLinkClient

PROVIDER = "sharadar"
SOURCE = "nasdaq_data_link"
TICKERS_TABLE = "SHARADAR/TICKERS"
DEFAULT_REFERENCE_DIR = Path("data/reference")

TICKERS_ALL_PARQUET_FILE = "tickers_all.parquet"
TICKERS_ALL_METADATA_FILE = "tickers_all.metadata.json"
TICKERS_PARQUET_FILE = "tickers.parquet"
TICKERS_METADATA_FILE = "tickers.metadata.json"

TICKER_COLUMNS = [
    "table",
    "permaticker",
    "ticker",
    "name",
    "exchange",
    "isdelisted",
    "category",
    "cusips",
    "siccode",
    "sicsector",
    "sicindustry",
    "figi",
    "famaindustry",
    "sector",
    "industry",
    "scalemarketcap",
    "scalerevenue",
    "relatedtickers",
    "currency",
    "location",
    "lastupdated",
    "firstadded",
    "firstpricedate",
    "lastpricedate",
    "firstquarter",
    "lastquarter",
    "secfilings",
    "companysite",
]

DEFAULT_EXCHANGES = ("NASDAQ", "NYSE", "NYSEMKT")
DEFAULT_CATEGORIES = ("Domestic Common Stock", "Domestic Common Stock Primary Class")
DEFAULT_MARKET_CAPS = ("4 - Mid", "5 - Large", "6 - Mega")

TickersDownloader = Callable[[str], list[dict[str, Any]]]


def resolve_reference_dir(reference_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        reference_dir,
        env_key="FINBOT_REFERENCE_DIR",
        default_path=DEFAULT_REFERENCE_DIR,
        data_root_subpath="reference",
    )


def get_nasdaq_data_link_api_key() -> str:
    api_key = get_env("NASDAQ_DATA_LINK_API_KEY") or get_env("QUANDL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing NASDAQ_DATA_LINK_API_KEY or QUANDL_API_KEY")
    return api_key


def download_tickers_all_rows(api_key: str, client: NasdaqDataLinkClient | None = None) -> list[dict[str, Any]]:
    client = client or NasdaqDataLinkClient(api_key=api_key)
    return client.get_table(TICKERS_TABLE, params={"table": "SEP"})


def normalize_tickers_frame(tickers: pd.DataFrame) -> pd.DataFrame:
    if tickers.empty:
        return pd.DataFrame(columns=TICKER_COLUMNS)

    frame = tickers.copy()
    for column in TICKER_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    for column in TICKER_COLUMNS:
        frame[column] = frame[column].astype("string")
    frame["ticker"] = frame["ticker"].str.upper()
    frame["exchange"] = frame["exchange"].str.upper()
    frame["isdelisted"] = frame["isdelisted"].str.upper()
    return frame[TICKER_COLUMNS].sort_values("ticker").reset_index(drop=True)


def filter_tickers(
    tickers: pd.DataFrame,
    exchanges: tuple[str, ...] = DEFAULT_EXCHANGES,
    categories: tuple[str, ...] = DEFAULT_CATEGORIES,
    market_caps: tuple[str, ...] = DEFAULT_MARKET_CAPS,
    active_only: bool = True,
) -> pd.DataFrame:
    frame = normalize_tickers_frame(tickers)
    if frame.empty:
        return frame

    mask = (
        (frame["table"] == "SEP")
        & frame["exchange"].isin(exchanges)
        & frame["category"].isin(categories)
        & frame["scalemarketcap"].isin(market_caps)
    )
    if active_only:
        mask &= frame["isdelisted"] == "N"

    return frame[mask].copy().sort_values("ticker").reset_index(drop=True)


def download_and_write_ticker_universe(
    output_dir: str | Path | None = None,
    downloader: TickersDownloader | None = None,
) -> dict[str, Path]:
    api_key = get_nasdaq_data_link_api_key()
    fetch = downloader or download_tickers_all_rows
    rows = fetch(api_key)
    tickers_all = normalize_tickers_frame(pd.DataFrame(rows))
    tickers = filter_tickers(tickers_all)
    resolved_output_dir = resolve_reference_dir(output_dir)

    all_path = write_tickers_snapshot(
        tickers_all,
        resolved_output_dir,
        parquet_name=TICKERS_ALL_PARQUET_FILE,
        metadata_name=TICKERS_ALL_METADATA_FILE,
        variant="tickers_all",
        metadata_extra={"raw_rows": len(rows)},
    )
    subset_path = write_tickers_snapshot(
        tickers,
        resolved_output_dir,
        parquet_name=TICKERS_PARQUET_FILE,
        metadata_name=TICKERS_METADATA_FILE,
        variant="tickers",
        metadata_extra={
            "input_file": TICKERS_ALL_PARQUET_FILE,
            "input_rows": len(tickers_all),
            "filter": {
                "active_only": True,
                "exchanges": list(DEFAULT_EXCHANGES),
                "categories": list(DEFAULT_CATEGORIES),
                "market_caps": list(DEFAULT_MARKET_CAPS),
            },
        },
    )
    return {"tickers_all": all_path, "tickers": subset_path}


def write_tickers_snapshot(
    tickers: pd.DataFrame,
    output_dir: str | Path,
    parquet_name: str,
    metadata_name: str,
    variant: str,
    metadata_extra: dict[str, Any] | None = None,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / parquet_name
    metadata_path = output_dir / metadata_name
    normalized = normalize_tickers_frame(tickers)
    normalized.to_parquet(output_path, index=False)

    collected_at_utc = datetime.utcnow()
    metadata = {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "provider": PROVIDER,
        "source": SOURCE,
        "source_table": TICKERS_TABLE,
        "dataset": "tickers",
        "variant": variant,
        "mode": "replace",
        "rows": int(len(normalized)),
        "tickers": int(normalized["ticker"].nunique()) if "ticker" in normalized.columns else 0,
        "parquet_file": output_path.name,
        **(metadata_extra or {}),
    }
    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".json", mode="w", encoding="utf-8", delete=False) as temp_metadata:
        temp_metadata_path = Path(temp_metadata.name)
        json.dump(metadata, temp_metadata, indent=2)
    try:
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_metadata_path.unlink(missing_ok=True)

    return output_path
