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
DEFAULT_RAW_TICKERS_DIR = Path("data/raw/nasdaq_data_link/sharadar/tickers")

RAW_TICKERS_JSONL_FILE = "tickers_sep_raw.jsonl"
RAW_TICKERS_METADATA_FILE = "tickers_sep_raw.download.json"
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


def resolve_raw_tickers_dir(raw_tickers_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        raw_tickers_dir,
        env_key="FINBOT_RAW_TICKERS_DIR",
        default_path=DEFAULT_RAW_TICKERS_DIR,
        data_root_subpath="raw/nasdaq_data_link/sharadar/tickers",
    )


def get_nasdaq_data_link_api_key() -> str:
    api_key = get_env("NASDAQ_DATA_LINK_API_KEY") or get_env("QUANDL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing NASDAQ_DATA_LINK_API_KEY or QUANDL_API_KEY")
    return api_key


def download_ticker_universe_all_rows(api_key: str, client: NasdaqDataLinkClient | None = None) -> list[dict[str, Any]]:
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
    raw_output_dir: str | Path | None = None,
    downloader: TickersDownloader | None = None,
) -> dict[str, Path]:
    api_key = get_nasdaq_data_link_api_key()
    fetch = downloader or download_ticker_universe_all_rows
    rows = fetch(api_key)
    tickers_all = normalize_tickers_frame(pd.DataFrame(rows))
    tickers = filter_tickers(tickers_all)
    resolved_output_dir = resolve_reference_dir(output_dir)
    resolved_raw_output_dir = resolve_raw_tickers_dir(raw_output_dir)

    raw_path = write_raw_tickers_snapshot(rows, resolved_raw_output_dir)
    subset_path = write_tickers_snapshot(
        tickers,
        resolved_output_dir,
        parquet_name=TICKERS_PARQUET_FILE,
        metadata_name=TICKERS_METADATA_FILE,
        variant="tickers",
        metadata_extra={
            "raw_input_file": str(raw_path),
            "input_rows": len(tickers_all),
            "filter": {
                "active_only": True,
                "exchanges": list(DEFAULT_EXCHANGES),
                "categories": list(DEFAULT_CATEGORIES),
                "market_caps": list(DEFAULT_MARKET_CAPS),
            },
        },
    )
    return {"tickers_raw": raw_path, "tickers": subset_path}


def write_raw_tickers_snapshot(rows: list[dict[str, Any]], output_dir: str | Path) -> Path:
    """Atomically write raw Sharadar ticker rows as JSONL plus download metadata."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / RAW_TICKERS_JSONL_FILE
    metadata_path = output_dir / RAW_TICKERS_METADATA_FILE

    collected_at_utc = datetime.utcnow()
    metadata = {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "provider": PROVIDER,
        "source": SOURCE,
        "source_table": TICKERS_TABLE,
        "dataset": "tickers",
        "variant": "tickers_raw",
        "mode": "replace",
        "rows": int(len(rows)),
        "api_params": {"table": "SEP"},
        "raw_file": output_path.name,
    }

    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".jsonl", mode="w", encoding="utf-8", delete=False) as temp_raw:
        temp_raw_path = Path(temp_raw.name)
        for row in rows:
            temp_raw.write(json.dumps(row, sort_keys=True))
            temp_raw.write("\n")
    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".json", mode="w", encoding="utf-8", delete=False) as temp_metadata:
        temp_metadata_path = Path(temp_metadata.name)
        json.dump(metadata, temp_metadata, indent=2)

    try:
        temp_raw_path.replace(output_path)
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_raw_path.unlink(missing_ok=True)
        temp_metadata_path.unlink(missing_ok=True)

    return output_path


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
