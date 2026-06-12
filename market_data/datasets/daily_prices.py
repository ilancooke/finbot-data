from __future__ import annotations

from datetime import date, datetime
import json
import logging
from pathlib import Path
import tempfile
import time
from typing import Any, Callable, Iterable
import zipfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from market_data.config import get_env, resolve_finbot_data_path
from market_data.providers.nasdaq_data_link import NasdaqDataLinkClient

logger = logging.getLogger(__name__)

PROVIDER = "sharadar"
SOURCE = "nasdaq_data_link"
SEP_TABLE = "SHARADAR/SEP"

DEFAULT_OUTPUT_DIR = Path("data/daily_bars")
DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_RAW_EXPORT_DIR = Path("data/raw/exports/daily_bars")
DEFAULT_CSV_CHUNK_ROWS = 500_000

HISTORICAL_PARQUET_FILE = "historical.parquet"
HISTORICAL_METADATA_FILE = "historical.metadata.json"

CANONICAL_PRICE_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume", "closeadj", "closeunadj", "lastupdated"]
TABLE_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "volume", "closeadj", "closeunadj", "lastupdated"]

RowsDownloader = Callable[[date, str], list[dict[str, Any]]]


def resolve_output_dir(output_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        output_dir,
        env_key="FINBOT_RAW_BARS_DIR",
        default_path=DEFAULT_OUTPUT_DIR,
        data_root_subpath=Path("market/daily_bars"),
    )


def resolve_reference_dir(reference_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        reference_dir,
        env_key="FINBOT_REFERENCE_DIR",
        default_path=DEFAULT_REFERENCE_DIR,
        data_root_subpath="reference",
    )


def resolve_raw_export_dir(raw_export_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        raw_export_dir,
        env_key="FINBOT_RAW_EXPORT_DIR",
        default_path=DEFAULT_RAW_EXPORT_DIR,
        data_root_subpath=Path("raw/exports/daily_bars"),
    )


def get_nasdaq_data_link_api_key() -> str:
    api_key = get_env("NASDAQ_DATA_LINK_API_KEY") or get_env("QUANDL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing NASDAQ_DATA_LINK_API_KEY or QUANDL_API_KEY")
    return api_key


def normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize provider SEP rows to Finbot's canonical daily price schema."""

    if frame.empty:
        return pd.DataFrame(columns=CANONICAL_PRICE_COLUMNS)

    result = frame.copy()
    for column in TABLE_COLUMNS:
        if column not in result.columns:
            result[column] = None

    normalized = pd.DataFrame(
        {
            "date": pd.to_datetime(result["date"], errors="coerce").dt.date,
            "symbol": result["ticker"].astype("string").str.upper(),
            "open": pd.to_numeric(result["open"], errors="coerce"),
            "high": pd.to_numeric(result["high"], errors="coerce"),
            "low": pd.to_numeric(result["low"], errors="coerce"),
            "close": pd.to_numeric(result["close"], errors="coerce"),
            "volume": pd.to_numeric(result["volume"], errors="coerce"),
            "closeadj": pd.to_numeric(result["closeadj"], errors="coerce"),
            "closeunadj": pd.to_numeric(result["closeunadj"], errors="coerce"),
            "lastupdated": pd.to_datetime(result["lastupdated"], errors="coerce").dt.date,
        }
    )
    return _normalize_canonical_price_frame(normalized)


def download_updated_price_rows(
    lastupdated_gte: date,
    api_key: str,
    client: NasdaqDataLinkClient | None = None,
) -> list[dict[str, Any]]:
    """Download SEP rows updated since lastupdated_gte through the paginated Tables API."""

    params: dict[str, Any] = {
        "lastupdated.gte": lastupdated_gte.isoformat(),
        "qopts.columns": ",".join(TABLE_COLUMNS),
    }
    client = client or NasdaqDataLinkClient(api_key=api_key)
    return client.get_table(SEP_TABLE, params=params)


def request_bulk_price_files(
    raw_export_dir: str | Path | None = None,
    client: NasdaqDataLinkClient | None = None,
    poll_seconds: float = 60.0,
    max_polls: int = 30,
) -> list[Path]:
    """Request a full SEP table export and stream the returned file locally."""

    api_key = get_nasdaq_data_link_api_key()
    client = client or NasdaqDataLinkClient(api_key=api_key, timeout=300)
    output_dir = resolve_raw_export_dir(raw_export_dir)
    file_info: dict[str, Any] = {}
    for attempt in range(max_polls + 1):
        payload = client.get_table_export(SEP_TABLE)
        export = payload.get("datatable_bulk_download") or {}
        file_info = export.get("file") or {}
        status = str(file_info.get("status", "")).lower()
        link = file_info.get("link")
        if status == "fresh" and link:
            break
        if status not in {"creating", "regenerating"} or attempt == max_polls:
            snapshot_time = file_info.get("data_snapshot_time")
            last_refreshed = (export.get("datatable") or {}).get("last_refreshed_time")
            raise RuntimeError(
                f"Table export is not ready status={status!r} "
                f"data_snapshot_time={snapshot_time!r} last_refreshed_time={last_refreshed!r}"
            )
        logger.info("Table export status=%s; sleeping %.1fs before retry", status, poll_seconds)
        time.sleep(poll_seconds)

    url = str(file_info["link"])
    suffix = Path(url.split("?", 1)[0]).suffix or ".zip"
    output_path = output_dir / f"bulk_file_001{suffix}"
    return [client.download_file(url, output_path)]


def build_historical_from_files(
    input_files: Iterable[str | Path],
    reference_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    chunk_rows: int = DEFAULT_CSV_CHUNK_ROWS,
) -> Path:
    """Stream bulk/export price files and write the canonical Finbot historical dataset."""

    resolved_output_dir = resolve_output_dir(output_dir)
    resolved_reference_dir = resolve_reference_dir(reference_dir)
    universe_path = resolved_reference_dir / "tickers.parquet"
    universe = pd.read_parquet(universe_path)
    allowed = set(universe["ticker"].dropna().astype(str).str.upper().tolist())
    input_paths = [Path(path) for path in input_files]
    output_path = resolved_output_dir / HISTORICAL_PARQUET_FILE
    metadata_path = resolved_output_dir / HISTORICAL_METADATA_FILE
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    temp_output_path = output_path.with_name(f".{output_path.name}.tmp")
    writer: pq.ParquetWriter | None = None
    rows = 0
    symbols: set[str] = set()
    dates: list[date] = []

    try:
        for frame in _iter_price_frames(input_paths, chunk_rows=chunk_rows):
            normalized = _normalize_input_price_frame(frame)
            if normalized.empty:
                continue
            filtered = normalized[normalized["symbol"].astype(str).str.upper().isin(allowed)].copy()
            filtered = _normalize_canonical_price_frame(filtered)
            if filtered.empty:
                continue
            rows += len(filtered)
            symbols.update(filtered["symbol"].dropna().astype(str).unique().tolist())
            chunk_min, chunk_max = date_range(filtered)
            if chunk_min is not None:
                dates.append(chunk_min)
            if chunk_max is not None:
                dates.append(chunk_max)
            table = pa.Table.from_pandas(filtered, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(temp_output_path, table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        pd.DataFrame(columns=CANONICAL_PRICE_COLUMNS).to_parquet(temp_output_path, index=False)
    temp_output_path.replace(output_path)
    data_min_date = min(dates).isoformat() if dates else None
    data_max_date = max(dates).isoformat() if dates else None

    write_metadata(
        metadata_path,
        base_metadata(
            rows=rows,
            symbols=len(symbols),
            extra={
                "parquet_file": output_path.name,
                "input_files": [str(path) for path in input_paths],
                "ticker_universe_file": str(universe_path),
                "input_tickers": len(allowed),
                "source_columns": TABLE_COLUMNS,
                "data_min_date": data_min_date,
                "data_max_date": data_max_date,
            },
        ),
    )
    return output_path


def update_historical(
    lastupdated_gte: date,
    reference_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    downloader: RowsDownloader | None = None,
) -> Path:
    """Fetch rows updated since lastupdated_gte, filter locally, and merge into historical."""

    resolved_output_dir = resolve_output_dir(output_dir)
    resolved_reference_dir = resolve_reference_dir(reference_dir)
    universe = pd.read_parquet(resolved_reference_dir / "tickers.parquet")
    allowed = set(universe["ticker"].dropna().astype(str).str.upper().unique().tolist())
    api_key = get_nasdaq_data_link_api_key()
    fetch = downloader or download_updated_price_rows

    raw_rows = fetch(lastupdated_gte, api_key)
    logger.info("Downloaded SEP update rows lastupdated_gte=%s raw_rows=%d", lastupdated_gte, len(raw_rows))

    updates = normalize_price_frame(pd.DataFrame(raw_rows))
    if not updates.empty:
        updates = updates[updates["symbol"].astype(str).str.upper().isin(allowed)].copy()
        updates = _normalize_canonical_price_frame(updates)
    source_path = resolved_output_dir / HISTORICAL_PARQUET_FILE
    existing = pd.read_parquet(source_path) if source_path.exists() else pd.DataFrame(columns=CANONICAL_PRICE_COLUMNS)
    frames = [frame for frame in [existing, updates] if not frame.empty]
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=CANONICAL_PRICE_COLUMNS)
    merged = (
        _normalize_canonical_price_frame(merged)
        .drop_duplicates(subset=["symbol", "date"], keep="last")
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )
    return write_historical_snapshot(
        merged,
        resolved_output_dir,
        metadata_extra={
            "lastupdated_gte": lastupdated_gte.isoformat(),
            "update_raw_rows": len(raw_rows),
            "update_rows": len(updates),
            "update_filter": "lastupdated.gte",
            "ticker_universe_file": str(resolved_reference_dir / "tickers.parquet"),
            "input_tickers": len(allowed),
        },
    )


def write_historical_snapshot(
    prices: pd.DataFrame,
    output_dir: str | Path,
    metadata_extra: dict[str, Any] | None = None,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / HISTORICAL_PARQUET_FILE
    metadata_path = output_dir / HISTORICAL_METADATA_FILE
    normalized = _normalize_canonical_price_frame(prices)
    data_min_date, data_max_date = date_range(normalized)
    normalized.to_parquet(output_path, index=False)
    write_metadata(
        metadata_path,
        base_metadata(
            rows=len(normalized),
            symbols=int(normalized["symbol"].nunique()) if "symbol" in normalized.columns else 0,
            extra={
                "parquet_file": output_path.name,
                "data_min_date": data_min_date.isoformat() if data_min_date is not None else None,
                "data_max_date": data_max_date.isoformat() if data_max_date is not None else None,
                **(metadata_extra or {}),
            },
        ),
    )
    return output_path


def write_metadata(metadata_path: Path, metadata: dict[str, Any]) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=metadata_path.parent, suffix=".json", mode="w", encoding="utf-8", delete=False) as temp_metadata:
        temp_metadata_path = Path(temp_metadata.name)
        json.dump(metadata, temp_metadata, indent=2)
    try:
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_metadata_path.unlink(missing_ok=True)


def base_metadata(*, rows: int, symbols: int, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    collected_at_utc = datetime.utcnow()
    return {
        "collected_date_utc": collected_at_utc.date().isoformat(),
        "collected_at_utc": collected_at_utc.isoformat(timespec="seconds") + "Z",
        "provider": PROVIDER,
        "source": SOURCE,
        "source_table": SEP_TABLE,
        "dataset": "daily_bars",
        "variant": "historical",
        "mode": "replace",
        "rows": rows,
        "symbols": symbols,
        **(extra or {}),
    }


def date_range(prices: pd.DataFrame) -> tuple[date | None, date | None]:
    if prices.empty or "date" not in prices.columns:
        return None, None
    dates = pd.to_datetime(prices["date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def _normalize_input_price_frame(prices: pd.DataFrame) -> pd.DataFrame:
    if "ticker" in prices.columns:
        return normalize_price_frame(prices)
    return _normalize_canonical_price_frame(prices)


def _normalize_canonical_price_frame(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=CANONICAL_PRICE_COLUMNS)
    frame = prices.copy()
    for column in CANONICAL_PRICE_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["symbol"] = frame["symbol"].astype("string").str.upper()
    frame["lastupdated"] = pd.to_datetime(frame["lastupdated"], errors="coerce").dt.date
    for column in ["open", "high", "low", "close", "volume", "closeadj", "closeunadj"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame[CANONICAL_PRICE_COLUMNS]
        .dropna(subset=["date", "symbol"])
        .drop_duplicates(subset=["symbol", "date"], keep="last")
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )


def _iter_price_frames(input_paths: list[Path], chunk_rows: int) -> Iterable[pd.DataFrame]:
    for path in input_paths:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            parquet_file = pq.ParquetFile(path)
            for batch in parquet_file.iter_batches(batch_size=chunk_rows):
                yield batch.to_pandas()
        elif suffix == ".zip":
            with zipfile.ZipFile(path) as archive:
                names = [name for name in archive.namelist() if not name.endswith("/")]
                if not names:
                    continue
                with archive.open(names[0]) as csv_file:
                    yield from pd.read_csv(csv_file, chunksize=chunk_rows)
        else:
            yield from pd.read_csv(path, chunksize=chunk_rows)
