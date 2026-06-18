from __future__ import annotations

from datetime import date
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
from market_data.metadata import (
    dataset_identity_metadata,
    dimension_entity_counts,
    frame_state_metadata,
    utc_timestamp_metadata,
)
from market_data.providers.nasdaq_data_link import NasdaqDataLinkClient

logger = logging.getLogger(__name__)

PROVIDER = "sharadar"
SOURCE = "nasdaq_data_link"
SF1_TABLE = "SHARADAR/SF1"

DEFAULT_OUTPUT_DIR = Path("data/fundamentals")
DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_RAW_EXPORT_DIR = Path("data/raw/nasdaq_data_link/sharadar/sf1")
DEFAULT_CSV_CHUNK_ROWS = 100_000

SF1_PARQUET_FILE = "sf1.parquet"
SF1_METADATA_FILE = "sf1.metadata.json"
SF1_DATASET_NAME = "fundamentals.sf1"
SF1_DATASET_GROUP = "fundamentals"
SF1_WRITE_MODE = "incremental_merge"
SF1_COMPLETENESS_PROFILE = "fundamental_filings"

SF1_PRIMARY_KEY = ["ticker", "dimension", "datekey", "reportperiod"]
SF1_DATE_COLUMNS = ["calendardate", "datekey", "reportperiod", "lastupdated"]
SF1_TEXT_COLUMNS = ["ticker", "dimension", "fiscalperiod"]
SF1_COLUMNS = [
    "ticker",
    "dimension",
    "calendardate",
    "datekey",
    "reportperiod",
    "fiscalperiod",
    "lastupdated",
    "accoci",
    "assets",
    "assetsavg",
    "assetsc",
    "assetsnc",
    "assetturnover",
    "bvps",
    "capex",
    "cashneq",
    "cashnequsd",
    "cor",
    "consolinc",
    "currentratio",
    "de",
    "debt",
    "debtc",
    "debtnc",
    "debtusd",
    "deferredrev",
    "depamor",
    "deposits",
    "divyield",
    "dps",
    "ebit",
    "ebitda",
    "ebitdamargin",
    "ebitdausd",
    "ebitusd",
    "ebt",
    "eps",
    "epsdil",
    "epsusd",
    "equity",
    "equityavg",
    "equityusd",
    "ev",
    "evebit",
    "evebitda",
    "fcf",
    "fcfps",
    "fxusd",
    "gp",
    "grossmargin",
    "intangibles",
    "intexp",
    "invcap",
    "invcapavg",
    "inventory",
    "investments",
    "investmentsc",
    "investmentsnc",
    "liabilities",
    "liabilitiesc",
    "liabilitiesnc",
    "marketcap",
    "ncf",
    "ncfbus",
    "ncfcommon",
    "ncfdebt",
    "ncfdiv",
    "ncff",
    "ncfi",
    "ncfinv",
    "ncfo",
    "ncfx",
    "netinc",
    "netinccmn",
    "netinccmnusd",
    "netincdis",
    "netincnci",
    "netmargin",
    "opex",
    "opinc",
    "payables",
    "payoutratio",
    "pb",
    "pe",
    "pe1",
    "ppnenet",
    "prefdivis",
    "price",
    "ps",
    "ps1",
    "receivables",
    "retearn",
    "revenue",
    "revenueusd",
    "rnd",
    "roa",
    "roe",
    "roic",
    "ros",
    "sbcomp",
    "sgna",
    "sharefactor",
    "sharesbas",
    "shareswa",
    "shareswadil",
    "sps",
    "tangibles",
    "taxassets",
    "taxexp",
    "taxliabilities",
    "tbvps",
    "workingcapital",
]

RowsDownloader = Callable[[date, str], list[dict[str, Any]]]


def resolve_output_dir(output_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        output_dir,
        env_key="FINBOT_FUNDAMENTALS_DIR",
        default_path=DEFAULT_OUTPUT_DIR,
        data_root_subpath="fundamentals",
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
        env_key="FINBOT_RAW_FUNDAMENTALS_EXPORT_DIR",
        default_path=DEFAULT_RAW_EXPORT_DIR,
        data_root_subpath="raw/nasdaq_data_link/sharadar/sf1",
    )


def get_nasdaq_data_link_api_key() -> str:
    api_key = get_env("NASDAQ_DATA_LINK_API_KEY") or get_env("QUANDL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing NASDAQ_DATA_LINK_API_KEY or QUANDL_API_KEY")
    return api_key


def download_updated_sf1_rows(
    lastupdated_gte: date,
    api_key: str,
    client: NasdaqDataLinkClient | None = None,
) -> list[dict[str, Any]]:
    """Download SF1 rows updated since lastupdated_gte through the paginated Tables API."""

    params: dict[str, Any] = {
        "lastupdated.gte": lastupdated_gte.isoformat(),
        "qopts.columns": ",".join(SF1_COLUMNS),
    }
    client = client or NasdaqDataLinkClient(api_key=api_key)
    return client.get_table(SF1_TABLE, params=params)


def request_bulk_sf1_files(
    raw_export_dir: str | Path | None = None,
    client: NasdaqDataLinkClient | None = None,
    poll_seconds: float = 60.0,
    max_polls: int = 30,
) -> list[Path]:
    """Request a full SF1 table export and stream the returned file locally."""

    api_key = get_nasdaq_data_link_api_key()
    client = client or NasdaqDataLinkClient(api_key=api_key, timeout=300)
    output_dir = resolve_raw_export_dir(raw_export_dir)
    file_info: dict[str, Any] = {}
    for attempt in range(max_polls + 1):
        payload = client.get_table_export(SF1_TABLE)
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
        logger.info("SF1 table export status=%s; sleeping %.1fs before retry", status, poll_seconds)
        time.sleep(poll_seconds)

    url = str(file_info["link"])
    suffix = Path(url.split("?", 1)[0]).suffix or ".zip"
    output_path = output_dir / f"bulk_file_001{suffix}"
    return [client.download_file(url, output_path)]


def build_sf1_from_files(
    input_files: Iterable[str | Path],
    reference_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    chunk_rows: int = DEFAULT_CSV_CHUNK_ROWS,
) -> Path:
    """Stream bulk/export SF1 files and write the filtered fundamentals dataset."""

    resolved_output_dir = resolve_output_dir(output_dir)
    resolved_reference_dir = resolve_reference_dir(reference_dir)
    universe_path = resolved_reference_dir / "tickers.parquet"
    universe = pd.read_parquet(universe_path)
    allowed = set(universe["ticker"].dropna().astype(str).str.upper().tolist())
    input_paths = [Path(path) for path in input_files]
    output_path = resolved_output_dir / SF1_PARQUET_FILE
    metadata_path = resolved_output_dir / SF1_METADATA_FILE
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    temp_output_path = output_path.with_name(f".{output_path.name}.tmp")
    writer: pq.ParquetWriter | None = None
    rows = 0
    tickers: set[str] = set()
    dimensions: set[str] = set()

    try:
        for frame in _iter_sf1_frames(input_paths, chunk_rows=chunk_rows):
            normalized = normalize_sf1_frame(frame)
            if normalized.empty:
                continue
            filtered = normalized[normalized["ticker"].astype(str).str.upper().isin(allowed)].copy()
            filtered = _normalize_canonical_sf1_frame(filtered)
            if filtered.empty:
                continue
            rows += len(filtered)
            tickers.update(filtered["ticker"].dropna().astype(str).unique().tolist())
            dimensions.update(filtered["dimension"].dropna().astype(str).unique().tolist())
            table = pa.Table.from_pandas(filtered, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(temp_output_path, table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        pd.DataFrame(columns=SF1_COLUMNS).to_parquet(temp_output_path, index=False)
    temp_output_path.replace(output_path)
    state_frame = pd.read_parquet(output_path, columns=["ticker", "dimension", "datekey", "reportperiod", "lastupdated"])
    output_columns = pq.read_schema(output_path).names
    period_min, period_max = _date_range(state_frame, "reportperiod")

    write_metadata(
        metadata_path,
        base_metadata(
            rows=rows,
            tickers=len(tickers),
            extra={
                "parquet_file": output_path.name,
                "input_files": [str(path) for path in input_paths],
                "ticker_universe_file": str(universe_path),
                "input_tickers": len(allowed),
                "dimensions": sorted(dimensions),
                "min_reportperiod": period_min.isoformat() if period_min is not None else None,
                "max_reportperiod": period_max.isoformat() if period_max is not None else None,
                **frame_state_metadata(
                    state_frame,
                    primary_key=SF1_PRIMARY_KEY,
                    date_column="datekey",
                    entity_column="ticker",
                    expected_entities=len(allowed),
                ),
                "dimension_ticker_counts": dimension_entity_counts(
                    state_frame,
                    dimension_column="dimension",
                    entity_column="ticker",
                ),
                "missing_required_columns": [column for column in SF1_COLUMNS if column not in output_columns],
            },
        ),
    )
    return output_path


def update_sf1(
    lastupdated_gte: date,
    reference_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    downloader: RowsDownloader | None = None,
) -> Path:
    """Fetch SF1 rows updated since lastupdated_gte, filter locally, and merge."""

    resolved_output_dir = resolve_output_dir(output_dir)
    resolved_reference_dir = resolve_reference_dir(reference_dir)
    universe = pd.read_parquet(resolved_reference_dir / "tickers.parquet")
    allowed = set(universe["ticker"].dropna().astype(str).str.upper().unique().tolist())
    api_key = get_nasdaq_data_link_api_key()
    fetch = downloader or download_updated_sf1_rows

    raw_rows = fetch(lastupdated_gte, api_key)
    logger.info("Downloaded SF1 update rows lastupdated_gte=%s raw_rows=%d", lastupdated_gte, len(raw_rows))

    updates = _normalize_canonical_sf1_frame(pd.DataFrame(raw_rows), drop_duplicate_keys=False)
    if not updates.empty:
        updates = updates[updates["ticker"].astype(str).str.upper().isin(allowed)].copy()
        updates = _normalize_canonical_sf1_frame(updates)
    source_path = resolved_output_dir / SF1_PARQUET_FILE
    existing = pd.read_parquet(source_path) if source_path.exists() else pd.DataFrame(columns=SF1_COLUMNS)
    frames = [frame for frame in [existing, updates] if not frame.empty]
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=SF1_COLUMNS)
    merged = (
        _normalize_canonical_sf1_frame(merged, drop_duplicate_keys=False)
        .sort_values([*SF1_PRIMARY_KEY, "lastupdated"], na_position="last")
        .drop_duplicates(subset=SF1_PRIMARY_KEY, keep="last")
        .sort_values(["ticker", "dimension", "datekey", "reportperiod"])
        .reset_index(drop=True)
    )
    return write_sf1_snapshot(
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


def normalize_sf1_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize provider SF1 rows to Finbot's canonical fundamentals schema."""

    return _normalize_canonical_sf1_frame(frame)


def write_sf1_snapshot(
    fundamentals: pd.DataFrame,
    output_dir: str | Path,
    metadata_extra: dict[str, Any] | None = None,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / SF1_PARQUET_FILE
    metadata_path = output_dir / SF1_METADATA_FILE
    normalized = _normalize_canonical_sf1_frame(fundamentals)
    period_min, period_max = _date_range(normalized, "reportperiod")
    state_metadata = frame_state_metadata(
        normalized,
        primary_key=SF1_PRIMARY_KEY,
        required_columns=SF1_COLUMNS,
        date_column="datekey",
        entity_column="ticker",
        expected_entities=(metadata_extra or {}).get("input_tickers"),
    )
    normalized.to_parquet(output_path, index=False)
    write_metadata(
        metadata_path,
        base_metadata(
            rows=len(normalized),
            tickers=int(normalized["ticker"].nunique()) if "ticker" in normalized.columns else 0,
            extra={
                "parquet_file": output_path.name,
                "dimensions": sorted(normalized["dimension"].dropna().astype(str).unique().tolist())
                if "dimension" in normalized.columns
                else [],
                "min_reportperiod": period_min.isoformat() if period_min is not None else None,
                "max_reportperiod": period_max.isoformat() if period_max is not None else None,
                **state_metadata,
                "dimension_ticker_counts": dimension_entity_counts(
                    normalized,
                    dimension_column="dimension",
                    entity_column="ticker",
                ),
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


def base_metadata(*, rows: int, tickers: int, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        **utc_timestamp_metadata(),
        "provider": PROVIDER,
        "source": SOURCE,
        "source_table": SF1_TABLE,
        **dataset_identity_metadata(
            dataset_name=SF1_DATASET_NAME,
            dataset_group=SF1_DATASET_GROUP,
            write_mode=SF1_WRITE_MODE,
            completeness_profile=SF1_COMPLETENESS_PROFILE,
            primary_key=SF1_PRIMARY_KEY,
            date_column="datekey",
            entity_column="ticker",
        ),
        "row_count": rows,
        "ticker_count": tickers,
        **(extra or {}),
    }


def _normalize_canonical_sf1_frame(fundamentals: pd.DataFrame, *, drop_duplicate_keys: bool = True) -> pd.DataFrame:
    if fundamentals.empty:
        return pd.DataFrame(columns=SF1_COLUMNS)
    frame = fundamentals.copy().reset_index(drop=True)
    missing_columns = [column for column in SF1_COLUMNS if column not in frame.columns]
    if missing_columns:
        frame = pd.concat(
            [frame, pd.DataFrame({column: [None] * len(frame) for column in missing_columns})],
            axis=1,
        )
    frame["ticker"] = frame["ticker"].astype("string").str.upper()
    frame["dimension"] = frame["dimension"].astype("string").str.upper()
    frame["fiscalperiod"] = frame["fiscalperiod"].astype("string")
    for column in SF1_DATE_COLUMNS:
        frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
    for column in SF1_COLUMNS:
        if column not in SF1_TEXT_COLUMNS and column not in SF1_DATE_COLUMNS:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame[SF1_COLUMNS].dropna(subset=SF1_PRIMARY_KEY)
    if drop_duplicate_keys:
        frame = frame.drop_duplicates(subset=SF1_PRIMARY_KEY, keep="last")
    return frame.sort_values(["ticker", "dimension", "datekey", "reportperiod"]).reset_index(drop=True)


def _date_range(frame: pd.DataFrame, column: str) -> tuple[date | None, date | None]:
    if frame.empty or column not in frame.columns:
        return None, None
    dates = pd.to_datetime(frame[column], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def _iter_sf1_frames(input_paths: list[Path], chunk_rows: int) -> Iterable[pd.DataFrame]:
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
