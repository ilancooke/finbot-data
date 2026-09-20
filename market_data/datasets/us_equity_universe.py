from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, date, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

from bs4 import BeautifulSoup
import pandas as pd

from market_data.config import get_alpaca_credentials, get_massive_api_key, resolve_finbot_data_path
from market_data.metadata import dataset_identity_metadata, frame_state_metadata, utc_timestamp_metadata
from market_data.providers.alpaca import ALPACA_TRADING_BASE_URL, AlpacaClient
from market_data.providers.massive import MassiveClient
from market_data.providers.reference_pages import download_text_page
from market_data.raw import write_raw_json_pages, write_raw_text_snapshot

HISTORY_START_DATE = date(2016, 1, 1)
NAREIT_URL = "https://www.reit.com/data-research/reit-indexes/reits-by-ticker-symbol"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_RAW_DIR = Path("data/raw/reference/us_equity_universe")

SECURITY_MASTER_FILE = "security_master.parquet"
SECURITY_MASTER_METADATA_FILE = "security_master.metadata.json"
TICKERS_FILE = "tickers.parquet"
TICKERS_METADATA_FILE = "tickers.metadata.json"
EXCLUSIONS_FILE = "universe_exclusions.parquet"
EXCLUSIONS_METADATA_FILE = "universe_exclusions.metadata.json"

ELIGIBLE_EXCHANGES = frozenset({"ARCX", "BATS", "IEXG", "LTSE", "XASE", "XNAS", "XNYS"})
COMMON_STOCK_TYPE = "CS"

SECURITY_MASTER_COLUMNS = [
    "security_id",
    "symbol",
    "company_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "identity_source",
    "instrument_type",
    "market",
    "locale",
    "currency",
    "primary_exchange",
    "active",
    "delisted_date",
    "last_updated_utc",
    "alpaca_asset_id",
    "alpaca_symbol",
    "alpaca_exchange",
    "alpaca_status",
    "alpaca_tradable",
    "sector",
    "industry",
    "is_nareit_listed",
    "is_sp500_reit",
    "is_known_reit",
    "is_excluded_structure",
    "structure_exclusion_reason",
    "has_alpaca_asset",
    "alpaca_asset_match_status",
    "coverage_reason",
    "in_price_history_window",
    "symbol_identity_conflict",
    "is_price_coverage_eligible",
    "exclusion_reason",
    "snapshot_date",
]

TICKERS_COLUMNS = [
    "episode_id",
    "security_id",
    "symbol",
    "provider_symbol",
    "company_name",
    "current_symbol",
    "symbol_valid_from",
    "symbol_valid_to",
    "is_current_symbol",
    "episode_status",
    "is_episode_mapping_eligible",
    "sector",
    "industry",
    "cik",
    "composite_figi",
    "share_class_figi",
    "primary_exchange",
    "active",
    "delisted_date",
    "alpaca_asset_id",
    "alpaca_status",
    "alpaca_tradable",
    "is_known_reit",
    "snapshot_date",
]

StructureRule = tuple[str, re.Pattern[str]]
STRUCTURE_RULES: tuple[StructureRule, ...] = (
    ("business_development_company", re.compile(r"\bBUSINESS DEVELOPMENT (?:COMPANY|CORPORATION)\b|\bBDC\b", re.I)),
    ("limited_partnership", re.compile(r"\bL\.?P\.?\b|\bLIMITED PARTNERSHIP\b", re.I)),
)

TextDownloader = Callable[[str], str]


def resolve_reference_dir(reference_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        reference_dir,
        env_key="FINBOT_REFERENCE_DIR",
        default_path=DEFAULT_REFERENCE_DIR,
        data_root_subpath="reference",
    )


def resolve_raw_dir(raw_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(
        raw_dir,
        env_key="FINBOT_RAW_REFERENCE_DIR",
        default_path=DEFAULT_RAW_DIR,
        data_root_subpath="raw/reference/us_equity_universe",
    )


def fetch_massive_common_stock_pages(client: MassiveClient) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    common_params = {
        "market": "stocks",
        "type": COMMON_STOCK_TYPE,
        "locale": "us",
        "limit": 1000,
        "sort": "ticker",
        "order": "asc",
    }
    for active in (True, False):
        pages.extend(
            client.iter_pages(
                "/v3/reference/tickers",
                params={**common_params, "active": str(active).lower()},
            )
        )
    return pages


def massive_rows_from_pages(pages: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for page in pages:
        results = page.get("results") or []
        if not isinstance(results, list):
            raise ValueError("Massive ticker page results must be an array")
        rows.extend(row for row in results if isinstance(row, dict))
    return rows


def normalize_alpaca_assets(rows: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    columns = [
        "symbol_join_key",
        "alpaca_asset_id",
        "alpaca_symbol",
        "alpaca_exchange",
        "alpaca_status",
        "alpaca_tradable",
    ]
    normalized = []
    for row in rows:
        symbol = _clean_string(row.get("symbol"), upper=True)
        if not symbol:
            continue
        normalized.append(
            {
                "symbol_join_key": _symbol_join_key(symbol),
                "alpaca_asset_id": _clean_string(row.get("id")),
                "alpaca_symbol": symbol,
                "alpaca_exchange": _clean_string(row.get("exchange"), upper=True),
                "alpaca_status": _clean_string(row.get("status"), lower=True),
                "alpaca_tradable": _nullable_bool(row.get("tradable")),
            }
        )
    if not normalized:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(normalized, columns=columns)
    return (
        frame.sort_values(["symbol_join_key", "alpaca_status", "alpaca_asset_id"], na_position="last")
        .drop_duplicates(subset=["symbol_join_key"], keep="first")
        .reset_index(drop=True)
    )


def parse_nareit_symbols(html: str) -> set[str]:
    return _symbols_from_html_table(
        html,
        ticker_headers={"rtc ticker", "ticker", "ticker symbol", "symbol"},
    )


def parse_sp500_classifications(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []
    for table in soup.find_all("table"):
        headers = _table_headers(table)
        symbol_index = _first_header_index(headers, {"symbol", "ticker"})
        sector_index = _first_header_index(headers, {"gics sector", "sector"})
        industry_index = _first_header_index(headers, {"gics sub-industry", "gics sub industry", "sub-industry", "industry"})
        if symbol_index is None or industry_index is None:
            continue
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) <= max(symbol_index, industry_index, sector_index or 0):
                continue
            symbol = _normalize_symbol(cells[symbol_index].get_text(" ", strip=True))
            if not symbol or symbol in {"SYMBOL", "TICKER"}:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "symbol_join_key": _symbol_join_key(symbol),
                    "sector": cells[sector_index].get_text(" ", strip=True) if sector_index is not None else "",
                    "industry": cells[industry_index].get_text(" ", strip=True),
                }
            )
        if rows:
            break
    if not rows:
        return pd.DataFrame(columns=["symbol", "symbol_join_key", "sector", "industry"])
    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["symbol_join_key"], keep="first")
        .sort_values("symbol")
        .reset_index(drop=True)
    )


def build_security_master(
    massive_rows: Iterable[Mapping[str, Any]],
    alpaca_assets: Iterable[Mapping[str, Any]],
    *,
    snapshot_date: date,
    nareit_symbols: set[str] | None = None,
    sp500_classifications: pd.DataFrame | None = None,
    history_start_date: date = HISTORY_START_DATE,
) -> pd.DataFrame:
    nareit_symbol_keys = {
        _symbol_join_key(value)
        for value in (nareit_symbols or set())
        if _symbol_join_key(value)
    }
    sp500 = _normalize_sp500_frame(sp500_classifications)
    alpaca = normalize_alpaca_assets(alpaca_assets)

    normalized_rows: list[dict[str, Any]] = []
    for raw in massive_rows:
        symbol = _normalize_symbol(raw.get("ticker"))
        if not symbol:
            continue
        company_name = _clean_string(raw.get("name"))
        share_class_figi = _clean_string(raw.get("share_class_figi"), upper=True)
        composite_figi = _clean_string(raw.get("composite_figi"), upper=True)
        cik = _normalize_cik(raw.get("cik"))
        exchange = _clean_string(raw.get("primary_exchange"), upper=True)
        security_id, identity_source = _security_identity(
            symbol=symbol,
            company_name=company_name,
            primary_exchange=exchange,
            share_class_figi=share_class_figi,
            composite_figi=composite_figi,
            cik=cik,
        )
        delisted_date = _to_date(raw.get("delisted_utc"))
        active = _nullable_bool(raw.get("active")) is True
        normalized_rows.append(
            {
                "security_id": security_id,
                "symbol": symbol,
                "symbol_join_key": _symbol_join_key(symbol),
                "company_name": company_name,
                "cik": cik,
                "composite_figi": composite_figi,
                "share_class_figi": share_class_figi,
                "identity_source": identity_source,
                "instrument_type": _clean_string(raw.get("type"), upper=True) or COMMON_STOCK_TYPE,
                "market": _clean_string(raw.get("market"), lower=True) or "stocks",
                "locale": _clean_string(raw.get("locale"), lower=True) or "us",
                "currency": _clean_string(raw.get("currency_name"), upper=True),
                "primary_exchange": exchange,
                "active": active,
                "delisted_date": delisted_date,
                "last_updated_utc": _to_datetime(raw.get("last_updated_utc")),
            }
        )

    if not normalized_rows:
        return pd.DataFrame(columns=SECURITY_MASTER_COLUMNS)
    frame = pd.DataFrame(normalized_rows)
    frame = _deduplicate_security_rows(frame)
    frame = frame.merge(
        alpaca,
        how="left",
        on="symbol_join_key",
        validate="many_to_one",
    )
    frame = frame.merge(
        sp500.drop(columns=["symbol"]),
        how="left",
        on="symbol_join_key",
        validate="many_to_one",
    )

    frame["is_nareit_listed"] = frame["symbol_join_key"].isin(nareit_symbol_keys)
    frame["is_sp500_reit"] = frame["industry"].fillna("").str.contains(r"\bREIT\b", case=False, regex=True)
    frame["is_known_reit"] = frame["is_nareit_listed"] | frame["is_sp500_reit"]
    structure_reasons = frame["company_name"].map(_structure_exclusion_reason)
    frame["structure_exclusion_reason"] = structure_reasons.astype("string")
    frame["is_excluded_structure"] = structure_reasons.notna()
    frame["has_alpaca_asset"] = frame["alpaca_asset_id"].notna()
    frame["alpaca_asset_match_status"] = frame.apply(_alpaca_asset_match_status, axis=1)
    frame["coverage_reason"] = frame["alpaca_asset_match_status"].map(
        {
            "normalized_symbol": "provider_symbol_spelling_differs",
            "no_current_asset": "no_current_alpaca_asset",
        }
    ).astype("string")
    frame["in_price_history_window"] = (
        frame["active"]
        | frame["delisted_date"].isna()
        | (frame["delisted_date"] >= history_start_date)
    )
    symbol_counts = frame.groupby("symbol")["security_id"].transform("nunique")
    frame["symbol_identity_conflict"] = symbol_counts > 1
    frame["snapshot_date"] = snapshot_date
    frame["exclusion_reason"] = frame.apply(_exclusion_reason, axis=1).astype("string")
    frame["is_price_coverage_eligible"] = frame["exclusion_reason"].isna()

    for column in SECURITY_MASTER_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return frame[SECURITY_MASTER_COLUMNS].sort_values(["symbol", "security_id"]).reset_index(drop=True)


def build_tickers(security_master: pd.DataFrame) -> pd.DataFrame:
    eligible = security_master[security_master["is_price_coverage_eligible"]].copy()
    if eligible.empty:
        return pd.DataFrame(columns=TICKERS_COLUMNS)
    eligible["provider_symbol"] = eligible["alpaca_symbol"].fillna(eligible["symbol"])
    eligible["episode_id"] = eligible["security_id"] + "|" + eligible["symbol"] + "|pending"
    eligible["current_symbol"] = eligible["symbol"].where(eligible["active"])
    eligible["symbol_valid_from"] = None
    eligible["symbol_valid_to"] = eligible["delisted_date"]
    eligible["is_current_symbol"] = eligible["active"]
    eligible["episode_status"] = "pending_episode_enrichment"
    eligible["is_episode_mapping_eligible"] = False
    for column in TICKERS_COLUMNS:
        if column not in eligible.columns:
            eligible[column] = None
    return eligible[TICKERS_COLUMNS].sort_values("provider_symbol").reset_index(drop=True)


def build_exclusions(security_master: pd.DataFrame) -> pd.DataFrame:
    excluded = security_master[~security_master["is_price_coverage_eligible"]].copy()
    return excluded[SECURITY_MASTER_COLUMNS].sort_values(["symbol", "security_id"]).reset_index(drop=True)


def download_and_write_security_master(
    *,
    snapshot_date: date | None = None,
    reference_dir: str | Path | None = None,
    raw_dir: str | Path | None = None,
    massive_client: MassiveClient | None = None,
    alpaca_client: AlpacaClient | None = None,
    text_downloader: TextDownloader = download_text_page,
) -> dict[str, Path]:
    snapshot_date = snapshot_date or datetime.now(UTC).date()
    reference_path = resolve_reference_dir(reference_dir)
    raw_root = resolve_raw_dir(raw_dir) / snapshot_date.isoformat()
    reference_path.mkdir(parents=True, exist_ok=True)
    raw_root.mkdir(parents=True, exist_ok=True)

    massive_client = massive_client or MassiveClient(get_massive_api_key())
    if alpaca_client is None:
        alpaca_key, alpaca_secret = get_alpaca_credentials()
        alpaca_client = AlpacaClient(
            alpaca_key,
            alpaca_secret,
            base_url=ALPACA_TRADING_BASE_URL,
        )

    massive_pages = fetch_massive_common_stock_pages(massive_client)
    massive_raw_path, _ = write_raw_json_pages(
        massive_pages,
        raw_root / "massive-common-stocks.jsonl.gz",
        provider="massive",
        source_endpoint="/v3/reference/tickers",
        request_params={
            "market": "stocks",
            "type": COMMON_STOCK_TYPE,
            "locale": "us",
            "active": [True, False],
            "limit": 1000,
        },
    )
    alpaca_assets = alpaca_client.get_assets(asset_class="us_equity")
    alpaca_raw_path, _ = write_raw_json_pages(
        [{"assets": alpaca_assets}],
        raw_root / "alpaca-assets.jsonl.gz",
        provider="alpaca",
        source_endpoint="/v2/assets",
        request_params={"asset_class": "us_equity"},
    )

    nareit_html = text_downloader(NAREIT_URL)
    sp500_html = text_downloader(SP500_URL)
    nareit_raw_path, _ = write_raw_text_snapshot(
        nareit_html,
        raw_root / "nareit-ticker-directory.html",
        provider="nareit",
        source_url=NAREIT_URL,
    )
    sp500_raw_path, _ = write_raw_text_snapshot(
        sp500_html,
        raw_root / "sp500.html",
        provider="wikipedia",
        source_url=SP500_URL,
    )

    security_master = build_security_master(
        massive_rows_from_pages(massive_pages),
        alpaca_assets,
        snapshot_date=snapshot_date,
        nareit_symbols=parse_nareit_symbols(nareit_html),
        sp500_classifications=parse_sp500_classifications(sp500_html),
    )
    tickers = build_tickers(security_master)
    exclusions = build_exclusions(security_master)
    return write_universe_datasets(
        security_master,
        tickers,
        exclusions,
        reference_dir=reference_path,
        snapshot_date=snapshot_date,
        raw_paths={
            "raw_massive_file": massive_raw_path,
            "raw_alpaca_file": alpaca_raw_path,
            "raw_nareit_file": nareit_raw_path,
            "raw_sp500_file": sp500_raw_path,
        },
    )


def write_universe_datasets(
    security_master: pd.DataFrame,
    tickers: pd.DataFrame,
    exclusions: pd.DataFrame,
    *,
    reference_dir: str | Path,
    snapshot_date: date,
    raw_paths: Mapping[str, Path] | None = None,
) -> dict[str, Path]:
    output_dir = Path(reference_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_metadata = {
        key: os.path.relpath(path, output_dir)
        for key, path in (raw_paths or {}).items()
    }
    exclusion_counts = _reason_counts(exclusions)
    shared = {
        "providers": ["massive", "alpaca", "nareit", "wikipedia"],
        "snapshot_date": snapshot_date.isoformat(),
        "history_start_date": HISTORY_START_DATE.isoformat(),
        "eligible_exchanges": sorted(ELIGIBLE_EXCHANGES),
        **raw_metadata,
    }
    definitions = [
        (
            "security_master",
            security_master,
            SECURITY_MASTER_FILE,
            SECURITY_MASTER_METADATA_FILE,
            "reference.security_master",
            "us_common_stocks_active_and_delisted",
            ["security_id", "symbol"],
            SECURITY_MASTER_COLUMNS,
            {
                "active_count": int(security_master["active"].sum()) if not security_master.empty else 0,
                "inactive_count": int((~security_master["active"]).sum()) if not security_master.empty else 0,
                "price_coverage_eligible_count": int(security_master["is_price_coverage_eligible"].sum()) if not security_master.empty else 0,
                "identity_conflict_count": int(security_master["symbol_identity_conflict"].sum()) if not security_master.empty else 0,
                "known_reit_count": int(security_master["is_known_reit"].sum()) if not security_master.empty else 0,
            },
        ),
        (
            "tickers",
            tickers,
            TICKERS_FILE,
            TICKERS_METADATA_FILE,
            "reference.tickers",
            "alpaca_price_coverage_active_and_delisted_common_stocks",
            ["episode_id"],
            TICKERS_COLUMNS,
            {
                "active_count": int(tickers["active"].sum()) if not tickers.empty else 0,
                "inactive_count": int((~tickers["active"]).sum()) if not tickers.empty else 0,
            },
        ),
        (
            "universe_exclusions",
            exclusions,
            EXCLUSIONS_FILE,
            EXCLUSIONS_METADATA_FILE,
            "reference.universe_exclusions",
            "us_common_stock_universe_exclusions",
            ["security_id", "symbol"],
            SECURITY_MASTER_COLUMNS,
            {"exclusion_counts": exclusion_counts},
        ),
    ]

    staged: list[tuple[Path, Path, Path, Path]] = []
    outputs: dict[str, Path] = {}
    try:
        for (
            key,
            frame,
            parquet_name,
            metadata_name,
            dataset_name,
            profile,
            primary_key,
            required_columns,
            extra,
        ) in definitions:
            output_path = output_dir / parquet_name
            metadata_path = output_dir / metadata_name
            temp_parquet = _temp_path(output_dir, ".parquet")
            temp_metadata = _temp_path(output_dir, ".json")
            frame.to_parquet(temp_parquet, index=False)
            metadata = {
                **utc_timestamp_metadata(),
                **shared,
                **dataset_identity_metadata(
                    dataset_name=dataset_name,
                    dataset_group="reference",
                    write_mode="replace_snapshot",
                    completeness_profile=profile,
                    primary_key=primary_key,
                    entity_column="security_id",
                ),
                "parquet_file": parquet_name,
                **frame_state_metadata(
                    frame,
                    primary_key=primary_key,
                    required_columns=required_columns,
                    entity_column="security_id",
                ),
                **extra,
            }
            temp_metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
            staged.append((temp_parquet, output_path, temp_metadata, metadata_path))
            outputs[key] = output_path

        for temp_parquet, output_path, temp_metadata, metadata_path in staged:
            temp_parquet.replace(output_path)
            temp_metadata.replace(metadata_path)
    finally:
        for temp_parquet, _, temp_metadata, _ in staged:
            temp_parquet.unlink(missing_ok=True)
            temp_metadata.unlink(missing_ok=True)
    return outputs


def _security_identity(
    *,
    symbol: str,
    company_name: str | None,
    primary_exchange: str | None,
    share_class_figi: str | None,
    composite_figi: str | None,
    cik: str | None,
) -> tuple[str, str]:
    if share_class_figi:
        return f"share_class_figi:{share_class_figi}", "share_class_figi"
    if composite_figi:
        return f"composite_figi:{composite_figi}", "composite_figi"
    if cik:
        return f"cik_symbol:{cik}:{symbol}", "cik_symbol"
    fallback = "|".join([primary_exchange or "", symbol, company_name or ""])
    digest = hashlib.sha256(fallback.encode("utf-8")).hexdigest()[:20]
    return f"massive_fallback:{digest}", "massive_fallback"


def _deduplicate_security_rows(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["last_updated_utc"] = pd.to_datetime(result["last_updated_utc"], errors="coerce", utc=True)
    return (
        result.sort_values(
            ["security_id", "symbol", "last_updated_utc", "active"],
            na_position="first",
        )
        .drop_duplicates(subset=["security_id", "symbol"], keep="last")
        .reset_index(drop=True)
    )


def _exclusion_reason(row: pd.Series) -> str | None:
    reasons: list[str] = []
    if row["instrument_type"] != COMMON_STOCK_TYPE:
        reasons.append("not_common_stock")
    if row["market"] != "stocks" or row["locale"] != "us":
        reasons.append("not_us_stock")
    if row["currency"] not in {None, "USD"} and not pd.isna(row["currency"]):
        reasons.append("non_usd_currency")
    if row["primary_exchange"] not in ELIGIBLE_EXCHANGES:
        reasons.append("ineligible_exchange")
    if bool(row["is_known_reit"]):
        reasons.append("known_reit_or_reoc")
    if bool(row["is_excluded_structure"]):
        reasons.append(str(row["structure_exclusion_reason"]))
    if not bool(row["in_price_history_window"]):
        reasons.append("delisted_before_history_window")
    if bool(row["symbol_identity_conflict"]):
        reasons.append("ambiguous_reused_symbol")
    return ";".join(dict.fromkeys(reasons)) or None


def _structure_exclusion_reason(value: Any) -> str | None:
    name = _clean_string(value) or ""
    for reason, pattern in STRUCTURE_RULES:
        if pattern.search(name):
            return reason
    return None


def _alpaca_asset_match_status(row: pd.Series) -> str:
    if pd.isna(row["alpaca_asset_id"]):
        return "no_current_asset"
    if row["symbol"] == row["alpaca_symbol"]:
        return "exact_symbol"
    return "normalized_symbol"


def _symbols_from_html_table(html: str, *, ticker_headers: set[str]) -> set[str]:
    soup = BeautifulSoup(html, "html.parser")
    symbols: set[str] = set()
    for table in soup.find_all("table"):
        headers = _table_headers(table)
        ticker_index = _first_header_index(headers, ticker_headers)
        if ticker_index is None:
            continue
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) <= ticker_index:
                continue
            raw = cells[ticker_index].get_text(" ", strip=True)
            for value in re.split(r"[,/\s]+", raw):
                symbol = _normalize_symbol(value)
                if symbol and symbol not in {"N/A", "NA", "TICKER", "SYMBOL"}:
                    symbols.add(symbol)
    return symbols


def _normalize_sp500_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    columns = ["symbol", "symbol_join_key", "sector", "industry"]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = None
    result["symbol"] = result["symbol"].map(_normalize_symbol)
    result["symbol_join_key"] = result["symbol"].map(_symbol_join_key)
    result["sector"] = result["sector"].astype("string").str.strip()
    result["industry"] = result["industry"].astype("string").str.strip()
    return (
        result[columns]
        .dropna(subset=["symbol_join_key"])
        .drop_duplicates("symbol_join_key")
        .reset_index(drop=True)
    )


def _reason_counts(exclusions: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    if exclusions.empty:
        return counts
    for value in exclusions["exclusion_reason"].dropna().astype(str):
        for reason in value.split(";"):
            counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _first_header_index(headers: list[str], candidates: set[str]) -> int | None:
    normalized_candidates = {_normalized_header(value) for value in candidates}
    return next((index for index, value in enumerate(headers) if value in normalized_candidates), None)


def _normalized_header(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower().replace("\u2011", "-").replace("\u2013", "-"))


def _normalize_symbol(value: Any) -> str | None:
    return _clean_string(value, upper=True)


def _symbol_join_key(value: Any) -> str | None:
    symbol = _normalize_symbol(value)
    return symbol.replace(".", "-") if symbol else None


def _table_headers(table: Any) -> list[str]:
    first_row = table.find("tr")
    if first_row is None:
        return []
    return [
        _normalized_header(cell.get_text(" ", strip=True))
        for cell in first_row.find_all(["th", "td"])
    ]


def _normalize_cik(value: Any) -> str | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    digits = re.sub(r"\D", "", cleaned)
    return digits.zfill(10) if digits else None


def _clean_string(value: Any, *, upper: bool = False, lower: bool = False) -> str | None:
    if value is None or pd.isna(value):
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if upper:
        return cleaned.upper()
    if lower:
        return cleaned.lower()
    return cleaned


def _nullable_bool(value: Any) -> bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return bool(value)


def _to_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    return None if pd.isna(parsed) else parsed.date()


def _to_datetime(value: Any) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    return None if pd.isna(parsed) else parsed.to_pydatetime(warn=False)


def _temp_path(directory: Path, suffix: str) -> Path:
    with tempfile.NamedTemporaryFile(dir=directory, suffix=suffix, delete=False) as temporary:
        return Path(temporary.name)
