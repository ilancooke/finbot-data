from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import tempfile
import time
from typing import Any, Iterable, Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from market_data.config import get_alpaca_credentials, resolve_finbot_data_path
from market_data.metadata import dataset_identity_metadata, utc_timestamp_metadata
from market_data.providers.alpaca import AlpacaClient
from market_data.raw import write_raw_json_pages

HISTORY_START_DATE = date(2016, 1, 1)
DEFAULT_OUTPUT_DIR = Path("data/market/daily_bars")
DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_RAW_DIR = Path("data/raw/alpaca/daily_bars")
DEFAULT_SYMBOL_BATCH_SIZE = 50
DEFAULT_UPDATE_OVERLAP_DAYS = 7
HISTORICAL_FILE = "historical.parquet"
HISTORICAL_METADATA_FILE = "historical.metadata.json"
QUARANTINE_FILE = "identity_quarantine.parquet"
QUARANTINE_METADATA_FILE = "identity_quarantine.metadata.json"
PILOT_FILE = "pilot.parquet"
PILOT_METADATA_FILE = "pilot.metadata.json"
PILOT_QUARANTINE_FILE = "pilot_identity_quarantine.parquet"
TICKERS_FILE = "tickers.parquet"
CORPORATE_ACTIONS_FILE = "corporate_actions.parquet"
CORPORATE_ACTIONS_METADATA_FILE = "corporate_actions.metadata.json"
PILOT_SYMBOLS = ("AAPL", "NVDA", "GE", "AMC", "SYMC", "NLOK", "GEN", "FB", "META", "TWTR", "VAPO", "MBGLW")

BAR_COLUMNS = [
    "security_id", "episode_id", "symbol", "current_symbol", "date",
    "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume", "raw_vwap", "raw_trade_count",
    "split_adjusted_open", "split_adjusted_high", "split_adjusted_low", "split_adjusted_close",
    "split_adjusted_volume", "split_adjusted_vwap", "split_adjusted_trade_count",
    "identity_status", "source_feed", "snapshot_date",
]
BAR_SCHEMA = pa.schema([
    pa.field("security_id", pa.string()), pa.field("episode_id", pa.string()),
    pa.field("symbol", pa.string(), nullable=False), pa.field("current_symbol", pa.string()),
    pa.field("date", pa.date32(), nullable=False),
    *[pa.field(name, pa.float64()) for name in (
        "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume", "raw_vwap"
    )],
    pa.field("raw_trade_count", pa.int64()),
    *[pa.field(name, pa.float64()) for name in (
        "split_adjusted_open", "split_adjusted_high", "split_adjusted_low", "split_adjusted_close",
        "split_adjusted_volume", "split_adjusted_vwap"
    )],
    pa.field("split_adjusted_trade_count", pa.int64()),
    pa.field("identity_status", pa.string(), nullable=False),
    pa.field("source_feed", pa.string(), nullable=False),
    pa.field("snapshot_date", pa.date32(), nullable=False),
])


def resolve_output_dir(output_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(output_dir, "FINBOT_RAW_BARS_DIR", DEFAULT_OUTPUT_DIR, "market/daily_bars")


def resolve_reference_dir(reference_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(reference_dir, "FINBOT_REFERENCE_DIR", DEFAULT_REFERENCE_DIR, "reference")


def resolve_raw_dir(raw_dir: str | Path | None) -> Path:
    return resolve_finbot_data_path(raw_dir, "FINBOT_RAW_ALPACA_BARS_DIR", DEFAULT_RAW_DIR, "raw/alpaca/daily_bars")


def last_completed_session_candidate(snapshot_date: date) -> date:
    candidate = snapshot_date - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def normalize_bar_pages(pages: Iterable[Mapping[str, Any]], *, prefix: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for page in pages:
        bars = page.get("bars") or {}
        if not isinstance(bars, Mapping):
            raise ValueError("bars must be an object keyed by symbol")
        for symbol, values in bars.items():
            if not isinstance(values, list):
                raise ValueError(f"bars.{symbol} must be an array")
            for raw in values:
                if not isinstance(raw, Mapping):
                    raise ValueError(f"bars.{symbol} entries must be objects")
                timestamp = pd.to_datetime(raw.get("t"), errors="coerce", utc=True)
                if pd.isna(timestamp):
                    continue
                rows.append({
                    "symbol": str(symbol).upper(), "date": timestamp.date(),
                    f"{prefix}_open": _number(raw.get("o")), f"{prefix}_high": _number(raw.get("h")),
                    f"{prefix}_low": _number(raw.get("l")), f"{prefix}_close": _number(raw.get("c")),
                    f"{prefix}_volume": _number(raw.get("v")), f"{prefix}_vwap": _number(raw.get("vw")),
                    f"{prefix}_trade_count": _integer(raw.get("n")),
                })
    columns = ["symbol", "date", *[f"{prefix}_{name}" for name in ("open", "high", "low", "close", "volume", "vwap", "trade_count")]]
    if not rows:
        return pd.DataFrame(columns=columns)
    return (pd.DataFrame(rows, columns=columns)
            .drop_duplicates(["symbol", "date"], keep="last")
            .sort_values(["symbol", "date"]).reset_index(drop=True))


def combine_and_map_bars(
    raw_bars: pd.DataFrame,
    split_bars: pd.DataFrame,
    episodes: pd.DataFrame,
    *,
    requested_symbols: set[str],
    snapshot_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    combined = raw_bars.merge(split_bars, on=["symbol", "date"], how="outer", indicator=True, validate="one_to_one")
    if combined.empty:
        empty = pd.DataFrame(columns=BAR_COLUMNS)
        return empty.copy(), empty.copy()
    combined["security_id"] = None
    combined["episode_id"] = None
    combined["current_symbol"] = None
    combined["identity_status"] = "outside_symbol_episode"
    combined["_match_count"] = 0
    combined.loc[~combined["symbol"].isin(requested_symbols), "identity_status"] = "unexpected_returned_symbol"

    relevant = episodes[episodes["provider_symbol"].astype("string").str.upper().isin(requested_symbols)].copy()
    relevant["symbol_valid_from"] = pd.to_datetime(relevant["symbol_valid_from"], errors="coerce").dt.date
    relevant["symbol_valid_to"] = pd.to_datetime(relevant["symbol_valid_to"], errors="coerce").dt.date
    for symbol, index in combined.groupby("symbol").groups.items():
        symbol_episodes = relevant[relevant["provider_symbol"].astype("string").str.upper().eq(symbol)]
        for episode in symbol_episodes.to_dict("records"):
            start_value = episode.get("symbol_valid_from")
            end_value = episode.get("symbol_valid_to")
            start = HISTORY_START_DATE if start_value is None or pd.isna(start_value) else start_value
            end = None if end_value is None or pd.isna(end_value) else end_value
            mask_index = [i for i in index if combined.at[i, "date"] >= start and (end is None or combined.at[i, "date"] <= end)]
            if not mask_index:
                continue
            first = combined.loc[mask_index, "_match_count"].eq(0)
            first_index = list(first[first].index)
            combined.loc[mask_index, "_match_count"] += 1
            if first_index:
                combined.loc[first_index, "security_id"] = episode.get("security_id")
                combined.loc[first_index, "episode_id"] = episode.get("episode_id")
                combined.loc[first_index, "current_symbol"] = episode.get("current_symbol")
                status = "mapped" if bool(episode.get("is_episode_mapping_eligible")) else "identity_quarantine"
                combined.loc[first_index, "identity_status"] = status

    combined.loc[combined["_match_count"].gt(1), ["security_id", "episode_id", "current_symbol"]] = None
    combined.loc[combined["_match_count"].gt(1), "identity_status"] = "ambiguous_episode_overlap"
    combined.loc[combined["_merge"].ne("both"), "identity_status"] = "missing_adjustment_pair"
    combined["source_feed"] = "sip"
    combined["snapshot_date"] = snapshot_date
    combined = combined[BAR_COLUMNS].sort_values(["symbol", "date"]).reset_index(drop=True)
    mapped = combined[combined["identity_status"].eq("mapped")].copy()
    quarantine = combined[~combined["identity_status"].eq("mapped")].copy()
    return mapped, quarantine


def run_daily_bars(
    *,
    mode: str,
    snapshot_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_dir: str | Path | None = None,
    reference_dir: str | Path | None = None,
    raw_dir: str | Path | None = None,
    symbol_batch_size: int = DEFAULT_SYMBOL_BATCH_SIZE,
    update_overlap_days: int = DEFAULT_UPDATE_OVERLAP_DAYS,
    force: bool = False,
    alpaca_client: AlpacaClient | None = None,
) -> dict[str, Path]:
    if mode not in {"pilot", "backfill", "update"}:
        raise ValueError("mode must be pilot, backfill, or update")
    if symbol_batch_size < 1:
        raise ValueError("symbol_batch_size must be at least 1")
    snapshot_date = snapshot_date or datetime.now(UTC).date()
    end_date = end_date or last_completed_session_candidate(snapshot_date)
    output_path = resolve_output_dir(output_dir)
    reference_path = resolve_reference_dir(reference_dir)
    raw_path = resolve_raw_dir(raw_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    tickers_path = reference_path / TICKERS_FILE
    if not tickers_path.exists():
        raise FileNotFoundError(f"episode-aware ticker manifest not found: {tickers_path}")
    episodes = pd.read_parquet(tickers_path)
    symbols = sorted(episodes["provider_symbol"].dropna().astype(str).str.upper().unique())
    if mode == "pilot":
        symbols = [symbol for symbol in PILOT_SYMBOLS if symbol in set(symbols)]
    if not symbols:
        raise ValueError("no symbols selected")
    historical_path = output_path / HISTORICAL_FILE
    historical_metadata_path = output_path / HISTORICAL_METADATA_FILE
    applied_split_ids: set[str] = set()
    ranges: list[tuple[list[str], date]]
    if mode in {"pilot", "backfill"}:
        request_start = start_date or HISTORY_START_DATE
        ranges = [(symbols, request_start)]
    else:
        if not historical_path.exists() or not historical_metadata_path.exists():
            raise FileNotFoundError("daily-bar update requires an existing historical dataset")
        previous = json.loads(historical_metadata_path.read_text(encoding="utf-8"))
        previous_max = _as_date(previous.get("max_date"))
        if previous_max is None:
            raise ValueError("historical metadata has no max_date")
        request_start = start_date or max(HISTORY_START_DATE, previous_max - timedelta(days=update_overlap_days))
        applied_split_ids = set(previous.get("applied_split_event_ids") or [])
        split_symbols, pending_split_ids = _pending_split_refreshes(reference_path, applied_split_ids)
        split_symbols &= set(symbols)
        ranges = []
        if split_symbols:
            ranges.append((sorted(split_symbols), HISTORY_START_DATE))
        remaining = sorted(set(symbols) - split_symbols)
        if remaining:
            ranges.append((remaining, request_start))
        applied_split_ids.update(pending_split_ids)
    if alpaca_client is None:
        key, secret = get_alpaca_credentials()
        alpaca_client = AlpacaClient(key, secret)

    universe_hash = hashlib.sha256("\n".join(symbols).encode()).hexdigest()
    run_key = f"{mode}-{min(start for _, start in ranges).isoformat()}-{end_date.isoformat()}-b{symbol_batch_size}-{universe_hash[:12]}"
    staging = output_path / ".staging" / run_key
    if force and staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)
    checkpoint_path = staging / "checkpoint.json"
    completed = set(_load_checkpoint(checkpoint_path).get("completed_batches") or [])
    started = time.perf_counter()
    request_count_before = alpaca_client._request_count
    batch_specs: list[tuple[str, list[str], date]] = []
    counter = 0
    for range_symbols, range_start in ranges:
        for batch in _batched(range_symbols, symbol_batch_size):
            batch_specs.append((f"batch-{counter:05d}", batch, range_start))
            counter += 1
    for batch_id, batch, batch_start in batch_specs:
        mapped_part = staging / f"{batch_id}.parquet"
        quarantine_part = staging / f"{batch_id}.quarantine.parquet"
        if batch_id in completed and mapped_part.exists() and quarantine_part.exists():
            continue
        raw_bars = _download_variant(alpaca_client, batch, batch_start, end_date, "raw", raw_path, snapshot_date, mode, batch_id)
        split_bars = _download_variant(alpaca_client, batch, batch_start, end_date, "split", raw_path, snapshot_date, mode, batch_id)
        mapped, quarantine = combine_and_map_bars(
            raw_bars, split_bars, episodes, requested_symbols=set(batch), snapshot_date=snapshot_date
        )
        _write_part(mapped, mapped_part)
        _write_part(quarantine, quarantine_part)
        completed.add(batch_id)
        _write_json_atomic(checkpoint_path, {
            "run_key": run_key, "mode": mode, "requested_end_date": end_date.isoformat(),
            "completed_batches": sorted(completed), "batch_count": len(batch_specs),
        })

    is_pilot = mode == "pilot"
    target = output_path / (PILOT_FILE if is_pilot else HISTORICAL_FILE)
    quarantine_target = output_path / (PILOT_QUARANTINE_FILE if is_pilot else QUARANTINE_FILE)
    replacement_ranges = None if mode != "update" else {symbol: start for group, start in ranges for symbol in group}
    _compact_parts([staging / f"{bid}.parquet" for bid, _, _ in batch_specs], target,
                   existing=historical_path if mode == "update" else None, replacement_ranges=replacement_ranges)
    _compact_parts([staging / f"{bid}.quarantine.parquet" for bid, _, _ in batch_specs], quarantine_target,
                   existing=(output_path / QUARANTINE_FILE) if mode == "update" else None,
                   replacement_ranges=replacement_ranges)
    metrics = _dataset_metrics(target)
    quarantine_metrics = _dataset_metrics(quarantine_target)
    elapsed = time.perf_counter() - started
    peak_mb = _peak_memory_mb()
    invocation_request_count = alpaca_client._request_count - request_count_before
    request_count = _raw_request_count(raw_path, mode, batch_specs)
    all_split_ids = _all_split_event_ids(reference_path)
    if mode == "backfill":
        applied_split_ids = all_split_ids
    pilot_passed = None
    if is_pilot:
        pilot_passed = (
            metrics["row_count"] > 0
            and metrics["duplicate_key_count"] == 0
            and metrics["missing_adjustment_pair_count"] == 0
            and metrics["split_adjustment_difference_count"] > 0
            and quarantine_metrics["row_count"] > 0
        )
    metadata_path = output_path / (PILOT_METADATA_FILE if is_pilot else HISTORICAL_METADATA_FILE)
    metadata = {
        **utc_timestamp_metadata(),
        **dataset_identity_metadata(
            dataset_name="market.daily_bars.pilot" if is_pilot else "market.daily_bars.historical",
            dataset_group="market", write_mode="replace_pilot" if is_pilot else "incremental_merge",
            completeness_profile="episode_bounded_raw_and_split_adjusted_daily_bars",
            primary_key=["symbol", "date"], date_column="date", entity_column="security_id",
        ),
        "provider": "alpaca", "feed": "sip", "adjustments": ["raw", "split"], "asof": "-",
        "parquet_file": target.name, "quarantine_file": quarantine_target.name,
        "snapshot_date": snapshot_date.isoformat(), "requested_start_date": min(start for _, start in ranges).isoformat(),
        "requested_end_date": end_date.isoformat(), "input_symbol_count": len(symbols), "symbol_batch_size": symbol_batch_size,
        "request_count": request_count, "invocation_request_count": invocation_request_count,
        "elapsed_seconds": round(elapsed, 3), "peak_memory_mb": peak_mb,
        "resumable_batch_count": len(batch_specs), "applied_split_event_ids": sorted(applied_split_ids),
        "pilot_passed": pilot_passed, **metrics,
    }
    _write_json_atomic(metadata_path, metadata)
    _write_quarantine_metadata(output_path, quarantine_target, quarantine_metrics, snapshot_date, is_pilot)
    shutil.rmtree(staging)
    return {"bars": target, "metadata": metadata_path, "quarantine": quarantine_target}


def _download_variant(client: AlpacaClient, symbols: list[str], start: date, end: date, adjustment: str,
                      raw_root: Path, snapshot_date: date, mode: str, batch_id: str) -> pd.DataFrame:
    captured: list[pd.DataFrame] = []
    def pages():
        for page in client.iter_stock_bar_pages(symbols, start=start, end=end, feed="sip", adjustment=adjustment, asof="-"):
            captured.append(normalize_bar_pages([page], prefix="raw" if adjustment == "raw" else "split_adjusted"))
            yield page
    symbol_hash = hashlib.sha256("\n".join(symbols).encode()).hexdigest()[:12]
    path = raw_root / snapshot_date.isoformat() / mode / f"{batch_id}-{symbol_hash}-{adjustment}.jsonl.gz"
    write_raw_json_pages(pages(), path, provider="alpaca", source_endpoint="/v2/stocks/bars", request_params={
        "symbols": symbols, "start": start.isoformat(), "end": end.isoformat(), "timeframe": "1Day",
        "feed": "sip", "adjustment": adjustment, "asof": "-", "sort": "asc",
    })
    if not captured:
        return normalize_bar_pages([], prefix="raw" if adjustment == "raw" else "split_adjusted")
    return (pd.concat(captured, ignore_index=True).drop_duplicates(["symbol", "date"], keep="last")
            .sort_values(["symbol", "date"]).reset_index(drop=True))


def _write_part(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame.reindex(columns=BAR_COLUMNS), schema=BAR_SCHEMA, preserve_index=False)
    temp = _temp_path(path.parent, ".parquet")
    try:
        pq.write_table(table, temp, compression="zstd", row_group_size=100_000)
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def _compact_parts(parts: list[Path], target: Path, *, existing: Path | None, replacement_ranges: dict[str, date] | None) -> None:
    temp = _temp_path(target.parent, ".parquet")
    writer = pq.ParquetWriter(temp, BAR_SCHEMA, compression="zstd")
    try:
        if existing is not None and existing.exists():
            for batch in pq.ParquetFile(existing).iter_batches(batch_size=100_000):
                frame = batch.to_pandas()
                if replacement_ranges:
                    replace = pd.Series(False, index=frame.index)
                    starts: dict[date, set[str]] = {}
                    for symbol, start in replacement_ranges.items():
                        starts.setdefault(start, set()).add(symbol)
                    for start, symbols in starts.items():
                        replace |= frame["symbol"].isin(symbols) & (frame["date"] >= start)
                    frame = frame[~replace]
                if not frame.empty:
                    writer.write_table(pa.Table.from_pandas(frame, schema=BAR_SCHEMA, preserve_index=False))
        for part in parts:
            for batch in pq.ParquetFile(part).iter_batches(batch_size=100_000):
                writer.write_batch(batch)
    finally:
        writer.close()
    temp.replace(target)


def _dataset_metrics(path: Path) -> dict[str, Any]:
    rows = 0; symbols: set[str] = set(); securities: set[str] = set(); min_date = None; max_date = None
    duplicates = 0; missing_pairs = 0; adjustment_differences = 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=100_000):
        frame = batch.to_pandas(); rows += len(frame)
        symbols.update(frame["symbol"].dropna().astype(str).unique()); securities.update(frame["security_id"].dropna().astype(str).unique())
        if not frame.empty:
            current_min, current_max = frame["date"].min(), frame["date"].max()
            min_date = current_min if min_date is None else min(min_date, current_min); max_date = current_max if max_date is None else max(max_date, current_max)
        duplicates += int(frame.duplicated(["symbol", "date"], keep=False).sum())
        missing_pairs += int(frame[["raw_close", "split_adjusted_close"]].isna().any(axis=1).sum())
        adjustment_differences += int((frame["raw_close"].notna() & frame["split_adjusted_close"].notna() &
                                      (frame["raw_close"].sub(frame["split_adjusted_close"]).abs() > 1e-9)).sum())
    return {"row_count": rows, "symbol_count": len(symbols), "security_id_count": len(securities),
            "min_date": min_date.isoformat() if min_date else None, "max_date": max_date.isoformat() if max_date else None,
            "duplicate_key_count": duplicates, "missing_required_columns": [],
            "missing_adjustment_pair_count": missing_pairs, "split_adjustment_difference_count": adjustment_differences}


def _pending_split_refreshes(reference_dir: Path, applied: set[str]) -> tuple[set[str], set[str]]:
    metadata_path = reference_dir / CORPORATE_ACTIONS_METADATA_FILE
    actions_path = reference_dir / CORPORATE_ACTIONS_FILE
    if not metadata_path.exists() or not actions_path.exists():
        return set(), set()
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    pending = set(metadata.get("split_history_refresh_event_ids") or []) - applied
    if not pending:
        return set(), set()
    actions = pd.read_parquet(actions_path, columns=["provider_event_id", "primary_symbol"])
    symbols = set(actions[actions["provider_event_id"].isin(pending)]["primary_symbol"].dropna().astype(str).str.upper())
    return symbols, pending


def _all_split_event_ids(reference_dir: Path) -> set[str]:
    path = reference_dir / CORPORATE_ACTIONS_FILE
    if not path.exists(): return set()
    frame = pd.read_parquet(path, columns=["provider_event_id", "requires_split_history_refresh"])
    return set(frame[frame["requires_split_history_refresh"]]["provider_event_id"].dropna().astype(str))


def _raw_request_count(raw_root: Path, mode: str, batch_specs: list[tuple[str, list[str], date]]) -> int:
    total = 0
    for batch_id, symbols, _ in batch_specs:
        symbol_hash = hashlib.sha256("\n".join(symbols).encode()).hexdigest()[:12]
        for adjustment in ("raw", "split"):
            name = f"{batch_id}-{symbol_hash}-{adjustment}.download.json"
            candidates = sorted(raw_root.glob(f"*/{mode}/{name}"))
            if candidates:
                metadata = json.loads(candidates[-1].read_text(encoding="utf-8"))
                total += int(metadata.get("page_count") or 0)
    return total


def _write_quarantine_metadata(output_dir: Path, path: Path, metrics: dict[str, Any], snapshot_date: date, pilot: bool) -> None:
    metadata_path = output_dir / ("pilot_identity_quarantine.metadata.json" if pilot else QUARANTINE_METADATA_FILE)
    _write_json_atomic(metadata_path, {**utc_timestamp_metadata(), **dataset_identity_metadata(
        dataset_name="market.daily_bars.pilot_identity_quarantine" if pilot else "market.daily_bars.identity_quarantine",
        dataset_group="market", write_mode="replace_pilot" if pilot else "incremental_merge",
        completeness_profile="unmapped_or_ambiguous_alpaca_daily_bars", primary_key=["symbol", "date"],
        date_column="date", entity_column="symbol"), "parquet_file": path.name,
        "snapshot_date": snapshot_date.isoformat(), **metrics})


def _load_checkpoint(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text()) if path.exists() else {}


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True); temp = _temp_path(path.parent, ".json")
    try:
        temp.write_text(json.dumps(dict(value), indent=2, sort_keys=True), encoding="utf-8"); temp.replace(path)
    finally: temp.unlink(missing_ok=True)


def _peak_memory_mb() -> float:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(value / 1024.0, 3)  # Linux/container reports KiB.


def _batched(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size): yield values[index:index + size]


def _number(value: Any) -> float | None:
    try: return None if value is None else float(value)
    except (TypeError, ValueError): return None


def _integer(value: Any) -> int | None:
    try: return None if value is None else int(value)
    except (TypeError, ValueError): return None


def _as_date(value: Any) -> date | None:
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _temp_path(directory: Path, suffix: str) -> Path:
    with tempfile.NamedTemporaryFile(dir=directory, suffix=suffix, delete=False) as handle: return Path(handle.name)
