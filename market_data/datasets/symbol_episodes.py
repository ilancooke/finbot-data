from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

import pandas as pd

from market_data.config import get_alpaca_credentials, resolve_finbot_data_path
from market_data.metadata import dataset_identity_metadata, frame_state_metadata, utc_timestamp_metadata
from market_data.providers.alpaca import AlpacaClient
from market_data.raw import write_raw_json_pages
from market_data.datasets.us_equity_universe import (
    TICKERS_COLUMNS,
    TICKERS_FILE,
    TICKERS_METADATA_FILE,
)

HISTORY_START_DATE = date(2016, 1, 1)
DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_RAW_DIR = Path("data/raw/alpaca/name_changes")
SECURITY_MASTER_FILE = "security_master.parquet"
EPISODES_FILE = "symbol_episodes.parquet"
EPISODES_METADATA_FILE = "symbol_episodes.metadata.json"
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")

EPISODE_COLUMNS = [
    "episode_id",
    "security_id",
    "symbol",
    "company_name",
    "current_symbol",
    "symbol_valid_from",
    "symbol_valid_to",
    "is_current_symbol",
    "episode_source",
    "episode_status",
    "is_price_coverage_eligible",
    "is_episode_mapping_eligible",
    "security_exclusion_reason",
    "symbol_identity_conflict",
    "share_class_figi",
    "composite_figi",
    "cik",
    "primary_exchange",
    "snapshot_date",
]


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
        env_key="FINBOT_RAW_ALPACA_NAME_CHANGES_DIR",
        default_path=DEFAULT_RAW_DIR,
        data_root_subpath="raw/alpaca/name_changes",
    )


def normalize_name_changes(pages: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for page in pages:
        actions = page.get("corporate_actions") or {}
        if not isinstance(actions, Mapping):
            raise ValueError("corporate_actions must be an object")
        for raw in actions.get("name_changes") or []:
            if not isinstance(raw, Mapping):
                continue
            old_symbol = _symbol(raw.get("old_symbol"))
            new_symbol = _symbol(raw.get("new_symbol"))
            process_date = _as_date(raw.get("process_date"))
            if (
                not old_symbol
                or not new_symbol
                or old_symbol == new_symbol
                or not process_date
                or not SYMBOL_PATTERN.fullmatch(old_symbol)
                or not SYMBOL_PATTERN.fullmatch(new_symbol)
            ):
                continue
            rows.append(
                {
                    "event_id": _clean(raw.get("id")),
                    "process_date": process_date,
                    "old_symbol": old_symbol,
                    "new_symbol": new_symbol,
                    "old_cusip": _clean(raw.get("old_cusip")),
                    "new_cusip": _clean(raw.get("new_cusip")),
                }
            )
    columns = ["event_id", "process_date", "old_symbol", "new_symbol", "old_cusip", "new_cusip"]
    if not rows:
        return pd.DataFrame(columns=columns)
    return (
        pd.DataFrame(rows, columns=columns)
        .sort_values(["process_date", "event_id", "old_symbol", "new_symbol"])
        .drop_duplicates(subset=["event_id"], keep="last")
        .reset_index(drop=True)
    )


def build_symbol_episodes(
    security_master: pd.DataFrame,
    name_changes: pd.DataFrame,
    *,
    snapshot_date: date,
) -> pd.DataFrame:
    required = {
        "security_id",
        "symbol",
        "company_name",
        "active",
        "delisted_date",
        "is_price_coverage_eligible",
        "exclusion_reason",
        "symbol_identity_conflict",
        "share_class_figi",
        "composite_figi",
        "cik",
        "primary_exchange",
    }
    missing = sorted(required - set(security_master.columns))
    if missing:
        raise ValueError(f"security master missing required columns: {missing}")

    master = security_master.copy()
    master["symbol"] = master["symbol"].map(_symbol)
    master["delisted_date"] = pd.to_datetime(master["delisted_date"], errors="coerce").dt.date
    events_by_new = (
        {
            symbol: group.sort_values(["process_date", "event_id"], ascending=[False, True]).to_dict("records")
            for symbol, group in name_changes.groupby("new_symbol")
        }
        if "new_symbol" in name_changes.columns
        else {}
    )
    output: list[dict[str, Any]] = []

    for security_id, group in master.groupby("security_id", sort=True):
        group = group.sort_values(["active", "delisted_date", "symbol"], na_position="last")
        active_rows = group[group["active"]]
        if len(active_rows) == 1:
            terminal = str(active_rows.iloc[0]["symbol"])
        else:
            terminal = str(group.iloc[-1]["symbol"])
        current_symbol = terminal if len(active_rows) == 1 else None
        starts: dict[str, date] = {}
        ends: dict[str, date] = {}
        sources: dict[str, set[str]] = {str(symbol): {"massive_inventory"} for symbol in group["symbol"]}
        chain_symbols = {terminal}
        boundary = snapshot_date + timedelta(days=1)
        cursor = terminal
        seen = {cursor}
        while True:
            candidates = [event for event in events_by_new.get(cursor, []) if event["process_date"] < boundary]
            if not candidates:
                break
            known = set(group["symbol"])
            preferred = [event for event in candidates if event["old_symbol"] in known]
            event = preferred[0] if len(preferred) == 1 else candidates[0]
            event_date = event["process_date"]
            old_symbol = event["old_symbol"]
            starts[cursor] = event_date
            ends[old_symbol] = event_date - timedelta(days=1)
            sources.setdefault(cursor, set()).add("alpaca_name_change")
            sources.setdefault(old_symbol, set()).add("alpaca_name_change")
            chain_symbols.add(old_symbol)
            if old_symbol in seen:
                break
            seen.add(old_symbol)
            cursor = old_symbol
            boundary = event_date

        symbols = set(group["symbol"]) | chain_symbols
        master_by_symbol = {str(row["symbol"]): row for _, row in group.iterrows()}
        representative = active_rows.iloc[0] if len(active_rows) == 1 else group.iloc[-1]
        for symbol, row in master_by_symbol.items():
            if symbol != current_symbol and symbol not in ends and pd.notna(row["delisted_date"]):
                ends[symbol] = row["delisted_date"] - timedelta(days=1)

        ordered = sorted(
            symbols,
            key=lambda value: (
                ends.get(value, date.max),
                starts.get(value, date.max),
                value,
            ),
        )
        for index, symbol in enumerate(ordered):
            if symbol not in starts:
                if index == 0:
                    starts[symbol] = HISTORY_START_DATE
                    sources.setdefault(symbol, set()).add("history_window_floor")
                else:
                    previous_end = ends.get(ordered[index - 1])
                    if previous_end:
                        starts[symbol] = previous_end + timedelta(days=1)
                        sources.setdefault(symbol, set()).add("adjacent_episode_inference")
            if symbol not in ends and index + 1 < len(ordered):
                next_start = starts.get(ordered[index + 1])
                if next_start:
                    ends[symbol] = next_start - timedelta(days=1)
                    sources.setdefault(symbol, set()).add("adjacent_episode_inference")

        for symbol in ordered:
            row = master_by_symbol.get(symbol, representative)
            valid_from = starts.get(symbol)
            valid_to = ends.get(symbol)
            invalid = valid_from is None or (valid_to is not None and valid_from > valid_to)
            status = "unresolved" if invalid else "current" if symbol == current_symbol else "bounded"
            price_eligible = bool(row["is_price_coverage_eligible"])
            output.append(
                {
                    "episode_id": f"{security_id}|{symbol}|{valid_from.isoformat() if valid_from else 'unknown'}",
                    "security_id": security_id,
                    "symbol": symbol,
                    "company_name": representative["company_name"],
                    "current_symbol": current_symbol,
                    "symbol_valid_from": valid_from,
                    "symbol_valid_to": valid_to,
                    "is_current_symbol": symbol == current_symbol,
                    "episode_source": ";".join(sorted(sources.get(symbol, {"massive_inventory"}))),
                    "episode_status": status,
                    "is_price_coverage_eligible": price_eligible,
                    "is_episode_mapping_eligible": price_eligible and not invalid,
                    "security_exclusion_reason": row["exclusion_reason"],
                    "symbol_identity_conflict": bool(row["symbol_identity_conflict"]),
                    "share_class_figi": representative["share_class_figi"],
                    "composite_figi": row["composite_figi"] if symbol in master_by_symbol else None,
                    "cik": representative["cik"],
                    "primary_exchange": row["primary_exchange"],
                    "snapshot_date": snapshot_date,
                }
            )

    frame = pd.DataFrame(output, columns=EPISODE_COLUMNS)
    _mark_cross_security_overlaps(frame)
    return frame.sort_values(["security_id", "symbol_valid_from", "symbol"]).reset_index(drop=True)


def download_and_write_symbol_episodes(
    *,
    snapshot_date: date | None = None,
    reference_dir: str | Path | None = None,
    raw_dir: str | Path | None = None,
    alpaca_client: AlpacaClient | None = None,
) -> dict[str, Path]:
    snapshot_date = snapshot_date or datetime.now(UTC).date()
    reference_path = resolve_reference_dir(reference_dir)
    security_master_path = reference_path / SECURITY_MASTER_FILE
    if not security_master_path.exists():
        raise FileNotFoundError(f"security master not found: {security_master_path}")
    if alpaca_client is None:
        key, secret = get_alpaca_credentials()
        alpaca_client = AlpacaClient(key, secret)
    pages = list(
        alpaca_client.iter_corporate_action_pages(
            start=HISTORY_START_DATE,
            end=snapshot_date - timedelta(days=1),
            types="name_change",
        )
    )
    raw_root = resolve_raw_dir(raw_dir) / snapshot_date.isoformat()
    raw_path, _ = write_raw_json_pages(
        pages,
        raw_root / "alpaca-name-changes.jsonl.gz",
        provider="alpaca",
        source_endpoint="/v1/corporate-actions",
        request_params={
            "types": "name_change",
            "start": HISTORY_START_DATE.isoformat(),
            "end": (snapshot_date - timedelta(days=1)).isoformat(),
            "data_quality": "all",
        },
    )
    security_master = pd.read_parquet(security_master_path)
    episodes = build_symbol_episodes(
        security_master,
        normalize_name_changes(pages),
        snapshot_date=snapshot_date,
    )
    outputs = write_symbol_episodes(
        episodes,
        reference_dir=reference_path,
        snapshot_date=snapshot_date,
        raw_path=raw_path,
    )
    outputs.update(
        write_episode_tickers(
            build_episode_tickers(episodes, security_master),
            reference_dir=reference_path,
            snapshot_date=snapshot_date,
            raw_path=raw_path,
        )
    )
    return outputs


def build_episode_tickers(episodes: pd.DataFrame, security_master: pd.DataFrame) -> pd.DataFrame:
    eligible = episodes[episodes["is_price_coverage_eligible"]].copy()
    master = security_master.sort_values(["active", "last_updated_utc"], na_position="first")
    exact = master.drop_duplicates(["security_id", "symbol"], keep="last").set_index(["security_id", "symbol"])
    representative = master.drop_duplicates("security_id", keep="last").set_index("security_id")
    rows: list[dict[str, Any]] = []
    for episode in eligible.to_dict("records"):
        key = (episode["security_id"], episode["symbol"])
        source = exact.loc[key] if key in exact.index else representative.loc[episode["security_id"]]
        rows.append(
            {
                "episode_id": episode["episode_id"],
                "security_id": episode["security_id"],
                "symbol": episode["symbol"],
                "provider_symbol": episode["symbol"],
                "company_name": episode["company_name"],
                "current_symbol": episode["current_symbol"],
                "symbol_valid_from": episode["symbol_valid_from"],
                "symbol_valid_to": episode["symbol_valid_to"],
                "is_current_symbol": episode["is_current_symbol"],
                "episode_status": episode["episode_status"],
                "is_episode_mapping_eligible": episode["is_episode_mapping_eligible"],
                "sector": source.get("sector"),
                "industry": source.get("industry"),
                "cik": episode["cik"],
                "composite_figi": episode["composite_figi"],
                "share_class_figi": episode["share_class_figi"],
                "primary_exchange": episode["primary_exchange"],
                "active": episode["is_current_symbol"],
                "delisted_date": episode["symbol_valid_to"],
                "alpaca_asset_id": source.get("alpaca_asset_id") if key in exact.index else None,
                "alpaca_status": source.get("alpaca_status") if key in exact.index else None,
                "alpaca_tradable": source.get("alpaca_tradable") if key in exact.index else None,
                "is_known_reit": False if pd.isna(source.get("is_known_reit")) else bool(source.get("is_known_reit")),
                "snapshot_date": episode["snapshot_date"],
            }
        )
    return pd.DataFrame(rows, columns=TICKERS_COLUMNS).sort_values(
        ["provider_symbol", "symbol_valid_from", "security_id"]
    ).reset_index(drop=True)


def write_episode_tickers(
    tickers: pd.DataFrame,
    *,
    reference_dir: str | Path,
    snapshot_date: date,
    raw_path: Path,
) -> dict[str, Path]:
    output_dir = Path(reference_dir)
    output_path = output_dir / TICKERS_FILE
    metadata_path = output_dir / TICKERS_METADATA_FILE
    temp_parquet = _temp_path(output_dir, ".parquet")
    temp_metadata = _temp_path(output_dir, ".json")
    try:
        tickers.to_parquet(temp_parquet, index=False)
        metadata = {
            **utc_timestamp_metadata(),
            **dataset_identity_metadata(
                dataset_name="reference.tickers",
                dataset_group="reference",
                write_mode="replace_snapshot",
                completeness_profile="alpaca_price_coverage_symbol_episodes",
                primary_key=["episode_id"],
                entity_column="security_id",
            ),
            "parquet_file": TICKERS_FILE,
            "snapshot_date": snapshot_date.isoformat(),
            "raw_name_changes_file": os.path.relpath(raw_path, output_dir),
            **frame_state_metadata(
                tickers,
                primary_key=["episode_id"],
                required_columns=TICKERS_COLUMNS,
                entity_column="security_id",
            ),
            "current_episode_count": int(tickers["is_current_symbol"].sum()),
            "historical_episode_count": int((~tickers["is_current_symbol"]).sum()),
            "mapping_eligible_episode_count": int(tickers["is_episode_mapping_eligible"].sum()),
            "mapping_quarantine_episode_count": int((~tickers["is_episode_mapping_eligible"]).sum()),
        }
        temp_metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        temp_parquet.replace(output_path)
        temp_metadata.replace(metadata_path)
    finally:
        temp_parquet.unlink(missing_ok=True)
        temp_metadata.unlink(missing_ok=True)
    return {"tickers": output_path, "tickers_metadata": metadata_path}


def write_symbol_episodes(
    episodes: pd.DataFrame,
    *,
    reference_dir: str | Path,
    snapshot_date: date,
    raw_path: Path,
) -> dict[str, Path]:
    output_dir = Path(reference_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / EPISODES_FILE
    metadata_path = output_dir / EPISODES_METADATA_FILE
    temp_parquet = _temp_path(output_dir, ".parquet")
    temp_metadata = _temp_path(output_dir, ".json")
    try:
        episodes.to_parquet(temp_parquet, index=False)
        metadata = {
            **utc_timestamp_metadata(),
            **dataset_identity_metadata(
                dataset_name="reference.symbol_episodes",
                dataset_group="reference",
                write_mode="replace_snapshot",
                completeness_profile="historical_symbol_identity_timeline",
                primary_key=["episode_id"],
                entity_column="security_id",
            ),
            "parquet_file": EPISODES_FILE,
            "snapshot_date": snapshot_date.isoformat(),
            "history_start_date": HISTORY_START_DATE.isoformat(),
            "raw_name_changes_file": os.path.relpath(raw_path, output_dir),
            **frame_state_metadata(
                episodes,
                primary_key=["episode_id"],
                required_columns=EPISODE_COLUMNS,
                entity_column="security_id",
            ),
            "current_episode_count": int(episodes["is_current_symbol"].sum()),
            "bounded_episode_count": int(episodes["episode_status"].eq("bounded").sum()),
            "unresolved_episode_count": int(episodes["episode_status"].eq("unresolved").sum()),
            "ineligible_episode_count": int((~episodes["is_price_coverage_eligible"]).sum()),
            "unmappable_episode_count": int((~episodes["is_episode_mapping_eligible"]).sum()),
            "mapping_quarantine_episode_count": int(
                (episodes["is_price_coverage_eligible"] & ~episodes["is_episode_mapping_eligible"]).sum()
            ),
        }
        temp_metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        temp_parquet.replace(output_path)
        temp_metadata.replace(metadata_path)
    finally:
        temp_parquet.unlink(missing_ok=True)
        temp_metadata.unlink(missing_ok=True)
    return {"symbol_episodes": output_path, "metadata": metadata_path, "raw": raw_path}


def _mark_cross_security_overlaps(frame: pd.DataFrame) -> None:
    for _, group in frame.groupby("symbol"):
        if group["security_id"].nunique() < 2:
            continue
        indexes = list(group.index)
        for position, left_index in enumerate(indexes):
            left = frame.loc[left_index]
            for right_index in indexes[position + 1 :]:
                right = frame.loc[right_index]
                if left["security_id"] == right["security_id"]:
                    continue
                if _overlaps(left["symbol_valid_from"], left["symbol_valid_to"], right["symbol_valid_from"], right["symbol_valid_to"]):
                    frame.loc[[left_index, right_index], "episode_status"] = "unresolved"
                    frame.loc[[left_index, right_index], "is_episode_mapping_eligible"] = False


def _overlaps(a_start: date | None, a_end: date | None, b_start: date | None, b_end: date | None) -> bool:
    if a_start is None or b_start is None:
        return True
    return a_start <= (b_end or date.max) and b_start <= (a_end or date.max)


def _symbol(value: Any) -> str | None:
    cleaned = _clean(value)
    return cleaned.upper() if cleaned else None


def _clean(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _as_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    return None if pd.isna(parsed) else parsed.date()


def _temp_path(directory: Path, suffix: str) -> Path:
    with tempfile.NamedTemporaryFile(dir=directory, suffix=suffix, delete=False) as temporary:
        return Path(temporary.name)
