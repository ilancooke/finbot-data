from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any

import pandas as pd

from market_data.config import get_alpaca_credentials, resolve_finbot_data_path
from market_data.metadata import dataset_identity_metadata, frame_state_metadata, utc_timestamp_metadata
from market_data.providers.alpaca import AlpacaClient
from market_data.raw import write_raw_json_pages

HISTORY_START_DATE = date(2016, 1, 1)
DEFAULT_OVERLAP_DAYS = 30
DEFAULT_SYMBOL_BATCH_SIZE = 200
DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_RAW_DIR = Path("data/raw/alpaca/corporate_actions")
CORPORATE_ACTIONS_FILE = "corporate_actions.parquet"
CORPORATE_ACTIONS_METADATA_FILE = "corporate_actions.metadata.json"
SYMBOL_EPISODES_FILE = "symbol_episodes.parquet"

ACTION_COLLECTIONS = {
    "capital_gains_distributions": "capital_gains_distribution",
    "cash_dividends": "cash_dividend",
    "cash_mergers": "cash_merger",
    "forward_splits": "forward_split",
    "name_changes": "name_change",
    "partial_calls": "partial_call",
    "redemptions": "redemption",
    "reorganizations": "reorganization",
    "reverse_splits": "reverse_split",
    "rights_distributions": "rights_distribution",
    "spin_offs": "spin_off",
    "stock_and_cash_mergers": "stock_and_cash_merger",
    "stock_dividends": "stock_dividend",
    "stock_mergers": "stock_merger",
    "unit_splits": "unit_split",
    "worthless_removals": "worthless_removal",
}
SPLIT_ACTION_TYPES = frozenset({"forward_split", "reverse_split", "unit_split"})

CORPORATE_ACTION_COLUMNS = [
    "provider_event_id",
    "action_type",
    "process_date",
    "effective_date",
    "ex_date",
    "record_date",
    "payable_date",
    "primary_symbol",
    "related_symbol",
    "primary_cusip",
    "related_cusip",
    "security_id",
    "related_security_id",
    "identity_match_status",
    "related_identity_match_status",
    "old_rate",
    "new_rate",
    "rate",
    "cash_rate",
    "currency",
    "sub_type",
    "special",
    "foreign",
    "requires_split_history_refresh",
    "raw_payload_sha256",
    "raw_payload_json",
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
        env_key="FINBOT_RAW_ALPACA_CORPORATE_ACTIONS_DIR",
        default_path=DEFAULT_RAW_DIR,
        data_root_subpath="raw/alpaca/corporate_actions",
    )


def normalize_corporate_action_pages(
    pages: Iterable[Mapping[str, Any]],
    symbol_episodes: pd.DataFrame,
    *,
    snapshot_date: date,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for page in pages:
        actions = page.get("corporate_actions") or {}
        if not isinstance(actions, Mapping):
            raise ValueError("corporate_actions must be an object")
        unknown = sorted(set(actions) - set(ACTION_COLLECTIONS))
        if unknown:
            raise ValueError(f"unsupported Alpaca corporate-action collections: {unknown}")
        for collection, values in actions.items():
            if not isinstance(values, list):
                raise ValueError(f"corporate_actions.{collection} must be an array")
            action_type = ACTION_COLLECTIONS[collection]
            for raw in values:
                if not isinstance(raw, Mapping):
                    raise ValueError(f"corporate_actions.{collection} entries must be objects")
                rows.append(_normalize_action(action_type, raw, symbol_episodes, snapshot_date))

    if not rows:
        return pd.DataFrame(columns=CORPORATE_ACTION_COLUMNS)
    frame = pd.DataFrame(rows, columns=CORPORATE_ACTION_COLUMNS)
    return (
        frame.sort_values(["process_date", "action_type", "provider_event_id"])
        .drop_duplicates("provider_event_id", keep="last")
        .reset_index(drop=True)
    )


def merge_corporate_actions(
    existing: pd.DataFrame | None,
    refreshed: pd.DataFrame,
    *,
    refresh_start: date,
) -> pd.DataFrame:
    if existing is None or existing.empty:
        combined = refreshed.copy()
    else:
        prior = existing.copy()
        prior_dates = pd.to_datetime(prior["process_date"], errors="coerce").dt.date
        combined = pd.concat([prior[prior_dates < refresh_start], refreshed], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=CORPORATE_ACTION_COLUMNS)
    for column in CORPORATE_ACTION_COLUMNS:
        if column not in combined.columns:
            combined[column] = None
    return (
        combined[CORPORATE_ACTION_COLUMNS]
        .sort_values(["process_date", "action_type", "provider_event_id"])
        .drop_duplicates("provider_event_id", keep="last")
        .reset_index(drop=True)
    )


def download_and_write_corporate_actions(
    *,
    snapshot_date: date | None = None,
    reference_dir: str | Path | None = None,
    raw_dir: str | Path | None = None,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
    symbol_batch_size: int = DEFAULT_SYMBOL_BATCH_SIZE,
    full_refresh: bool = False,
    alpaca_client: AlpacaClient | None = None,
) -> dict[str, Path]:
    if overlap_days < 1:
        raise ValueError("overlap_days must be at least 1")
    if symbol_batch_size < 1:
        raise ValueError("symbol_batch_size must be at least 1")
    snapshot_date = snapshot_date or datetime.now(UTC).date()
    end_date = snapshot_date - timedelta(days=1)
    reference_path = resolve_reference_dir(reference_dir)
    episodes_path = reference_path / SYMBOL_EPISODES_FILE
    if not episodes_path.exists():
        raise FileNotFoundError(f"symbol episodes not found: {episodes_path}")
    output_path = reference_path / CORPORATE_ACTIONS_FILE
    existing = pd.read_parquet(output_path) if output_path.exists() and not full_refresh else None
    refresh_start = _refresh_start(existing, end_date=end_date, overlap_days=overlap_days)
    if full_refresh:
        refresh_start = HISTORY_START_DATE
    if alpaca_client is None:
        key, secret = get_alpaca_credentials()
        alpaca_client = AlpacaClient(key, secret)

    episodes = pd.read_parquet(episodes_path)
    symbols = sorted(episodes["symbol"].dropna().astype(str).str.upper().unique())
    if not symbols:
        raise ValueError("symbol episodes contain no symbols")
    symbols_sha256 = hashlib.sha256("\n".join(symbols).encode("utf-8")).hexdigest()
    captured_frames: list[pd.DataFrame] = []

    def pages_for_storage() -> Iterable[Mapping[str, Any]]:
        for symbol_batch in _batched(symbols, symbol_batch_size):
            for page in alpaca_client.iter_corporate_action_pages(
                start=refresh_start,
                end=end_date,
                symbols=symbol_batch,
                data_quality="all",
                region="us",
            ):
                captured_frames.append(
                    normalize_corporate_action_pages([page], episodes, snapshot_date=snapshot_date)
                )
                yield page

    raw_root = resolve_raw_dir(raw_dir) / snapshot_date.isoformat()
    raw_filename = f"alpaca-corporate-actions-{refresh_start.isoformat()}-{end_date.isoformat()}.jsonl.gz"
    raw_path, _ = write_raw_json_pages(
        pages_for_storage(),
        raw_root / raw_filename,
        provider="alpaca",
        source_endpoint="/v1/corporate-actions",
        request_params={
            "start": refresh_start.isoformat(),
            "end": end_date.isoformat(),
            "data_quality": "all",
            "region": "us",
            "symbol_scope": "reference.symbol_episodes",
            "symbol_count": len(symbols),
            "symbols_sha256": symbols_sha256,
            "symbol_batch_size": symbol_batch_size,
        },
    )
    refreshed = (
        pd.concat(captured_frames, ignore_index=True)
        if captured_frames
        else pd.DataFrame(columns=CORPORATE_ACTION_COLUMNS)
    )
    if not refreshed.empty:
        refreshed = (
            refreshed.sort_values(["process_date", "action_type", "provider_event_id"])
            .drop_duplicates("provider_event_id", keep="last")
            .reset_index(drop=True)
        )
    existing_hashes = _payload_hashes(existing)
    changed_split_ids = sorted(
        row["provider_event_id"]
        for row in refreshed.to_dict("records")
        if row["requires_split_history_refresh"]
        and existing_hashes.get(row["provider_event_id"]) != row["raw_payload_sha256"]
    )
    combined = merge_corporate_actions(existing, refreshed, refresh_start=refresh_start)
    return write_corporate_actions(
        combined,
        reference_dir=reference_path,
        snapshot_date=snapshot_date,
        raw_path=raw_path,
        refresh_start=refresh_start,
        refresh_end=end_date,
        overlap_days=overlap_days,
        symbol_count=len(symbols),
        symbols_sha256=symbols_sha256,
        changed_split_ids=changed_split_ids,
    )


def write_corporate_actions(
    actions: pd.DataFrame,
    *,
    reference_dir: str | Path,
    snapshot_date: date,
    raw_path: Path,
    refresh_start: date,
    refresh_end: date,
    overlap_days: int,
    symbol_count: int,
    symbols_sha256: str,
    changed_split_ids: list[str],
) -> dict[str, Path]:
    output_dir = Path(reference_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / CORPORATE_ACTIONS_FILE
    metadata_path = output_dir / CORPORATE_ACTIONS_METADATA_FILE
    temp_parquet = _temp_path(output_dir, ".parquet")
    temp_metadata = _temp_path(output_dir, ".json")
    try:
        actions.to_parquet(temp_parquet, index=False)
        metadata = {
            **utc_timestamp_metadata(),
            **dataset_identity_metadata(
                dataset_name="reference.corporate_actions",
                dataset_group="reference",
                write_mode="overlapping_incremental_replace",
                completeness_profile="alpaca_us_corporate_actions_from_2016",
                primary_key=["provider_event_id"],
                date_column="process_date",
                entity_column="security_id",
            ),
            "parquet_file": CORPORATE_ACTIONS_FILE,
            "snapshot_date": snapshot_date.isoformat(),
            "history_start_date": HISTORY_START_DATE.isoformat(),
            "refresh_start_date": refresh_start.isoformat(),
            "refresh_end_date": refresh_end.isoformat(),
            "overlap_days": overlap_days,
            "symbol_scope": "reference.symbol_episodes",
            "symbol_count": symbol_count,
            "symbols_sha256": symbols_sha256,
            "raw_snapshot_file": os.path.relpath(raw_path, output_dir),
            **frame_state_metadata(
                actions,
                primary_key=["provider_event_id"],
                required_columns=CORPORATE_ACTION_COLUMNS,
                date_column="process_date",
                entity_column="security_id",
            ),
            "action_type_counts": {
                str(key): int(value)
                for key, value in actions["action_type"].value_counts().sort_index().items()
            },
            "matched_event_count": int(actions["security_id"].notna().sum()),
            "unmatched_event_count": int(actions["security_id"].isna().sum()),
            "split_history_refresh_event_count": len(changed_split_ids),
            "split_history_refresh_event_ids": changed_split_ids,
        }
        temp_metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        temp_parquet.replace(output_path)
        temp_metadata.replace(metadata_path)
    finally:
        temp_parquet.unlink(missing_ok=True)
        temp_metadata.unlink(missing_ok=True)
    return {"corporate_actions": output_path, "metadata": metadata_path, "raw": raw_path}


def _normalize_action(
    action_type: str,
    raw: Mapping[str, Any],
    episodes: pd.DataFrame,
    snapshot_date: date,
) -> dict[str, Any]:
    event_id = _clean(raw.get("id"))
    if not event_id:
        raise ValueError(f"Alpaca {action_type} record is missing id")
    process_date = _as_date(raw.get("process_date"))
    if process_date is None:
        raise ValueError(f"Alpaca corporate action {event_id} is missing process_date")
    effective_date = _as_date(raw.get("effective_date"))
    ex_date = _as_date(raw.get("ex_date"))
    event_date = effective_date or ex_date or _as_date(raw.get("payable_date")) or process_date
    primary_symbol, related_symbol, primary_cusip, related_cusip = _identity_fields(action_type, raw)
    primary_date = event_date - timedelta(days=1) if action_type in {
        "name_change",
        "cash_merger",
        "stock_merger",
        "stock_and_cash_merger",
        "unit_split",
    } else event_date
    security_id, match_status = _match_symbol_episode(episodes, primary_symbol, primary_date)
    related_security_id, related_status = _match_symbol_episode(episodes, related_symbol, event_date)
    canonical_payload = json.dumps(dict(raw), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {
        "provider_event_id": event_id,
        "action_type": action_type,
        "process_date": process_date,
        "effective_date": effective_date,
        "ex_date": ex_date,
        "record_date": _as_date(raw.get("record_date")),
        "payable_date": _as_date(raw.get("payable_date")),
        "primary_symbol": primary_symbol,
        "related_symbol": related_symbol,
        "primary_cusip": primary_cusip,
        "related_cusip": related_cusip,
        "security_id": security_id,
        "related_security_id": related_security_id,
        "identity_match_status": match_status,
        "related_identity_match_status": related_status,
        "old_rate": _clean(raw.get("old_rate") or raw.get("source_rate") or raw.get("acquiree_rate")),
        "new_rate": _clean(raw.get("new_rate") or raw.get("acquirer_rate")),
        "rate": _clean(raw.get("rate")),
        "cash_rate": _clean(raw.get("cash_rate")),
        "currency": _clean(raw.get("currency")),
        "sub_type": _clean(raw.get("sub_type")),
        "special": _optional_bool(raw.get("special")),
        "foreign": _optional_bool(raw.get("foreign")),
        "requires_split_history_refresh": action_type in SPLIT_ACTION_TYPES,
        "raw_payload_sha256": hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest(),
        "raw_payload_json": canonical_payload,
        "snapshot_date": snapshot_date,
    }


def _identity_fields(action_type: str, raw: Mapping[str, Any]) -> tuple[str | None, str | None, str | None, str | None]:
    if action_type in {"cash_merger", "stock_merger", "stock_and_cash_merger"}:
        return (
            _symbol(raw.get("acquiree_symbol")),
            _symbol(raw.get("acquirer_symbol")),
            _clean(raw.get("acquiree_cusip")),
            _clean(raw.get("acquirer_cusip")),
        )
    if action_type == "name_change":
        return (
            _symbol(raw.get("old_symbol")),
            _symbol(raw.get("new_symbol")),
            _clean(raw.get("old_cusip")),
            _clean(raw.get("new_cusip")),
        )
    if action_type in {"spin_off", "rights_distribution"}:
        return (
            _symbol(raw.get("source_symbol")),
            _symbol(raw.get("new_symbol")),
            _clean(raw.get("source_cusip")),
            _clean(raw.get("new_cusip")),
        )
    if action_type == "unit_split":
        return (
            _symbol(raw.get("old_symbol")),
            _symbol(raw.get("new_symbol")),
            _clean(raw.get("old_cusip")),
            _clean(raw.get("new_cusip")),
        )
    return (
        _symbol(raw.get("symbol")),
        _symbol(raw.get("new_symbol")),
        _clean(raw.get("cusip") or raw.get("old_cusip")),
        _clean(raw.get("new_cusip")),
    )


def _match_symbol_episode(
    episodes: pd.DataFrame,
    symbol: str | None,
    event_date: date,
) -> tuple[str | None, str]:
    if not symbol:
        return None, "not_applicable"
    matches = episodes[episodes["symbol"].astype("string").str.upper() == symbol]
    if matches.empty:
        return None, "unmatched_symbol"
    starts = pd.to_datetime(matches["symbol_valid_from"], errors="coerce")
    ends = pd.to_datetime(matches["symbol_valid_to"], errors="coerce")
    event_timestamp = pd.Timestamp(event_date)
    dated = matches[
        (starts.isna() | (starts <= event_timestamp))
        & (ends.isna() | (ends >= event_timestamp))
    ]
    if dated.empty:
        return None, "unmatched_date"
    security_ids = dated["security_id"].dropna().astype(str).unique()
    if len(security_ids) != 1:
        return None, "ambiguous"
    if not dated["is_episode_mapping_eligible"].fillna(False).all():
        return None, "identity_quarantine"
    return str(security_ids[0]), "matched"


def _refresh_start(existing: pd.DataFrame | None, *, end_date: date, overlap_days: int) -> date:
    if existing is None or existing.empty or "process_date" not in existing.columns:
        return HISTORY_START_DATE
    dates = pd.to_datetime(existing["process_date"], errors="coerce").dropna()
    if dates.empty:
        return HISTORY_START_DATE
    return max(HISTORY_START_DATE, min(end_date, dates.max().date()) - timedelta(days=overlap_days))


def _payload_hashes(existing: pd.DataFrame | None) -> dict[str, str]:
    if existing is None or existing.empty:
        return {}
    return {
        str(row["provider_event_id"]): str(row["raw_payload_sha256"])
        for row in existing.to_dict("records")
        if row.get("provider_event_id") and row.get("raw_payload_sha256")
    }


def _batched(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


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


def _optional_bool(value: Any) -> bool | None:
    return None if value is None or pd.isna(value) else bool(value)


def _temp_path(directory: Path, suffix: str) -> Path:
    with tempfile.NamedTemporaryFile(dir=directory, suffix=suffix, delete=False) as temporary:
        return Path(temporary.name)
