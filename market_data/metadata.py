from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd


def utc_timestamp_metadata() -> dict[str, str]:
    observed_at_utc = datetime.utcnow()
    observed_at = observed_at_utc.isoformat(timespec="seconds") + "Z"
    return {
        "generated_at_utc": observed_at,
    }


def dataset_identity_metadata(
    *,
    dataset_name: str,
    dataset_group: str,
    write_mode: str,
    completeness_profile: str,
    primary_key: list[str] | None = None,
    date_column: str | None = None,
    entity_column: str | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "dataset_name": dataset_name,
        "dataset_group": dataset_group,
        "write_mode": write_mode,
        "completeness_profile": completeness_profile,
    }
    if primary_key is not None:
        metadata["primary_key"] = primary_key
    if date_column is not None:
        metadata["date_column"] = date_column
    if entity_column is not None:
        metadata["entity_column"] = entity_column
    return metadata


def frame_state_metadata(
    frame: pd.DataFrame,
    *,
    primary_key: list[str] | None = None,
    required_columns: list[str] | None = None,
    date_column: str | None = None,
    entity_column: str | None = None,
    expected_entities: int | None = None,
    provider_lastupdated_column: str = "lastupdated",
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "row_count": int(len(frame)),
    }
    if entity_column is not None:
        metadata[f"{entity_column}_count"] = _nunique(frame, entity_column)
    if primary_key:
        metadata["duplicate_key_count"] = duplicate_key_count(frame, primary_key)
    if required_columns:
        metadata["missing_required_columns"] = [column for column in required_columns if column not in frame.columns]
    if date_column is not None:
        min_date, max_date = date_range(frame, date_column)
        metadata["min_date"] = _date_to_iso(min_date)
        metadata["max_date"] = _date_to_iso(max_date)
        metadata.update(
            latest_date_coverage_metadata(
                frame,
                date_column=date_column,
                entity_column=entity_column,
                expected_entities=expected_entities,
            )
        )
    if provider_lastupdated_column in frame.columns:
        min_lastupdated, max_lastupdated = date_range(frame, provider_lastupdated_column)
        metadata["provider_min_lastupdated"] = _date_to_iso(min_lastupdated)
        metadata["provider_max_lastupdated"] = _date_to_iso(max_lastupdated)
    return metadata


def latest_date_coverage_metadata(
    frame: pd.DataFrame,
    *,
    date_column: str,
    entity_column: str | None,
    expected_entities: int | None = None,
) -> dict[str, Any]:
    min_date, max_date = date_range(frame, date_column)
    if max_date is None or entity_column is None or entity_column not in frame.columns:
        return {
            "latest_date": _date_to_iso(max_date),
            "latest_date_coverage_count": None,
            "latest_date_coverage_pct": None,
        }
    dates = pd.to_datetime(frame[date_column], errors="coerce").dt.date
    latest_frame = frame[dates == max_date]
    coverage_count = _nunique(latest_frame, entity_column)
    coverage_pct = None
    if expected_entities and expected_entities > 0:
        coverage_pct = round(coverage_count / expected_entities, 6)
    return {
        "latest_date": max_date.isoformat(),
        "latest_date_coverage_count": coverage_count,
        "latest_date_coverage_pct": coverage_pct,
    }


def duplicate_key_count(frame: pd.DataFrame, primary_key: list[str]) -> int | None:
    if frame.empty:
        return 0
    if any(column not in frame.columns for column in primary_key):
        return None
    return int(frame.duplicated(subset=primary_key, keep=False).sum())


def date_range(frame: pd.DataFrame, column: str) -> tuple[date | None, date | None]:
    if frame.empty or column not in frame.columns:
        return None, None
    dates = pd.to_datetime(frame[column], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def dimension_entity_counts(
    frame: pd.DataFrame,
    *,
    dimension_column: str,
    entity_column: str,
) -> dict[str, int]:
    if frame.empty or dimension_column not in frame.columns or entity_column not in frame.columns:
        return {}
    counts = (
        frame.dropna(subset=[dimension_column, entity_column])
        .groupby(dimension_column, dropna=True)[entity_column]
        .nunique()
        .sort_index()
    )
    return {str(dimension): int(count) for dimension, count in counts.items()}


def _nunique(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].dropna().nunique())


def _date_to_iso(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None
