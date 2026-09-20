from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
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
AUDIT_SAMPLE_VERSION = "v1"
DEFAULT_REFERENCE_DIR = Path("data/reference")
DEFAULT_RAW_DIR = Path("data/raw/alpaca/coverage_audit")
SECURITY_MASTER_FILE = "security_master.parquet"
SYMBOL_EPISODES_FILE = "symbol_episodes.parquet"
AUDIT_FILE = "alpaca_coverage_audit.parquet"
AUDIT_METADATA_FILE = "alpaca_coverage_audit.metadata.json"
RECENT_DELISTING_COUNT = 4
OLDER_DELISTING_COUNT = 4
TICKER_REUSE_ROW_COUNT = 10
DATE_TOLERANCE_DAYS = 14
ACTIVE_STALENESS_DAYS = 7
IEX_COMPARISON_DAYS = 90
IEX_COMPARISON_SYMBOLS = frozenset({"AAPL", "GE", "NVDA"})

AUDIT_COLUMNS = [
    "sample_id",
    "security_id",
    "symbol",
    "company_name",
    "active",
    "delisted_date",
    "has_alpaca_asset",
    "symbol_identity_conflict",
    "sample_categories",
    "sample_roles",
    "known_event_type",
    "known_event_date",
    "symbol_valid_from",
    "symbol_valid_to",
    "request_start_date",
    "request_end_date",
    "feed",
    "adjustment",
    "asof",
    "returned_symbols",
    "raw_bar_count",
    "bar_count",
    "out_of_episode_bar_count",
    "first_bar_date",
    "last_bar_date",
    "total_volume",
    "pre_event_bar_count",
    "post_event_bar_count",
    "iex_comparison_start_date",
    "iex_bar_count",
    "iex_total_volume",
    "sip_comparison_volume",
    "sip_to_iex_volume_ratio",
    "request_error",
    "audit_status",
    "coverage_reason",
    "snapshot_date",
]


@dataclass(frozen=True)
class CuratedCase:
    symbol: str
    categories: tuple[str, ...]
    roles: tuple[str, ...]
    event_type: str | None = None
    event_date: date | None = None


CURATED_CASES = (
    CuratedCase("AAPL", ("active", "forward_split"), ("full_window", "event_both_sides"), "forward_split", date(2020, 8, 31)),
    CuratedCase("NVDA", ("active", "forward_split"), ("full_window", "event_both_sides"), "forward_split", date(2024, 6, 10)),
    CuratedCase("GE", ("active", "reverse_split"), ("full_window", "event_both_sides"), "reverse_split", date(2021, 8, 2)),
    CuratedCase("AMC", ("active", "reverse_split"), ("full_window", "event_both_sides"), "reverse_split", date(2023, 8, 24)),
    CuratedCase("TWTR", ("acquisition",), ("expected_end",), "acquisition"),
    CuratedCase("ATVI", ("acquisition",), ("expected_end",), "acquisition"),
    CuratedCase("CERN", ("acquisition",), ("expected_end",), "acquisition"),
    CuratedCase("WORK", ("acquisition",), ("expected_end",), "acquisition"),
)

TICKER_CHANGE_PAIRS = (("CTL", "LUMN"), ("SYMC", "GEN"))


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
        env_key="FINBOT_RAW_ALPACA_COVERAGE_DIR",
        default_path=DEFAULT_RAW_DIR,
        data_root_subpath="raw/alpaca/coverage_audit",
    )


def build_audit_sample(
    security_master: pd.DataFrame,
    *,
    audit_end_date: date,
    symbol_episodes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    required = {
        "security_id",
        "symbol",
        "company_name",
        "active",
        "delisted_date",
        "has_alpaca_asset",
        "symbol_identity_conflict",
        "is_price_coverage_eligible",
    }
    missing = sorted(required - set(security_master.columns))
    if missing:
        raise ValueError(f"security master missing required columns: {missing}")

    master = security_master.copy()
    master["delisted_date"] = pd.to_datetime(master["delisted_date"], errors="coerce").dt.date
    cases: dict[tuple[str, str], dict[str, Any]] = {}

    for curated in CURATED_CASES:
        matches = master[master["symbol"] == curated.symbol]
        for _, row in matches.iterrows():
            event_date = curated.event_date or row["delisted_date"]
            _add_case(cases, row, curated.categories, curated.roles, curated.event_type, event_date)

    for old_symbol, new_symbol in TICKER_CHANGE_PAIRS:
        old_rows = master[master["symbol"] == old_symbol]
        for _, old_row in old_rows.iterrows():
            event_date = old_row["delisted_date"]
            _add_case(cases, old_row, ("ticker_change",), ("expected_end",), "ticker_change", event_date)
            new_rows = master[
                (master["symbol"] == new_symbol)
                & (master["security_id"] == old_row["security_id"])
            ]
            for _, new_row in new_rows.iterrows():
                _add_case(cases, new_row, ("ticker_change",), ("expected_start",), "ticker_change", event_date)

    eligible_delisted = master[
        (~master["active"])
        & master["is_price_coverage_eligible"]
        & (~master["symbol_identity_conflict"])
        & master["delisted_date"].notna()
    ]
    recent_start = audit_end_date - timedelta(days=3 * 366)
    recent = eligible_delisted[eligible_delisted["delisted_date"] >= recent_start]
    older = eligible_delisted[
        (eligible_delisted["delisted_date"] >= HISTORY_START_DATE)
        & (eligible_delisted["delisted_date"] < recent_start)
    ]
    for category, candidates, count in (
        ("recent_delisting", recent, RECENT_DELISTING_COUNT),
        ("older_delisting", older, OLDER_DELISTING_COUNT),
    ):
        for _, row in _deterministic_rows(candidates, category, count).iterrows():
            _add_case(cases, row, (category,), ("expected_end",), None, row["delisted_date"])

    conflicts = master[master["symbol_identity_conflict"]]
    for _, row in _deterministic_rows(conflicts, "ticker_reuse", TICKER_REUSE_ROW_COUNT).iterrows():
        _add_case(cases, row, ("ticker_reuse",), ("ambiguous",), None, None)

    if symbol_episodes is not None and not symbol_episodes.empty:
        ticker_change_security_ids = {
            security_id
            for (security_id, _), value in cases.items()
            if "ticker_change" in value["sample_categories"]
        }
        for _, episode in symbol_episodes[
            symbol_episodes["security_id"].isin(ticker_change_security_ids)
        ].iterrows():
            key = (str(episode["security_id"]), str(episode["symbol"]))
            if key not in cases:
                representative = master[master["security_id"] == key[0]].iloc[-1].copy()
                representative["symbol"] = key[1]
                representative["active"] = bool(episode["is_current_symbol"])
                representative["delisted_date"] = episode["symbol_valid_to"]
                _add_case(cases, representative, ("ticker_change",), (), "ticker_change", None)
            valid_from = _as_date(episode.get("symbol_valid_from"))
            valid_to = _as_date(episode.get("symbol_valid_to"))
            cases[key]["symbol_valid_from"] = valid_from
            cases[key]["symbol_valid_to"] = valid_to
            if valid_from and valid_from > HISTORY_START_DATE:
                cases[key]["sample_roles"].add("expected_start")
            if valid_to:
                cases[key]["sample_roles"].add("expected_end")

        episode_bounds = symbol_episodes.set_index(["security_id", "symbol"])
        for key, value in cases.items():
            if key in episode_bounds.index:
                episode = episode_bounds.loc[key]
                if isinstance(episode, pd.DataFrame):
                    episode = episode.iloc[0]
                value["symbol_valid_from"] = _as_date(episode.get("symbol_valid_from"))
                value["symbol_valid_to"] = _as_date(episode.get("symbol_valid_to"))

    if not cases:
        return pd.DataFrame(columns=_sample_columns())
    result = pd.DataFrame(cases.values())
    for column in ("symbol_valid_from", "symbol_valid_to"):
        if column not in result.columns:
            result[column] = None
    result["sample_categories"] = result["sample_categories"].map(lambda values: ";".join(sorted(values)))
    result["sample_roles"] = result["sample_roles"].map(lambda values: ";".join(sorted(values)))
    return result[_sample_columns()].sort_values(["symbol", "security_id"]).reset_index(drop=True)


def run_coverage_audit(
    *,
    snapshot_date: date | None = None,
    reference_dir: str | Path | None = None,
    raw_dir: str | Path | None = None,
    alpaca_client: AlpacaClient | None = None,
) -> dict[str, Path]:
    snapshot_date = snapshot_date or datetime.now(UTC).date()
    audit_end_date = snapshot_date - timedelta(days=1)
    reference_path = resolve_reference_dir(reference_dir)
    raw_root = resolve_raw_dir(raw_dir) / snapshot_date.isoformat()
    security_master_path = reference_path / SECURITY_MASTER_FILE
    if not security_master_path.exists():
        raise FileNotFoundError(f"security master not found: {security_master_path}")
    security_master = pd.read_parquet(security_master_path)
    episodes_path = reference_path / SYMBOL_EPISODES_FILE
    symbol_episodes = pd.read_parquet(episodes_path) if episodes_path.exists() else None
    sample = build_audit_sample(
        security_master,
        audit_end_date=audit_end_date,
        symbol_episodes=symbol_episodes,
    )
    _validate_sample_categories(sample)

    if alpaca_client is None:
        key, secret = get_alpaca_credentials()
        alpaca_client = AlpacaClient(key, secret)

    raw_root.mkdir(parents=True, exist_ok=True)
    sip_results: dict[str, dict[str, Any]] = {}
    iex_results: dict[str, dict[str, Any]] = {}
    symbols = sorted(sample["symbol"].unique())
    comparison_start = audit_end_date - timedelta(days=IEX_COMPARISON_DAYS)
    raw_pages = _audit_raw_pages(
        alpaca_client,
        symbols=symbols,
        audit_end_date=audit_end_date,
        comparison_start=comparison_start,
        sip_results=sip_results,
        iex_results=iex_results,
    )
    raw_path, _ = write_raw_json_pages(
        raw_pages,
        raw_root / "alpaca-coverage-audit.jsonl.gz",
        provider="alpaca",
        source_endpoint="/v2/stocks/bars",
        request_params={
            "sample_version": AUDIT_SAMPLE_VERSION,
            "symbol_count": len(symbols),
            "timeframe": "1Day",
            "start": HISTORY_START_DATE.isoformat(),
            "end": audit_end_date.isoformat(),
            "feed": ["sip", "iex"],
            "adjustment": "raw",
            "asof": "-",
        },
    )
    report = build_coverage_report(
        sample,
        sip_results=sip_results,
        iex_results=iex_results,
        snapshot_date=snapshot_date,
        audit_end_date=audit_end_date,
        comparison_start=comparison_start,
    )
    return write_coverage_report(
        report,
        reference_dir=reference_path,
        snapshot_date=snapshot_date,
        audit_end_date=audit_end_date,
        raw_path=raw_path,
    )


def build_coverage_report(
    sample: pd.DataFrame,
    *,
    sip_results: Mapping[str, Mapping[str, Any]],
    iex_results: Mapping[str, Mapping[str, Any]],
    snapshot_date: date,
    audit_end_date: date,
    comparison_start: date,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in sample.to_dict("records"):
        symbol = record["symbol"]
        sip = dict(sip_results.get(symbol, _empty_result("missing audit result")))
        iex = dict(iex_results.get(symbol, {}))
        event_date = _as_date(record.get("known_event_date"))
        symbol_valid_from = _as_date(record.get("symbol_valid_from"))
        symbol_valid_to = _as_date(record.get("symbol_valid_to"))
        observations = sip.get("observations", [])
        raw_exact = [item for item in observations if item[0] == symbol]
        exact = [
            item
            for item in raw_exact
            if (symbol_valid_from is None or item[1] >= symbol_valid_from)
            and (symbol_valid_to is None or item[1] <= symbol_valid_to)
        ]
        exact_dates = [item[1] for item in exact]
        bar_count = len(exact)
        first_bar_date = min(exact_dates) if exact_dates else None
        last_bar_date = max(exact_dates) if exact_dates else None
        total_volume = sum(item[2] for item in exact)
        pre_event_count = sum(day < event_date for day in exact_dates) if event_date else None
        post_event_count = sum(day >= event_date for day in exact_dates) if event_date else None
        sip_comparison_volume = sum(
            volume for _, day, volume in exact if day >= comparison_start
        )
        iex_exact = [item for item in iex.get("observations", []) if item[0] == symbol]
        iex_volume = sum(item[2] for item in iex_exact)
        ratio = round(sip_comparison_volume / iex_volume, 6) if iex_volume else None
        returned_symbols = sorted(set(sip.get("returned_symbols", [])))
        reasons, status = _assess_row(
            record,
            request_error=sip.get("request_error"),
            returned_symbols=returned_symbols,
            bar_count=bar_count,
            first_bar_date=first_bar_date,
            last_bar_date=last_bar_date,
            event_date=event_date,
            symbol_valid_from=symbol_valid_from,
            symbol_valid_to=symbol_valid_to,
            out_of_episode_bar_count=len(raw_exact) - bar_count,
            pre_event_count=pre_event_count,
            post_event_count=post_event_count,
            audit_end_date=audit_end_date,
            iex_bar_count=len(iex_exact),
            sip_to_iex_volume_ratio=ratio,
        )
        rows.append(
            {
                **record,
                "known_event_date": event_date,
                "symbol_valid_from": symbol_valid_from,
                "symbol_valid_to": symbol_valid_to,
                "request_start_date": HISTORY_START_DATE,
                "request_end_date": audit_end_date,
                "feed": "sip",
                "adjustment": "raw",
                "asof": "-",
                "returned_symbols": ";".join(returned_symbols),
                "raw_bar_count": len(raw_exact),
                "bar_count": bar_count,
                "out_of_episode_bar_count": len(raw_exact) - bar_count,
                "first_bar_date": first_bar_date,
                "last_bar_date": last_bar_date,
                "total_volume": total_volume,
                "pre_event_bar_count": pre_event_count,
                "post_event_bar_count": post_event_count,
                "iex_comparison_start_date": comparison_start if symbol in IEX_COMPARISON_SYMBOLS else None,
                "iex_bar_count": len(iex_exact) if symbol in IEX_COMPARISON_SYMBOLS else None,
                "iex_total_volume": iex_volume if symbol in IEX_COMPARISON_SYMBOLS else None,
                "sip_comparison_volume": sip_comparison_volume if symbol in IEX_COMPARISON_SYMBOLS else None,
                "sip_to_iex_volume_ratio": ratio if symbol in IEX_COMPARISON_SYMBOLS else None,
                "request_error": sip.get("request_error"),
                "audit_status": status,
                "coverage_reason": ";".join(reasons) if reasons else None,
                "snapshot_date": snapshot_date,
            }
        )
    return pd.DataFrame(rows, columns=AUDIT_COLUMNS).sort_values(["symbol", "security_id"]).reset_index(drop=True)


def write_coverage_report(
    report: pd.DataFrame,
    *,
    reference_dir: str | Path,
    snapshot_date: date,
    audit_end_date: date,
    raw_path: Path,
) -> dict[str, Path]:
    output_dir = Path(reference_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / AUDIT_FILE
    metadata_path = output_dir / AUDIT_METADATA_FILE
    summary = summarize_coverage_report(report)
    temp_parquet = _temp_path(output_dir, ".parquet")
    temp_metadata = _temp_path(output_dir, ".json")
    try:
        report.to_parquet(temp_parquet, index=False)
        metadata = {
            **utc_timestamp_metadata(),
            **dataset_identity_metadata(
                dataset_name="reference.alpaca_coverage_audit",
                dataset_group="reference",
                write_mode="replace_snapshot",
                completeness_profile="alpaca_delisted_coverage_gate",
                primary_key=["sample_id"],
                entity_column="security_id",
            ),
            "parquet_file": AUDIT_FILE,
            "provider": "alpaca",
            "sample_version": AUDIT_SAMPLE_VERSION,
            "snapshot_date": snapshot_date.isoformat(),
            "audit_start_date": HISTORY_START_DATE.isoformat(),
            "audit_end_date": audit_end_date.isoformat(),
            "raw_audit_file": os.path.relpath(raw_path, output_dir),
            "request_contract": {
                "endpoint": "/v2/stocks/bars",
                "timeframe": "1Day",
                "feed": "sip",
                "adjustment": "raw",
                "asof": "-",
            },
            **frame_state_metadata(
                report,
                primary_key=["sample_id"],
                required_columns=AUDIT_COLUMNS,
                entity_column="security_id",
            ),
            **summary,
        }
        temp_metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        temp_parquet.replace(output_path)
        temp_metadata.replace(metadata_path)
    finally:
        temp_parquet.unlink(missing_ok=True)
        temp_metadata.unlink(missing_ok=True)
    return {"coverage_audit": output_path, "metadata": metadata_path, "raw": raw_path}


def summarize_coverage_report(report: pd.DataFrame) -> dict[str, Any]:
    if report.empty:
        return {
            "status_counts": {},
            "category_counts": {},
            "delisted_coverage_count": 0,
            "delisted_sample_count": 0,
            "delisted_coverage_pct": None,
            "sip_access_verified": False,
            "sip_volume_exceeds_iex": False,
            "symbol_remapping_observed": False,
            "backfill_recommendation": "blocked",
            "backfill_recommendation_reason": "empty_audit_sample",
        }
    status_counts = {str(key): int(value) for key, value in report["audit_status"].value_counts().sort_index().items()}
    category_counts: dict[str, int] = {}
    for value in report["sample_categories"]:
        for category in str(value).split(";"):
            category_counts[category] = category_counts.get(category, 0) + 1
    non_ambiguous = ~report["sample_roles"].str.contains("ambiguous")
    delisted = report["sample_roles"].str.contains("expected_end") & non_ambiguous
    covered = delisted & report["bar_count"].gt(0) & ~report["coverage_reason"].fillna("").str.contains(
        "no_bars|returned_under_different_symbol|truncated_end"
    )
    delisted_count = int(delisted.sum())
    covered_count = int(covered.sum())
    coverage_pct = round(covered_count / delisted_count, 6) if delisted_count else None
    sip_access = bool((report["bar_count"] > 0).any())
    comparisons = report[report["iex_bar_count"].notna()]
    sip_exceeds_iex = bool(
        not comparisons.empty
        and comparisons["sip_to_iex_volume_ratio"].notna().all()
        and (comparisons["sip_to_iex_volume_ratio"] > 1.0).all()
    )
    remapping = bool(report["coverage_reason"].fillna("").str.contains("returned_under_different_symbol").any())
    ticker_changes = report[report["sample_categories"].str.contains("ticker_change")]
    ticker_change_blockers = ticker_changes["coverage_reason"].fillna("").map(
        lambda value: ";".join(
            reason for reason in value.split(";") if reason and reason != "bars_outside_episode"
        )
    )
    ticker_change_adequate = bool(
        not ticker_changes.empty
        and ticker_changes["bar_count"].gt(0).all()
        and ticker_changes["symbol_valid_from"].notna().all()
        and ticker_change_blockers.eq("").all()
    )
    reasons: list[str] = []
    if not sip_access:
        reasons.append("sip_access_not_verified")
    if coverage_pct is None or coverage_pct < 0.8:
        reasons.append("delisted_coverage_below_80_pct")
    if not sip_exceeds_iex:
        reasons.append("sip_volume_not_confirmed_against_iex")
    if remapping:
        reasons.append("symbol_remapping_observed")
    if not ticker_change_adequate:
        reasons.append("ticker_change_coverage_inadequate")
    return {
        "status_counts": status_counts,
        "category_counts": dict(sorted(category_counts.items())),
        "delisted_coverage_count": covered_count,
        "delisted_sample_count": delisted_count,
        "delisted_coverage_pct": coverage_pct,
        "sip_access_verified": sip_access,
        "sip_volume_exceeds_iex": sip_exceeds_iex,
        "symbol_remapping_observed": remapping,
        "ticker_change_coverage_adequate": ticker_change_adequate,
        "backfill_recommendation": "proceed" if not reasons else "blocked",
        "backfill_recommendation_reason": ";".join(reasons) if reasons else "audit_gate_passed",
    }


def _audit_raw_pages(
    client: AlpacaClient,
    *,
    symbols: list[str],
    audit_end_date: date,
    comparison_start: date,
    sip_results: dict[str, dict[str, Any]],
    iex_results: dict[str, dict[str, Any]],
) -> Iterator[dict[str, Any]]:
    for symbol in symbols:
        yield from _request_pages(
            client,
            symbol=symbol,
            start=HISTORY_START_DATE,
            end=audit_end_date,
            feed="sip",
            destination=sip_results,
        )
    for symbol in sorted(IEX_COMPARISON_SYMBOLS.intersection(symbols)):
        yield from _request_pages(
            client,
            symbol=symbol,
            start=comparison_start,
            end=audit_end_date,
            feed="iex",
            destination=iex_results,
        )


def _request_pages(
    client: AlpacaClient,
    *,
    symbol: str,
    start: date,
    end: date,
    feed: str,
    destination: dict[str, dict[str, Any]],
) -> Iterator[dict[str, Any]]:
    request = {
        "symbols": [symbol],
        "timeframe": "1Day",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "feed": feed,
        "adjustment": "raw",
        "asof": "-",
    }
    result = {"observations": [], "returned_symbols": set(), "request_error": None}
    destination[symbol] = result
    try:
        for payload in client.iter_stock_bar_pages(
            [symbol],
            start=start,
            end=end,
            feed=feed,
            adjustment="raw",
            asof="-",
        ):
            for returned_symbol, bars in payload["bars"].items():
                result["returned_symbols"].add(returned_symbol)
                for bar in bars:
                    if not isinstance(bar, dict):
                        raise ValueError("Alpaca bar must be an object")
                    bar_date = _as_date(bar.get("t"))
                    if bar_date is not None:
                        result["observations"].append(
                            (returned_symbol, bar_date, int(bar.get("v") or 0))
                        )
            yield {"request": request, "response": payload}
    except Exception as exc:
        result["request_error"] = str(exc)
        yield {"request": request, "error": str(exc)}
    finally:
        result["returned_symbols"] = sorted(result["returned_symbols"])


def _assess_row(
    record: Mapping[str, Any],
    *,
    request_error: str | None,
    returned_symbols: list[str],
    bar_count: int,
    first_bar_date: date | None,
    last_bar_date: date | None,
    event_date: date | None,
    symbol_valid_from: date | None,
    symbol_valid_to: date | None,
    out_of_episode_bar_count: int,
    pre_event_count: int | None,
    post_event_count: int | None,
    audit_end_date: date,
    iex_bar_count: int,
    sip_to_iex_volume_ratio: float | None,
) -> tuple[list[str], str]:
    failures: list[str] = []
    warnings: list[str] = []
    symbol = str(record["symbol"])
    roles = set(str(record["sample_roles"]).split(";"))
    if request_error:
        failures.append("request_error")
    if returned_symbols and returned_symbols != [symbol]:
        failures.append("returned_under_different_symbol")
    if bar_count == 0:
        failures.append("no_bars")
    if "ambiguous" in roles:
        failures.append("ambiguous_reused_symbol")
    if out_of_episode_bar_count:
        warnings.append("bars_outside_episode")
    if bar_count:
        if "full_window" in roles and first_bar_date and first_bar_date > HISTORY_START_DATE + timedelta(days=14):
            failures.append("truncated_start")
        if bool(record["active"]) and last_bar_date and last_bar_date < audit_end_date - timedelta(days=ACTIVE_STALENESS_DAYS):
            failures.append("active_history_stale")
        expected_end = symbol_valid_to or (event_date - timedelta(days=1) if event_date else None)
        expected_start = symbol_valid_from or event_date
        if "expected_end" in roles and expected_end and last_bar_date:
            if last_bar_date < expected_end - timedelta(days=DATE_TOLERANCE_DAYS):
                failures.append("truncated_end")
            elif last_bar_date > expected_end + timedelta(days=DATE_TOLERANCE_DAYS):
                warnings.append("bars_after_expected_end")
        if "expected_start" in roles and expected_start and first_bar_date:
            if first_bar_date > expected_start + timedelta(days=DATE_TOLERANCE_DAYS):
                failures.append("truncated_start")
        if "event_both_sides" in roles:
            if pre_event_count == 0:
                failures.append("missing_pre_event_bars")
            if post_event_count == 0:
                failures.append("missing_post_event_bars")
    if symbol in IEX_COMPARISON_SYMBOLS:
        if iex_bar_count == 0:
            failures.append("iex_no_bars")
        elif sip_to_iex_volume_ratio is None or sip_to_iex_volume_ratio <= 1.0:
            warnings.append("sip_volume_not_greater_than_iex")
    reasons = list(dict.fromkeys([*failures, *warnings]))
    return reasons, "fail" if failures else "warning" if warnings else "pass"


def _add_case(
    cases: dict[tuple[str, str], dict[str, Any]],
    row: pd.Series,
    categories: tuple[str, ...],
    roles: tuple[str, ...],
    event_type: str | None,
    event_date: date | None,
) -> None:
    key = (str(row["security_id"]), str(row["symbol"]))
    if key not in cases:
        cases[key] = {
            "sample_id": f"{key[0]}|{key[1]}",
            "security_id": key[0],
            "symbol": key[1],
            "company_name": row.get("company_name"),
            "active": bool(row["active"]),
            "delisted_date": _as_date(row.get("delisted_date")),
            "has_alpaca_asset": bool(row["has_alpaca_asset"]),
            "symbol_identity_conflict": bool(row["symbol_identity_conflict"]),
            "sample_categories": set(),
            "sample_roles": set(),
            "known_event_type": event_type,
            "known_event_date": event_date,
            "symbol_valid_from": None,
            "symbol_valid_to": None,
        }
    cases[key]["sample_categories"].update(categories)
    cases[key]["sample_roles"].update(roles)
    if event_type and not cases[key]["known_event_type"]:
        cases[key]["known_event_type"] = event_type
    if event_date and not cases[key]["known_event_date"]:
        cases[key]["known_event_date"] = event_date


def _deterministic_rows(frame: pd.DataFrame, category: str, count: int) -> pd.DataFrame:
    if frame.empty:
        return frame
    ranked = frame.copy()
    ranked["_audit_rank"] = ranked.apply(
        lambda row: hashlib.sha256(
            f"{AUDIT_SAMPLE_VERSION}|{category}|{row['security_id']}|{row['symbol']}".encode("utf-8")
        ).hexdigest(),
        axis=1,
    )
    return ranked.sort_values(["_audit_rank", "security_id", "symbol"]).head(count).drop(columns=["_audit_rank"])


def _validate_sample_categories(sample: pd.DataFrame) -> None:
    required = {
        "active",
        "recent_delisting",
        "older_delisting",
        "acquisition",
        "ticker_change",
        "ticker_reuse",
        "forward_split",
        "reverse_split",
    }
    observed: set[str] = set()
    for value in sample["sample_categories"]:
        observed.update(str(value).split(";"))
    missing = sorted(required - observed)
    if missing:
        raise ValueError(f"audit sample missing required categories: {missing}")


def _sample_columns() -> list[str]:
    return AUDIT_COLUMNS[:14]


def _empty_result(error: str) -> dict[str, Any]:
    return {"observations": [], "returned_symbols": [], "request_error": error}


def _as_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    return None if pd.isna(parsed) else parsed.date()


def _temp_path(directory: Path, suffix: str) -> Path:
    with tempfile.NamedTemporaryFile(dir=directory, suffix=suffix, delete=False) as temporary:
        return Path(temporary.name)
