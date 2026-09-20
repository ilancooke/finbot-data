from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pandas as pd

from market_data.datasets.alpaca_coverage_audit import (
    AUDIT_COLUMNS,
    build_audit_sample,
    build_coverage_report,
    run_coverage_audit,
    summarize_coverage_report,
)


AUDIT_END = date(2026, 9, 18)
SNAPSHOT = date(2026, 9, 19)


def security(symbol: str, *, security_id: str | None = None, **overrides):
    row = {
        "security_id": security_id or f"figi:{symbol}",
        "symbol": symbol,
        "company_name": f"{symbol} Company",
        "active": True,
        "delisted_date": None,
        "has_alpaca_asset": True,
        "symbol_identity_conflict": False,
        "is_price_coverage_eligible": True,
    }
    row.update(overrides)
    return row


def complete_master() -> pd.DataFrame:
    rows = [
        *(security(symbol) for symbol in ("AAPL", "NVDA", "GE", "AMC")),
        security("TWTR", active=False, delisted_date=date(2022, 10, 31), has_alpaca_asset=False),
        security("ATVI", active=False, delisted_date=date(2023, 10, 16), has_alpaca_asset=False),
        security("CERN", active=False, delisted_date=date(2022, 6, 9)),
        security("WORK", active=False, delisted_date=date(2021, 7, 22)),
        security("CTL", security_id="figi:lumen", active=False, delisted_date=date(2020, 9, 18)),
        security("LUMN", security_id="figi:lumen"),
        security("SYMC", security_id="figi:gen", active=False, delisted_date=date(2019, 11, 5)),
        security("GEN", security_id="figi:gen"),
        security("RECENT", active=False, delisted_date=date(2025, 5, 1), has_alpaca_asset=False),
        security("RECENT2", active=False, delisted_date=date(2024, 6, 1), has_alpaca_asset=False),
        security("OLD", active=False, delisted_date=date(2018, 5, 1), has_alpaca_asset=False),
        security("OLD2", active=False, delisted_date=date(2017, 6, 1), has_alpaca_asset=False),
        security("REUSE", security_id="figi:reuse-old", active=False, delisted_date=date(2020, 1, 1), symbol_identity_conflict=True, is_price_coverage_eligible=False),
        security("REUSE", security_id="figi:reuse-new", symbol_identity_conflict=True, is_price_coverage_eligible=False),
    ]
    return pd.DataFrame(rows)


def test_audit_sample_is_deterministic_and_covers_required_categories():
    master = complete_master()

    first = build_audit_sample(master, audit_end_date=AUDIT_END)
    second = build_audit_sample(master.iloc[::-1].reset_index(drop=True), audit_end_date=AUDIT_END)

    pd.testing.assert_frame_equal(first, second)
    categories = {category for value in first["sample_categories"] for category in value.split(";")}
    assert categories == {
        "active",
        "recent_delisting",
        "older_delisting",
        "acquisition",
        "ticker_change",
        "ticker_reuse",
        "forward_split",
        "reverse_split",
    }
    assert set(first[first["sample_categories"].str.contains("ticker_change")]["symbol"]) == {
        "CTL",
        "GEN",
        "LUMN",
        "SYMC",
    }
    assert len(first[first["symbol"] == "REUSE"]) == 2


def test_coverage_report_flags_truncation_relabeling_and_ambiguity():
    sample = build_audit_sample(complete_master(), audit_end_date=AUDIT_END)
    sip_results = {}
    for symbol in sample["symbol"].unique():
        sip_results[symbol] = {
            "observations": [
                (symbol, date(2016, 1, 4), 100),
                (symbol, AUDIT_END, 200),
            ],
            "returned_symbols": [symbol],
            "request_error": None,
        }
    sip_results["TWTR"] = {
        "observations": [("TWTR", date(2022, 9, 1), 100)],
        "returned_symbols": ["TWTR"],
        "request_error": None,
    }
    sip_results["CTL"] = {
        "observations": [("LUMN", date(2020, 1, 2), 100)],
        "returned_symbols": ["LUMN"],
        "request_error": None,
    }
    iex_results = {
        symbol: {
            "observations": [(symbol, AUDIT_END, 10)],
            "returned_symbols": [symbol],
            "request_error": None,
        }
        for symbol in ("AAPL", "GE", "NVDA")
    }

    report = build_coverage_report(
        sample,
        sip_results=sip_results,
        iex_results=iex_results,
        snapshot_date=SNAPSHOT,
        audit_end_date=AUDIT_END,
        comparison_start=date(2026, 6, 20),
    )

    assert list(report.columns) == AUDIT_COLUMNS
    assert "truncated_end" in report.loc[report["symbol"] == "TWTR", "coverage_reason"].item()
    assert "returned_under_different_symbol" in report.loc[report["symbol"] == "CTL", "coverage_reason"].item()
    assert set(report.loc[report["symbol"] == "REUSE", "audit_status"]) == {"fail"}
    assert report.loc[report["symbol"] == "AAPL", "sip_to_iex_volume_ratio"].item() == 20.0
    summary = summarize_coverage_report(report)
    assert summary["sip_access_verified"] is True
    assert summary["sip_volume_exceeds_iex"] is True
    assert summary["symbol_remapping_observed"] is True
    assert summary["ticker_change_coverage_adequate"] is False
    assert summary["backfill_recommendation"] == "blocked"


class FakeAlpacaClient:
    def iter_stock_bar_pages(self, symbols, *, start, end, feed, adjustment, asof, limit=10000):
        symbol = symbols[0]
        volume = 10 if feed == "iex" else 100
        yield {
            "bars": {
                symbol: [
                    {"t": "2016-01-04T05:00:00Z", "v": volume},
                    {"t": f"{end.isoformat()}T04:00:00Z", "v": volume},
                ]
            },
            "next_page_token": None,
        }


def test_coverage_audit_job_writes_raw_report_and_metadata(tmp_path: Path):
    reference = tmp_path / "reference"
    reference.mkdir()
    complete_master().to_parquet(reference / "security_master.parquet", index=False)

    outputs = run_coverage_audit(
        snapshot_date=SNAPSHOT,
        reference_dir=reference,
        raw_dir=tmp_path / "raw",
        alpaca_client=FakeAlpacaClient(),
    )

    report = pd.read_parquet(outputs["coverage_audit"])
    metadata = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert not report.empty
    assert metadata["dataset_name"] == "reference.alpaca_coverage_audit"
    assert metadata["request_contract"]["asof"] == "-"
    assert metadata["request_contract"]["feed"] == "sip"
    assert metadata["duplicate_key_count"] == 0
    assert metadata["missing_required_columns"] == []
    assert outputs["raw"].exists()
