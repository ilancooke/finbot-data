from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pandas as pd

from market_data.datasets.us_equity_universe import (
    NAREIT_URL,
    SECURITY_MASTER_COLUMNS,
    SP500_URL,
    TICKERS_COLUMNS,
    build_exclusions,
    build_security_master,
    build_tickers,
    download_and_write_security_master,
    parse_nareit_symbols,
    parse_sp500_classifications,
    write_universe_datasets,
)


SNAPSHOT_DATE = date(2026, 9, 18)


def massive_row(symbol: str, name: str, **overrides):
    row = {
        "ticker": symbol,
        "name": name,
        "type": "CS",
        "market": "stocks",
        "locale": "us",
        "currency_name": "usd",
        "primary_exchange": "XNAS",
        "active": True,
        "last_updated_utc": "2026-09-18T00:00:00Z",
    }
    row.update(overrides)
    return row


def alpaca_asset(symbol: str, **overrides):
    row = {
        "id": f"asset-{symbol}",
        "symbol": symbol,
        "exchange": "NASDAQ",
        "status": "active",
        "tradable": True,
    }
    row.update(overrides)
    return row


def test_reference_html_parsers_use_the_matching_table_header_row():
    nareit_html = """
    <table><tr><th>Unrelated</th></tr><tr><td>NOPE</td></tr></table>
    <table>
      <tr><th>Company</th><th>Ticker Symbol</th></tr>
      <tr><td>Example REIT</td><td>EXR</td></tr>
      <tr><td>Classed REIT</td><td>ABC.P</td></tr>
    </table>
    """
    sp500_html = """
    <table>
      <tr><th>Symbol</th><th>Security</th><th>GICS Sector</th><th>GICS Sub-Industry</th></tr>
      <tr><td>BRK.B</td><td>Berkshire Hathaway</td><td>Financials</td><td>Multi-Sector Holdings</td></tr>
      <tr><td>PLD</td><td>Prologis</td><td>Real Estate</td><td>Industrial REITs</td></tr>
    </table>
    """

    assert parse_nareit_symbols(nareit_html) == {"ABC.P", "EXR"}
    classifications = parse_sp500_classifications(sp500_html)
    assert classifications[["symbol", "sector", "industry"]].to_dict("records") == [
        {
            "symbol": "BRK.B",
            "sector": "Financials",
            "industry": "Multi-Sector Holdings",
        },
        {"symbol": "PLD", "sector": "Real Estate", "industry": "Industrial REITs"},
    ]


def test_security_master_preserves_symbol_episodes_and_reconciles_provider_spelling():
    rows = [
        massive_row("OLD", "Renamed Company", share_class_figi="BBG001", active=False, delisted_utc="2021-05-01"),
        massive_row("NEW", "Renamed Company", share_class_figi="BBG001"),
        massive_row("BRK.B", "Berkshire Hathaway", share_class_figi="BBG002"),
        massive_row("NOASSET", "Historical Only", share_class_figi="BBG003", active=False, delisted_utc="2020-01-01"),
    ]

    frame = build_security_master(
        rows,
        [alpaca_asset("NEW"), alpaca_asset("BRK-B")],
        snapshot_date=SNAPSHOT_DATE,
    )

    episodes = frame[frame["share_class_figi"] == "BBG001"]
    assert episodes["security_id"].nunique() == 1
    assert set(episodes["symbol"]) == {"NEW", "OLD"}
    assert frame.loc[frame["symbol"] == "BRK.B", "alpaca_symbol"].item() == "BRK-B"
    assert frame.loc[frame["symbol"] == "BRK.B", "alpaca_asset_match_status"].item() == "normalized_symbol"
    assert frame.loc[frame["symbol"] == "NOASSET", "has_alpaca_asset"].item() is False
    assert frame.loc[frame["symbol"] == "NOASSET", "coverage_reason"].item() == "no_current_alpaca_asset"
    assert frame.loc[frame["symbol"] == "NOASSET", "is_price_coverage_eligible"].item() is True
    tickers = build_tickers(frame)
    assert tickers.loc[tickers["symbol"] == "BRK.B", "provider_symbol"].item() == "BRK-B"
    assert "NOASSET" in set(tickers["provider_symbol"])


def test_security_master_filters_scope_and_reports_reused_symbols():
    rows = [
        massive_row("REUSE", "Old Issuer", active=False, delisted_utc="2020-01-01"),
        massive_row("REUSE", "New Issuer"),
        massive_row("REIT", "Example Realty", share_class_figi="REIT1"),
        massive_row("BDC", "Example Business Development Company", share_class_figi="BDC1"),
        massive_row("LP", "Example Holdings L.P.", share_class_figi="LP1"),
        massive_row("OTC", "OTC Company", share_class_figi="OTC1", primary_exchange="OTCM"),
        massive_row("OLD", "Too Old", share_class_figi="OLD1", active=False, delisted_utc="2015-12-31"),
        massive_row("GOOD", "Good Company", share_class_figi="GOOD1"),
    ]
    frame = build_security_master(
        rows,
        [],
        snapshot_date=SNAPSHOT_DATE,
        nareit_symbols={"REIT"},
    )

    reasons = frame.set_index("symbol")["exclusion_reason"].to_dict()
    assert "ambiguous_reused_symbol" in reasons["REUSE"]
    assert reasons["REIT"] == "known_reit_or_reoc"
    assert reasons["BDC"] == "business_development_company"
    assert reasons["LP"] == "limited_partnership"
    assert reasons["OTC"] == "ineligible_exchange"
    assert reasons["OLD"] == "delisted_before_history_window"
    assert reasons["GOOD"] is pd.NA or pd.isna(reasons["GOOD"])
    assert set(build_tickers(frame)["symbol"]) == {"GOOD"}
    assert set(build_exclusions(frame)["symbol"]) == {"BDC", "LP", "OLD", "OTC", "REIT", "REUSE"}


def test_write_universe_datasets_has_stable_keys_and_metadata(tmp_path: Path):
    master = build_security_master(
        [massive_row("GOOD", "Good Company", share_class_figi="GOOD1")],
        [alpaca_asset("GOOD")],
        snapshot_date=SNAPSHOT_DATE,
    )
    outputs = write_universe_datasets(
        master,
        build_tickers(master),
        build_exclusions(master),
        reference_dir=tmp_path,
        snapshot_date=SNAPSHOT_DATE,
    )

    assert set(outputs) == {"security_master", "tickers", "universe_exclusions"}
    assert list(pd.read_parquet(outputs["security_master"]).columns) == SECURITY_MASTER_COLUMNS
    assert list(pd.read_parquet(outputs["tickers"]).columns) == TICKERS_COLUMNS
    metadata = json.loads((tmp_path / "security_master.metadata.json").read_text(encoding="utf-8"))
    assert metadata["dataset_name"] == "reference.security_master"
    assert metadata["duplicate_key_count"] == 0
    assert metadata["missing_required_columns"] == []
    assert metadata["price_coverage_eligible_count"] == 1


class FakeMassiveClient:
    def iter_pages(self, path, params):
        assert path == "/v3/reference/tickers"
        active = params["active"] == "true"
        symbol = "ACTIVE" if active else "DELISTED"
        yield {
            "results": [
                massive_row(
                    symbol,
                    f"{symbol} Company",
                    share_class_figi=f"FIGI-{symbol}",
                    active=active,
                    **({} if active else {"delisted_utc": "2020-01-01"}),
                )
            ]
        }


class FakeAlpacaClient:
    def get_assets(self, *, asset_class):
        assert asset_class == "us_equity"
        return [alpaca_asset("ACTIVE")]


def test_download_job_preserves_raw_inputs_and_writes_all_outputs(tmp_path: Path):
    nareit_html = "<table><tr><th>Ticker</th></tr><tr><td>NONE</td></tr></table>"
    sp500_html = (
        "<table><tr><th>Symbol</th><th>GICS Sector</th><th>GICS Sub-Industry</th></tr>"
        "<tr><td>ACTIVE</td><td>Industrials</td><td>Machinery</td></tr></table>"
    )

    def download(url):
        return nareit_html if url == NAREIT_URL else sp500_html if url == SP500_URL else ""

    outputs = download_and_write_security_master(
        snapshot_date=SNAPSHOT_DATE,
        reference_dir=tmp_path / "reference",
        raw_dir=tmp_path / "raw",
        massive_client=FakeMassiveClient(),
        alpaca_client=FakeAlpacaClient(),
        text_downloader=download,
    )

    assert len(pd.read_parquet(outputs["security_master"])) == 2
    assert len(pd.read_parquet(outputs["tickers"])) == 2
    raw_snapshot = tmp_path / "raw" / SNAPSHOT_DATE.isoformat()
    assert (raw_snapshot / "massive-common-stocks.jsonl.gz").exists()
    assert (raw_snapshot / "alpaca-assets.jsonl.gz").exists()
    assert (raw_snapshot / "nareit-ticker-directory.html").exists()
    assert (raw_snapshot / "sp500.html").exists()
