from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pandas as pd

from market_data.datasets import daily_bars
from market_data.datasets.daily_bars import days_window_start_date, download_history, window_start_date
from market_data.datasets.daily_prices import (
    CANONICAL_PRICE_COLUMNS,
    HISTORICAL_PARQUET_FILE,
    build_historical_from_files,
    normalize_price_frame,
    request_bulk_price_files,
    update_historical,
)
from market_data.datasets.financials import download_financials_history, read_financial_statement
from market_data.datasets.fundamentals import (
    SF1_PARQUET_FILE,
    build_sf1_from_files,
    normalize_sf1_frame,
    request_bulk_sf1_files,
    update_sf1,
)
from market_data.datasets.ratios import download_ratios_snapshot, read_ratios
from market_data.datasets.related_tickers import download_related_tickers_snapshot, read_related_tickers
from market_data.datasets.ticker_universe import (
    download_and_write_ticker_universe,
    filter_tickers,
    normalize_tickers_frame,
)
from market_data.datasets.ticker_details import (
    download_ticker_details_snapshot,
    read_ticker_details,
    write_ticker_details_snapshot,
)
from market_data.http import MassiveClient, MassiveHttpError
from market_data.normalize import BAR_COLUMNS, normalize_bars_frame
from market_data.providers.nasdaq_data_link import NasdaqDataLinkClient, NasdaqDataLinkError
from market_data.providers.massive import (
    BALANCE_SHEET_COLUMNS,
    CASH_FLOW_STATEMENT_COLUMNS,
    INCOME_STATEMENT_COLUMNS,
    RATIO_COLUMNS,
    RELATED_TICKER_COLUMNS,
    TICKER_DETAIL_COLUMNS,
    normalize_financial_statement_response,
    normalize_grouped_daily_response,
    normalize_ratios_response,
    normalize_related_tickers_response,
    normalize_ticker_details_response,
)
from market_data.storage import write_daily_snapshot
from market_data.universe import (
    fetch_ticker_universe,
    filter_symbol_ticker_universe,
    filter_common_stocks,
    normalize_tickers,
    read_known_universe_symbols,
    read_ticker_universe,
    write_ticker_universe,
)
from scripts.download_daily_bars import main
from scripts.download_financials import main as financials_main
from scripts.download_fundamentals import main as fundamentals_main
from scripts.download_historical_prices import main as historical_prices_main
from scripts.download_ratios import main as ratios_main
from scripts.download_related_tickers import main as related_tickers_main
from scripts.download_ticker_universe import main as ticker_universe_main
from scripts.download_ticker_details import main as ticker_details_main
from scripts.download_tickers import main as tickers_main
from scripts.update_fundamentals import main as update_fundamentals_main


class FakeResponse:
    def __init__(self, status_code, payload=None, text="", headers=None):
        self.status_code = status_code
        self.payload = payload or {}
        self.text = text
        self.headers = headers or {}

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return self.responses.pop(0)


def test_normalize_bars_frame_uses_canonical_schema():
    provider_frame = pd.DataFrame(
        {
            "timestamp": ["2026-05-01"],
            "symbol": ["AAPL"],
            "open": [100.0],
            "high": [105.0],
            "low": [99.0],
            "close": [104.0],
            "volume": [1000],
        }
    ).set_index("timestamp")

    normalized = normalize_bars_frame(provider_frame)

    assert list(normalized.columns) == BAR_COLUMNS
    assert normalized.loc[0, "date"] == date(2026, 5, 1)


def test_massive_client_collects_paginated_results():
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "results": [{"ticker": "AAPL"}],
                    "next_url": "https://api.massive.com/v3/reference/tickers?cursor=abc",
                },
            ),
            FakeResponse(200, {"results": [{"ticker": "MSFT"}]}),
        ]
    )
    client = MassiveClient(api_key="test-key", session=session)

    rows = client.get_paginated("/v3/reference/tickers", params={"market": "stocks"}, calls_per_minute=0)

    assert rows == [{"ticker": "AAPL"}, {"ticker": "MSFT"}]
    assert session.calls[0]["params"]["apiKey"] == "test-key"
    assert session.calls[0]["params"]["market"] == "stocks"
    assert session.calls[1]["params"]["apiKey"] == "test-key"


def test_massive_client_error_message_omits_api_key():
    session = FakeSession([FakeResponse(403, text="forbidden")])
    client = MassiveClient(api_key="secret-key", session=session)

    try:
        client.get_json("/v3/reference/tickers")
    except MassiveHttpError as exc:
        assert "secret-key" not in str(exc)
        assert exc.status_code == 403
    else:
        raise AssertionError("Expected MassiveHttpError")


def test_nasdaq_data_link_client_collects_paginated_table_rows():
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "datatable": {
                        "data": [["AAPL", "2018-09-04", 100.0]],
                        "columns": [
                            {"name": "ticker"},
                            {"name": "date"},
                            {"name": "close"},
                        ],
                    },
                    "meta": {"next_cursor_id": "cursor-1"},
                },
            ),
            FakeResponse(
                200,
                {
                    "datatable": {
                        "data": [["MSFT", "2018-09-04", 200.0]],
                        "columns": [
                            {"name": "ticker"},
                            {"name": "date"},
                            {"name": "close"},
                        ],
                    },
                    "meta": {"next_cursor_id": None},
                },
            ),
        ]
    )
    client = NasdaqDataLinkClient(api_key="test-key", session=session)

    rows = client.get_table("SHARADAR/SEP", params={"ticker": "AAPL,MSFT"})

    assert rows == [
        {"ticker": "AAPL", "date": "2018-09-04", "close": 100.0},
        {"ticker": "MSFT", "date": "2018-09-04", "close": 200.0},
    ]
    assert session.calls[0]["params"]["api_key"] == "test-key"
    assert session.calls[0]["params"]["ticker"] == "AAPL,MSFT"
    assert session.calls[1]["params"]["qopts.cursor_id"] == "cursor-1"


def test_nasdaq_data_link_client_error_message_omits_api_key():
    session = FakeSession([FakeResponse(403, text="forbidden")])
    client = NasdaqDataLinkClient(api_key="secret-key", session=session)

    try:
        client.get_json("/api/v3/datatables/SHARADAR/SEP.json")
    except NasdaqDataLinkError as exc:
        assert "secret-key" not in str(exc)
        assert exc.status_code == 403
    else:
        raise AssertionError("Expected NasdaqDataLinkError")


def test_fetch_ticker_universe_normalizes_and_sorts_rows():
    class FakeClient:
        def get_paginated(self, path, params=None, calls_per_minute=0):
            assert path == "/v3/reference/tickers"
            assert params["market"] == "stocks"
            assert params["active"] == "true"
            assert calls_per_minute == 5
            return [
                {"ticker": "msft", "name": "Microsoft", "market": "stocks", "active": True},
                {"ticker": "AAPL", "name": "Apple", "market": "stocks", "active": True},
            ]

    tickers = fetch_ticker_universe(FakeClient(), as_of=date(2026, 5, 1), calls_per_minute=5)

    assert tickers["ticker"].tolist() == ["AAPL", "MSFT"]
    assert "primary_exchange" in tickers.columns


def test_filter_common_stocks_keeps_active_listed_us_common_stocks():
    tickers = normalize_tickers(
        [
            {
                "ticker": "AAPL",
                "market": "stocks",
                "locale": "us",
                "primary_exchange": "XNAS",
                "type": "CS",
                "active": True,
            },
            {
                "ticker": "SPY",
                "market": "stocks",
                "locale": "us",
                "primary_exchange": "ARCX",
                "type": "ETF",
                "active": True,
            },
            {
                "ticker": "OTC1",
                "market": "stocks",
                "locale": "us",
                "primary_exchange": None,
                "type": "CS",
                "active": True,
            },
            {
                "ticker": "OLD",
                "market": "stocks",
                "locale": "us",
                "primary_exchange": "XNYS",
                "type": "CS",
                "active": False,
            },
        ]
    )

    filtered = filter_common_stocks(tickers)

    assert filtered["ticker"].tolist() == ["AAPL"]


def test_read_known_universe_symbols_accepts_sp_constituent_csv(tmp_path):
    csv_path = tmp_path / "sp500constituents.csv"
    csv_path.write_text(
        "Symbol,Security,GICS Sector\n"
        "aapl,Apple Inc.,Information Technology\n"
        "BRK.B,Berkshire Hathaway,Financials\n"
        "AAPL,Apple Inc.,Information Technology\n",
        encoding="utf-8",
    )

    symbols = read_known_universe_symbols(csv_path)

    assert symbols == ["AAPL", "BRK.B"]


def test_filter_symbol_ticker_universe_keeps_only_configured_symbols():
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "market": "stocks"},
            {"ticker": "msft", "market": "stocks"},
            {"ticker": "IBM", "market": "stocks"},
        ]
    )

    filtered = filter_symbol_ticker_universe(tickers, ["AAPL", "MSFT"])

    assert filtered["ticker"].tolist() == ["AAPL", "MSFT"]


def test_write_and_read_ticker_universe(tmp_path):
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
        ]
    )

    output_path = write_ticker_universe(tickers, tmp_path, metadata={"provider": "massive"})
    restored = read_ticker_universe(tmp_path)
    metadata = json.loads((tmp_path / "tickers.metadata.json").read_text(encoding="utf-8"))

    assert output_path == tmp_path / "tickers.parquet"
    assert restored["ticker"].tolist() == ["AAPL"]
    assert metadata["provider"] == "massive"
    assert metadata["rows"] == 1
    assert metadata["tickers"] == 1
    assert metadata["parquet_file"] == "tickers.parquet"


def test_download_tickers_cli_writes_universe(tmp_path, monkeypatch):
    known_universe_file = tmp_path / "sp500constituents.csv"
    known_universe_file.write_text("Symbol,Security\nAAPL,Apple Inc.\nMSFT,Microsoft Corp.\n", encoding="utf-8")

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

    def fake_fetch(client, as_of=None, active=True, calls_per_minute=0):
        assert client.api_key == "test-key"
        assert as_of == date(2026, 5, 1)
        assert active is True
        assert calls_per_minute == 5
        return normalize_tickers(
            [
                {
                    "ticker": "aapl",
                    "name": "Apple",
                    "market": "stocks",
                    "locale": "us",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                },
                {
                    "ticker": "spy",
                    "name": "SPDR S&P 500 ETF",
                    "market": "stocks",
                    "locale": "us",
                    "primary_exchange": "ARCX",
                    "type": "ETF",
                    "active": True,
                },
            ]
        )

    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setattr("scripts.download_tickers.MassiveClient", FakeClient)
    monkeypatch.setattr("scripts.download_tickers.fetch_ticker_universe", fake_fetch)

    exit_code = tickers_main(
        [
            "--date",
            "2026-05-01",
            "--calls-per-minute",
            "5",
            "--output-dir",
            str(tmp_path),
            "--known-universe-file",
            str(known_universe_file),
        ]
    )
    tickers = pd.read_parquet(tmp_path / "tickers.parquet")
    metadata = json.loads((tmp_path / "tickers.metadata.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert tickers["ticker"].tolist() == ["AAPL"]
    assert metadata["provider"] == "massive"
    assert metadata["dataset"] == "ticker_universe"
    assert metadata["mode"] == "replace"
    assert metadata["universe_strategy"] == "sp500"
    assert metadata["universe_name"] == "S&P 500"
    assert metadata["known_universe_file"] == str(known_universe_file)
    assert metadata["known_universe_symbols"] == 2
    assert metadata["universe_date"] == "2026-05-01"
    assert metadata["input_rows"] == 2
    assert metadata["common_stock_rows"] == 1
    assert metadata["output_rows"] == 1
    assert metadata["rows"] == 1
    assert metadata["filter"]["type"] == "CS"
    assert metadata["filter"]["known_universe"] == "sp500"
    assert metadata["filter"]["missing_known_tickers"] == ["MSFT"]


def test_normalize_massive_grouped_daily_response_uses_canonical_schema():
    payload = {
        "status": "OK",
        "results": [
            {
                "T": "aapl",
                "t": 1777593600000,
                "o": 100.0,
                "h": 105.0,
                "l": 99.0,
                "c": 104.0,
                "v": 1000,
            },
            {
                "T": "MSFT",
                "t": 1777593600000,
                "o": 200.0,
                "h": 205.0,
                "l": 199.0,
                "c": 204.0,
                "v": 2000,
            },
        ],
    }

    normalized = normalize_grouped_daily_response(payload, date(2026, 5, 1))

    assert list(normalized.columns) == BAR_COLUMNS
    assert normalized["symbol"].tolist() == ["AAPL", "MSFT"]
    assert normalized["date"].tolist() == [date(2026, 5, 1), date(2026, 5, 1)]


def test_normalize_massive_ticker_details_response_keeps_native_sic_fields():
    payload = {
        "status": "OK",
        "results": {
            "ticker": "aapl",
            "name": "Apple Inc.",
            "market": "stocks",
            "locale": "us",
            "primary_exchange": "XNAS",
            "type": "CS",
            "active": True,
            "sic_code": "3571",
            "sic_description": "ELECTRONIC COMPUTERS",
            "description": "Consumer electronics and services company.",
            "homepage_url": "https://www.apple.com",
            "market_cap": 1000,
            "total_employees": 100,
        },
    }

    normalized = normalize_ticker_details_response(payload, "AAPL")

    assert list(normalized.columns) == TICKER_DETAIL_COLUMNS
    assert normalized.loc[0, "ticker"] == "AAPL"
    assert normalized.loc[0, "sic_code"] == "3571"
    assert normalized.loc[0, "sic_description"] == "ELECTRONIC COMPUTERS"
    assert "sector" not in normalized.columns
    assert "gics_sector" not in normalized.columns


def test_download_ticker_details_snapshot_reuses_cached_rows_and_fetches_missing(tmp_path, monkeypatch):
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
            {"ticker": "msft", "name": "Microsoft", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, tmp_path, metadata={"provider": "massive"})
    write_ticker_details_snapshot(
        pd.DataFrame(
            [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "sic_code": "3571",
                    "sic_description": "ELECTRONIC COMPUTERS",
                }
            ]
        ),
        tmp_path,
        metadata={"provider": "massive"},
    )
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    calls = []

    def fake_downloader(ticker, api_key, as_of):
        calls.append(ticker)
        assert api_key == "test-key"
        assert as_of == date(2026, 5, 1)
        return pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "name": "Microsoft Corporation",
                    "sic_code": "7372",
                    "sic_description": "SERVICES-PREPACKAGED SOFTWARE",
                }
            ]
        )

    output_path = download_ticker_details_snapshot(
        details_date=date(2026, 5, 1),
        input_dir=tmp_path,
        output_dir=tmp_path,
        calls_per_minute=0,
        downloader=fake_downloader,
    )
    details = read_ticker_details(tmp_path)
    metadata = json.loads((tmp_path / "ticker_details.metadata.json").read_text(encoding="utf-8"))

    assert output_path == tmp_path / "ticker_details.parquet"
    assert calls == ["MSFT"]
    assert details["ticker"].tolist() == ["AAPL", "MSFT"]
    assert details.loc[details["ticker"] == "AAPL", "sic_code"].item() == "3571"
    assert details.loc[details["ticker"] == "MSFT", "sic_code"].item() == "7372"
    assert metadata["dataset"] == "ticker_details"
    assert metadata["mode"] == "cache-merge"
    assert metadata["details_date"] == "2026-05-01"
    assert metadata["cached_tickers"] == 1
    assert metadata["requested_tickers"] == 1
    assert metadata["fetched_tickers"] == 1


def test_download_ticker_details_cli_writes_details(tmp_path, monkeypatch):
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, tmp_path, metadata={"provider": "massive"})

    def fake_download_ticker_details(ticker, api_key, as_of=None):
        assert ticker == "AAPL"
        assert api_key == "test-key"
        assert as_of == date(2026, 5, 1)
        return pd.DataFrame(
            [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "sic_code": "3571",
                    "sic_description": "ELECTRONIC COMPUTERS",
                }
            ]
        )

    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setattr("market_data.datasets.ticker_details.download_ticker_details", fake_download_ticker_details)

    exit_code = ticker_details_main(
        [
            "--date",
            "2026-05-01",
            "--input-dir",
            str(tmp_path),
            "--calls-per-minute",
            "0",
        ]
    )
    details = pd.read_parquet(tmp_path / "ticker_details.parquet")
    metadata = json.loads((tmp_path / "ticker_details.metadata.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert details["ticker"].tolist() == ["AAPL"]
    assert details["sic_code"].tolist() == ["3571"]
    assert "sector" not in details.columns
    assert metadata["provider"] == "massive"
    assert metadata["dataset"] == "ticker_details"
    assert metadata["requested_tickers"] == 1


def test_normalize_massive_related_tickers_response_uses_source_and_related_schema():
    payload = {
        "status": "OK",
        "ticker": "AAPL",
        "results": [
            {"ticker": "MSFT"},
            {"ticker": "GOOGL"},
            {"ticker": "MSFT"},
            {},
        ],
    }

    normalized = normalize_related_tickers_response(payload, "aapl")

    assert list(normalized.columns) == RELATED_TICKER_COLUMNS
    assert normalized["ticker"].tolist() == ["AAPL", "AAPL"]
    assert normalized["related_ticker"].tolist() == ["MSFT", "GOOGL"]
    assert normalized["result_order"].tolist() == [1, 2]


def test_normalize_massive_ratios_response_uses_latest_ratios_schema():
    payload = {
        "status": "OK",
        "results": [
            {
                "ticker": "aapl",
                "cik": "320193",
                "date": "2024-09-19",
                "price": 228.87,
                "average_volume": 47500000,
                "market_cap": 3479770835190,
                "earnings_per_share": 6.57,
                "price_to_earnings": 34.84,
                "price_to_book": 52.16,
                "price_to_sales": 9.02,
                "price_to_cash_flow": 30.78,
                "price_to_free_cash_flow": 33.35,
                "dividend_yield": 0.0044,
                "return_on_assets": 0.3075,
                "return_on_equity": 1.5284,
                "debt_to_equity": 1.52,
                "current": 0.68,
                "quick": 0.63,
                "cash": 0.19,
                "ev_to_sales": 9.22,
                "ev_to_ebitda": 26.98,
                "enterprise_value": 3555509835190,
                "free_cash_flow": 104339000000,
            }
        ],
    }

    normalized = normalize_ratios_response(payload)

    assert list(normalized.columns) == RATIO_COLUMNS
    assert normalized.loc[0, "ticker"] == "AAPL"
    assert normalized.loc[0, "date"] == date(2024, 9, 19)
    assert normalized.loc[0, "price_to_earnings"] == 34.84


def test_normalize_financial_statement_response_expands_to_universe_tickers_and_nulls_missing_fields():
    payload = {
        "status": "OK",
        "results": [
            {
                "cik": "0000320193",
                "tickers": ["aapl", "AAPL.B"],
                "period_end": "2025-06-28",
                "filing_date": "2025-08-01",
                "fiscal_year": 2025,
                "fiscal_quarter": 3,
                "timeframe": "quarterly",
                "total_assets": 331495000000,
            }
        ],
    }

    normalized = normalize_financial_statement_response(payload, "balance_sheets", ticker_universe={"AAPL"})

    assert list(normalized.columns) == BALANCE_SHEET_COLUMNS
    assert normalized["ticker"].tolist() == ["AAPL"]
    assert normalized.loc[0, "period_end"] == date(2025, 6, 28)
    assert normalized.loc[0, "filing_date"] == date(2025, 8, 1)
    assert normalized.loc[0, "tickers"] == ["AAPL", "AAPL.B"]
    assert normalized.loc[0, "total_assets"] == 331495000000
    assert pd.isna(normalized.loc[0, "accounts_payable"])


def test_financial_statement_schemas_include_documented_statement_fields():
    assert "total_liabilities_and_equity" in BALANCE_SHEET_COLUMNS
    assert "net_cash_from_operating_activities" in CASH_FLOW_STATEMENT_COLUMNS
    assert "revenue" in INCOME_STATEMENT_COLUMNS


def test_download_financials_history_writes_statement_files_for_ticker_universe(tmp_path, monkeypatch):
    reference_dir = tmp_path / "reference"
    financials_dir = tmp_path / "financials"
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
            {"ticker": "msft", "name": "Microsoft", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, reference_dir, metadata={"provider": "massive"})
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    calls = []

    def fake_downloader(statement, api_key, start_date, end_date, batch, limit, calls_per_minute):
        calls.append((statement, batch))
        assert api_key == "test-key"
        assert start_date == date(2024, 5, 2)
        assert end_date == date(2026, 5, 1)
        assert limit == 100
        assert calls_per_minute == 0
        if statement == "balance_sheets":
            return [
                {
                    "cik": "0000320193",
                    "tickers": ["AAPL"],
                    "period_end": "2025-06-28",
                    "filing_date": "2025-08-01",
                    "fiscal_year": 2025,
                    "fiscal_quarter": 3,
                    "timeframe": "quarterly",
                    "total_assets": 331495000000,
                },
                {
                    "cik": "0000000000",
                    "tickers": ["SPY"],
                    "period_end": "2025-06-28",
                    "timeframe": "quarterly",
                    "total_assets": 1,
                },
            ]
        return []

    output_paths = download_financials_history(
        end_date=date(2026, 5, 1),
        years=2,
        statements=["balance_sheets"],
        input_dir=reference_dir,
        output_dir=financials_dir,
        calls_per_minute=0,
        limit=100,
        ticker_batch_size=2,
        downloader=fake_downloader,
    )
    balance_sheets = read_financial_statement(financials_dir, "balance_sheets")
    metadata = json.loads((financials_dir / "balance_sheets.metadata.json").read_text(encoding="utf-8"))

    assert output_paths["balance_sheets"] == financials_dir / "balance_sheets.parquet"
    assert calls == [("balance_sheets", ["AAPL", "MSFT"])]
    assert balance_sheets["ticker"].tolist() == ["AAPL"]
    assert balance_sheets["total_assets"].tolist() == [331495000000]
    assert metadata["provider"] == "massive"
    assert metadata["dataset"] == "balance_sheets"
    assert metadata["mode"] == "replace"
    assert metadata["requested_start_date"] == "2024-05-02"
    assert metadata["requested_end_date"] == "2026-05-01"
    assert metadata["history_years"] == 2
    assert metadata["input_tickers"] == 2
    assert metadata["requested_tickers"] == 2
    assert metadata["raw_rows"] == 2
    assert metadata["output_rows"] == 1
    assert metadata["data_min_date"] == "2025-06-28"
    assert metadata["data_max_date"] == "2025-06-28"
    assert metadata["data_min_period_end"] == "2025-06-28"
    assert metadata["data_max_period_end"] == "2025-06-28"


def test_download_financials_cli_writes_selected_statement(tmp_path, monkeypatch):
    reference_dir = tmp_path / "reference"
    financials_dir = tmp_path / "financials"
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, reference_dir, metadata={"provider": "massive"})

    def fake_download_financial_statement_rows(statement, api_key, start_date, end_date, tickers, limit=50000, calls_per_minute=0):
        assert statement == "income_statements"
        assert api_key == "test-key"
        assert start_date == date(2025, 5, 2)
        assert end_date == date(2026, 5, 1)
        assert tickers == ["AAPL"]
        assert limit == 100
        assert calls_per_minute == 0
        return [
            {
                "cik": "0000320193",
                "tickers": ["AAPL"],
                "period_end": "2025-06-28",
                "filing_date": "2025-08-01",
                "fiscal_year": 2025,
                "fiscal_quarter": 3,
                "timeframe": "quarterly",
                "revenue": 94036000000,
            }
        ]

    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setattr("market_data.datasets.financials.download_financial_statement_rows", fake_download_financial_statement_rows)

    exit_code = financials_main(
        [
            "--end-date",
            "2026-05-01",
            "--years",
            "1",
            "--statement",
            "income_statements",
            "--input-dir",
            str(reference_dir),
            "--output-dir",
            str(financials_dir),
            "--calls-per-minute",
            "0",
            "--limit",
            "100",
        ]
    )
    income_statements = pd.read_parquet(financials_dir / "income_statements.parquet")
    metadata = json.loads((financials_dir / "income_statements.metadata.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert income_statements["ticker"].tolist() == ["AAPL"]
    assert income_statements["revenue"].tolist() == [94036000000]
    assert metadata["dataset"] == "income_statements"
    assert metadata["requested_start_date"] == "2025-05-02"
    assert metadata["output_rows"] == 1


def test_download_ratios_snapshot_filters_to_ticker_universe(tmp_path, monkeypatch):
    reference_dir = tmp_path / "reference"
    ratios_dir = tmp_path / "ratios"
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
            {"ticker": "msft", "name": "Microsoft", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, reference_dir, metadata={"provider": "massive"})
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    calls = []

    def fake_downloader(api_key, limit, calls_per_minute):
        calls.append((api_key, limit, calls_per_minute))
        return pd.DataFrame(
            [
                {"ticker": "AAPL", "date": date(2024, 9, 19), "price_to_earnings": 34.84},
                {"ticker": "MSFT", "date": date(2024, 9, 19), "price_to_earnings": 35.25},
                {"ticker": "SPY", "date": date(2024, 9, 19), "price_to_earnings": 25.0},
            ]
        )

    output_path = download_ratios_snapshot(
        input_dir=reference_dir,
        output_dir=ratios_dir,
        calls_per_minute=0,
        limit=100,
        downloader=fake_downloader,
    )
    ratios = read_ratios(ratios_dir)
    metadata = json.loads((ratios_dir / "ratios.metadata.json").read_text(encoding="utf-8"))

    assert output_path == ratios_dir / "ratios.parquet"
    assert calls == [("test-key", 100, 0)]
    assert ratios["ticker"].tolist() == ["AAPL", "MSFT"]
    assert metadata["provider"] == "massive"
    assert metadata["dataset"] == "ratios"
    assert metadata["mode"] == "replace"
    assert metadata["input_tickers"] == 2
    assert metadata["raw_rows"] == 3
    assert metadata["output_rows"] == 2
    assert metadata["data_min_date"] == "2024-09-19"
    assert metadata["data_max_date"] == "2024-09-19"


def test_download_ratios_cli_writes_ratios(tmp_path, monkeypatch):
    reference_dir = tmp_path / "reference"
    ratios_dir = tmp_path / "ratios"
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, reference_dir, metadata={"provider": "massive"})

    def fake_download_ratios(api_key, limit=50000, calls_per_minute=0):
        assert api_key == "test-key"
        assert limit == 100
        assert calls_per_minute == 0
        return pd.DataFrame(
            [
                {"ticker": "AAPL", "date": date(2024, 9, 19), "price_to_earnings": 34.84},
            ]
        )

    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setattr("market_data.datasets.ratios.download_ratios", fake_download_ratios)

    exit_code = ratios_main(
        [
            "--input-dir",
            str(reference_dir),
            "--output-dir",
            str(ratios_dir),
            "--calls-per-minute",
            "0",
            "--limit",
            "100",
        ]
    )
    ratios = pd.read_parquet(ratios_dir / "ratios.parquet")
    metadata = json.loads((ratios_dir / "ratios.metadata.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert ratios["ticker"].tolist() == ["AAPL"]
    assert ratios["price_to_earnings"].tolist() == [34.84]
    assert metadata["dataset"] == "ratios"
    assert metadata["raw_rows"] == 1
    assert metadata["output_rows"] == 1


def test_download_related_tickers_snapshot_writes_replace_snapshot(tmp_path, monkeypatch):
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
            {"ticker": "msft", "name": "Microsoft", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, tmp_path, metadata={"provider": "massive"})
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    calls = []

    def fake_downloader(ticker, api_key):
        calls.append(ticker)
        assert api_key == "test-key"
        if ticker == "MSFT":
            return pd.DataFrame(columns=RELATED_TICKER_COLUMNS)
        return pd.DataFrame(
            [
                {"ticker": ticker, "related_ticker": "MSFT", "result_order": 1},
                {"ticker": ticker, "related_ticker": "GOOGL", "result_order": 2},
            ]
        )

    output_path = download_related_tickers_snapshot(
        input_dir=tmp_path,
        output_dir=tmp_path,
        calls_per_minute=0,
        downloader=fake_downloader,
    )
    related = read_related_tickers(tmp_path)
    metadata = json.loads((tmp_path / "related_tickers.metadata.json").read_text(encoding="utf-8"))

    assert output_path == tmp_path / "related_tickers.parquet"
    assert calls == ["AAPL", "MSFT"]
    assert related["ticker"].tolist() == ["AAPL", "AAPL"]
    assert related["related_ticker"].tolist() == ["MSFT", "GOOGL"]
    assert metadata["provider"] == "massive"
    assert metadata["dataset"] == "related_tickers"
    assert metadata["mode"] == "replace"
    assert metadata["input_tickers"] == 2
    assert metadata["requested_tickers"] == 2
    assert metadata["empty_tickers"] == ["MSFT"]


def test_download_related_tickers_snapshot_keeps_failures_out_of_empty_tickers(tmp_path, monkeypatch):
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
            {"ticker": "msft", "name": "Microsoft", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, tmp_path, metadata={"provider": "massive"})
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")

    def fake_downloader(ticker, api_key):
        assert api_key == "test-key"
        if ticker == "AAPL":
            raise RuntimeError("provider auth failed")
        return pd.DataFrame(columns=RELATED_TICKER_COLUMNS)

    download_related_tickers_snapshot(
        input_dir=tmp_path,
        output_dir=tmp_path,
        calls_per_minute=0,
        downloader=fake_downloader,
    )
    metadata = json.loads((tmp_path / "related_tickers.metadata.json").read_text(encoding="utf-8"))

    assert metadata["empty_tickers"] == ["MSFT"]
    assert metadata["failed_tickers"] == [
        {
            "ticker": "AAPL",
            "error_type": "RuntimeError",
            "message": "provider auth failed",
        }
    ]


def test_download_related_tickers_cli_writes_related_rows(tmp_path, monkeypatch):
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, tmp_path, metadata={"provider": "massive"})

    def fake_download_related_tickers(ticker, api_key):
        assert ticker == "AAPL"
        assert api_key == "test-key"
        return pd.DataFrame(
            [
                {"ticker": "AAPL", "related_ticker": "MSFT", "result_order": 1},
            ]
        )

    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setattr("market_data.datasets.related_tickers.download_related_tickers", fake_download_related_tickers)

    exit_code = related_tickers_main(
        [
            "--input-dir",
            str(tmp_path),
            "--calls-per-minute",
            "0",
            "--limit",
            "1",
        ]
    )
    related = pd.read_parquet(tmp_path / "related_tickers.parquet")
    metadata = json.loads((tmp_path / "related_tickers.metadata.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert related["ticker"].tolist() == ["AAPL"]
    assert related["related_ticker"].tolist() == ["MSFT"]
    assert metadata["dataset"] == "related_tickers"
    assert metadata["partial"] is True
    assert metadata["requested_tickers"] == 1


def test_write_daily_snapshot_writes_parquet_and_metadata(tmp_path):
    bars = pd.DataFrame(
        {
            "date": [date(2026, 5, 1), date(2026, 5, 1)],
            "symbol": ["AAPL", "MSFT"],
            "open": [100.0, 200.0],
            "high": [105.0, 205.0],
            "low": [99.0, 199.0],
            "close": [104.0, 204.0],
            "volume": [1000, 2000],
        }
    )

    output_path = write_daily_snapshot(date(2026, 5, 2), bars, tmp_path)
    metadata = json.loads((tmp_path / "historical.metadata.json").read_text(encoding="utf-8"))

    assert output_path == tmp_path / "historical.parquet"
    assert output_path.exists()
    assert metadata["data_date"] == "2026-05-02"
    assert metadata["rows"] == 2
    assert metadata["symbols"] == 2
    assert metadata["data_min_date"] == "2026-05-01"
    assert metadata["data_max_date"] == "2026-05-01"


def test_download_script_writes_empty_snapshot_when_network_disabled(tmp_path, monkeypatch):
    output_dir = tmp_path / "bars"
    monkeypatch.setenv("FINBOT_INGEST_DISABLE_NETWORK", "1")
    monkeypatch.delenv("FINBOT_RAW_BARS_DIR", raising=False)

    exit_code = main(["--date", "2026-05-02", "--output-dir", str(output_dir), "--all-symbols"])
    metadata = json.loads((output_dir / "historical.metadata.json").read_text(encoding="utf-8"))
    bars = pd.read_parquet(output_dir / "historical.parquet")

    assert exit_code == 0
    assert list(bars.columns) == BAR_COLUMNS
    assert bars.empty
    assert metadata["data_date"] == "2026-05-02"
    assert metadata["rows"] == 0


def test_daily_bars_default_output_can_resolve_from_data_root(tmp_path, monkeypatch):
    monkeypatch.delenv("FINBOT_RAW_BARS_DIR", raising=False)
    monkeypatch.setenv("FINBOT_DATA_ROOT", str(tmp_path))

    assert daily_bars.resolve_output_dir(None) == tmp_path / "market/daily_bars"


def test_dataset_specific_output_dir_env_overrides_data_root(tmp_path, monkeypatch):
    raw_bars_dir = tmp_path / "legacy-bars"
    monkeypatch.setenv("FINBOT_DATA_ROOT", str(tmp_path / "root"))
    monkeypatch.setenv("FINBOT_RAW_BARS_DIR", str(raw_bars_dir))

    assert daily_bars.resolve_output_dir(None) == raw_bars_dir


def test_download_script_supports_days_window_when_network_disabled(tmp_path, monkeypatch):
    output_dir = tmp_path / "bars"
    monkeypatch.setenv("FINBOT_INGEST_DISABLE_NETWORK", "1")
    monkeypatch.delenv("FINBOT_RAW_BARS_DIR", raising=False)

    exit_code = main(
        [
            "--end-date",
            "2026-05-01",
            "--days",
            "10",
            "--calls-per-minute",
            "0",
            "--output-dir",
            str(output_dir),
            "--all-symbols",
        ]
    )
    metadata = json.loads((output_dir / "historical.metadata.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert metadata["mode"] == "replace"
    assert metadata["requested_start_date"] == "2026-04-22"
    assert metadata["requested_end_date"] == "2026-05-01"
    assert metadata["history_days"] == 10


def test_window_start_date_excludes_exact_year_boundary():
    assert window_start_date(date(2026, 5, 1), 2) == date(2024, 5, 2)


def test_days_window_start_date_includes_requested_number_of_calendar_days():
    assert days_window_start_date(date(2026, 5, 1), 10) == date(2026, 4, 22)


def test_download_history_replaces_existing_snapshot_and_records_empty_dates(tmp_path, monkeypatch):
    output_dir = tmp_path / "bars"
    write_daily_snapshot(
        date(2026, 5, 1),
        pd.DataFrame(
            {
                "date": [date(2026, 5, 1)],
                "symbol": ["OLD"],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1],
            }
        ),
        output_dir,
    )
    monkeypatch.setattr(daily_bars, "window_start_date", lambda end_date, years: date(2026, 5, 1))
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")

    def fake_downloader(data_date, api_key):
        if data_date == date(2026, 5, 4):
            return pd.DataFrame(columns=BAR_COLUMNS)
        if data_date == date(2026, 5, 5):
            return pd.DataFrame(
                {
                    "date": [data_date, data_date],
                    "symbol": ["AAPL", "AAPL"],
                    "open": [100.0, 101.0],
                    "high": [105.0, 106.0],
                    "low": [99.0, 100.0],
                    "close": [104.0, 105.0],
                    "volume": [1000, 1100],
                }
            )
        return pd.DataFrame(
            {
                "date": [data_date],
                "symbol": ["AAPL"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [104.0],
                "volume": [1000],
            }
        )

    output_path = download_history(
        end_date=date(2026, 5, 5),
        years=None,
        days=5,
        output_dir=output_dir,
        calls_per_minute=0,
        downloader=fake_downloader,
    )
    metadata = json.loads((output_dir / "historical.metadata.json").read_text(encoding="utf-8"))
    bars = pd.read_parquet(output_path)

    assert bars["symbol"].tolist() == ["AAPL", "AAPL"]
    assert bars["date"].tolist() == [date(2026, 5, 1), date(2026, 5, 5)]
    assert bars.loc[bars["date"] == date(2026, 5, 5), "close"].item() == 105.0
    assert metadata["mode"] == "replace"
    assert metadata["requested_start_date"] == "2026-05-01"
    assert metadata["requested_end_date"] == "2026-05-05"
    assert metadata["history_days"] == 5
    assert metadata["empty_market_dates"] == ["2026-05-04"]


def test_download_history_can_filter_to_ticker_universe(tmp_path, monkeypatch):
    output_dir = tmp_path / "bars"
    reference_dir = tmp_path / "reference"
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
            {"ticker": "msft", "name": "Microsoft", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, reference_dir, metadata={"provider": "massive"})
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")

    def fake_downloader(data_date, api_key):
        return pd.DataFrame(
            {
                "date": [data_date, data_date, data_date],
                "symbol": ["AAPL", "MSFT", "ZZZ"],
                "open": [100.0, 200.0, 300.0],
                "high": [105.0, 205.0, 305.0],
                "low": [99.0, 199.0, 299.0],
                "close": [104.0, 204.0, 304.0],
                "volume": [1000, 2000, 3000],
            }
        )

    output_path = download_history(
        end_date=date(2026, 5, 1),
        years=None,
        days=1,
        output_dir=output_dir,
        calls_per_minute=0,
        downloader=fake_downloader,
        filter_to_ticker_universe=True,
        ticker_universe_dir=reference_dir,
    )
    bars = pd.read_parquet(output_path)
    metadata = json.loads((output_dir / "historical.metadata.json").read_text(encoding="utf-8"))

    assert bars["symbol"].tolist() == ["AAPL", "MSFT"]
    assert metadata["symbols"] == 2
    assert metadata["ticker_universe_filter"]["enabled"] is True
    assert metadata["ticker_universe_filter"]["input_tickers"] == 2
    assert metadata["ticker_universe_filter"]["input_file"] == str(reference_dir / "tickers.parquet")


def test_download_script_filters_to_ticker_universe_by_default(tmp_path, monkeypatch):
    output_dir = tmp_path / "bars"
    reference_dir = tmp_path / "reference"
    tickers = normalize_tickers(
        [
            {"ticker": "aapl", "name": "Apple", "market": "stocks", "active": True},
        ]
    )
    write_ticker_universe(tickers, reference_dir, metadata={"provider": "massive"})
    monkeypatch.setenv("FINBOT_INGEST_DISABLE_NETWORK", "1")
    monkeypatch.setenv("FINBOT_REFERENCE_DIR", str(reference_dir))

    exit_code = main(["--date", "2026-05-01", "--output-dir", str(output_dir)])
    metadata = json.loads((output_dir / "historical.metadata.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert metadata["ticker_universe_filter"]["enabled"] is True
    assert metadata["ticker_universe_filter"]["input_tickers"] == 1


def test_normalize_price_frame_uses_canonical_schema():
    rows = [
        {
            "ticker": "msft",
            "date": "2018-09-04",
            "open": "110.85",
            "high": "111.95",
            "low": "110.22",
            "close": "111.71",
            "volume": "22634600",
            "lastupdated": "2026-05-11",
        },
        {
            "ticker": "AAPL",
            "date": "2018-09-04",
            "open": 57.1,
            "high": 57.29,
            "low": 56.66,
            "close": 57.09,
            "volume": 109560400,
            "lastupdated": "2026-05-11",
        },
    ]

    normalized = normalize_price_frame(pd.DataFrame(rows))

    assert list(normalized.columns) == CANONICAL_PRICE_COLUMNS
    assert normalized["symbol"].tolist() == ["AAPL", "MSFT"]
    assert normalized["date"].tolist() == [date(2018, 9, 4), date(2018, 9, 4)]
    assert normalized["close"].tolist() == [57.09, 111.71]
    assert normalized["lastupdated"].tolist() == [date(2026, 5, 11), date(2026, 5, 11)]


def test_download_and_write_ticker_universe_writes_raw_and_filtered(tmp_path, monkeypatch):
    def fake_downloader(api_key):
        assert api_key == "test-key"
        return [
            {
                "table": "SEP",
                "permaticker": "1",
                "ticker": "AAPL",
                "name": "Apple",
                "exchange": "NASDAQ",
                "isdelisted": "N",
                "category": "Domestic Common Stock",
                "scalemarketcap": "6 - Mega",
                "currency": "USD",
            },
            {
                "table": "SEP",
                "permaticker": "2",
                "ticker": "ZZZ",
                "name": "Tiny",
                "exchange": "NASDAQ",
                "isdelisted": "N",
                "category": "Domestic Common Stock",
                "scalemarketcap": "2 - Micro",
                "currency": "USD",
            },
        ]

    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", "test-key")
    raw_output_dir = tmp_path / "raw" / "nasdaq_data_link" / "sharadar" / "tickers"
    paths = download_and_write_ticker_universe(
        output_dir=tmp_path,
        raw_output_dir=raw_output_dir,
        downloader=fake_downloader,
    )
    raw_rows = [json.loads(line) for line in paths["tickers_raw"].read_text(encoding="utf-8").splitlines()]
    raw_metadata = json.loads((raw_output_dir / "tickers_sep_raw.download.json").read_text(encoding="utf-8"))
    tickers = pd.read_parquet(paths["tickers"])
    metadata = json.loads((tmp_path / "tickers.metadata.json").read_text(encoding="utf-8"))

    assert paths["tickers_raw"] == raw_output_dir / "tickers_sep_raw.jsonl"
    assert [row["ticker"] for row in raw_rows] == ["AAPL", "ZZZ"]
    assert raw_metadata["raw_file"] == "tickers_sep_raw.jsonl"
    assert raw_metadata["rows"] == 2
    assert tickers["ticker"].tolist() == ["AAPL"]
    assert metadata["provider"] == "sharadar"
    assert metadata["variant"] == "tickers"
    assert metadata["raw_input_file"] == str(paths["tickers_raw"])
    assert metadata["filter"]["market_caps"] == ["4 - Mid", "5 - Large", "6 - Mega"]


def test_filter_tickers_keeps_active_mid_large_common_stocks():
    frame = normalize_tickers_frame(
        pd.DataFrame(
            [
                {
                    "table": "SEP",
                    "ticker": "AAPL",
                    "exchange": "NASDAQ",
                    "isdelisted": "N",
                    "category": "Domestic Common Stock",
                    "scalemarketcap": "6 - Mega",
                },
                {
                    "table": "SEP",
                    "ticker": "MICRO",
                    "exchange": "NASDAQ",
                    "isdelisted": "N",
                    "category": "Domestic Common Stock",
                    "scalemarketcap": "2 - Micro",
                },
                {
                    "table": "SEP",
                    "ticker": "PREF",
                    "exchange": "NYSE",
                    "isdelisted": "N",
                    "category": "Domestic Preferred Stock",
                    "scalemarketcap": "5 - Large",
                },
            ]
        )
    )

    filtered = filter_tickers(frame)

    assert filtered["ticker"].tolist() == ["AAPL"]


def test_build_historical_from_csv_file_filters_to_ticker_universe(tmp_path):
    reference_dir = tmp_path / "reference"
    reference_dir.mkdir()
    pd.DataFrame({"ticker": ["AAPL"]}).to_parquet(reference_dir / "tickers.parquet", index=False)
    input_file = tmp_path / "sep.csv"
    input_file.write_text(
        "\n".join(
            [
                "ticker,date,open,high,low,close,volume,closeadj,closeunadj,lastupdated",
                "MSFT,2018-09-04,110.85,111.95,110.22,111.71,22634600,105.0,111.71,2026-05-11",
                "AAPL,2018-09-04,57.1,57.29,56.66,57.09,109560400,54.0,57.09,2026-05-11",
            ]
        ),
        encoding="utf-8",
    )

    output_path = build_historical_from_files([input_file], reference_dir=reference_dir, output_dir=tmp_path, chunk_rows=1)
    prices = pd.read_parquet(output_path)
    metadata = json.loads((tmp_path / "historical.metadata.json").read_text(encoding="utf-8"))

    assert output_path == tmp_path / HISTORICAL_PARQUET_FILE
    assert prices["symbol"].tolist() == ["AAPL"]
    assert metadata["provider"] == "sharadar"
    assert metadata["variant"] == "historical"
    assert metadata["input_tickers"] == 1
    assert metadata["data_min_date"] == "2018-09-04"
    assert metadata["data_max_date"] == "2018-09-04"


def test_request_bulk_price_files_uses_table_export(tmp_path, monkeypatch):
    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", "test-key")

    class FakeExportClient:
        def __init__(self):
            self.calls = 0
            self.downloaded_url = None

        def get_table_export(self, table_code):
            self.calls += 1
            if self.calls == 1:
                return {"datatable_bulk_download": {"file": {"status": "creating"}}}
            return {
                "datatable_bulk_download": {
                    "file": {
                        "status": "fresh",
                        "link": "https://example.test/export.zip?token=abc",
                        "data_snapshot_time": "2026-06-11T00:00:00Z",
                    }
                }
            }

        def download_file(self, url, output_path):
            self.downloaded_url = url
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"zip-data")
            return output_path

    client = FakeExportClient()

    paths = request_bulk_price_files(tmp_path, client=client, poll_seconds=0, max_polls=1)

    assert paths == [tmp_path / "bulk_file_001.zip"]
    assert paths[0].read_bytes() == b"zip-data"
    assert client.calls == 2
    assert client.downloaded_url == "https://example.test/export.zip?token=abc"


def test_update_historical_merges_lastupdated_rows(tmp_path, monkeypatch):
    reference_dir = tmp_path / "reference"
    output_dir = tmp_path / "daily_bars"
    reference_dir.mkdir()
    output_dir.mkdir()
    pd.DataFrame({"ticker": ["AAPL", "MSFT"]}).to_parquet(reference_dir / "tickers.parquet", index=False)
    pd.DataFrame(
        {
            "date": [date(2018, 9, 4)],
            "symbol": ["AAPL"],
            "open": [50.0],
            "high": [51.0],
            "low": [49.0],
            "close": [50.5],
            "volume": [100],
            "closeadj": [50.0],
            "closeunadj": [50.5],
            "lastupdated": [date(2026, 5, 10)],
        }
    ).to_parquet(output_dir / HISTORICAL_PARQUET_FILE, index=False)

    calls = []

    def fake_downloader(lastupdated_gte, api_key):
        calls.append((lastupdated_gte, api_key))
        assert lastupdated_gte == date(2026, 5, 11)
        assert api_key == "test-key"
        return [
            {
                "ticker": "AAPL",
                "date": "2018-09-04",
                "open": 57.1,
                "high": 57.29,
                "low": 56.66,
                "close": 57.09,
                "volume": 109560400,
                "closeadj": 54.0,
                "closeunadj": 57.09,
                "lastupdated": "2026-05-11",
            },
            {
                "ticker": "MSFT",
                "date": "2018-09-04",
                "open": 110.85,
                "high": 111.95,
                "low": 110.22,
                "close": 111.71,
                "volume": 22634600,
                "closeadj": 105.0,
                "closeunadj": 111.71,
                "lastupdated": "2026-05-11",
            },
            {
                "ticker": "ZZZ",
                "date": "2018-09-04",
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 100,
                "closeadj": 1.0,
                "closeunadj": 1.0,
                "lastupdated": "2026-05-11",
            },
        ]

    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", "test-key")
    output_path = update_historical(
        lastupdated_gte=date(2026, 5, 11),
        reference_dir=reference_dir,
        output_dir=output_dir,
        downloader=fake_downloader,
    )
    prices = pd.read_parquet(output_path)
    metadata = json.loads((output_dir / "historical.metadata.json").read_text(encoding="utf-8"))

    assert output_path == output_dir / HISTORICAL_PARQUET_FILE
    assert calls == [(date(2026, 5, 11), "test-key")]
    assert prices["symbol"].tolist() == ["AAPL", "MSFT"]
    assert prices.loc[prices["symbol"] == "AAPL", "close"].item() == 57.09
    assert metadata["variant"] == "historical"
    assert metadata["update_filter"] == "lastupdated.gte"
    assert metadata["input_tickers"] == 2
    assert metadata["update_raw_rows"] == 3
    assert metadata["update_rows"] == 2
    assert metadata["data_min_date"] == "2018-09-04"
    assert metadata["data_max_date"] == "2018-09-04"


def test_normalize_sf1_frame_preserves_schema_and_dates():
    normalized = normalize_sf1_frame(
        pd.DataFrame(
            [
                {
                    "ticker": "aapl",
                    "dimension": "art",
                    "calendardate": "2026-03-31",
                    "datekey": "2026-05-01",
                    "reportperiod": "2026-03-28",
                    "fiscalperiod": "2026-Q2",
                    "lastupdated": "2026-05-04",
                    "revenue": "95359000000",
                    "pe": "30.5",
                }
            ]
        )
    )

    assert normalized["ticker"].tolist() == ["AAPL"]
    assert normalized["dimension"].tolist() == ["ART"]
    assert normalized["datekey"].tolist() == [date(2026, 5, 1)]
    assert normalized["reportperiod"].tolist() == [date(2026, 3, 28)]
    assert normalized["revenue"].tolist() == [95359000000]
    assert normalized["pe"].tolist() == [30.5]


def test_build_sf1_from_files_filters_to_ticker_universe(tmp_path):
    reference_dir = tmp_path / "reference"
    output_dir = tmp_path / "fundamentals"
    reference_dir.mkdir()
    pd.DataFrame({"ticker": ["AAPL"]}).to_parquet(reference_dir / "tickers.parquet", index=False)
    input_file = tmp_path / "sf1.csv"
    input_file.write_text(
        "\n".join(
            [
                "ticker,dimension,calendardate,datekey,reportperiod,fiscalperiod,lastupdated,revenue,assets,pe",
                "MSFT,ART,2026-03-31,2026-04-30,2026-03-31,2026-Q3,2026-05-01,700,2000,25.0",
                "AAPL,ART,2026-03-31,2026-05-01,2026-03-28,2026-Q2,2026-05-04,1000,3000,30.0",
                "AAPL,MRQ,2026-03-31,2026-03-28,2026-03-28,2026-Q2,2026-05-04,250,3000,30.0",
            ]
        ),
        encoding="utf-8",
    )

    output_path = build_sf1_from_files([input_file], reference_dir=reference_dir, output_dir=output_dir, chunk_rows=1)
    fundamentals = pd.read_parquet(output_path)
    metadata = json.loads((output_dir / "sf1.metadata.json").read_text(encoding="utf-8"))

    assert output_path == output_dir / SF1_PARQUET_FILE
    assert fundamentals["ticker"].tolist() == ["AAPL", "AAPL"]
    assert fundamentals["dimension"].tolist() == ["ART", "MRQ"]
    assert metadata["provider"] == "sharadar"
    assert metadata["source_table"] == "SHARADAR/SF1"
    assert metadata["input_tickers"] == 1
    assert metadata["dimensions"] == ["ART", "MRQ"]
    assert metadata["data_min_date"] == "2026-03-28"
    assert metadata["data_max_date"] == "2026-05-01"
    assert metadata["reportperiod_min_date"] == "2026-03-28"
    assert metadata["reportperiod_max_date"] == "2026-03-28"


def test_request_bulk_sf1_files_uses_table_export(tmp_path, monkeypatch):
    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", "test-key")

    class FakeExportClient:
        def __init__(self):
            self.calls = 0
            self.downloaded_url = None

        def get_table_export(self, table_code):
            self.calls += 1
            assert table_code == "SHARADAR/SF1"
            if self.calls == 1:
                return {"datatable_bulk_download": {"file": {"status": "creating"}}}
            return {
                "datatable_bulk_download": {
                    "file": {
                        "status": "fresh",
                        "link": "https://example.test/sf1.zip?token=abc",
                        "data_snapshot_time": "2026-06-11T00:00:00Z",
                    }
                }
            }

        def download_file(self, url, output_path):
            self.downloaded_url = url
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"zip-data")
            return output_path

    client = FakeExportClient()

    paths = request_bulk_sf1_files(tmp_path, client=client, poll_seconds=0, max_polls=1)

    assert paths == [tmp_path / "bulk_file_001.zip"]
    assert paths[0].read_bytes() == b"zip-data"
    assert client.calls == 2
    assert client.downloaded_url == "https://example.test/sf1.zip?token=abc"


def test_update_sf1_merges_lastupdated_rows_by_primary_key(tmp_path, monkeypatch):
    reference_dir = tmp_path / "reference"
    output_dir = tmp_path / "fundamentals"
    reference_dir.mkdir()
    output_dir.mkdir()
    pd.DataFrame({"ticker": ["AAPL", "MSFT"]}).to_parquet(reference_dir / "tickers.parquet", index=False)
    pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "dimension": ["ART"],
            "calendardate": [date(2026, 3, 31)],
            "datekey": [date(2026, 5, 1)],
            "reportperiod": [date(2026, 3, 28)],
            "fiscalperiod": ["2026-Q2"],
            "lastupdated": [date(2026, 5, 3)],
            "revenue": [900.0],
            "assets": [3000.0],
            "pe": [29.0],
        }
    ).to_parquet(output_dir / SF1_PARQUET_FILE, index=False)

    calls = []

    def fake_downloader(lastupdated_gte, api_key):
        calls.append((lastupdated_gte, api_key))
        assert lastupdated_gte == date(2026, 5, 4)
        assert api_key == "test-key"
        return [
            {
                "ticker": "AAPL",
                "dimension": "ART",
                "calendardate": "2026-03-31",
                "datekey": "2026-05-01",
                "reportperiod": "2026-03-28",
                "fiscalperiod": "2026-Q2",
                "lastupdated": "2026-05-04",
                "revenue": 1000,
                "assets": 3000,
                "pe": 30.0,
            },
            {
                "ticker": "MSFT",
                "dimension": "MRQ",
                "calendardate": "2026-03-31",
                "datekey": "2026-03-31",
                "reportperiod": "2026-03-31",
                "fiscalperiod": "2026-Q3",
                "lastupdated": "2026-05-04",
                "revenue": 700,
                "assets": 2000,
                "pe": 25.0,
            },
            {
                "ticker": "ZZZ",
                "dimension": "ART",
                "calendardate": "2026-03-31",
                "datekey": "2026-05-01",
                "reportperiod": "2026-03-31",
                "fiscalperiod": "2026-Q1",
                "lastupdated": "2026-05-04",
                "revenue": 1,
            },
        ]

    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", "test-key")
    output_path = update_sf1(
        lastupdated_gte=date(2026, 5, 4),
        reference_dir=reference_dir,
        output_dir=output_dir,
        downloader=fake_downloader,
    )
    fundamentals = pd.read_parquet(output_path)
    metadata = json.loads((output_dir / "sf1.metadata.json").read_text(encoding="utf-8"))

    assert output_path == output_dir / SF1_PARQUET_FILE
    assert calls == [(date(2026, 5, 4), "test-key")]
    assert fundamentals[["ticker", "dimension"]].values.tolist() == [["AAPL", "ART"], ["MSFT", "MRQ"]]
    assert fundamentals.loc[fundamentals["ticker"] == "AAPL", "revenue"].item() == 1000
    assert metadata["variant"] == "sf1"
    assert metadata["update_filter"] == "lastupdated.gte"
    assert metadata["input_tickers"] == 2
    assert metadata["update_raw_rows"] == 3
    assert metadata["update_rows"] == 2
    assert metadata["primary_key"] == ["ticker", "dimension", "datekey", "reportperiod"]


def test_download_ticker_universe_cli_writes_datasets(tmp_path, monkeypatch):
    def fake_download_and_write_ticker_universe(output_dir=None, raw_output_dir=None, downloader=None):
        assert output_dir == str(tmp_path / "reference")
        assert raw_output_dir == str(tmp_path / "raw_tickers")
        return {"tickers_raw": tmp_path / "tickers_sep_raw.jsonl", "tickers": tmp_path / "tickers.parquet"}

    monkeypatch.setattr(
        "scripts.download_ticker_universe.download_and_write_ticker_universe",
        fake_download_and_write_ticker_universe,
    )

    exit_code = ticker_universe_main(
        [
            "--output-dir",
            str(tmp_path / "reference"),
            "--raw-output-dir",
            str(tmp_path / "raw_tickers"),
        ]
    )

    assert exit_code == 0


def test_download_historical_prices_cli_builds_from_input_file(tmp_path, monkeypatch):
    input_file = tmp_path / "sep.csv"
    input_file.write_text("ticker,date,open,high,low,close,volume,closeadj,closeunadj,lastupdated\n", encoding="utf-8")

    def fake_build_historical_from_files(input_files, reference_dir=None, output_dir=None, chunk_rows=500_000):
        assert input_files == [input_file]
        assert reference_dir == str(tmp_path / "reference")
        assert output_dir == str(tmp_path / "daily_bars")
        assert chunk_rows == 10
        return tmp_path / "daily_bars" / "historical.parquet"

    monkeypatch.setattr(
        "scripts.download_historical_prices.build_historical_from_files",
        fake_build_historical_from_files,
    )

    exit_code = historical_prices_main(
        [
            "--input-file",
            str(input_file),
            "--reference-dir",
            str(tmp_path / "reference"),
            "--output-dir",
            str(tmp_path / "daily_bars"),
            "--chunk-rows",
            "10",
        ]
    )

    assert exit_code == 0


def test_download_historical_prices_cli_defaults_to_bulk_download(tmp_path, monkeypatch):
    bulk_file = tmp_path / "bulk.parquet"

    def fake_request_bulk_price_files(raw_export_dir=None, poll_seconds=60.0, max_polls=30):
        assert raw_export_dir is None
        assert poll_seconds == 0
        assert max_polls == 1
        return [bulk_file]

    def fake_build_historical_from_files(input_files, reference_dir=None, output_dir=None, chunk_rows=500_000):
        assert input_files == [bulk_file]
        assert output_dir == str(tmp_path / "daily_bars")
        return tmp_path / "daily_bars" / "historical.parquet"

    monkeypatch.setattr(
        "scripts.download_historical_prices.request_bulk_price_files",
        fake_request_bulk_price_files,
    )
    monkeypatch.setattr(
        "scripts.download_historical_prices.build_historical_from_files",
        fake_build_historical_from_files,
    )

    exit_code = historical_prices_main(
        [
            "--output-dir",
            str(tmp_path / "daily_bars"),
            "--poll-seconds",
            "0",
            "--max-polls",
            "1",
        ]
    )

    assert exit_code == 0


def test_download_fundamentals_cli_builds_from_input_file(tmp_path, monkeypatch):
    input_file = tmp_path / "sf1.csv"
    input_file.write_text("ticker,dimension,calendardate,datekey,reportperiod,lastupdated,revenue\n", encoding="utf-8")

    def fake_build_sf1_from_files(input_files, reference_dir=None, output_dir=None, chunk_rows=100_000):
        assert input_files == [input_file]
        assert reference_dir == str(tmp_path / "reference")
        assert output_dir == str(tmp_path / "fundamentals")
        assert chunk_rows == 10
        return tmp_path / "fundamentals" / "sf1.parquet"

    monkeypatch.setattr(
        "scripts.download_fundamentals.build_sf1_from_files",
        fake_build_sf1_from_files,
    )

    exit_code = fundamentals_main(
        [
            "--input-file",
            str(input_file),
            "--reference-dir",
            str(tmp_path / "reference"),
            "--output-dir",
            str(tmp_path / "fundamentals"),
            "--chunk-rows",
            "10",
        ]
    )

    assert exit_code == 0


def test_download_fundamentals_cli_defaults_to_bulk_download(tmp_path, monkeypatch):
    bulk_file = tmp_path / "sf1-bulk.parquet"

    def fake_request_bulk_sf1_files(raw_export_dir=None, poll_seconds=60.0, max_polls=30):
        assert raw_export_dir is None
        assert poll_seconds == 0
        assert max_polls == 1
        return [bulk_file]

    def fake_build_sf1_from_files(input_files, reference_dir=None, output_dir=None, chunk_rows=100_000):
        assert input_files == [bulk_file]
        assert output_dir == str(tmp_path / "fundamentals")
        return tmp_path / "fundamentals" / "sf1.parquet"

    monkeypatch.setattr(
        "scripts.download_fundamentals.request_bulk_sf1_files",
        fake_request_bulk_sf1_files,
    )
    monkeypatch.setattr(
        "scripts.download_fundamentals.build_sf1_from_files",
        fake_build_sf1_from_files,
    )

    exit_code = fundamentals_main(
        [
            "--output-dir",
            str(tmp_path / "fundamentals"),
            "--poll-seconds",
            "0",
            "--max-polls",
            "1",
        ]
    )

    assert exit_code == 0


def test_update_fundamentals_cli_updates_sf1(tmp_path, monkeypatch):
    def fake_update_sf1(lastupdated_gte, reference_dir=None, output_dir=None):
        assert lastupdated_gte == date(2026, 5, 4)
        assert reference_dir == str(tmp_path / "reference")
        assert output_dir == str(tmp_path / "fundamentals")
        return tmp_path / "fundamentals" / "sf1.parquet"

    monkeypatch.setattr("scripts.update_fundamentals.update_sf1", fake_update_sf1)

    exit_code = update_fundamentals_main(
        [
            "--lastupdated-gte",
            "2026-05-04",
            "--reference-dir",
            str(tmp_path / "reference"),
            "--output-dir",
            str(tmp_path / "fundamentals"),
        ]
    )

    assert exit_code == 0
