from __future__ import annotations

from datetime import date
import json

import pandas as pd

from market_data.datasets import daily_bars
from market_data.datasets.daily_bars import days_window_start_date, download_history, window_start_date
from market_data.datasets.ratios import download_ratios_snapshot, read_ratios
from market_data.datasets.related_tickers import download_related_tickers_snapshot, read_related_tickers
from market_data.datasets.ticker_details import (
    download_ticker_details_snapshot,
    read_ticker_details,
    write_ticker_details_snapshot,
)
from market_data.http import MassiveClient, MassiveHttpError
from market_data.normalize import BAR_COLUMNS, normalize_bars_frame
from market_data.providers.massive import (
    RATIO_COLUMNS,
    RELATED_TICKER_COLUMNS,
    TICKER_DETAIL_COLUMNS,
    normalize_grouped_daily_response,
    normalize_ratios_response,
    normalize_related_tickers_response,
    normalize_ticker_details_response,
)
from market_data.storage import write_daily_snapshot
from market_data.universe import (
    fetch_ticker_universe,
    filter_common_stocks,
    normalize_tickers,
    read_ticker_universe,
    write_ticker_universe,
)
from scripts.download_daily_bars import main
from scripts.download_ratios import main as ratios_main
from scripts.download_related_tickers import main as related_tickers_main
from scripts.download_ticker_details import main as ticker_details_main
from scripts.download_tickers import main as tickers_main


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

    exit_code = tickers_main(["--date", "2026-05-01", "--calls-per-minute", "5", "--output-dir", str(tmp_path)])
    tickers = pd.read_parquet(tmp_path / "tickers.parquet")
    metadata = json.loads((tmp_path / "tickers.metadata.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert tickers["ticker"].tolist() == ["AAPL"]
    assert metadata["provider"] == "massive"
    assert metadata["dataset"] == "ticker_universe"
    assert metadata["mode"] == "replace"
    assert metadata["universe_date"] == "2026-05-01"
    assert metadata["input_rows"] == 2
    assert metadata["output_rows"] == 1
    assert metadata["rows"] == 1
    assert metadata["filter"]["type"] == "CS"


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

    exit_code = main(["--date", "2026-05-02", "--output-dir", str(output_dir)])
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
