from __future__ import annotations

from datetime import date
import json

import pandas as pd

from market_data.datasets import daily_bars
from market_data.datasets.daily_bars import days_window_start_date, download_history, window_start_date
from market_data.http import MassiveClient, MassiveHttpError
from market_data.normalize import BAR_COLUMNS, normalize_bars_frame
from market_data.providers.massive import normalize_grouped_daily_response
from market_data.storage import write_daily_snapshot
from market_data.universe import (
    fetch_ticker_universe,
    filter_common_stocks,
    normalize_tickers,
    read_ticker_universe,
    write_ticker_universe,
)
from scripts.download_daily_bars import main
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
