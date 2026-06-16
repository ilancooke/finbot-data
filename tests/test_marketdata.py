from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pandas as pd

from market_data.datasets.daily_prices import (
    CANONICAL_PRICE_COLUMNS,
    HISTORICAL_PARQUET_FILE,
    build_historical_from_files,
    normalize_price_frame,
    request_bulk_price_files,
    update_historical,
)
from market_data.datasets.daily_valuation_metrics import (
    DAILY_VALUATION_METRICS_PARQUET_FILE,
    build_daily_valuation_metrics_from_files,
    normalize_daily_valuation_metrics_frame,
    request_bulk_daily_valuation_metric_files,
    update_daily_valuation_metrics,
)
from market_data.datasets.fundamentals import (
    SF1_PARQUET_FILE,
    build_sf1_from_files,
    normalize_sf1_frame,
    request_bulk_sf1_files,
    update_sf1,
)
from market_data.datasets.ticker_universe import (
    download_and_write_ticker_universe,
    filter_tickers,
    normalize_tickers_frame,
)
from market_data.providers.nasdaq_data_link import NasdaqDataLinkClient, NasdaqDataLinkError
from scripts.download_daily_valuation_metrics import main as daily_valuation_metrics_main
from scripts.download_fundamentals import main as fundamentals_main
from scripts.download_historical_prices import main as historical_prices_main
from scripts.download_ticker_universe import main as ticker_universe_main
from scripts.update_daily_valuation_metrics import main as update_daily_valuation_metrics_main
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


def test_normalize_daily_valuation_metrics_frame_preserves_schema_and_dates():
    normalized = normalize_daily_valuation_metrics_frame(
        pd.DataFrame(
            [
                {
                    "ticker": "aapl",
                    "date": "2026-06-12",
                    "lastupdated": "2026-06-12",
                    "ev": "4315069.0",
                    "evebit": "29.2",
                    "evebitda": "26.9",
                    "marketcap": "4275930.0",
                    "pb": "40.2",
                    "pe": "34.9",
                    "ps": "9.5",
                }
            ]
        )
    )

    assert normalized["ticker"].tolist() == ["AAPL"]
    assert normalized["date"].tolist() == [date(2026, 6, 12)]
    assert normalized["lastupdated"].tolist() == [date(2026, 6, 12)]
    assert normalized["ev"].tolist() == [4315069.0]
    assert normalized["pe"].tolist() == [34.9]


def test_build_daily_valuation_metrics_from_files_filters_to_ticker_universe(tmp_path):
    reference_dir = tmp_path / "reference"
    output_dir = tmp_path / "fundamentals"
    reference_dir.mkdir()
    pd.DataFrame({"ticker": ["AAPL"]}).to_parquet(reference_dir / "tickers.parquet", index=False)
    input_file = tmp_path / "daily.csv"
    input_file.write_text(
        "\n".join(
            [
                "ticker,date,lastupdated,ev,evebit,evebitda,marketcap,pb,pe,ps",
                "MSFT,2026-06-12,2026-06-12,1000,10.0,9.0,900,5.0,20.0,4.0",
                "AAPL,2026-06-12,2026-06-12,4315069.0,29.2,26.9,4275930.0,40.2,34.9,9.5",
                "AAPL,2026-06-11,2026-06-11,4381162.1,29.7,27.3,4342023.1,40.8,35.4,9.6",
            ]
        ),
        encoding="utf-8",
    )

    output_path = build_daily_valuation_metrics_from_files([input_file], reference_dir=reference_dir, output_dir=output_dir, chunk_rows=1)
    metrics = pd.read_parquet(output_path)
    metadata = json.loads((output_dir / "daily_valuation_metrics.metadata.json").read_text(encoding="utf-8"))

    assert output_path == output_dir / DAILY_VALUATION_METRICS_PARQUET_FILE
    assert metrics["ticker"].tolist() == ["AAPL", "AAPL"]
    assert sorted(metrics["date"].tolist()) == [date(2026, 6, 11), date(2026, 6, 12)]
    assert metadata["provider"] == "sharadar"
    assert metadata["source_table"] == "SHARADAR/DAILY"
    assert metadata["variant"] == "daily_valuation_metrics"
    assert metadata["input_tickers"] == 1
    assert metadata["primary_key"] == ["ticker", "date"]
    assert metadata["data_min_date"] == "2026-06-11"
    assert metadata["data_max_date"] == "2026-06-12"


def test_request_bulk_daily_valuation_metric_files_uses_table_export(tmp_path, monkeypatch):
    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", "test-key")

    class FakeExportClient:
        def __init__(self):
            self.calls = 0
            self.downloaded_url = None

        def get_table_export(self, table_code):
            self.calls += 1
            assert table_code == "SHARADAR/DAILY"
            if self.calls == 1:
                return {"datatable_bulk_download": {"file": {"status": "creating"}}}
            return {
                "datatable_bulk_download": {
                    "file": {
                        "status": "fresh",
                        "link": "https://example.test/daily.zip?token=abc",
                        "data_snapshot_time": "2026-06-12T00:00:00Z",
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

    paths = request_bulk_daily_valuation_metric_files(tmp_path, client=client, poll_seconds=0, max_polls=1)

    assert paths == [tmp_path / "bulk_file_001.zip"]
    assert paths[0].read_bytes() == b"zip-data"
    assert client.calls == 2
    assert client.downloaded_url == "https://example.test/daily.zip?token=abc"


def test_update_daily_valuation_metrics_merges_lastupdated_rows_by_ticker_date(tmp_path, monkeypatch):
    reference_dir = tmp_path / "reference"
    output_dir = tmp_path / "fundamentals"
    reference_dir.mkdir()
    output_dir.mkdir()
    pd.DataFrame({"ticker": ["AAPL", "MSFT"]}).to_parquet(reference_dir / "tickers.parquet", index=False)
    pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": [date(2026, 6, 12)],
            "lastupdated": [date(2026, 6, 12)],
            "ev": [4315069.0],
            "evebit": [29.2],
            "evebitda": [26.9],
            "marketcap": [4275930.0],
            "pb": [40.2],
            "pe": [34.9],
            "ps": [9.5],
        }
    ).to_parquet(output_dir / DAILY_VALUATION_METRICS_PARQUET_FILE, index=False)

    calls = []

    def fake_downloader(lastupdated_gte, api_key):
        calls.append((lastupdated_gte, api_key))
        assert lastupdated_gte == date(2026, 6, 13)
        assert api_key == "test-key"
        return [
            {
                "ticker": "AAPL",
                "date": "2026-06-12",
                "lastupdated": "2026-06-13",
                "ev": 4316000.0,
                "evebit": 29.3,
                "evebitda": 27.0,
                "marketcap": 4277000.0,
                "pb": 40.3,
                "pe": 35.0,
                "ps": 9.6,
            },
            {
                "ticker": "MSFT",
                "date": "2026-06-12",
                "lastupdated": "2026-06-13",
                "ev": 1000,
                "evebit": 10.0,
                "evebitda": 9.0,
                "marketcap": 900,
                "pb": 5.0,
                "pe": 20.0,
                "ps": 4.0,
            },
            {
                "ticker": "ZZZ",
                "date": "2026-06-12",
                "lastupdated": "2026-06-13",
                "ev": 1,
            },
        ]

    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", "test-key")
    output_path = update_daily_valuation_metrics(
        lastupdated_gte=date(2026, 6, 13),
        reference_dir=reference_dir,
        output_dir=output_dir,
        downloader=fake_downloader,
    )
    metrics = pd.read_parquet(output_path)
    metadata = json.loads((output_dir / "daily_valuation_metrics.metadata.json").read_text(encoding="utf-8"))

    assert output_path == output_dir / DAILY_VALUATION_METRICS_PARQUET_FILE
    assert calls == [(date(2026, 6, 13), "test-key")]
    assert metrics["ticker"].tolist() == ["AAPL", "MSFT"]
    assert metrics.loc[metrics["ticker"] == "AAPL", "ev"].item() == 4316000.0
    assert metadata["variant"] == "daily_valuation_metrics"
    assert metadata["update_filter"] == "lastupdated.gte"
    assert metadata["input_tickers"] == 2
    assert metadata["update_raw_rows"] == 3
    assert metadata["update_rows"] == 2
    assert metadata["primary_key"] == ["ticker", "date"]


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


def test_download_daily_valuation_metrics_cli_builds_from_input_file(tmp_path, monkeypatch):
    input_file = tmp_path / "daily.csv"
    input_file.write_text("ticker,date,lastupdated,ev,evebit,evebitda,marketcap,pb,pe,ps\n", encoding="utf-8")

    def fake_build_daily_valuation_metrics_from_files(input_files, reference_dir=None, output_dir=None, chunk_rows=500_000):
        assert input_files == [input_file]
        assert reference_dir == str(tmp_path / "reference")
        assert output_dir == str(tmp_path / "fundamentals")
        assert chunk_rows == 10
        return tmp_path / "fundamentals" / "daily_valuation_metrics.parquet"

    monkeypatch.setattr(
        "scripts.download_daily_valuation_metrics.build_daily_valuation_metrics_from_files",
        fake_build_daily_valuation_metrics_from_files,
    )

    exit_code = daily_valuation_metrics_main(
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


def test_download_daily_valuation_metrics_cli_defaults_to_bulk_download(tmp_path, monkeypatch):
    bulk_file = tmp_path / "daily-bulk.parquet"

    def fake_request_bulk_daily_valuation_metric_files(raw_export_dir=None, poll_seconds=60.0, max_polls=30):
        assert raw_export_dir is None
        assert poll_seconds == 0
        assert max_polls == 1
        return [bulk_file]

    def fake_build_daily_valuation_metrics_from_files(input_files, reference_dir=None, output_dir=None, chunk_rows=500_000):
        assert input_files == [bulk_file]
        assert output_dir == str(tmp_path / "fundamentals")
        return tmp_path / "fundamentals" / "daily_valuation_metrics.parquet"

    monkeypatch.setattr(
        "scripts.download_daily_valuation_metrics.request_bulk_daily_valuation_metric_files",
        fake_request_bulk_daily_valuation_metric_files,
    )
    monkeypatch.setattr(
        "scripts.download_daily_valuation_metrics.build_daily_valuation_metrics_from_files",
        fake_build_daily_valuation_metrics_from_files,
    )

    exit_code = daily_valuation_metrics_main(
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


def test_update_daily_valuation_metrics_cli_updates_dataset(tmp_path, monkeypatch):
    def fake_update_daily_valuation_metrics(lastupdated_gte, reference_dir=None, output_dir=None):
        assert lastupdated_gte == date(2026, 6, 13)
        assert reference_dir == str(tmp_path / "reference")
        assert output_dir == str(tmp_path / "fundamentals")
        return tmp_path / "fundamentals" / "daily_valuation_metrics.parquet"

    monkeypatch.setattr(
        "scripts.update_daily_valuation_metrics.update_daily_valuation_metrics",
        fake_update_daily_valuation_metrics,
    )

    exit_code = update_daily_valuation_metrics_main(
        [
            "--lastupdated-gte",
            "2026-06-13",
            "--reference-dir",
            str(tmp_path / "reference"),
            "--output-dir",
            str(tmp_path / "fundamentals"),
        ]
    )

    assert exit_code == 0
