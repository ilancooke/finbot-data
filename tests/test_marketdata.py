from __future__ import annotations

from datetime import date
import json

import pandas as pd

from market_data.datasets import daily_bars
from market_data.datasets.daily_bars import days_window_start_date, download_history, window_start_date
from market_data.normalize import BAR_COLUMNS, normalize_bars_frame
from market_data.providers.massive import normalize_grouped_daily_response
from market_data.storage import write_daily_snapshot
from scripts.download_daily_bars import main


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
