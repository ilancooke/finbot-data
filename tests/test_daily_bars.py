from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pandas as pd

from market_data.datasets.daily_bars import (
    combine_and_map_bars,
    last_completed_session_candidate,
    normalize_bar_pages,
    run_daily_bars,
)


def ticker_rows() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "episode_id": "figi:aapl|AAPL|2016-01-01", "security_id": "figi:aapl",
            "symbol": "AAPL", "provider_symbol": "AAPL", "current_symbol": "AAPL",
            "symbol_valid_from": date(2016, 1, 1), "symbol_valid_to": None,
            "is_episode_mapping_eligible": True,
        },
        {
            "episode_id": "figi:gen|GEN|2022-11-08", "security_id": "figi:gen",
            "symbol": "GEN", "provider_symbol": "GEN", "current_symbol": "GEN",
            "symbol_valid_from": date(2022, 11, 8), "symbol_valid_to": None,
            "is_episode_mapping_eligible": True,
        },
        {
            "episode_id": "figi:blocked|SAME|2016-01-01", "security_id": "figi:blocked",
            "symbol": "SAME", "provider_symbol": "SAME", "current_symbol": "SAME",
            "symbol_valid_from": date(2016, 1, 1), "symbol_valid_to": None,
            "is_episode_mapping_eligible": False,
        },
    ])


def bar(timestamp: str, close: float) -> dict:
    return {"t": timestamp, "o": close - 1, "h": close + 1, "l": close - 2, "c": close,
            "v": 1000, "vw": close - 0.5, "n": 100}


def test_normalize_and_map_bars_enforces_episode_boundaries_and_quarantine():
    raw = normalize_bar_pages([{"bars": {
        "AAPL": [bar("2020-01-02T05:00:00Z", 100)],
        "GEN": [bar("2020-01-02T05:00:00Z", 20), bar("2023-01-03T05:00:00Z", 25)],
        "SAME": [bar("2023-01-03T05:00:00Z", 10)],
    }}], prefix="raw")
    split = normalize_bar_pages([{"bars": {
        "AAPL": [bar("2020-01-02T05:00:00Z", 25)],
        "GEN": [bar("2020-01-02T05:00:00Z", 20), bar("2023-01-03T05:00:00Z", 25)],
        "SAME": [bar("2023-01-03T05:00:00Z", 10)],
    }}], prefix="split_adjusted")

    mapped, quarantine = combine_and_map_bars(
        raw, split, ticker_rows(), requested_symbols={"AAPL", "GEN", "SAME"}, snapshot_date=date(2026, 9, 19)
    )

    assert set(zip(mapped["symbol"], mapped["date"])) == {
        ("AAPL", date(2020, 1, 2)), ("GEN", date(2023, 1, 3))
    }
    statuses = quarantine.set_index(["symbol", "date"])["identity_status"]
    assert statuses.loc[("GEN", date(2020, 1, 2))] == "outside_symbol_episode"
    assert statuses.loc[("SAME", date(2023, 1, 3))] == "identity_quarantine"
    assert mapped.loc[mapped["symbol"].eq("AAPL"), "raw_close"].item() == 100
    assert mapped.loc[mapped["symbol"].eq("AAPL"), "split_adjusted_close"].item() == 25


class FakeAlpacaClient:
    def __init__(self):
        self._request_count = 0
        self.calls = []

    def iter_stock_bar_pages(self, symbols, **kwargs):
        self._request_count += 1
        self.calls.append((list(symbols), kwargs))
        adjustment = kwargs["adjustment"]
        values = {}
        if "AAPL" in symbols:
            values["AAPL"] = [bar("2020-01-02T05:00:00Z", 25 if adjustment == "split" else 100)]
        if "GEN" in symbols:
            values["GEN"] = [bar("2020-01-02T05:00:00Z", 20), bar("2023-01-03T05:00:00Z", 25)]
        yield {"bars": values, "next_page_token": None}


def test_pilot_writes_bounded_outputs_and_passes_validation(tmp_path: Path):
    reference = tmp_path / "reference"; reference.mkdir()
    ticker_rows().to_parquet(reference / "tickers.parquet", index=False)
    client = FakeAlpacaClient()

    outputs = run_daily_bars(
        mode="pilot", snapshot_date=date(2026, 9, 19), end_date=date(2023, 1, 3),
        output_dir=tmp_path / "market", reference_dir=reference, raw_dir=tmp_path / "raw",
        symbol_batch_size=2, alpaca_client=client,
    )

    metadata = json.loads(outputs["metadata"].read_text())
    assert metadata["pilot_passed"] is True
    assert metadata["row_count"] == 2
    assert metadata["duplicate_key_count"] == 0
    assert metadata["split_adjustment_difference_count"] == 1
    assert metadata["request_count"] == 2
    assert len(pd.read_parquet(outputs["quarantine"])) == 1
    assert all(call[1]["asof"] == "-" for call in client.calls)


def test_last_completed_candidate_skips_weekend():
    assert last_completed_session_candidate(date(2026, 9, 21)) == date(2026, 9, 18)
