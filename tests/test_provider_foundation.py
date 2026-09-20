from __future__ import annotations

from datetime import date
import gzip
import json
from pathlib import Path

import pytest
import requests

from market_data.config import get_alpaca_credentials, get_massive_api_key
from market_data.http import JsonApiError
from market_data.providers.alpaca import AlpacaClient
from market_data.providers.massive import MassiveClient
from market_data.raw import write_raw_json_pages


class FakeResponse:
    def __init__(self, status_code: int, payload=None, text: str = "", headers=None):
        self.status_code = status_code
        self.payload = payload
        self.text = text
        self.headers = headers or {}

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "params": dict(params or {}),
                "headers": dict(headers or {}),
                "timeout": timeout,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_massive_client_collects_next_url_pages_and_authenticates_each_request():
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "results": [{"ticker": "AAPL"}],
                    "next_url": "https://api.massive.test/v3/reference/tickers?cursor=next",
                },
            ),
            FakeResponse(200, {"results": [{"ticker": "MSFT"}], "next_url": None}),
        ]
    )
    client = MassiveClient(
        "massive-secret",
        base_url="https://api.massive.test",
        calls_per_minute=0,
        session=session,
    )

    rows = client.get_paginated("/v3/reference/tickers", params={"limit": 1})

    assert rows == [{"ticker": "AAPL"}, {"ticker": "MSFT"}]
    assert session.calls[0]["params"] == {"apiKey": "massive-secret", "limit": 1}
    assert session.calls[1]["params"] == {"apiKey": "massive-secret"}
    assert session.calls[1]["url"].endswith("?cursor=next")


def test_massive_client_retries_rate_limit_and_obeys_retry_after():
    session = FakeSession(
        [
            FakeResponse(429, {"error": "slow down"}, text="slow down", headers={"Retry-After": "3"}),
            FakeResponse(200, {"results": []}),
        ]
    )
    sleeps = []
    client = MassiveClient(
        "massive-secret",
        calls_per_minute=0,
        session=session,
        sleep_func=sleeps.append,
    )

    assert client.get_json("/v3/reference/tickers") == {"results": []}
    assert sleeps == [3.0]
    assert len(session.calls) == 2


def test_massive_client_applies_configured_request_pacing():
    session = FakeSession([FakeResponse(200, {}), FakeResponse(200, {})])
    sleeps = []
    client = MassiveClient(
        "massive-secret",
        calls_per_minute=5,
        session=session,
        sleep_func=sleeps.append,
    )

    client.get_json("/first")
    client.get_json("/second")

    assert sleeps == [12.0]


def test_massive_error_removes_credentials_from_url_and_body():
    session = FakeSession(
        [FakeResponse(403, text="invalid key massive-secret for https://example.test?apiKey=massive-secret")]
    )
    client = MassiveClient(
        "massive-secret",
        calls_per_minute=0,
        session=session,
        base_url="https://api.massive.test",
    )

    with pytest.raises(JsonApiError) as raised:
        client.get_json("https://api.massive.test/failure?cursor=x&apiKey=massive-secret")

    message = str(raised.value)
    assert "massive-secret" not in message
    assert "?" not in raised.value.url
    assert "[REDACTED]" in message


def test_client_retries_transient_network_error():
    session = FakeSession(
        [
            requests.ConnectionError("temporary outage"),
            FakeResponse(200, {"results": []}),
        ]
    )
    sleeps = []
    client = MassiveClient(
        "massive-secret",
        calls_per_minute=0,
        session=session,
        sleep_func=sleeps.append,
    )

    assert client.get_json("/v3/reference/tickers") == {"results": []}
    assert sleeps == [1.0]


def test_client_rejects_invalid_json_response():
    session = FakeSession([FakeResponse(200, ValueError("invalid"))])
    client = MassiveClient("massive-secret", calls_per_minute=0, session=session)

    with pytest.raises(JsonApiError, match="not valid JSON"):
        client.get_json("/v3/reference/tickers")


def test_alpaca_client_uses_headers_and_page_tokens():
    session = FakeSession(
        [
            FakeResponse(200, {"events": [{"id": "one"}], "next_page_token": "next-token"}),
            FakeResponse(200, {"events": [{"id": "two"}], "next_page_token": None}),
        ]
    )
    client = AlpacaClient(
        "alpaca-key",
        "alpaca-secret",
        calls_per_minute=0,
        session=session,
    )

    rows = client.get_paginated("/v1/events", params={"limit": 1}, results_key="events")

    assert rows == [{"id": "one"}, {"id": "two"}]
    assert session.calls[0]["headers"]["APCA-API-KEY-ID"] == "alpaca-key"
    assert session.calls[0]["headers"]["APCA-API-SECRET-KEY"] == "alpaca-secret"
    assert session.calls[0]["params"] == {"limit": 1}
    assert session.calls[1]["params"] == {"limit": 1, "page_token": "next-token"}


def test_alpaca_client_gets_us_equity_assets_without_status_filter():
    session = FakeSession([FakeResponse(200, [{"id": "one", "symbol": "AAPL"}])])
    client = AlpacaClient(
        "alpaca-key",
        "alpaca-secret",
        calls_per_minute=0,
        session=session,
    )

    assert client.get_assets() == [{"id": "one", "symbol": "AAPL"}]
    assert session.calls[0]["url"].endswith("/v2/assets")
    assert session.calls[0]["params"] == {"asset_class": "us_equity"}


def test_alpaca_stock_bars_disable_symbol_mapping_and_keep_page_parameters():
    session = FakeSession(
        [
            FakeResponse(200, {"bars": {"FB": [{"t": "2022-01-03T05:00:00Z", "v": 1}]}, "next_page_token": "next"}),
            FakeResponse(200, {"bars": {"FB": [{"t": "2022-01-04T05:00:00Z", "v": 2}]}, "next_page_token": None}),
        ]
    )
    client = AlpacaClient("alpaca-key", "alpaca-secret", calls_per_minute=0, session=session)

    pages = list(
        client.iter_stock_bar_pages(
            ["FB"],
            start=date(2016, 1, 1),
            end=date(2022, 6, 8),
            feed="sip",
        )
    )

    assert len(pages) == 2
    assert session.calls[0]["url"].endswith("/v2/stocks/bars")
    assert session.calls[0]["params"] == {
        "symbols": "FB",
        "timeframe": "1Day",
        "start": "2016-01-01",
        "end": "2022-06-08",
        "feed": "sip",
        "adjustment": "raw",
        "asof": "-",
        "limit": 10000,
        "sort": "asc",
    }
    assert session.calls[1]["params"]["page_token"] == "next"
    assert session.calls[1]["params"]["asof"] == "-"


def test_alpaca_corporate_actions_paginate_name_changes():
    session = FakeSession(
        [
            FakeResponse(200, {"corporate_actions": {"name_changes": []}, "next_page_token": "next"}),
            FakeResponse(200, {"corporate_actions": {"name_changes": []}, "next_page_token": None}),
        ]
    )
    client = AlpacaClient("alpaca-key", "alpaca-secret", calls_per_minute=0, session=session)

    pages = list(
        client.iter_corporate_action_pages(
            start=date(2016, 1, 1),
            end=date(2026, 9, 18),
            types="name_change",
        )
    )

    assert len(pages) == 2
    assert session.calls[0]["url"].endswith("/v1/corporate-actions")
    assert session.calls[0]["params"]["types"] == "name_change"
    assert session.calls[0]["params"]["data_quality"] == "all"
    assert session.calls[0]["params"]["region"] == "us"
    assert session.calls[1]["params"]["page_token"] == "next"


def test_alpaca_error_removes_both_credentials():
    session = FakeSession([FakeResponse(401, text="alpaca-key alpaca-secret")])
    client = AlpacaClient(
        "alpaca-key",
        "alpaca-secret",
        calls_per_minute=0,
        session=session,
    )

    with pytest.raises(JsonApiError) as raised:
        client.get_json("/v2/stocks/AAPL/bars")

    message = str(raised.value)
    assert "alpaca-key" not in message
    assert "alpaca-secret" not in message
    assert message.count("[REDACTED]") == 2


def test_provider_credentials_read_environment_and_validate_missing(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("MASSIVE_API_KEY", "massive-key")
    monkeypatch.setenv("ALPACA_API_KEY", "alpaca-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "alpaca-secret")

    assert get_massive_api_key() == "massive-key"
    assert get_alpaca_credentials() == ("alpaca-key", "alpaca-secret")

    monkeypatch.delenv("ALPACA_API_SECRET_KEY")
    with pytest.raises(RuntimeError, match="ALPACA_API_SECRET_KEY"):
        get_alpaca_credentials(dotenv_path=tmp_path / "missing.env")


def test_provider_credentials_can_read_dotenv(monkeypatch, tmp_path: Path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "MASSIVE_API_KEY=massive-dotenv\n"
        "ALPACA_API_KEY=alpaca-dotenv\n"
        "ALPACA_API_SECRET_KEY=alpaca-secret-dotenv\n",
        encoding="utf-8",
    )
    for key in ("MASSIVE_API_KEY", "ALPACA_API_KEY", "ALPACA_API_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)

    assert get_massive_api_key(dotenv_path=dotenv) == "massive-dotenv"
    assert get_alpaca_credentials(dotenv_path=dotenv) == (
        "alpaca-dotenv",
        "alpaca-secret-dotenv",
    )


def test_raw_page_writer_compresses_pages_and_redacts_sensitive_metadata(tmp_path: Path):
    output_path = tmp_path / "raw" / "massive" / "tickers-20260918.jsonl.gz"

    raw_path, metadata_path = write_raw_json_pages(
        [{"results": [{"ticker": "AAPL"}]}, {"results": [{"ticker": "MSFT"}]}],
        output_path,
        provider="massive",
        source_endpoint="/v3/reference/tickers",
        request_params={"active": "true", "apiKey": "must-not-leak", "page_token": "must-not-leak"},
    )

    with gzip.open(raw_path, "rt", encoding="utf-8") as source:
        pages = [json.loads(line) for line in source]
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    assert pages[0]["results"][0]["ticker"] == "AAPL"
    assert pages[1]["results"][0]["ticker"] == "MSFT"
    assert metadata["page_count"] == 2
    assert metadata["byte_count"] == raw_path.stat().st_size
    assert len(metadata["sha256"]) == 64
    assert metadata["provider"] == "massive"
    assert metadata["request_params"] == {
        "active": "true",
        "apiKey": "[REDACTED]",
        "page_token": "[REDACTED]",
    }
    assert "must-not-leak" not in metadata_path.read_text(encoding="utf-8")


def test_raw_page_writer_requires_compressed_jsonl_suffix(tmp_path: Path):
    with pytest.raises(ValueError, match="jsonl.gz"):
        write_raw_json_pages(
            [],
            tmp_path / "raw.json",
            provider="alpaca",
            source_endpoint="/v1/corporate-actions",
        )
