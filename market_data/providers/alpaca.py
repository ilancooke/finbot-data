from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import date
from typing import Any

from market_data.http import JsonApiError, JsonRestClient

ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
ALPACA_TRADING_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_FREE_CALLS_PER_MINUTE = 200

AlpacaApiError = JsonApiError


class AlpacaClient(JsonRestClient):
    """Minimal Alpaca REST client with page-token pagination."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = ALPACA_DATA_BASE_URL,
        timeout: int = 60,
        max_retries: int = 3,
        calls_per_minute: float = ALPACA_FREE_CALLS_PER_MINUTE,
        session: Any | None = None,
        sleep_func: Any = None,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if sleep_func is not None:
            kwargs["sleep_func"] = sleep_func
        super().__init__(
            provider="Alpaca",
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            calls_per_minute=calls_per_minute,
            default_headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
            secrets=(api_key, api_secret),
            session=session,
            **kwargs,
        )

    def iter_pages(
        self,
        path_or_url: str,
        params: Mapping[str, Any] | None = None,
        *,
        token_key: str = "next_page_token",
        token_param: str = "page_token",
    ) -> Iterator[dict[str, Any]]:
        request_params = dict(params or {})
        while True:
            payload = self.get_json(path_or_url, params=request_params)
            if not isinstance(payload, dict):
                raise self._error(url=self._url(path_or_url), status_code=200, body="paginated response must be an object")
            yield payload
            token = payload.get(token_key)
            if not token:
                break
            request_params[token_param] = token

    def get_paginated(
        self,
        path_or_url: str,
        params: Mapping[str, Any] | None = None,
        *,
        results_key: str,
        token_key: str = "next_page_token",
        token_param: str = "page_token",
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for payload in self.iter_pages(
            path_or_url,
            params=params,
            token_key=token_key,
            token_param=token_param,
        ):
            page_rows = payload.get(results_key) or []
            if not isinstance(page_rows, list) or not all(isinstance(row, dict) for row in page_rows):
                raise self._error(
                    url=self._url(path_or_url),
                    status_code=200,
                    body=f"{results_key!r} must be an array of objects",
                )
            rows.extend(page_rows)
        return rows

    def get_assets(
        self,
        *,
        status: str | None = None,
        asset_class: str = "us_equity",
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"asset_class": asset_class}
        if status is not None:
            params["status"] = status
        payload = self.get_json("/v2/assets", params=params)
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise self._error(
                url=self._url("/v2/assets"),
                status_code=200,
                body="assets response must be an array of objects",
            )
        return payload

    def iter_stock_bar_pages(
        self,
        symbols: list[str],
        *,
        start: date | str,
        end: date | str,
        feed: str = "sip",
        adjustment: str = "raw",
        asof: str = "-",
        limit: int = 10000,
    ) -> Iterator[dict[str, Any]]:
        """Iterate daily stock-bar pages without historical symbol remapping."""

        if not symbols:
            raise ValueError("at least one stock symbol is required")
        params = {
            "symbols": ",".join(symbols),
            "timeframe": "1Day",
            "start": start.isoformat() if isinstance(start, date) else start,
            "end": end.isoformat() if isinstance(end, date) else end,
            "feed": feed,
            "adjustment": adjustment,
            "asof": asof,
            "limit": limit,
            "sort": "asc",
        }
        for payload in self.iter_pages("/v2/stocks/bars", params=params):
            bars = payload.get("bars")
            if not isinstance(bars, dict) or not all(
                isinstance(symbol, str) and isinstance(values, list)
                for symbol, values in bars.items()
            ):
                raise self._error(
                    url=self._url("/v2/stocks/bars"),
                    status_code=200,
                    body="bars must be an object keyed by symbol",
                )
            yield payload

    def iter_corporate_action_pages(
        self,
        *,
        start: date | str,
        end: date | str,
        types: str | None = None,
        symbols: list[str] | None = None,
        data_quality: str = "all",
        region: str = "us",
        limit: int = 1000,
    ) -> Iterator[dict[str, Any]]:
        params: dict[str, Any] = {
            "start": start.isoformat() if isinstance(start, date) else start,
            "end": end.isoformat() if isinstance(end, date) else end,
            "data_quality": data_quality,
            "region": region,
            "limit": limit,
            "sort": "asc",
        }
        if types:
            params["types"] = types
        if symbols:
            params["symbols"] = ",".join(symbols)
        for payload in self.iter_pages("/v1/corporate-actions", params=params):
            actions = payload.get("corporate_actions")
            if not isinstance(actions, dict) or not all(
                isinstance(action_type, str) and isinstance(values, list)
                for action_type, values in actions.items()
            ):
                raise self._error(
                    url=self._url("/v1/corporate-actions"),
                    status_code=200,
                    body="corporate_actions must be an object keyed by action type",
                )
            yield payload
