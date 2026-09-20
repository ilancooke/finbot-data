from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from market_data.http import JsonApiError, JsonRestClient

MASSIVE_BASE_URL = "https://api.massive.com"
MASSIVE_FREE_CALLS_PER_MINUTE = 5

MassiveApiError = JsonApiError


class MassiveClient(JsonRestClient):
    """Minimal Massive REST client with next-URL pagination."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = MASSIVE_BASE_URL,
        timeout: int = 60,
        max_retries: int = 3,
        calls_per_minute: float = MASSIVE_FREE_CALLS_PER_MINUTE,
        session: Any | None = None,
        sleep_func: Any = None,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if sleep_func is not None:
            kwargs["sleep_func"] = sleep_func
        super().__init__(
            provider="Massive",
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            calls_per_minute=calls_per_minute,
            default_params={"apiKey": api_key},
            secrets=(api_key,),
            session=session,
            **kwargs,
        )

    def iter_pages(
        self,
        path_or_url: str,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        next_url: str | None = path_or_url
        next_params = dict(params or {})
        while next_url:
            payload = self.get_json(next_url, params=next_params)
            if not isinstance(payload, dict):
                raise self._error(url=self._url(next_url), status_code=200, body="paginated response must be an object")
            yield payload
            value = payload.get("next_url")
            next_url = str(value) if value else None
            next_params = {}

    def get_paginated(
        self,
        path_or_url: str,
        params: Mapping[str, Any] | None = None,
        *,
        results_key: str = "results",
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for payload in self.iter_pages(path_or_url, params=params):
            page_rows = payload.get(results_key) or []
            if not isinstance(page_rows, list) or not all(isinstance(row, dict) for row in page_rows):
                raise self._error(
                    url=self._url(path_or_url),
                    status_code=200,
                    body=f"{results_key!r} must be an array of objects",
                )
            rows.extend(page_rows)
        return rows
