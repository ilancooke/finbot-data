from __future__ import annotations

import logging
from time import sleep
from typing import Any

import requests

logger = logging.getLogger(__name__)

MASSIVE_BASE_URL = "https://api.massive.com"


class MassiveHttpError(RuntimeError):
    """HTTP error returned by Massive, with API credentials omitted."""

    def __init__(self, method: str, url: str, status_code: int, body: str) -> None:
        self.method = method
        self.url = url
        self.status_code = status_code
        self.body = body[:500]
        super().__init__(
            f"Massive request failed method={method} url={url} "
            f"status_code={status_code} body={self.body!r}"
        )


class MassiveClient:
    """Small Massive REST client with sanitized errors and pagination helpers."""

    def __init__(
        self,
        api_key: str,
        base_url: str = MASSIVE_BASE_URL,
        timeout: int = 60,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

    def get_json(
        self,
        path_or_url: str,
        params: dict[str, Any] | None = None,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """GET a Massive JSON response, retrying 429s with simple backoff."""

        url = self._url(path_or_url)
        request_params = {"apiKey": self.api_key, **(params or {})}
        for attempt in range(max_retries + 1):
            response = self.session.get(
                url,
                params=request_params,
                headers={"Accept": "application/json"},
                timeout=self.timeout,
            )
            if response.status_code != 429:
                if response.status_code >= 400:
                    raise MassiveHttpError("GET", url, response.status_code, response.text)
                return response.json()

            if attempt == max_retries:
                raise MassiveHttpError("GET", url, response.status_code, response.text)
            wait_seconds = self._retry_after_seconds(response, attempt)
            logger.warning("Massive rate limited url=%s wait_seconds=%.1f", url, wait_seconds)
            sleep(wait_seconds)

        raise AssertionError("unreachable")

    def get_paginated(
        self,
        path_or_url: str,
        params: dict[str, Any] | None = None,
        results_key: str = "results",
        calls_per_minute: float = 0,
    ) -> list[dict[str, Any]]:
        """Collect all pages from a Massive endpoint that returns next_url."""

        rows: list[dict[str, Any]] = []
        next_url: str | None = path_or_url
        next_params = params
        page_number = 0
        while next_url:
            page_number += 1
            payload = self.get_json(next_url, params=next_params)
            page_rows = payload.get(results_key) or []
            rows.extend(page_rows)
            next_url = payload.get("next_url")
            next_params = None
            logger.info(
                "Fetched Massive page page=%d rows=%d total_rows=%d next_page=%s",
                page_number,
                len(page_rows),
                len(rows),
                bool(next_url),
            )
            if next_url:
                wait_seconds = self.rate_limit_wait_seconds(calls_per_minute)
                if wait_seconds > 0:
                    logger.info("Sleeping %.1fs before next Massive page", wait_seconds)
                self.sleep_for_rate_limit(calls_per_minute)
        return rows

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            return path_or_url
        return f"{self.base_url}/{path_or_url.lstrip('/')}"

    @staticmethod
    def _retry_after_seconds(response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
        return min(2.0**attempt, 60.0)

    @staticmethod
    def sleep_for_rate_limit(calls_per_minute: float) -> None:
        wait_seconds = MassiveClient.rate_limit_wait_seconds(calls_per_minute)
        if wait_seconds > 0:
            sleep(wait_seconds)

    @staticmethod
    def rate_limit_wait_seconds(calls_per_minute: float) -> float:
        if calls_per_minute <= 0:
            return 0.0
        return 60.0 / calls_per_minute
