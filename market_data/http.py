from __future__ import annotations

from collections.abc import Callable, Mapping
from email.utils import parsedate_to_datetime
import json
import logging
from time import sleep
from time import monotonic
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
JsonValue = dict[str, Any] | list[Any]


class JsonApiError(RuntimeError):
    """Provider HTTP/JSON error whose message has credentials removed."""

    def __init__(
        self,
        *,
        provider: str,
        method: str,
        url: str,
        status_code: int | None,
        body: str,
        secrets: tuple[str, ...] = (),
    ) -> None:
        self.provider = provider
        self.method = method
        self.url = _safe_url(url)
        self.status_code = status_code
        self.body = _redact(body[:500], secrets)
        super().__init__(
            f"{provider} request failed method={method} url={self.url} "
            f"status_code={status_code} body={self.body!r}"
        )


class JsonRestClient:
    """Small requests-based JSON client with retries and conservative pacing."""

    def __init__(
        self,
        *,
        provider: str,
        base_url: str,
        timeout: int = 60,
        max_retries: int = 3,
        calls_per_minute: float = 0,
        default_headers: Mapping[str, str] | None = None,
        default_params: Mapping[str, Any] | None = None,
        secrets: tuple[str, ...] = (),
        session: requests.Session | None = None,
        sleep_func: Callable[[float], None] = sleep,
    ) -> None:
        self.provider = provider
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.calls_per_minute = calls_per_minute
        self.default_headers = dict(default_headers or {})
        self.default_params = dict(default_params or {})
        self.secrets = tuple(secret for secret in secrets if secret)
        self.session = session or requests.Session()
        self.sleep_func = sleep_func
        self._request_count = 0
        self._last_request_started: float | None = None

    def get_json(
        self,
        path_or_url: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> JsonValue:
        url = self._url(path_or_url)
        request_params = {**self.default_params, **dict(params or {})}
        request_headers = {"Accept": "application/json", **self.default_headers, **dict(headers or {})}

        for attempt in range(self.max_retries + 1):
            self._pace_request()
            try:
                response = self.session.get(
                    url,
                    params=request_params,
                    headers=request_headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    self._sleep_before_retry(None, attempt)
                    continue
                raise self._error(url=url, status_code=None, body=str(exc)) from exc

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                self._sleep_before_retry(response, attempt)
                continue
            if response.status_code >= 400:
                raise self._error(url=url, status_code=response.status_code, body=response.text)

            try:
                payload = response.json()
            except (json.JSONDecodeError, ValueError) as exc:
                raise self._error(
                    url=url,
                    status_code=response.status_code,
                    body="response body is not valid JSON",
                ) from exc
            if not isinstance(payload, (dict, list)):
                raise self._error(
                    url=url,
                    status_code=response.status_code,
                    body="response JSON must be an object or array",
                )
            return payload

        raise AssertionError("unreachable")

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            return path_or_url
        return f"{self.base_url}/{path_or_url.lstrip('/')}"

    def _pace_request(self) -> None:
        now = monotonic()
        interval = 60.0 / self.calls_per_minute if self.calls_per_minute > 0 else 0.0
        if self._last_request_started is not None and interval > 0:
            elapsed = now - self._last_request_started
            wait_seconds = round(max(interval - elapsed, 0.0), 3)
            if wait_seconds > 0:
                self.sleep_func(wait_seconds)
                now += wait_seconds
        self._last_request_started = now
        self._request_count += 1

    def _sleep_before_retry(self, response: Any | None, attempt: int) -> None:
        wait_seconds = _retry_after_seconds(response, attempt)
        logger.warning(
            "%s request retry attempt=%d wait_seconds=%.1f",
            self.provider,
            attempt + 1,
            wait_seconds,
        )
        self.sleep_func(wait_seconds)

    def _error(self, *, url: str, status_code: int | None, body: str) -> JsonApiError:
        return JsonApiError(
            provider=self.provider,
            method="GET",
            url=url,
            status_code=status_code,
            body=body,
            secrets=self.secrets,
        )


def _retry_after_seconds(response: Any | None, attempt: int) -> float:
    retry_after = None if response is None else response.headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 0.0)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                now = parsedate_to_datetime(response.headers.get("Date")) if response.headers.get("Date") else None
                if now is not None:
                    return max((retry_at - now).total_seconds(), 0.0)
            except (TypeError, ValueError, OverflowError):
                pass
    return min(2.0**attempt, 60.0)


def _safe_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _redact(value: str, secrets: tuple[str, ...]) -> str:
    redacted = value
    for secret in secrets:
        redacted = redacted.replace(secret, "[REDACTED]")
    return redacted
