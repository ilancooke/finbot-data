from __future__ import annotations

from collections.abc import Callable
import logging
from time import sleep
from typing import Any

import requests

from market_data.http import RETRYABLE_STATUS_CODES

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "finbot-data/1.0 (reference-data acquisition)"


class ReferencePageError(RuntimeError):
    pass


def download_text_page(
    url: str,
    *,
    timeout: int = 60,
    max_retries: int = 3,
    session: requests.Session | None = None,
    sleep_func: Callable[[float], None] = sleep,
) -> str:
    """Download a public reference page with bounded transient retries."""

    client = session or requests.Session()
    for attempt in range(max_retries + 1):
        try:
            response = client.get(
                url,
                headers={"Accept": "text/html", "User-Agent": DEFAULT_USER_AGENT},
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt < max_retries:
                sleep_func(min(2.0**attempt, 60.0))
                continue
            raise ReferencePageError(f"Reference page request failed url={url!r}: {exc}") from exc

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries:
            wait_seconds = _retry_seconds(response, attempt)
            logger.warning("Reference page retry url=%s wait_seconds=%.1f", url, wait_seconds)
            sleep_func(wait_seconds)
            continue
        if response.status_code >= 400:
            raise ReferencePageError(
                f"Reference page request failed url={url!r} status_code={response.status_code}"
            )
        return response.text

    raise AssertionError("unreachable")


def _retry_seconds(response: Any, attempt: int) -> float:
    value = response.headers.get("Retry-After")
    if value:
        try:
            return max(float(value), 0.0)
        except ValueError:
            pass
    return min(2.0**attempt, 60.0)
