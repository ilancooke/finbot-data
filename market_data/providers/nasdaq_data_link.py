from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

NASDAQ_DATA_LINK_BASE_URL = "https://data.nasdaq.com"


class NasdaqDataLinkError(RuntimeError):
    """HTTP error returned by Nasdaq Data Link, with API credentials omitted."""

    def __init__(self, method: str, url: str, status_code: int, body: str) -> None:
        self.method = method
        self.url = url
        self.status_code = status_code
        self.body = body[:500]
        super().__init__(
            f"Nasdaq Data Link request failed method={method} url={url} "
            f"status_code={status_code} body={self.body!r}"
        )


class NasdaqDataLinkClient:
    """Small Nasdaq Data Link Tables API client."""

    def __init__(
        self,
        api_key: str,
        base_url: str = NASDAQ_DATA_LINK_BASE_URL,
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
    ) -> dict[str, Any]:
        url = self._url(path_or_url)
        request_params = {"api_key": self.api_key, **(params or {})}
        response = self.session.get(
            url,
            params=request_params,
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise NasdaqDataLinkError("GET", url, response.status_code, response.text)
        payload = response.json()
        if not isinstance(payload, dict):
            raise NasdaqDataLinkError("GET", url, response.status_code, "response JSON must be an object")
        return payload

    def get_bulk_download(
        self,
        table_code: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Request or check a Nasdaq Data Link bulk download job."""

        url = self._url(f"/api/v1/bulkdownloads/{table_code}")
        response = self.session.get(
            url,
            params=params or {},
            headers={
                "Accept": "application/json",
                "X-Api-Token": self.api_key,
            },
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise NasdaqDataLinkError("GET", url, response.status_code, response.text)
        payload = response.json()
        if not isinstance(payload, dict):
            raise NasdaqDataLinkError("GET", url, response.status_code, "response JSON must be an object")
        return payload

    def get_table_export(
        self,
        table_code: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Request a Tables API export job with qopts.export=true."""

        request_params = {"qopts.export": "true", **(params or {})}
        return self.get_json(f"/api/v3/datatables/{table_code}.json", request_params)

    def download_file(self, url: str, output_path: str | Path, chunk_size: int = 1024 * 1024) -> Path:
        """Download a bulk file using Nasdaq Data Link token authentication."""

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        response = self.session.get(
            url,
            headers={"X-Api-Token": self.api_key},
            stream=True,
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise NasdaqDataLinkError("GET", url, response.status_code, response.text)

        with output_path.open("wb") as output:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    output.write(chunk)
        return output_path

    def get_table(
        self,
        table_code: str,
        params: dict[str, Any] | None = None,
        paginate: bool = True,
    ) -> list[dict[str, Any]]:
        """Read rows from a Nasdaq Data Link datatable as dictionaries."""

        path = f"/api/v3/datatables/{table_code}.json"
        request_params = dict(params or {})
        rows: list[dict[str, Any]] = []
        page_number = 0

        while True:
            page_number += 1
            payload = self.get_json(path, request_params)
            datatable = payload.get("datatable") or {}
            columns = datatable.get("columns") or []
            column_names = [column.get("name") for column in columns if isinstance(column, dict)]
            page_rows = [
                dict(zip(column_names, row, strict=False))
                for row in datatable.get("data") or []
            ]
            rows.extend(page_rows)

            cursor = (payload.get("meta") or {}).get("next_cursor_id")
            logger.info(
                "Fetched Nasdaq Data Link table page table=%s page=%d rows=%d total_rows=%d next_page=%s",
                table_code,
                page_number,
                len(page_rows),
                len(rows),
                bool(cursor),
            )
            if not paginate or not cursor:
                return rows
            request_params["qopts.cursor_id"] = cursor

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            return path_or_url
        return f"{self.base_url}/{path_or_url.lstrip('/')}"
