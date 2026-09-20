from __future__ import annotations

from collections.abc import Iterable, Mapping
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Any

from market_data.metadata import utc_timestamp_metadata

SENSITIVE_KEY_PARTS = ("api_key", "apikey", "secret", "token", "authorization")


def write_raw_json_pages(
    pages: Iterable[Mapping[str, Any]],
    output_path: str | Path,
    *,
    provider: str,
    source_endpoint: str,
    request_params: Mapping[str, Any] | None = None,
) -> tuple[Path, Path]:
    """Atomically preserve provider response pages as compressed JSONL plus metadata."""

    path = Path(output_path)
    if not path.name.endswith(".jsonl.gz"):
        raise ValueError("raw provider snapshot path must end with .jsonl.gz")
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path = path.with_name(path.name.removesuffix(".jsonl.gz") + ".download.json")

    page_count = 0
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".jsonl.gz", delete=False) as temp_raw:
        temp_raw_path = Path(temp_raw.name)
    try:
        with gzip.open(temp_raw_path, "wt", encoding="utf-8") as output:
            for page in pages:
                output.write(json.dumps(dict(page), sort_keys=True))
                output.write("\n")
                page_count += 1

        raw_sha256 = _sha256_file(temp_raw_path)

        metadata = {
            **utc_timestamp_metadata(),
            "provider": provider,
            "source_endpoint": source_endpoint,
            "format": "jsonl.gz",
            "page_count": page_count,
            "byte_count": temp_raw_path.stat().st_size,
            "sha256": raw_sha256,
            "request_params": redact_mapping(request_params or {}),
            "raw_file": path.name,
        }
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            suffix=".json",
            mode="w",
            encoding="utf-8",
            delete=False,
        ) as temp_metadata:
            temp_metadata_path = Path(temp_metadata.name)
            json.dump(metadata, temp_metadata, indent=2, sort_keys=True)

        temp_raw_path.replace(path)
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_raw_path.unlink(missing_ok=True)
        if "temp_metadata_path" in locals():
            temp_metadata_path.unlink(missing_ok=True)

    return path, metadata_path


def write_raw_text_snapshot(
    text: str,
    output_path: str | Path,
    *,
    provider: str,
    source_url: str,
) -> tuple[Path, Path]:
    """Atomically preserve a public reference page plus download metadata."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path = path.with_name(path.name + ".download.json")

    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        suffix=path.suffix,
        mode="w",
        encoding="utf-8",
        delete=False,
    ) as temp_raw:
        temp_raw_path = Path(temp_raw.name)
        temp_raw.write(text)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        suffix=".json",
        mode="w",
        encoding="utf-8",
        delete=False,
    ) as temp_metadata:
        temp_metadata_path = Path(temp_metadata.name)
        json.dump(
            {
                **utc_timestamp_metadata(),
                "provider": provider,
                "source_url": source_url,
                "format": path.suffix.lstrip(".") or "text",
                "byte_count": len(text.encode("utf-8")),
                "raw_file": path.name,
            },
            temp_metadata,
            indent=2,
            sort_keys=True,
        )

    try:
        temp_raw_path.replace(path)
        temp_metadata_path.replace(metadata_path)
    finally:
        temp_raw_path.unlink(missing_ok=True)
        temp_metadata_path.unlink(missing_ok=True)

    return path, metadata_path


def redact_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): "[REDACTED]" if _is_sensitive_key(str(key)) else value
        for key, value in values.items()
    }


def _is_sensitive_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in SENSITIVE_KEY_PARTS)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
