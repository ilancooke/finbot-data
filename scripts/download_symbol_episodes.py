"""Build dated historical ticker episodes for stable Finbot security identities."""

from __future__ import annotations

import argparse
from datetime import date
import json
import logging
from pathlib import Path
import sys
import time
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_data.datasets.symbol_episodes import download_and_write_symbol_episodes

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Finbot symbol-episode boundaries")
    parser.add_argument("--snapshot-date", type=_parse_date, default=None)
    parser.add_argument("--reference-dir", default=None)
    parser.add_argument("--raw-dir", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    args = _parse_args(argv)
    started = time.perf_counter()
    try:
        outputs = download_and_write_symbol_episodes(
            snapshot_date=args.snapshot_date,
            reference_dir=args.reference_dir,
            raw_dir=args.raw_dir,
        )
        metadata = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
        logger.info(
            "Symbol episodes completed rows=%s unresolved=%s elapsed_seconds=%.1f",
            metadata["row_count"],
            metadata["unresolved_episode_count"],
            time.perf_counter() - started,
        )
        return 0
    except Exception:
        logger.exception("Symbol episode build failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
