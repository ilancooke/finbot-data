"""Prepare container output directories, then run the requested job."""

from __future__ import annotations

import os
from pathlib import Path
import sys


DEFAULT_UID = 1000
DEFAULT_GID = 1000
DEFAULT_DATA_ROOT = Path("/data")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        return int(value)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer, got {value!r}") from exc


def _output_dirs() -> list[Path]:
    data_root = Path(os.getenv("FINBOT_DATA_ROOT", str(DEFAULT_DATA_ROOT)))
    return [
        data_root,
        Path(os.getenv("FINBOT_RAW_BARS_DIR", str(data_root / "market/daily_bars"))),
        Path(os.getenv("FINBOT_REFERENCE_DIR", str(data_root / "reference"))),
        Path(os.getenv("FINBOT_RATIOS_DIR", str(data_root / "ratios"))),
        Path(os.getenv("FINBOT_FINANCIALS_DIR", str(data_root / "financials"))),
        Path(os.getenv("FINBOT_FUNDAMENTALS_DIR", str(data_root / "fundamentals"))),
        Path(os.getenv("FINBOT_RAW_EXPORT_DIR", str(data_root / "raw/exports/daily_bars"))),
        Path(os.getenv("FINBOT_RAW_FUNDAMENTALS_EXPORT_DIR", str(data_root / "raw/exports/fundamentals/sf1"))),
    ]


def _chown_tree(path: Path, uid: int, gid: int) -> None:
    if not path.exists():
        return

    os.chown(path, uid, gid)
    if not path.is_dir():
        return

    for root, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            os.chown(Path(root) / dirname, uid, gid)
        for filename in filenames:
            os.chown(Path(root) / filename, uid, gid)


def _prepare_data_dirs(uid: int, gid: int) -> None:
    data_root = Path(os.getenv("FINBOT_DATA_ROOT", str(DEFAULT_DATA_ROOT)))
    for output_dir in _output_dirs():
        output_dir.mkdir(parents=True, exist_ok=True)

    _chown_tree(data_root, uid, gid)


def _drop_privileges(uid: int, gid: int) -> None:
    os.setgroups([])
    os.setgid(gid)
    os.setuid(uid)


def main() -> int:
    command = sys.argv[1:]
    if not command:
        raise SystemExit("No command provided to Docker entrypoint")

    uid = _env_int("HOST_UID", DEFAULT_UID)
    gid = _env_int("HOST_GID", DEFAULT_GID)

    if os.geteuid() == 0:
        _prepare_data_dirs(uid, gid)
        _drop_privileges(uid, gid)

    os.execvp(command[0], command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
