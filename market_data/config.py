from __future__ import annotations

import os
from pathlib import Path

DEFAULT_DOTENV_FILE = ".env"


def parse_dotenv_value(key: str, dotenv_path: Path | None = None) -> str | None:
    dotenv_path = dotenv_path or Path.cwd() / DEFAULT_DOTENV_FILE
    if not dotenv_path.exists():
        return None

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        env_key, env_value = line.split("=", 1)
        if env_key.strip() == key:
            return env_value.strip().strip('"').strip("'")
    return None


def get_env(key: str, default: str = "", dotenv_path: Path | None = None) -> str:
    value = os.getenv(key)
    if value:
        return value

    dotenv_value = parse_dotenv_value(key, dotenv_path=dotenv_path)
    if dotenv_value:
        return dotenv_value

    return default


def resolve_finbot_data_path(
    explicit_path: str | Path | None,
    env_key: str,
    default_path: str | Path,
    data_root_subpath: str | Path,
) -> Path:
    """Resolve a dataset path from CLI, dataset env var, data root, or legacy default."""

    if explicit_path is not None:
        return Path(explicit_path)

    env_value = get_env(env_key)
    if env_value:
        return Path(env_value)

    data_root = get_env("FINBOT_DATA_ROOT")
    if data_root:
        return Path(data_root) / data_root_subpath

    return Path(default_path)
