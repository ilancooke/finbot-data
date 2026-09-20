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


def get_massive_api_key(dotenv_path: Path | None = None) -> str:
    api_key = get_env("MASSIVE_API_KEY", dotenv_path=dotenv_path)
    if not api_key:
        raise RuntimeError("Missing MASSIVE_API_KEY")
    return api_key


def get_alpaca_credentials(dotenv_path: Path | None = None) -> tuple[str, str]:
    api_key = get_env("ALPACA_API_KEY", dotenv_path=dotenv_path)
    api_secret = get_env("ALPACA_API_SECRET_KEY", dotenv_path=dotenv_path)
    missing = [
        name
        for name, value in (
            ("ALPACA_API_KEY", api_key),
            ("ALPACA_API_SECRET_KEY", api_secret),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing {', '.join(missing)}")
    return api_key, api_secret


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
