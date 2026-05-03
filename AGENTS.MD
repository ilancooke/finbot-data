# Agent Guidelines

This repository is a plain-script market data downloader for Finbot. Future coding agents should keep the project small, operationally boring, and focused on data acquisition.

## Project Context

- This repo was split out from the main Finbot modeling repo.
- Other finbot repos should own feature engineering, preprocessing, model training, and inference.
- This repo should only download, normalize, and store market data.
- The current data provider is Massive, formerly Polygon.
- Alpaca provider code may exist as migration context, but new work should not use Alpaca unless explicitly requested.
- This is intentionally not an installable Python package right now. Do not add packaging metadata, console scripts, or a `src/` package layout unless the user asks to revisit that decision.

## Current File Structure

- `scripts/`: thin command-line entrypoints. Each data pull should have its own script here.
- `market_data/`: shared implementation code used by scripts.
- `market_data/providers/`: provider-specific API adapters.
- `market_data/datasets/`: dataset-specific orchestration.
- `market_data/http.py`: shared Massive HTTP client, pagination, retry, throttling, and sanitized errors.
- `market_data/storage.py`: shared parquet and metadata writing for daily bars.
- `market_data/universe.py`: ticker universe fetching, filtering, reading, and writing.
- `tests/`: pytest coverage for normalization, storage, CLI behavior, Massive HTTP behavior, and ticker filtering.
- `data/`: local output directory, ignored by git.
- `Dockerfile` and `compose.yaml`: container workflow for scheduled jobs.

## Development Rules

- Keep dependencies minimal and data-download focused.
- Do not add dashboard, modeling, training, feature engineering, or inference code here.
- Prefer one focused script per data pull instead of one large downloader.
- Put reusable logic in `market_data/`; keep files in `scripts/` as thin CLI entrypoints.
- Keep output files under `data/` by default.
- Write parquet data plus sidecar metadata JSON for durable datasets.
- Avoid exposing API keys in logs, test output, errors, README examples, or commit messages.
- Preserve support for `.env` and environment variables.
- Preserve Docker Compose support for homelab scheduling.
- When adding a new data-pull script, add a matching Docker Compose service that reuses the shared image, environment, data mount, and Docker entrypoint.
- When adding a new Massive endpoint, use the shared Massive HTTP client unless there is a clear reason not to.
- When adding per-symbol jobs, use `data/reference/tickers.parquet` as the ticker universe input instead of hardcoding ticker lists.

## Data Storage Mental Model

- Daily bars are currently maintained as a replace-style historical snapshot.
- The default daily bars output is `data/daily_bars/historical.parquet`.
- The daily bars metadata file is `data/daily_bars/historical.metadata.json`.
- The ticker universe output is `data/reference/tickers.parquet`.
- The ticker universe metadata file is `data/reference/tickers.metadata.json`.
- For duplicate `symbol,date` rows in daily bars, keep the newest downloaded row.
- For Massive adjusted grouped bars, remember that adjusted volume may be fractional because Massive adjusts volume along with prices.

## Docker Mental Model

- Containers are intended for scheduled homelab operation.
- Compose reads `.env` for `MASSIVE_API_KEY`.
- Compose mounts host `./data` to container `/app/data`.
- The Docker entrypoint creates output directories, fixes ownership, then drops privileges to `HOST_UID:HOST_GID`.
- Compose passes `HOST_UID` and `HOST_GID` into the container so output files are host-user owned instead of root-owned.
- Each focused data-pull script should have a dedicated Compose service for manual and scheduled operation; adding scheduling itself is separate work and should only be done when requested.
- If code, dependencies, `Dockerfile`, or `compose.yaml` change, rebuild the image before running the job.
- `.env` changes do not require rebuilding the image.

## Testing

Before finishing code changes, run:

```bash
.venv/bin/python -m compileall market_data scripts
.venv/bin/python -m pytest
```

If `.venv` is unavailable, use the active Python environment:

```bash
python -m compileall market_data scripts
pytest
```

Tests should avoid real Massive API calls. Use fake clients, fake sessions, temporary directories, and deterministic dates.

## README And Roadmap

- Update `README.md` when commands, environment variables, outputs, or schemas change.
- Update `ROADMAP.md` when future plans materially change.
- Keep docs practical and operational. Prefer commands the user can run directly.
