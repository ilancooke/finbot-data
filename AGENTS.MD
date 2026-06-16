# Agent Guidelines

This repository is a plain-script market data downloader for Finbot. Future coding agents should keep the project small, operationally boring, and focused on data acquisition.

## Project Context

- This repo was split out from the main Finbot modeling repo.
- Other finbot repos should own feature engineering, preprocessing, model training, and inference.
- This repo should only download, normalize, and store market data.
- Finbot uses Sharadar via Nasdaq Data Link as its data provider for daily prices, ticker reference metadata, SF1 fundamentals, and daily valuation metrics.
- This is intentionally not an installable Python package right now. Do not add packaging metadata, console scripts, or a `src/` package layout unless the user asks to revisit that decision.

## Current File Structure

- `scripts/`: thin command-line entrypoints. Each data pull should have its own script here.
- `market_data/`: shared implementation code used by scripts.
- `market_data/providers/`: provider-specific API adapters.
- `market_data/datasets/`: dataset-specific orchestration.
- `tests/`: pytest coverage for normalization, CLI behavior, provider clients, and ticker filtering.
- `data/`: local output directory, ignored by git.
- `Dockerfile` and `compose.yaml`: container workflow for scheduled jobs.

## Development Rules

- Keep dependencies minimal and data-download focused.
- Do not add dashboard, modeling, training, feature engineering, or inference code here.
- Prefer one focused script per data pull instead of one large downloader.
- Put reusable logic in `market_data/`; keep files in `scripts/` as thin CLI entrypoints.
- Keep output files under `data/` by default.
- Write parquet data plus sidecar metadata JSON for durable datasets.
- Prefer `FINBOT_DATA_ROOT` for shared multipackage deployments. Keep dataset-specific env vars such as `FINBOT_RAW_BARS_DIR` and `FINBOT_REFERENCE_DIR` as explicit overrides.
- Avoid exposing API keys in logs, test output, errors, README examples, or commit messages.
- Preserve support for `.env` and environment variables.
- Preserve Docker Compose support for homelab scheduling.
- When adding a new data-pull script, add a matching Docker Compose service that reuses the shared image, environment, data mount, and Docker entrypoint.
- When adding a new Nasdaq Data Link endpoint, keep the client small and sanitize API keys from errors.
- When adding per-symbol jobs, use `data/reference/tickers.parquet` as the ticker universe input instead of hardcoding ticker lists.

## Data Storage Mental Model

- Daily prices are maintained as the canonical Finbot historical dataset built from Nasdaq Data Link Sharadar SEP table export files.
- The filtered daily price output is `data/daily_bars/historical.parquet`.
- When `FINBOT_DATA_ROOT` is set, daily prices default to `FINBOT_DATA_ROOT/market/daily_bars`.
- Daily price metadata uses a matching `historical.metadata.json` sidecar.
- Initial daily price backfills download raw export files under `data/raw/nasdaq_data_link/sharadar/sep`, or `FINBOT_DATA_ROOT/raw/nasdaq_data_link/sharadar/sep` when `FINBOT_DATA_ROOT` is set.
- Daily price updates use one paginated `lastupdated.gte` query across the SEP table, filter locally to `tickers.parquet`, and merge by `symbol,date`.
- SF1 fundamentals are maintained as the canonical Finbot fundamentals dataset built from Nasdaq Data Link Sharadar SF1 table export files.
- The filtered SF1 fundamentals output is `data/fundamentals/sf1.parquet`.
- Initial SF1 backfills download raw export files under `data/raw/nasdaq_data_link/sharadar/sf1`, or `FINBOT_DATA_ROOT/raw/nasdaq_data_link/sharadar/sf1` when `FINBOT_DATA_ROOT` is set.
- SF1 updates use one paginated `lastupdated.gte` query across the SF1 table, filter locally to `tickers.parquet`, and merge by `ticker,dimension,datekey,reportperiod`.
- Daily valuation metrics are maintained from the Nasdaq Data Link Sharadar DAILY table.
- The filtered daily valuation metrics output is `data/fundamentals/daily_valuation_metrics.parquet`.
- Initial DAILY backfills download raw export files under `data/raw/nasdaq_data_link/sharadar/daily`, or `FINBOT_DATA_ROOT/raw/nasdaq_data_link/sharadar/daily` when `FINBOT_DATA_ROOT` is set.
- DAILY updates use one paginated `lastupdated.gte` query across the DAILY table, filter locally to `tickers.parquet`, and merge by `ticker,date`.
- Keep all SF1 dimensions in one provider-normalized parquet at the data layer. Dimension-specific point-in-time feature views belong in `finbot-features`.
- For later feature engineering, prefer AR dimensions for backtest-safe features: `ART` for trailing-twelve-month fundamentals, `ARQ` for quarterly change features, and `ARY` for annual context. Treat AR `datekey` as the information availability date. MR dimensions include restatements and are mainly for current/restated business analysis.
- Do not create provider-specific dataset names; put provider/source fields in metadata JSON.
- The full Sharadar ticker universe raw download is `data/raw/nasdaq_data_link/sharadar/tickers/tickers_sep_raw.jsonl`.
- The ticker universe output is `data/reference/tickers.parquet`.
- When `FINBOT_DATA_ROOT` is set, reference datasets default to `FINBOT_DATA_ROOT/reference`.
- When `FINBOT_DATA_ROOT` is set, raw ticker downloads default to `FINBOT_DATA_ROOT/raw/nasdaq_data_link/sharadar/tickers`.
- The ticker universe metadata file is `data/reference/tickers.metadata.json`.
- For duplicate `symbol,date` rows in daily bars, keep the newest downloaded row.

## Docker Mental Model

- Containers are intended for scheduled homelab operation.
- Compose reads `.env` for `NASDAQ_DATA_LINK_API_KEY`.
- Compose mounts `${FINBOT_HOST_DATA_ROOT:-./data}` to container `/data`.
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

Tests should avoid real provider API calls. Use fake clients, fake sessions, temporary directories, and deterministic dates.

## README And Roadmap

- Update `README.md` when commands, environment variables, outputs, or schemas change.
- Update `ROADMAP.md` when future plans materially change.
- Keep docs practical and operational. Prefer commands the user can run directly.
