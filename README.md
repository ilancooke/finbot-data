# finbot-data

Plain-script market data downloader for Finbot.

This repository only downloads, normalizes, and stores data. Feature engineering, preprocessing,
model training, inference, and dashboard code live elsewhere.

Finbot uses Sharadar via Nasdaq Data Link for:

- ticker reference metadata from `SHARADAR/TICKERS`
- historical daily prices from `SHARADAR/SEP`
- SF1 fundamentals from `SHARADAR/SF1`
- daily valuation metrics from `SHARADAR/DAILY`

## Setup

Create and activate a Python 3.11 virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Dependencies are intentionally limited to the active downloader and its tests:

- `pandas`
- `pyarrow`
- `pytest`
- `requests`

## Project Layout

- `scripts/`: command-line entrypoints.
- `market_data/datasets/`: dataset-specific orchestration.
- `market_data/providers/`: provider API clients.
- `market_data/config.py`: environment and `.env` helpers.
- `tests/`: local test suite.

## Ticker Universe Workflow

Download the full Sharadar ticker universe and write the filtered Finbot universe:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
NASDAQ_DATA_LINK_API_KEY=... \
python scripts/download_ticker_universe.py
```

This writes:

- `FINBOT_DATA_ROOT/raw/nasdaq_data_link/sharadar/tickers/tickers_sep_raw.jsonl`
- `FINBOT_DATA_ROOT/raw/nasdaq_data_link/sharadar/tickers/tickers_sep_raw.download.json`
- `FINBOT_DATA_ROOT/reference/tickers.parquet`
- `FINBOT_DATA_ROOT/reference/tickers.metadata.json`

The filtered `tickers.parquet` keeps active domestic mid, large, and mega cap common stocks on
`NASDAQ`, `NYSE`, or `NYSEMKT`.

## Daily Price Workflow

Download a Nasdaq Data Link table export and build the historical daily price dataset:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
NASDAQ_DATA_LINK_API_KEY=... \
python scripts/download_historical_prices.py
```

This downloads the provider table export zip under `FINBOT_DATA_ROOT/raw/exports/daily_bars`,
streams through it in chunks, filters to `reference/tickers.parquet`, and writes:

- `FINBOT_DATA_ROOT/market/daily_bars/historical.parquet`
- `FINBOT_DATA_ROOT/market/daily_bars/historical.metadata.json`

If you already have an export file, convert it directly:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
python scripts/download_historical_prices.py \
  --input-file /path/to/SHARADAR_SEP.zip
```

Incremental updates use one paginated `lastupdated` filter across the SEP table, filter locally to
`reference/tickers.parquet`, and merge changed rows by `symbol,date`:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
NASDAQ_DATA_LINK_API_KEY=... \
python scripts/update_historical_prices.py --lastupdated-gte 2026-06-11
```

## Fundamentals Workflow

Download a Nasdaq Data Link table export and build the SF1 fundamentals dataset:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
NASDAQ_DATA_LINK_API_KEY=... \
python scripts/download_fundamentals.py
```

This downloads the provider table export zip under
`FINBOT_DATA_ROOT/raw/exports/fundamentals/sf1`, streams through it in chunks, filters to
`reference/tickers.parquet`, and writes:

- `FINBOT_DATA_ROOT/fundamentals/sf1.parquet`
- `FINBOT_DATA_ROOT/fundamentals/sf1.metadata.json`

If you already have an export file, convert it directly:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
python scripts/download_fundamentals.py \
  --input-file /path/to/SHARADAR_SF1.zip
```

Incremental SF1 updates use one paginated `lastupdated` filter across the SF1 table, filter locally
to `reference/tickers.parquet`, and merge changed rows by Sharadar's primary key:

- `ticker`
- `dimension`
- `datekey`
- `reportperiod`

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
NASDAQ_DATA_LINK_API_KEY=... \
python scripts/update_fundamentals.py --lastupdated-gte 2026-06-11
```

The SF1 workflow preserves all six Sharadar dimensions in one parquet dataset:

- `ARQ`, `ART`, `ARY`: as-reported quarterly, trailing-twelve-month, and annual data.
- `MRQ`, `MRT`, `MRY`: most-recent reported quarterly, trailing-twelve-month, and annual data.

Downstream feature packages should treat `fundamentals/sf1.parquet` as a provider-normalized input
table, not as a finished feature dataset. For point-in-time alpha research and backtests, prefer
the as-reported dimensions first.

## Daily Valuation Metrics Workflow

Download a Nasdaq Data Link table export and build the daily valuation metrics dataset:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
NASDAQ_DATA_LINK_API_KEY=... \
python scripts/download_daily_valuation_metrics.py
```

This downloads the provider table export zip under
`FINBOT_DATA_ROOT/raw/exports/fundamentals/daily_valuation_metrics`, streams through it in chunks,
filters to `reference/tickers.parquet`, and writes:

- `FINBOT_DATA_ROOT/fundamentals/daily_valuation_metrics.parquet`
- `FINBOT_DATA_ROOT/fundamentals/daily_valuation_metrics.metadata.json`

If you already have an export file, convert it directly:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
python scripts/download_daily_valuation_metrics.py \
  --input-file /path/to/SHARADAR_DAILY.zip
```

Incremental DAILY updates use one paginated `lastupdated` filter across the DAILY table, filter
locally to `reference/tickers.parquet`, and merge changed rows by `ticker,date`:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
NASDAQ_DATA_LINK_API_KEY=... \
python scripts/update_daily_valuation_metrics.py --lastupdated-gte 2026-06-11
```

The dataset contains:

| Column | Type | Description |
| --- | --- | --- |
| `ticker` | string | Ticker symbol |
| `date` | date | Daily metric date |
| `lastupdated` | date | Provider row update date |
| `ev` | float | Enterprise value |
| `evebit` | float | Enterprise value over EBIT |
| `evebitda` | float | Enterprise value over EBITDA |
| `marketcap` | float | Market capitalization |
| `pb` | float | Price-to-book value |
| `pe` | float | Price-to-earnings, Damodaran method |
| `ps` | float | Price-to-sales, Damodaran method |

## Environment

Nasdaq Data Link credentials are read from environment variables or from a `.env` file in the
current working directory:

```bash
NASDAQ_DATA_LINK_API_KEY=...
```

`QUANDL_API_KEY` is also supported as a fallback for Nasdaq Data Link.

Supported Finbot environment variables:

- `FINBOT_DATA_ROOT`: shared Finbot data root.
- `FINBOT_RAW_BARS_DIR`: daily price output directory fallback when `--output-dir` is not supplied.
- `FINBOT_REFERENCE_DIR`: ticker universe output directory fallback when `--output-dir` is not supplied.
- `FINBOT_RAW_TICKERS_DIR`: raw Sharadar ticker JSONL output directory fallback when `--raw-output-dir` is not supplied.
- `FINBOT_FUNDAMENTALS_DIR`: SF1 and daily valuation metrics output directory fallback when `--output-dir` is not supplied.
- `FINBOT_RAW_EXPORT_DIR`: raw downloaded daily price export file directory fallback when `--raw-export-dir` is not supplied.
- `FINBOT_RAW_FUNDAMENTALS_EXPORT_DIR`: raw downloaded SF1 export file directory fallback when `--raw-export-dir` is not supplied.
- `FINBOT_RAW_DAILY_VALUATION_METRICS_EXPORT_DIR`: raw downloaded DAILY export file directory fallback when `--raw-export-dir` is not supplied.
- `NASDAQ_DATA_LINK_API_KEY`: Nasdaq Data Link API key for Sharadar downloads.
- `QUANDL_API_KEY`: fallback API key name for Nasdaq Data Link.

## Docker

Build the local image:

```bash
docker compose build
```

Compose reads `.env` for credentials and mounts `${FINBOT_HOST_DATA_ROOT:-./data}` to `/data` in
the container, so output files persist on the host. Set `FINBOT_HOST_DATA_ROOT=/srv/finbot/data` on
the VM, or a local development path on your workstation, to keep downloaded data outside the
repository clone.

The Docker entrypoint creates output directories, fixes ownership, then runs the job as
`${HOST_UID:-1000}:${HOST_GID:-1000}` so files written to the mounted data root are owned by the
host user instead of root.

Show script help from the container:

```bash
docker compose run --rm finbot-data --help
docker compose run --rm finbot-ticker-universe --help
docker compose run --rm finbot-historical-prices --help
docker compose run --rm finbot-update-historical-prices --help
docker compose run --rm finbot-fundamentals --help
docker compose run --rm finbot-update-fundamentals --help
docker compose run --rm finbot-daily-valuation-metrics --help
docker compose run --rm finbot-update-daily-valuation-metrics --help
```

Run the current data jobs:

```bash
docker compose run --rm finbot-ticker-universe
docker compose run --rm finbot-historical-prices
docker compose run --rm finbot-update-historical-prices --lastupdated-gte 2026-06-11
docker compose run --rm finbot-fundamentals
docker compose run --rm finbot-update-fundamentals --lastupdated-gte 2026-06-11
docker compose run --rm finbot-daily-valuation-metrics
docker compose run --rm finbot-update-daily-valuation-metrics --lastupdated-gte 2026-06-11
```

## Testing

Run:

```bash
.venv/bin/python -m compileall market_data scripts
.venv/bin/python -m pytest
```

If `.venv` is unavailable, use the active Python environment:

```bash
python -m compileall market_data scripts
pytest
```

Tests should avoid real provider API calls. Use fake clients, fake sessions, temporary directories,
and deterministic dates.
