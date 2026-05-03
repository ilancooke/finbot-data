# finbot-data

Plain-script market data downloader for Finbot.

This repository only downloads, normalizes, and stores market data. Feature engineering, preprocessing, model training, inference, and dashboard code live elsewhere.

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
- `market_data/datasets/`: dataset-specific orchestration such as daily bars.
- `market_data/providers/`: provider API clients such as Massive.
- `market_data/http.py`: shared Massive HTTP, pagination, retry, and sanitized error handling.
- `market_data/universe.py`: ticker universe fetching, filtering, and parquet helpers for per-symbol jobs.
- `market_data/storage.py`: shared parquet and metadata writing.
- `market_data/config.py`: environment and `.env` helpers.
- `tests/`: local test suite.

## Run

Use the script directly from the repository root:

```bash
python scripts/download_daily_bars.py --end-date 2026-05-01 --years 2
```

The scheduled production path downloads a rolling adjusted history window from Massive grouped daily bars, then replaces the existing snapshot. With the free Massive plan, the default 2-year window is expected to take about 100 minutes because the script paces requests at 5 calls per minute. The start date is calculated as the day after the exact year boundary, so `--end-date 2026-05-01 --years 2` starts at `2024-05-02`.

The script requests weekdays in the date window. It does not use a NYSE holiday calendar; holidays and other dates with no returned data are recorded in `empty_market_dates`.

Arguments:

- `--end-date YYYY-MM-DD`: inclusive end date for a rolling history replace.
- `--years N`: history window in years. Defaults to `2`.
- `--days N`: history window in calendar days, useful for short test runs. Cannot be combined with `--years`.
- `--calls-per-minute N`: request pacing for Massive. Defaults to `5`; use `0` to disable pacing on a paid plan.
- `--date YYYY-MM-DD`: optional single-day smoke/debug mode instead of `--end-date`.
- `--output-dir PATH`: optional output directory. Defaults to `data/daily_bars`.

Example with an explicit output directory:

```bash
python scripts/download_daily_bars.py --end-date 2026-05-01 --years 2 --output-dir data/daily_bars
```

Short test run:

```bash
python scripts/download_daily_bars.py --end-date 2026-05-01 --days 10 --output-dir /tmp/finbot-data-test
```

The script writes:

- `historical.parquet`
- `historical.metadata.json`

If `historical.parquet` already exists in replace mode, it is atomically replaced by the freshly downloaded window. Single-day mode also writes a fresh one-day snapshot and is intended for smoke/debug runs.

## Ticker Universe

Download the current active US common-stock universe:

```bash
python scripts/download_tickers.py
```

Point-in-time universe:

```bash
python scripts/download_tickers.py --date 2026-05-01
```

Ticker pagination is paced at 5 calls per minute by default. Use `--calls-per-minute 0` to disable pacing on a paid plan.

The script writes the filtered universe to `tickers.parquet` and `tickers.metadata.json` in `data/reference` by default. The parquet file includes only active US common stocks whose primary exchange is one of `XNYS`, `XNAS`, `ARCX`, `XASE`, or `BATS`; ETFs, warrants, preferreds, inactive tickers, and OTC-like rows are excluded.

## Ticker Details

Download Massive ticker overview details for the filtered ticker universe:

```bash
python scripts/download_ticker_details.py
```

Point-in-time details:

```bash
python scripts/download_ticker_details.py --date 2026-05-01
```

Ticker details are fetched one symbol at a time from Massive's Ticker Overview endpoint, so the job is much slower than the ticker universe download on the free 5-calls-per-minute plan. By default, the script reads `data/reference/tickers.parquet`, reuses any existing rows in `data/reference/ticker_details.parquet`, and fetches only missing tickers. Use `--refresh-all` to refetch every ticker.

Short smoke run:

```bash
python scripts/download_ticker_details.py --limit 10 --calls-per-minute 0
```

The script writes `ticker_details.parquet` and `ticker_details.metadata.json` in `data/reference` by default. It stores Massive-native reference fields, including `sic_code` and `sic_description`; it does not derive sector, GICS, or other feature-engineering labels.

## Related Tickers

Download Massive related tickers for the filtered ticker universe:

```bash
python scripts/download_related_tickers.py
```

Related tickers are fetched one symbol at a time from Massive's Related Tickers endpoint and are updated daily by Massive. This job writes a replace-style snapshot by default.

Short smoke run:

```bash
python scripts/download_related_tickers.py --limit 10 --calls-per-minute 0
```

The script writes `related_tickers.parquet` and `related_tickers.metadata.json` in `data/reference` by default. The parquet stores source ticker to related ticker rows and preserves the result order returned by Massive; it does not derive peer groups, scores, or portfolio labels.

## Environment

Massive credentials are read from environment variables or from a `.env` file in the current working directory:

```bash
MASSIVE_API_KEY=...
```

`POLYGON_API_KEY` is also supported as a fallback.

Supported Finbot environment variables:

- `FINBOT_RAW_BARS_DIR`: output directory fallback when `--output-dir` is not supplied.
- `FINBOT_REFERENCE_DIR`: filtered ticker universe output directory fallback when `--output-dir` is not supplied.
- `FINBOT_INGEST_DISABLE_NETWORK`: set to `1`, `true`, or `yes` to skip network calls and write an empty snapshot for smoke tests.
- `FINBOT_HISTORY_YEARS`: default history window for `--end-date` mode. Defaults to `2`.
- `MASSIVE_CALLS_PER_MINUTE`: default request pacing. Defaults to `5`.

Massive's free Stocks Basic plan is expected to provide 5 calls per minute, 2 years of historical data, and end-of-day data. Paid plans can use a longer `--years` value and `--calls-per-minute 0`.

## Output Schema

`historical.parquet` uses this canonical daily bar schema:

| Column | Type | Description |
| --- | --- | --- |
| `date` | date | Daily bar date |
| `symbol` | string | Ticker symbol |
| `open` | float | Adjusted open |
| `high` | float | Adjusted high |
| `low` | float | Adjusted low |
| `close` | float | Adjusted close |
| `volume` | number | Adjusted daily volume from Massive |

`historical.metadata.json` always includes:

- `collected_date_utc`
- `collected_at_utc`
- `provider`
- `adjusted`
- `mode`
- `rows`
- `symbols`
- `data_min_date`
- `data_max_date`
- `parquet_file`

Replace mode also includes:

- `requested_start_date`
- `requested_end_date`
- `history_years` or `history_days`
- `market_dates_requested`
- `calls_per_minute`
- `empty_market_dates`

Single-date mode also includes:

- `data_date`

`tickers.parquet` uses this ticker universe schema:

| Column | Type | Description |
| --- | --- | --- |
| `ticker` | string | Ticker symbol |
| `name` | string | Security name |
| `market` | string | Massive market |
| `locale` | string | Market locale |
| `primary_exchange` | string | Primary listing exchange |
| `type` | string | Massive security type |
| `active` | bool | Whether the ticker is active |
| `currency_name` | string | Trading currency |
| `cik` | string | Central Index Key when available |
| `composite_figi` | string | Composite FIGI when available |
| `share_class_figi` | string | Share class FIGI when available |
| `last_updated_utc` | string | Massive reference timestamp |

`tickers.metadata.json` always includes:

- `collected_date_utc`
- `collected_at_utc`
- `provider`
- `dataset`
- `mode`
- `universe_date`
- `rows`
- `tickers`
- `input_rows`
- `output_rows`
- `calls_per_minute`
- `filter`
- `parquet_file`

`ticker_details.parquet` uses this Massive-native ticker details schema:

| Column | Type | Description |
| --- | --- | --- |
| `ticker` | string | Ticker symbol |
| `name` | string | Security or company name |
| `market` | string | Massive market |
| `locale` | string | Market locale |
| `primary_exchange` | string | Primary listing exchange |
| `type` | string | Massive security type |
| `active` | bool | Whether the ticker is active |
| `currency_name` | string | Trading currency |
| `cik` | string | Central Index Key when available |
| `composite_figi` | string | Composite FIGI when available |
| `share_class_figi` | string | Share class FIGI when available |
| `sic_code` | string | Massive-provided SIC code |
| `sic_description` | string | Massive-provided SIC description |
| `description` | string | Company description when available |
| `homepage_url` | string | Company homepage when available |
| `market_cap` | number | Massive market capitalization when available |
| `total_employees` | number | Approximate employee count when available |
| `list_date` | string | Public listing date when available |
| `delisted_utc` | string | Delisting timestamp when available |
| `ticker_root` | string | Ticker root when available |
| `ticker_suffix` | string | Ticker suffix when available |
| `phone_number` | string | Company phone number when available |
| `share_class_shares_outstanding` | number | Share-class shares outstanding when available |
| `weighted_shares_outstanding` | number | Weighted shares outstanding when available |
| `round_lot` | number | Round lot size when available |

`ticker_details.metadata.json` always includes:

- `collected_date_utc`
- `collected_at_utc`
- `provider`
- `dataset`
- `mode`
- `details_date`
- `input_file`
- `input_tickers`
- `cached_tickers`
- `fetch_candidates`
- `requested_tickers`
- `pending_tickers`
- `fetched_tickers`
- `missing_tickers`
- `failed_tickers`
- `calls_per_minute`
- `parquet_file`

`related_tickers.parquet` uses this Massive-native related ticker schema:

| Column | Type | Description |
| --- | --- | --- |
| `ticker` | string | Source ticker symbol queried |
| `related_ticker` | string | Related ticker returned by Massive |
| `result_order` | number | One-based result order returned by Massive |

`related_tickers.metadata.json` always includes:

- `collected_date_utc`
- `collected_at_utc`
- `provider`
- `dataset`
- `mode`
- `input_file`
- `input_tickers`
- `requested_tickers`
- `partial`
- `pending_tickers`
- `empty_tickers`
- `failed_tickers`
- `calls_per_minute`
- `rows`
- `tickers`
- `related_tickers`
- `parquet_file`

## Local Tests

```bash
pytest
```

Optional syntax check:

```bash
python -m compileall market_data scripts
```

## Docker

Build the local image:

```bash
docker compose build
```

Compose reads `.env` for Massive credentials and mounts local `./data` to `/app/data` in the container, so output files persist on the host. The daily bars service writes to `/app/data/daily_bars`, and the reference-data services write to `/app/data/reference`; these map to `./data/daily_bars` and `./data/reference` on the host.

The Docker entrypoint creates the output directories, fixes ownership, then runs the job as `${HOST_UID:-1000}:${HOST_GID:-1000}` so files written to `./data` are owned by the host user instead of root. Set `HOST_UID` and `HOST_GID` in `.env` if your machine does not use `1000:1000`; use `id -u` and `id -g` to find the values.

Show script help from the container:

```bash
docker compose run --rm finbot-data --help
```

Show ticker universe help:

```bash
docker compose run --rm finbot-tickers --help
```

Show ticker details help:

```bash
docker compose run --rm finbot-ticker-details --help
```

Show related tickers help:

```bash
docker compose run --rm finbot-related-tickers --help
```

No-network smoke test:

```bash
docker compose run --rm \
  -e FINBOT_INGEST_DISABLE_NETWORK=1 \
  finbot-data --end-date 2026-05-01 --days 10 --calls-per-minute 0
```

Short Massive API test:

```bash
docker compose run --rm \
  finbot-data --end-date 2026-05-01 --days 10 --calls-per-minute 5
```

Full free-plan history refresh:

```bash
docker compose run --rm \
  finbot-data --end-date 2026-05-01 --years 2 --calls-per-minute 5
```

For a scheduled homelab run, let the host provide the date:

```bash
docker compose run --rm \
  finbot-data --end-date "$(date +%F)" --years 2 --calls-per-minute 5
```

Download the filtered ticker universe from the container:

```bash
docker compose run --rm finbot-tickers
```

Download ticker details for the first missing 10 tickers from the container:

```bash
docker compose run --rm finbot-ticker-details --limit 10 --calls-per-minute 5
```

Refetch ticker details for 10 tickers from the container:

```bash
docker compose run --rm finbot-ticker-details --refresh-all --limit 10 --calls-per-minute 5
```

Download related tickers for the first 10 source tickers from the container:

```bash
docker compose run --rm finbot-related-tickers --limit 10 --calls-per-minute 5
```
