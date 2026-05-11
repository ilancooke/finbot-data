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
- `market_data/datasets/`: dataset-specific orchestration such as daily bars and ratios.
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

By default, local script runs write under this repository's `data/` directory. For a shared multipackage data root, set `FINBOT_DATA_ROOT`; daily bars then write to `FINBOT_DATA_ROOT/market/daily_bars` unless `FINBOT_RAW_BARS_DIR` is set.

The scheduled production path downloads a rolling adjusted history window from Massive grouped daily bars, filters to the current `tickers.parquet` universe, then replaces the existing snapshot. By default, request pacing is disabled. Use `--calls-per-minute N` if you need explicit pacing for a lower-throughput plan. The start date is calculated as the day after the exact year boundary, so `--end-date 2026-05-01 --years 2` starts at `2024-05-02`.

The script requests weekdays in the date window. It does not use a NYSE holiday calendar; holidays and other dates with no returned data are recorded in `empty_market_dates`.

Arguments:

- `--end-date YYYY-MM-DD`: inclusive end date for a rolling history replace.
- `--years N`: history window in years. Defaults to `2`.
- `--days N`: history window in calendar days, useful for short test runs. Cannot be combined with `--years`.
- `--calls-per-minute N`: request pacing for Massive. Defaults to `0`, which disables built-in pacing.
- `--date YYYY-MM-DD`: optional single-day smoke/debug mode instead of `--end-date`.
- `--output-dir PATH`: optional output directory. Defaults to `data/daily_bars`, or `FINBOT_DATA_ROOT/market/daily_bars` when `FINBOT_DATA_ROOT` is set.
- `--all-symbols`: keep all symbols returned by Massive grouped bars instead of filtering to `tickers.parquet`.
- `--ticker-universe-dir PATH`: optional directory containing `tickers.parquet`. Defaults to `FINBOT_REFERENCE_DIR`, or `FINBOT_DATA_ROOT/reference` when `FINBOT_DATA_ROOT` is set.

Example with an explicit output directory:

```bash
python scripts/download_daily_bars.py --end-date 2026-05-01 --years 2 --output-dir data/daily_bars
```

Short test run:

```bash
python scripts/download_daily_bars.py --end-date 2026-05-01 --days 10 --output-dir /tmp/finbot-data-test
```

Shared-data run filtered to the current ticker universe:

```bash
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data \
python scripts/download_daily_bars.py --end-date 2026-05-10 --years 2
```

The script writes:

- `historical.parquet`
- `historical.metadata.json`

If `historical.parquet` already exists in replace mode, it is atomically replaced by the freshly downloaded window. Single-day mode also writes a fresh one-day snapshot and is intended for smoke/debug runs.

## Ticker Universe

Download the current S&P 500 known universe, validated against Massive's active US
common-stock universe:

```bash
python scripts/download_tickers.py
```

Point-in-time universe:

```bash
python scripts/download_tickers.py --date 2026-05-01
```

Ticker pagination is unpaced by default. Use `--calls-per-minute N` if you need explicit pacing.

The script writes the filtered universe to `tickers.parquet` and `tickers.metadata.json` in `data/reference` by default, or `FINBOT_DATA_ROOT/reference` when `FINBOT_DATA_ROOT` is set. The parquet file first filters to active US common stocks whose primary exchange is one of `XNYS`, `XNAS`, `ARCX`, `XASE`, or `BATS`; ETFs, warrants, preferreds, inactive tickers, and OTC-like rows are excluded. It then applies the selected known-universe strategy.

The default known-universe strategy is `sp500`. Until Finbot has a programmatic index-constituent downloader, the script reads a CSV seed file with a `Symbol` or `Ticker` column. By default it looks for `sp500constituents.csv` in `FINBOT_DATA_ROOT`, then `./data`, then the workspace-level `data` directory.

Use an explicit CSV path:

```bash
python scripts/download_tickers.py --known-universe-file /path/to/sp500constituents.csv
```

Use all active US common stocks without an index-constituent filter:

```bash
python scripts/download_tickers.py --universe all_common_stocks
```

## Ticker Details

Download Massive ticker overview details for the filtered ticker universe:

```bash
python scripts/download_ticker_details.py
```

Point-in-time details:

```bash
python scripts/download_ticker_details.py --date 2026-05-01
```

Ticker details are fetched one symbol at a time from Massive's Ticker Overview endpoint, so the job is much slower than the ticker universe download. By default, the script reads `data/reference/tickers.parquet`, reuses any existing rows in `data/reference/ticker_details.parquet`, and fetches only missing tickers. With `FINBOT_DATA_ROOT` set, those paths move to `FINBOT_DATA_ROOT/reference`. Use `--refresh-all` to refetch every ticker.

Short smoke run:

```bash
python scripts/download_ticker_details.py --limit 10 --calls-per-minute 0
```

The script writes `ticker_details.parquet` and `ticker_details.metadata.json` in `data/reference` by default, or `FINBOT_DATA_ROOT/reference` when `FINBOT_DATA_ROOT` is set. It stores Massive-native reference fields, including `sic_code` and `sic_description`; it does not derive sector, GICS, or other feature-engineering labels.

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

The script writes `related_tickers.parquet` and `related_tickers.metadata.json` in `data/reference` by default, or `FINBOT_DATA_ROOT/reference` when `FINBOT_DATA_ROOT` is set. The parquet stores source ticker to related ticker rows and preserves the result order returned by Massive; it does not derive peer groups, scores, or portfolio labels.

## Ratios

Download latest Massive financial ratios for the filtered ticker universe:

```bash
python scripts/download_ratios.py
```

The ratios endpoint returns latest rows rather than historical point-in-time rows. The script reads `data/reference/tickers.parquet`, fetches the latest ratios from Massive's Financials and Ratios endpoint, filters rows to the active common-stock universe, and writes a replace-style snapshot.

Ratios pagination is unpaced by default because the Financials and Ratios addon guidance allows substantially higher throughput than the free stocks plan. Use `--calls-per-minute N` or `MASSIVE_RATIOS_CALLS_PER_MINUTE` if you want explicit pacing.

Run with an explicit Massive result limit per request:

```bash
python scripts/download_ratios.py --limit 100
```

The script writes `ratios.parquet` and `ratios.metadata.json` in `data/ratios` by default, or `FINBOT_DATA_ROOT/ratios` when `FINBOT_DATA_ROOT` is set.

## Financial Statements

Download historical Massive financial statements for the filtered ticker universe:

```bash
python scripts/download_financials.py --end-date 2026-05-01 --years 2
```

The financials job reads `data/reference/tickers.parquet`, requests statement rows whose `period_end` falls inside the requested history window, filters rows to the active common-stock universe, and writes replace-style historical snapshots. By default it downloads balance sheets, cash flow statements, and income statements.

Download only one statement type:

```bash
python scripts/download_financials.py --end-date 2026-05-01 --years 2 --statement income_statements
```

Short smoke run for the first 10 tickers:

```bash
python scripts/download_financials.py --end-date 2026-05-01 --years 1 --ticker-limit 10 --calls-per-minute 0
```

The script writes these files in `data/financials` by default, or `FINBOT_DATA_ROOT/financials` when `FINBOT_DATA_ROOT` is set:

- `balance_sheets.parquet` and `balance_sheets.metadata.json`
- `cash_flow_statements.parquet` and `cash_flow_statements.metadata.json`
- `income_statements.parquet` and `income_statements.metadata.json`

Each parquet stores one row per ticker, CIK, `period_end`, and `timeframe`. The provider's full `tickers` array is preserved, and documented statement fields that are not present for a company or period are written as nulls.

Arguments:

- `--end-date YYYY-MM-DD`: inclusive `period_end` upper bound. Defaults to today.
- `--years N`: history window in years. Defaults to `FINBOT_FINANCIALS_HISTORY_YEARS`, then `FINBOT_HISTORY_YEARS`, then `2`.
- `--statement NAME`: one of `balance_sheets`, `cash_flow_statements`, or `income_statements`. Repeat to download multiple statements. Defaults to all.
- `--input-dir PATH`: directory containing `tickers.parquet`. Defaults to `data/reference`, or `FINBOT_DATA_ROOT/reference` when `FINBOT_DATA_ROOT` is set.
- `--output-dir PATH`: output directory. Defaults to `data/financials`, or `FINBOT_DATA_ROOT/financials` when `FINBOT_DATA_ROOT` is set.
- `--ticker-limit N`: optional smoke-test limit for the ticker universe.
- `--ticker-batch-size N`: ticker count per Massive `tickers.any_of` request. Defaults to `100`.
- `--limit N`: Massive result limit per request. Defaults to `50000`.
- `--calls-per-minute N`: request pacing. Defaults to `MASSIVE_FINANCIALS_CALLS_PER_MINUTE` or `0`.

## Environment

Massive credentials are read from environment variables or from a `.env` file in the current working directory:

```bash
MASSIVE_API_KEY=...
```

`POLYGON_API_KEY` is also supported as a fallback.

Supported Finbot environment variables:

- `FINBOT_DATA_ROOT`: shared Finbot data root. When set, daily bars default to `FINBOT_DATA_ROOT/market/daily_bars`, reference datasets default to `FINBOT_DATA_ROOT/reference`, ratios default to `FINBOT_DATA_ROOT/ratios`, and financial statements default to `FINBOT_DATA_ROOT/financials`.
- `FINBOT_RAW_BARS_DIR`: output directory fallback when `--output-dir` is not supplied.
- `FINBOT_REFERENCE_DIR`: filtered ticker universe output directory fallback when `--output-dir` is not supplied.
- `FINBOT_TICKER_UNIVERSE`: default ticker universe strategy. Defaults to `sp500`; set to `all_common_stocks` to disable the known-universe CSV filter.
- `FINBOT_KNOWN_UNIVERSE_FILE`: CSV file containing a `Symbol` or `Ticker` column for the selected known universe.
- `FINBOT_RATIOS_DIR`: ratios output directory fallback when `--output-dir` is not supplied.
- `FINBOT_FINANCIALS_DIR`: financial statements output directory fallback when `--output-dir` is not supplied.
- `FINBOT_INGEST_DISABLE_NETWORK`: set to `1`, `true`, or `yes` to skip network calls and write an empty snapshot for smoke tests.
- `FINBOT_HISTORY_YEARS`: default history window for `--end-date` mode. Defaults to `2`.
- `FINBOT_FINANCIALS_HISTORY_YEARS`: default financial statements history window. Defaults to `FINBOT_HISTORY_YEARS`, then `2`.
- `MASSIVE_CALLS_PER_MINUTE`: default request pacing. Defaults to `0`.
- `MASSIVE_RATIOS_CALLS_PER_MINUTE`: default ratios pagination pacing. Defaults to `0`.
- `MASSIVE_FINANCIALS_CALLS_PER_MINUTE`: default financial statements pagination and ticker-batch pacing. Defaults to `0`.

Massive's free Stocks Basic plan was historically expected to provide 5 calls per minute, 2 years of historical data, and end-of-day data. Use `--calls-per-minute N` or `MASSIVE_CALLS_PER_MINUTE` if your current plan needs explicit pacing.

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
- `ticker_universe_filter` unless `--all-symbols` is used

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
- `universe_strategy`
- `universe_name`
- `known_universe_file`
- `known_universe_symbols`
- `universe_date`
- `rows`
- `tickers`
- `input_rows`
- `common_stock_rows`
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

`ratios.parquet` uses this Massive-native latest ratios schema:

| Column | Type | Description |
| --- | --- | --- |
| `ticker` | string | Ticker symbol |
| `cik` | string | Central Index Key when available |
| `date` | date | Trading date used for the ratio calculations |
| `price` | number | Stock price used in ratio calculations |
| `average_volume` | number | Average trading volume over the last 30 trading days |
| `market_cap` | number | Market capitalization |
| `earnings_per_share` | number | Earnings per share |
| `price_to_earnings` | number | Price-to-earnings ratio |
| `price_to_book` | number | Price-to-book ratio |
| `price_to_sales` | number | Price-to-sales ratio |
| `price_to_cash_flow` | number | Price-to-cash-flow ratio |
| `price_to_free_cash_flow` | number | Price-to-free-cash-flow ratio |
| `dividend_yield` | number | Dividend yield |
| `return_on_assets` | number | Return on assets ratio |
| `return_on_equity` | number | Return on equity ratio |
| `debt_to_equity` | number | Debt-to-equity ratio |
| `current` | number | Current ratio |
| `quick` | number | Quick ratio |
| `cash` | number | Cash ratio |
| `ev_to_sales` | number | Enterprise value to sales ratio |
| `ev_to_ebitda` | number | Enterprise value to EBITDA ratio |
| `enterprise_value` | number | Enterprise value |
| `free_cash_flow` | number | Free cash flow |

`ratios.metadata.json` always includes:

- `collected_date_utc`
- `collected_at_utc`
- `provider`
- `dataset`
- `mode`
- `input_file`
- `input_tickers`
- `raw_rows`
- `output_rows`
- `calls_per_minute`
- `limit`
- `rows`
- `tickers`
- `data_min_date`
- `data_max_date`
- `parquet_file`

`balance_sheets.parquet`, `cash_flow_statements.parquet`, and `income_statements.parquet` use Massive-native financial statement fields plus these common columns:

| Column | Type | Description |
| --- | --- | --- |
| `ticker` | string | Ticker from the filtered Finbot universe matched to the provider statement row |
| `cik` | string | Central Index Key |
| `period_end` | date | Last date of the reporting period |
| `filing_date` | date | SEC filing date when available |
| `fiscal_year` | number | Fiscal year |
| `fiscal_quarter` | number | Fiscal quarter |
| `timeframe` | string | Reporting period type, such as `quarterly`, `annual`, or `trailing_twelve_months` |
| `tickers` | array[string] | Full Massive ticker list for the reporting company |

Financial statement metadata files always include:

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
- `requested_start_date`
- `requested_end_date`
- `history_years`
- `raw_rows`
- `output_rows`
- `calls_per_minute`
- `limit`
- `ticker_batch_size`
- `rows`
- `tickers`
- `data_min_date`
- `data_max_date`
- `data_min_period_end`
- `data_max_period_end`
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

Compose reads `.env` for Massive credentials and mounts `${FINBOT_HOST_DATA_ROOT:-./data}` to `/data` in the container, so output files persist on the host. The daily bars service writes to `/data/market/daily_bars`, the reference-data services write to `/data/reference`, the ratios service writes to `/data/ratios`, and the financials service writes to `/data/financials`. Set `FINBOT_HOST_DATA_ROOT=/srv/finbot/data` on the VM, or a local development path on your workstation, to keep downloaded data outside the repository clone.

The Docker entrypoint creates the output directories, fixes ownership, then runs the job as `${HOST_UID:-1000}:${HOST_GID:-1000}` so files written to the mounted data root are owned by the host user instead of root. Set `HOST_UID` and `HOST_GID` in `.env` if your machine does not use `1000:1000`; use `id -u` and `id -g` to find the values.

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

Show ratios help:

```bash
docker compose run --rm finbot-ratios --help
```

Show financials help:

```bash
docker compose run --rm finbot-financials --help
```

No-network smoke test:

```bash
docker compose run --rm \
  -e FINBOT_INGEST_DISABLE_NETWORK=1 \
  finbot-data --end-date 2026-05-01 --days 10 --calls-per-minute 0 --all-symbols
```

Short Massive API test:

```bash
docker compose run --rm \
  finbot-data --end-date 2026-05-01 --days 10
```

Full history refresh:

```bash
docker compose run --rm \
  finbot-data --end-date 2026-05-01 --years 2
```

For a scheduled homelab run, let the host provide the date:

```bash
docker compose run --rm \
  finbot-data --end-date "$(date +%F)" --years 2
```

Download the filtered ticker universe from the container:

```bash
docker compose run --rm finbot-tickers
```

Download ticker details for the first missing 10 tickers from the container:

```bash
docker compose run --rm finbot-ticker-details --limit 10
```

Refetch ticker details for 10 tickers from the container:

```bash
docker compose run --rm finbot-ticker-details --refresh-all --limit 10
```

Download related tickers for the first 10 source tickers from the container:

```bash
docker compose run --rm finbot-related-tickers --limit 10
```

Download latest ratios from the container:

```bash
docker compose run --rm finbot-ratios
```

Download two years of financial statements from the container:

```bash
docker compose run --rm \
  finbot-financials --end-date "$(date +%F)" --years 2
```
