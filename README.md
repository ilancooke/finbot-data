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

## Environment

Massive credentials are read from environment variables or from a `.env` file in the current working directory:

```bash
MASSIVE_API_KEY=...
```

`POLYGON_API_KEY` is also supported as a fallback.

Supported Finbot environment variables:

- `FINBOT_RAW_BARS_DIR`: output directory fallback when `--output-dir` is not supplied.
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

## Local Tests

```bash
pytest
```

Optional syntax check:

```bash
python -m compileall finbot_data scripts
```
