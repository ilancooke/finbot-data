# finbot-data

Plain-script market and reference data acquisition for Finbot.

This repository downloads, lightly normalizes, and stores provider data. Feature engineering,
research-universe qualification, labels, modeling, dashboards, and trading logic belong in other
Finbot repositories.

## Prototype and Deployment Model

This repository is currently being built and validated as a complete prototype in the development
environment. Development runs may perform full historical backfills, rebuild datasets, and use the
resources available on that machine without treating it as the production constraint.

After the prototype is accepted, it will be deployed to a small VM hosted in Proxmox. The VM will
hold the complete Finbot data tree—including raw snapshots and canonical historical datasets—and
will run scheduled incremental updates and occasional targeted historical refreshes. Production
jobs therefore remain batched, streamed, checkpointed, and restartable so working memory stays
bounded even though the full retained dataset resides on the VM's persistent storage. Initial
backfills may be generated in development and copied to the VM or run directly on the VM.

## Provider Direction

The active provider foundation is:

- Massive for US equity reference data, including active and inactive tickers and current company
  details.
- Alpaca for historical daily bars and corporate actions from 2016 onward.

Sharadar/Nasdaq Data Link is retired. Historical files under the shared data root are operational
or archived state and are not deleted by this package.

The current implementation includes the provider foundation, the Massive-derived US equity
security master, Alpaca-backed symbol episodes, and the Alpaca historical-coverage gate.
The full historical-bar job remains a later milestone in the workspace `ROADMAP.md`.

## Setup

Create and activate a Python 3.11 virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Configure credentials through environment variables or a local `.env` file:

```bash
MASSIVE_API_KEY=...
ALPACA_API_KEY=...
ALPACA_API_SECRET_KEY=...
FINBOT_DATA_ROOT=/Users/ilan/workspace/finbot/data
```

Credentials are never written to raw-download metadata or included in provider error messages.

## Provider Access Checks

Run small, read-only requests to confirm credentials and plan access:

```bash
python scripts/check_provider_access.py massive
python scripts/check_provider_access.py alpaca
```

The Alpaca check requests one historical AAPL daily SIP bar from 2016. It does not trade, mutate an
account, or write data.

## US Equity Security Master

Build the reference inventory and Alpaca acquisition manifest:

```bash
python scripts/download_security_master.py
```

The job downloads active and inactive Massive US common stocks, reconciles current Alpaca assets,
and uses the Nareit ticker directory plus S&P 500 classifications for known-REIT exclusions. It
writes:

```text
data/reference/
├── security_master.parquet
├── security_master.metadata.json
├── tickers.parquet
├── tickers.metadata.json
├── universe_exclusions.parquet
└── universe_exclusions.metadata.json
```

`security_master` retains eligible and excluded provider inventory records. Stable identity uses
share-class FIGI, then composite FIGI, then conservative documented fallbacks. The initial
`tickers` output is a pending manifest that the symbol-episode job replaces with dated acquisition
rows. A missing current Alpaca asset is recorded but does not remove a historical Massive
security. Ambiguous ticker reuse, unsupported exchanges, known REITs
or REOCs, BDCs, partnerships, and securities delisted before 2016 are excluded with explicit
reasons.

The Nareit and S&P classifications are current reference aids, not point-in-time classifications.
Their raw HTML is retained so each snapshot remains auditable. Actual historical Alpaca coverage
is not asserted here; that is the Milestone 3 coverage audit.

## Symbol Episodes and Continuity

After building the security master, build the dated symbol history:

```bash
python scripts/download_symbol_episodes.py
```

This job combines the Massive inventory with Alpaca name-change actions from 2016 onward. It
writes `reference/symbol_episodes.parquet`, its metadata sidecar, and a dated raw snapshot under
`raw/alpaca/name_changes/`. It also replaces `reference/tickers.parquet` with the episode-aware
Alpaca acquisition manifest. A chain such as `SYMC` → `NLOK` → `GEN` keeps one stable
`security_id`, while each ticker has its own inclusive `symbol_valid_from` and `symbol_valid_to`.
The current symbol remains available for dashboard display and the historical symbol remains on
each episode for research and auditing.

The two eligibility concepts are intentionally separate:

- `is_price_coverage_eligible` controls which episodes are retained for raw Alpaca acquisition.
- `is_episode_mapping_eligible` controls whether episode-bounded bars may be attached to the
  stable security identity for canonical data and research.

Thus an unresolved reused ticker is still downloaded when otherwise in scope, but is quarantined
from stable-identity mapping. Historical ingestion must request the exact symbol with remapping
disabled and slice the returned bars to the episode boundaries.

## Alpaca Corporate Actions

After building symbol episodes, download the corporate-action history for the complete Finbot
security inventory:

```bash
python scripts/download_corporate_actions.py
```

The job batches every active, delisted, eligible, and excluded symbol in
`reference.symbol_episodes`; this keeps production resource use bounded and avoids API calls for
ETFs and other instruments outside the Finbot inventory. The first run downloads events from
`2016-01-01` through the day before the snapshot. Later runs
replace a 30-day overlapping process-date window, so late and revised events supersede previous
records deterministically. Use `--full-refresh` to rebuild the entire history or
`--overlap-days N` to widen the normal refresh window.

The job writes:

- `reference/corporate_actions.parquet`
- `reference/corporate_actions.metadata.json`
- dated compressed provider pages and download metadata under `raw/alpaca/corporate_actions/`

One normalized row is stored per Alpaca event ID. The table covers splits, dividends, mergers,
name changes, reorganizations, spin-offs, redemptions, partial calls, rights distributions,
worthless removals, unit splits, and capital-gains distributions. Common dates, symbols, rates,
identity matches, and split-refresh flags are columns; the complete canonical provider payload and
its SHA-256 hash are retained for lossless auditing.

Primary and related symbols are matched through dated symbol episodes. Ambiguous or quarantined
identities are left unmapped with an explicit status rather than attached to the wrong security.
New or revised forward, reverse, and unit splits are listed in metadata as requiring refresh of
split-adjusted history.

## Alpaca Daily Bars

Milestone 5 exposes three modes using the same episode-aware pipeline:

```bash
python scripts/download_daily_bars.py pilot
python scripts/download_daily_bars.py backfill
python scripts/download_daily_bars.py update
```

The pilot uses a deterministic group spanning active, delisted, renamed, acquired, forward-split,
and reverse-split securities. A passing pilot requires paired raw and split-adjusted observations,
no duplicate `symbol,date` keys, observable split adjustments, and explicit quarantine evidence
for bars outside safe symbol episodes.

The backfill requests every unique symbol in `reference/tickers.parquet` from `2016-01-01` through
the latest completed weekday. Both requests use SIP and `asof=-`; returned bars are sliced to the
inclusive symbol-episode boundaries. Safely mapped observations are written to
`market/daily_bars/historical.parquet`. Out-of-episode, ambiguous, quarantined, unexpected, or
unpaired observations are retained in raw responses and summarized in
`market/daily_bars/identity_quarantine.parquet` rather than assigned to a security.

Work is checkpointed by symbol batch under a temporary staging directory. A retry skips completed
batches, and final Parquet files are assembled by row group and replaced atomically. The staging
files are removed only after successful finalization. Metadata records request count, runtime,
peak memory, date coverage, duplicate keys, adjustment pairing, and applied split-event IDs.

Update mode replaces a seven-day overlap for all symbols. When corporate-action metadata reports
a new or revised split, the affected symbol is refreshed from 2016 so split-adjusted history stays
current. The full dataset resides on persistent storage; only a symbol batch or Parquet row group
is processed in memory.

The completed prototype backfill covers `2016-01-04` through `2026-09-18`: 12,587,410 canonical
rows across 9,331 historical symbols and 8,591 stable securities. Acquisition used 2,890 paginated
API requests over about 84 minutes. The canonical Parquet file is approximately 680 MiB; the
explicit identity quarantine is approximately 30 MiB, and retained compressed raw backfill pages
are approximately 635 MiB. Observed peak process memory was 590 MiB. A subsequent no-new-session
incremental update completed in 142 seconds with 414 requests, retained the same row count, and
used 528 MiB peak memory. These are prototype measurements for sizing the eventual Proxmox VM,
not limits imposed on the current build environment.

## Alpaca Historical-Coverage Audit

Run the bounded audit before authorizing a full bar backfill:

```bash
python scripts/audit_alpaca_coverage.py
```

The deterministic sample includes active stocks, recent and older delistings, acquisitions,
ticker-change episodes, ambiguous ticker reuse, forward splits, and reverse splits. Every SIP
request uses the exact historical symbol with `asof=-`, `adjustment=raw`, and an explicit historical
end date. A small active-stock subset is also requested from IEX so consolidated SIP volume can be
confirmed against the narrower feed.

The job writes:

- `reference/alpaca_coverage_audit.parquet`
- `reference/alpaca_coverage_audit.metadata.json`
- dated raw request/response envelopes under `raw/alpaca/coverage_audit/`

The report records missing bars, truncated date ranges, unexpected returned symbols, ambiguous
reused tickers, event-boundary coverage, request errors, and SIP/IEX volume comparisons. Its
metadata contains a reproducible `proceed` or `blocked` recommendation. A `proceed` result requires
historical SIP access, at least 80% coverage of the sampled delisted episodes, SIP volume above IEX
for every comparison symbol, no observed symbol relabeling, and adequate ticker-change coverage.

The episode-aware live audit recommends **proceed**. It found bars for 14 of 15 sampled delisted
episodes (93.3%) and adequate ticker-change coverage. Exact-symbol queries can contain bars outside
the intended issuer episode—`GEN` exposed this behavior—but those bars are now measured and
excluded by the dated boundaries. Unresolved reused tickers remain in raw acquisition scope but
are not eligible for stable-identity mapping.

## REST Client Behavior

Both provider clients:

- use explicit timeouts;
- retry network failures, `429`, and transient `5xx` responses;
- honor numeric `Retry-After` headers;
- pace calls conservatively for the documented free tiers;
- sanitize credentials from exceptions;
- expose page iterators so later dataset jobs can stream provider responses.

Massive pagination follows `next_url`. Alpaca pagination follows `next_page_token`.

## Raw Provider Responses

Dataset jobs should preserve provider pages under the shared data root before normalization:

```text
data/
└── raw/
    ├── massive/
    ├── alpaca/
    └── reference/
        └── us_equity_universe/
            └── YYYY-MM-DD/
```

Use `market_data.raw.write_raw_json_pages` to write compressed JSONL plus a download metadata JSON
file atomically. Request parameters with credential-like names are redacted.

## Docker

Build the shared image:

```bash
docker compose build
```

Run provider access checks:

```bash
docker compose run --rm finbot-check-massive
docker compose run --rm finbot-check-alpaca
```

Build the security master:

```bash
docker compose run --rm finbot-security-master
```

Build the symbol episodes and episode-aware acquisition manifest:

```bash
docker compose run --rm finbot-symbol-episodes
```

Download or incrementally refresh corporate actions:

```bash
docker compose run --rm finbot-corporate-actions
```

Run the daily-bar stages:

```bash
docker compose run --rm finbot-daily-bars-pilot
docker compose run --rm finbot-daily-bars-backfill
docker compose run --rm finbot-daily-bars-update
```

Run the historical-coverage gate:

```bash
docker compose run --rm finbot-alpaca-coverage-audit
```

Compose mounts `${FINBOT_HOST_DATA_ROOT:-./data}` at `/data` and sets `FINBOT_DATA_ROOT=/data`.
The entrypoint creates provider output directories and drops privileges to `HOST_UID:HOST_GID`.

## Project Layout

- `market_data/providers/`: provider REST clients.
- `market_data/config.py`: environment and `.env` helpers.
- `market_data/http.py`: shared retry, pacing, JSON, and sanitized-error behavior.
- `market_data/raw.py`: atomic raw-response storage.
- `market_data/metadata.py`: durable dataset metadata helpers.
- `scripts/`: thin operational entrypoints.
- `tests/`: deterministic tests with fake HTTP sessions and temporary directories.

## Tests

```bash
.venv/bin/python -m compileall market_data scripts
.venv/bin/python -m pytest
```

Tests do not call real providers or mutate the shared data root.
