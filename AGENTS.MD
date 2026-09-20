# Agent Guidelines

This repository is a plain-script data downloader for Finbot. Keep it small, operationally
boring, and limited to acquisition, light normalization, and durable provider data storage.

## Active Provider Direction

- Massive owns US equity reference discovery, active/inactive ticker inventory, ticker history,
  and current company-detail enrichment.
- Alpaca owns historical daily bars and corporate actions from 2016 onward.
- Sharadar/Nasdaq Data Link is retired. Do not restore Sharadar jobs unless the user explicitly
  changes provider direction.
- Do not delete historical or archived generated data from the shared data root.

## Repository Boundaries

- Do not add feature engineering, research-universe qualification, labels, modeling, dashboards,
  portfolio construction, backtesting, or trading logic.
- `finbot-features` owns the monthly liquidity qualification rule and research-universe lifecycle.
- Keep this repository as plain scripts; do not add packaging metadata or a `src/` layout unless
  the user explicitly revisits that choice.
- Prefer one focused script per data pull with reusable implementation under `market_data/`.

## Provider Client Rules

- Use the requests-based clients in `market_data/providers`.
- Preserve explicit timeouts, bounded transient retries, `Retry-After` handling, and conservative
  free-tier pacing.
- Keep credentials out of URLs, logs, exceptions, tests, metadata, and raw filenames.
- Inject fake sessions and sleep functions in tests; never make real provider calls in tests.
- Expose streaming/page iterators for large pulls rather than accumulating every response page in
  memory.

## Storage Rules

- Prefer `FINBOT_DATA_ROOT`; containers use `/data`.
- Store raw provider responses beneath `FINBOT_DATA_ROOT/raw/massive` or
  `FINBOT_DATA_ROOT/raw/alpaca`.
- Store public classification snapshots for the security-master job beneath
  `FINBOT_DATA_ROOT/raw/reference/us_equity_universe`.
- Store coverage-audit responses beneath `FINBOT_DATA_ROOT/raw/alpaca/coverage_audit` and keep
  `asof=-`, the requested feed, and adjustment mode explicit in raw request envelopes.
- Store Alpaca name-change snapshots beneath `FINBOT_DATA_ROOT/raw/alpaca/name_changes` and build
  `reference/symbol_episodes.parquet` before auditing or downloading historical bars.
- Store complete Alpaca corporate-action pulls beneath
  `FINBOT_DATA_ROOT/raw/alpaca/corporate_actions`. The normalized
  `reference/corporate_actions.parquet` uses provider event ID as its key and refreshes an
  overlapping process-date window so revisions replace prior payloads.
- Store Alpaca daily-bar pages beneath `FINBOT_DATA_ROOT/raw/alpaca/daily_bars`. Canonical mapped
  bars live in `market/daily_bars/historical.parquet`; bars with unsafe or missing identity
  mappings live in the adjacent `identity_quarantine.parquet` report.
- Use compressed JSONL plus download metadata for paginated raw responses.
- Write durable datasets as Parquet plus sidecar metadata JSON.
- Use temporary files and atomic replacement for durable or raw snapshots.
- Preserve exact provider symbols in durable outputs. Use a separate normalized join key only for
  cross-provider reconciliation.
- Keep price acquisition separate from identity mapping: download every price-eligible episode,
  but attach bars to a stable `security_id` only when `is_episode_mapping_eligible` is true. Slice
  exact-symbol bars to `symbol_valid_from` and `symbol_valid_to` before canonical mapping.
- Preserve each corporate-action provider payload and SHA-256 hash. Do not map an event through an
  ambiguous or quarantined symbol episode; record an explicit unmatched status instead.
- Keep processing bounded by API page, symbol batch, or Parquet row group for the small VM.
- Treat the current environment as the full prototype/build environment. The accepted prototype
  will later be deployed to a small Proxmox VM that stores the complete data tree; memory-bounded
  processing is a production portability requirement, not a restriction on development backfills.
- Do not commit generated data unless explicitly requested.

## Credentials

- `MASSIVE_API_KEY`
- `ALPACA_API_KEY`
- `ALPACA_API_SECRET_KEY`

Support environment variables and a local `.env` file.

## Docker

- Compose mounts `FINBOT_HOST_DATA_ROOT` at `/data` and sets `FINBOT_DATA_ROOT=/data`.
- Each focused data job should receive its own Compose service using the shared image and
  entrypoint.
- Rebuild the image when code, dependencies, Dockerfile, or Compose configuration changes.
- `.env` changes alone do not require rebuilding.

## Documentation

- The workspace-level `../../ROADMAP.md` is the source of truth for the cross-repository provider
  migration.
- Update this README when commands, schemas, paths, environment variables, or container behavior
  change.
- Record material implementation discoveries or changed decisions in the workspace roadmap.

## Tests

Before finishing changes, run:

```bash
.venv/bin/python -m compileall market_data scripts
.venv/bin/python -m pytest
```

Tests must use fake sessions, temporary directories, and deterministic inputs.
