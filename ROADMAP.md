# finbot-data Roadmap

The workspace-level `../../ROADMAP.md` is the source of truth for the Massive/Alpaca migration.
This file records package-local status only.

## Completed

- Provider-neutral requests foundation with timeouts, bounded retries, `Retry-After` handling,
  conservative pacing, and credential-safe errors.
- Massive REST client with `next_url` pagination.
- Alpaca REST client with `next_page_token` pagination.
- Atomic compressed raw-page storage with sanitized download metadata.
- Read-only provider credential/access checks.
- Retirement of active Sharadar code, commands, configuration, tests, and Compose services.
- Massive active/inactive common-stock security master with stable security identifiers and symbol
  episodes.
- Alpaca asset reconciliation that records missing current assets without excluding historical
  candidates.
- Explicit exchange, REIT/REOC, BDC, partnership, history-window, and ticker-reuse exclusions.
- Durable `security_master`, `tickers`, and `universe_exclusions` datasets with metadata and raw
  input snapshots.
- Deterministic Alpaca delisted-coverage audit with exact historical symbols, disabled symbol
  remapping, explicit SIP/IEX comparison, and a backfill gate.
- Dated symbol episodes derived from Massive inventory and Alpaca name changes, including missing
  intermediate symbols such as `NLOK` in the `SYMC` → `NLOK` → `GEN` chain.
- Episode-aware `tickers` acquisition manifest with raw acquisition separated from safe stable-ID
  mapping.
- Live audit result: delayed SIP and delisted coverage are adequate; episode-bounded ingestion
  resolves the exact-symbol contamination gate and the full backfill recommendation is `proceed`.
- Complete Alpaca corporate-action snapshot for the Finbot symbol inventory from 2016 onward,
  normalized across all supported event families with raw payload hashes, stable-identity match
  statuses, overlapping revision refreshes, and split-history refresh indicators.
- Episode-aware Alpaca daily-bar pilot, resumable full backfill, and overlapping incremental
  update with paired raw and split-adjusted history, explicit identity quarantine, atomic
  publication, and bounded-memory compaction.

## Next

- Workspace Milestone 6 moves to `finbot-features`: derive the point-in-time research-universe
  lifecycle from the canonical daily bars. The next `finbot-data` work is Milestone 7 current
  Massive ticker-detail enrichment after that lifecycle identifies qualified securities.

Do not add research-universe qualification here; that belongs in `finbot-features`.
