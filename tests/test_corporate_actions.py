from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path

import pandas as pd

from market_data.datasets.corporate_actions import (
    ACTION_COLLECTIONS,
    download_and_write_corporate_actions,
    merge_corporate_actions,
    normalize_corporate_action_pages,
)

SNAPSHOT = date(2026, 9, 19)


def episodes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "figi:aapl",
                "symbol": "AAPL",
                "symbol_valid_from": date(2016, 1, 1),
                "symbol_valid_to": None,
                "is_episode_mapping_eligible": True,
            },
            {
                "security_id": "figi:buyer",
                "symbol": "BUY",
                "symbol_valid_from": date(2016, 1, 1),
                "symbol_valid_to": None,
                "is_episode_mapping_eligible": True,
            },
            {
                "security_id": "figi:blocked",
                "symbol": "SAME",
                "symbol_valid_from": date(2016, 1, 1),
                "symbol_valid_to": None,
                "is_episode_mapping_eligible": False,
            },
        ]
    )


def action(event_id: str, **values):
    return {"id": event_id, "process_date": "2026-09-10", **values}


def test_normalizes_every_supported_action_collection_and_hashes_payloads():
    payloads = {}
    for index, collection in enumerate(ACTION_COLLECTIONS):
        values = {"symbol": "AAPL"}
        if collection == "name_changes":
            values = {"old_symbol": "AAPL", "new_symbol": "APPL"}
        elif collection in {"cash_mergers", "stock_mergers", "stock_and_cash_mergers"}:
            values = {"acquiree_symbol": "AAPL", "acquirer_symbol": "BUY", "effective_date": "2026-09-10"}
        elif collection in {"spin_offs", "rights_distributions"}:
            values = {"source_symbol": "AAPL", "new_symbol": "CHILD", "ex_date": "2026-09-10"}
        elif collection == "unit_splits":
            values = {"old_symbol": "AAPL", "new_symbol": "APPL", "effective_date": "2026-09-10"}
        payloads[collection] = [action(f"event-{index}", **values)]

    frame = normalize_corporate_action_pages(
        [{"corporate_actions": payloads}], episodes(), snapshot_date=SNAPSHOT
    )

    assert set(frame["action_type"]) == set(ACTION_COLLECTIONS.values())
    assert frame["provider_event_id"].is_unique
    assert frame["raw_payload_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert frame.loc[frame["action_type"] == "forward_split", "requires_split_history_refresh"].item()
    merger = frame[frame["action_type"] == "cash_merger"].iloc[0]
    assert merger["security_id"] == "figi:aapl"
    assert merger["related_security_id"] == "figi:buyer"


def test_quarantined_episode_is_not_attached_to_security():
    frame = normalize_corporate_action_pages(
        [{"corporate_actions": {"cash_dividends": [action("blocked", symbol="SAME", ex_date="2026-09-10")]}}],
        episodes(),
        snapshot_date=SNAPSHOT,
    )

    assert pd.isna(frame.iloc[0]["security_id"])
    assert frame.iloc[0]["identity_match_status"] == "identity_quarantine"


def test_overlap_merge_replaces_revisions_and_is_idempotent():
    old = normalize_corporate_action_pages(
        [{"corporate_actions": {"cash_dividends": [action("same-id", symbol="AAPL", rate="1.00")]}}],
        episodes(),
        snapshot_date=SNAPSHOT,
    )
    refreshed = normalize_corporate_action_pages(
        [{"corporate_actions": {"cash_dividends": [action("same-id", symbol="AAPL", rate="1.25")]}}],
        episodes(),
        snapshot_date=SNAPSHOT,
    )

    first = merge_corporate_actions(old, refreshed, refresh_start=date(2026, 9, 1))
    second = merge_corporate_actions(first, refreshed, refresh_start=date(2026, 9, 1))

    assert len(first) == 1
    assert first.iloc[0]["rate"] == "1.25"
    pd.testing.assert_frame_equal(first, second)


class FakeAlpacaClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def iter_corporate_action_pages(self, **kwargs):
        self.calls.append(kwargs)
        yield from self.pages


def test_download_uses_overlap_and_reports_changed_splits(tmp_path: Path):
    reference = tmp_path / "reference"
    reference.mkdir()
    episodes().to_parquet(reference / "symbol_episodes.parquet", index=False)
    initial_payload = action(
        "split-1", symbol="AAPL", ex_date="2026-09-10", old_rate="1", new_rate="4"
    )
    first_client = FakeAlpacaClient(
        [{"corporate_actions": {"forward_splits": [initial_payload]}}]
    )

    first = download_and_write_corporate_actions(
        snapshot_date=SNAPSHOT,
        reference_dir=reference,
        raw_dir=tmp_path / "raw",
        alpaca_client=first_client,
    )
    first_metadata = json.loads(first["metadata"].read_text())
    assert first_client.calls[0]["start"] == date(2016, 1, 1)
    assert set(first_client.calls[0]["symbols"]) == {"AAPL", "BUY", "SAME"}
    assert first_metadata["split_history_refresh_event_ids"] == ["split-1"]
    assert "2016-01-01-2026-09-18" in first["raw"].name

    second_client = FakeAlpacaClient(
        [{"corporate_actions": {"forward_splits": [initial_payload]}}]
    )
    second = download_and_write_corporate_actions(
        snapshot_date=SNAPSHOT,
        reference_dir=reference,
        raw_dir=tmp_path / "raw",
        alpaca_client=second_client,
    )
    second_metadata = json.loads(second["metadata"].read_text())
    assert second_client.calls[0]["start"] == date(2026, 8, 11)
    assert second_metadata["split_history_refresh_event_ids"] == []
    assert "2026-08-11-2026-09-18" in second["raw"].name
    assert pd.read_parquet(second["corporate_actions"])["provider_event_id"].tolist() == ["split-1"]


def test_raw_hash_is_canonical_and_changes_with_provider_payload():
    one = action("event", symbol="AAPL", rate="1.00")
    same_different_order = {"rate": "1.00", "symbol": "AAPL", "process_date": "2026-09-10", "id": "event"}
    changed = action("event", symbol="AAPL", rate="1.01")

    def digest(payload):
        frame = normalize_corporate_action_pages(
            [{"corporate_actions": {"cash_dividends": [payload]}}], episodes(), snapshot_date=SNAPSHOT
        )
        return frame.iloc[0]["raw_payload_sha256"]

    assert digest(one) == digest(same_different_order)
    assert digest(one) != digest(changed)
    expected = hashlib.sha256(json.dumps(one, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert digest(one) == expected
