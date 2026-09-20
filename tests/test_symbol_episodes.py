from __future__ import annotations

from datetime import date

import pandas as pd

from market_data.datasets.symbol_episodes import (
    build_episode_tickers,
    build_symbol_episodes,
    normalize_name_changes,
)


SNAPSHOT = date(2026, 9, 19)


def master_row(symbol: str, security_id: str, **overrides):
    row = {
        "security_id": security_id,
        "symbol": symbol,
        "company_name": "Example Company",
        "active": True,
        "delisted_date": None,
        "is_price_coverage_eligible": True,
        "exclusion_reason": None,
        "symbol_identity_conflict": False,
        "share_class_figi": security_id,
        "composite_figi": None,
        "cik": "0000000001",
        "primary_exchange": "XNAS",
        "last_updated_utc": "2026-09-18T00:00:00Z",
        "sector": "Technology",
        "industry": "Software",
        "alpaca_asset_id": None,
        "alpaca_status": None,
        "alpaca_tradable": None,
        "is_known_reit": False,
    }
    row.update(overrides)
    return row


def name_change(event_id: str, old: str, new: str, process_date: str):
    return {
        "id": event_id,
        "old_symbol": old,
        "new_symbol": new,
        "old_cusip": "111111111",
        "new_cusip": "111111111",
        "process_date": process_date,
    }


def test_symbol_episode_chain_adds_missing_intermediate_symbol():
    master = pd.DataFrame(
        [
            master_row("SYMC", "figi:gen", active=False, delisted_date=date(2019, 11, 5)),
            master_row("GEN", "figi:gen"),
        ]
    )
    changes = normalize_name_changes(
        [
            {
                "corporate_actions": {
                    "name_changes": [name_change("one", "NLOK", "GEN", "2022-11-08")]
                }
            }
        ]
    )

    episodes = build_symbol_episodes(master, changes, snapshot_date=SNAPSHOT).set_index("symbol")

    assert set(episodes.index) == {"SYMC", "NLOK", "GEN"}
    assert episodes.loc["SYMC", "symbol_valid_from"] == date(2016, 1, 1)
    assert episodes.loc["SYMC", "symbol_valid_to"] == date(2019, 11, 4)
    assert episodes.loc["NLOK", "symbol_valid_from"] == date(2019, 11, 5)
    assert episodes.loc["NLOK", "symbol_valid_to"] == date(2022, 11, 7)
    assert episodes.loc["GEN", "symbol_valid_from"] == date(2022, 11, 8)
    assert pd.isna(episodes.loc["GEN", "symbol_valid_to"])
    assert episodes.loc["GEN", "is_current_symbol"]
    assert episodes.loc["NLOK", "security_id"] == "figi:gen"


def test_episode_tickers_include_intermediate_symbol_and_boundaries():
    master = pd.DataFrame(
        [
            master_row("SYMC", "figi:gen", active=False, delisted_date=date(2019, 11, 5)),
            master_row("GEN", "figi:gen"),
        ]
    )
    changes = normalize_name_changes(
        [{"corporate_actions": {"name_changes": [name_change("one", "NLOK", "GEN", "2022-11-08")]}}]
    )

    episodes = build_symbol_episodes(master, changes, snapshot_date=SNAPSHOT)
    tickers = build_episode_tickers(episodes, master).set_index("symbol")

    assert set(tickers.index) == {"SYMC", "NLOK", "GEN"}
    assert tickers.loc["NLOK", "symbol_valid_from"] == date(2019, 11, 5)
    assert tickers.loc["NLOK", "symbol_valid_to"] == date(2022, 11, 7)
    assert bool(tickers.loc["NLOK", "is_episode_mapping_eligible"])


def test_backward_chain_ignores_unrelated_future_transition_from_reused_symbol():
    master = pd.DataFrame([master_row("META", "figi:meta")])
    changes = normalize_name_changes(
        [
            {
                "corporate_actions": {
                    "name_changes": [
                        name_change("etf", "META", "METV", "2022-01-31"),
                        name_change("facebook", "FB", "META", "2022-06-09"),
                    ]
                }
            }
        ]
    )

    episodes = build_symbol_episodes(master, changes, snapshot_date=SNAPSHOT)

    assert set(episodes["symbol"]) == {"FB", "META"}
    assert "METV" not in set(episodes["symbol"])


def test_overlapping_reused_symbols_remain_ineligible():
    master = pd.DataFrame(
        [
            master_row("SAME", "figi:old", active=False, delisted_date=date(2020, 1, 10), symbol_identity_conflict=True, is_price_coverage_eligible=False),
            master_row("SAME", "figi:new", symbol_identity_conflict=True, is_price_coverage_eligible=False),
        ]
    )

    episodes = build_symbol_episodes(master, pd.DataFrame(), snapshot_date=SNAPSHOT)

    assert set(episodes["episode_status"]) == {"unresolved"}
    assert not episodes["is_episode_mapping_eligible"].any()


def test_unresolved_price_eligible_episode_stays_in_acquisition_manifest():
    master = pd.DataFrame(
        [
            master_row("SAME", "figi:old", active=False, delisted_date=date(2020, 1, 10)),
            master_row("SAME", "figi:new"),
        ]
    )

    episodes = build_symbol_episodes(master, pd.DataFrame(), snapshot_date=SNAPSHOT)
    tickers = build_episode_tickers(episodes, master)

    assert len(tickers) == 2
    assert set(tickers["episode_status"]) == {"unresolved"}
    assert not tickers["is_episode_mapping_eligible"].any()


def test_name_change_normalization_rejects_cusip_pseudo_symbols():
    changes = normalize_name_changes(
        [
            {
                "corporate_actions": {
                    "name_changes": [
                        name_change("valid", "SQ", "XYZ", "2025-01-21"),
                        name_change("cusip", "007975113", "22112H119", "2024-08-12"),
                    ]
                }
            }
        ]
    )

    assert changes[["old_symbol", "new_symbol"]].to_dict("records") == [
        {"old_symbol": "SQ", "new_symbol": "XYZ"}
    ]
