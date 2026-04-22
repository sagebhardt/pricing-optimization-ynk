"""Tests for add_backorder_features — merging DW backorder signal into weekly features."""

import pandas as pd

from src.features.build_features_brand import add_backorder_features


def _weekly(skus_weeks):
    """skus_weeks: list of (sku, centro, week_str)."""
    return pd.DataFrame([
        {"sku": sku, "centro": centro, "week": pd.Timestamp(w)}
        for (sku, centro, w) in skus_weeks
    ])


class TestAddBackorderFeatures:
    def test_matched_rows_get_open_po_units(self):
        weekly = _weekly([("HK1", "7501", "2026-04-20"), ("HK2", "7501", "2026-04-20")])
        bo = pd.DataFrame([{
            "cod_padre": "HK1", "sku": "HK1", "centro": "7501",
            "open_qty": 12.0, "earliest_delivery": "2026-05-15", "n_open_pos": 2,
        }])
        out = add_backorder_features(weekly, bo)
        matched = out[out["sku"] == "HK1"].iloc[0]
        unmatched = out[out["sku"] == "HK2"].iloc[0]
        assert matched["open_po_units"] == 12.0
        assert matched["n_open_pos"] == 2
        # 2026-05-15 minus 2026-04-20 week start = 25 days
        assert matched["days_to_delivery"] == 25
        # Unmatched row gets 0 / 9999 sentinel
        assert unmatched["open_po_units"] == 0.0
        assert unmatched["n_open_pos"] == 0
        assert unmatched["days_to_delivery"] == 9999

    def test_past_due_delivery_produces_negative_days(self):
        weekly = _weekly([("HK1", "7501", "2026-04-20")])
        bo = pd.DataFrame([{
            "cod_padre": "HK1", "sku": "HK1", "centro": "7501",
            "open_qty": 5.0, "earliest_delivery": "2025-07-23", "n_open_pos": 1,
        }])
        out = add_backorder_features(weekly, bo)
        # Far in the past — stale PO
        assert out.iloc[0]["days_to_delivery"] < 0

    def test_dupe_rows_are_summed_not_dropped(self):
        """If upstream re-run produces two rows for the same (sku, centro), we want
        the TOTAL open units, not just one row's. Keep the earliest delivery as a
        conservative proxy for 'first arrival helps soonest'."""
        weekly = _weekly([("HK1", "7501", "2026-04-20")])
        bo = pd.DataFrame([
            {"cod_padre": "HK1", "sku": "HK1", "centro": "7501",
             "open_qty": 10.0, "earliest_delivery": "2026-06-01", "n_open_pos": 1},
            {"cod_padre": "HK1", "sku": "HK1", "centro": "7501",
             "open_qty": 5.0, "earliest_delivery": "2026-07-01", "n_open_pos": 1},
        ])
        out = add_backorder_features(weekly, bo)
        assert out.iloc[0]["open_po_units"] == 15.0
        assert out.iloc[0]["n_open_pos"] == 2
        # Earliest of the two deliveries: 2026-06-01 (42 days from 2026-04-20)
        assert out.iloc[0]["days_to_delivery"] == 42

    def test_historical_weeks_are_zeroed_to_prevent_leakage(self):
        """Backorder is a today-snapshot. For training rows in earlier weeks, the
        snapshot wasn't known, so we must zero them out to prevent lookahead."""
        weekly = _weekly([
            ("HK1", "7501", "2026-01-05"),  # historical
            ("HK1", "7501", "2026-04-20"),  # most recent
        ])
        bo = pd.DataFrame([{
            "cod_padre": "HK1", "sku": "HK1", "centro": "7501",
            "open_qty": 10.0, "earliest_delivery": "2026-06-01", "n_open_pos": 1,
        }])
        out = add_backorder_features(weekly, bo).sort_values("week").reset_index(drop=True)
        # Historical row: zeroed
        assert out.iloc[0]["open_po_units"] == 0.0
        assert out.iloc[0]["n_open_pos"] == 0
        assert out.iloc[0]["days_to_delivery"] == 9999
        # Most recent row: carries the signal
        assert out.iloc[1]["open_po_units"] == 10.0
        assert out.iloc[1]["n_open_pos"] == 1

    def test_empty_backorder_merges_cleanly(self):
        """No open POs in the brand's data — every row gets the zero/sentinel defaults."""
        weekly = _weekly([("HK1", "7501", "2026-04-20")])
        bo = pd.DataFrame(columns=["cod_padre", "sku", "centro", "open_qty",
                                    "earliest_delivery", "n_open_pos"])
        out = add_backorder_features(weekly, bo)
        assert out.iloc[0]["open_po_units"] == 0.0
        assert out.iloc[0]["days_to_delivery"] == 9999
