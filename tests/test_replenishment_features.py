"""Tests for add_replenishment_features — merging DW replenishment signal into weekly features."""

import pandas as pd

from src.features.build_features_brand import add_replenishment_features


def _weekly(skus_weeks):
    """skus_weeks: list of (sku, centro, week_str)."""
    return pd.DataFrame([
        {"sku": sku, "centro": centro, "week": pd.Timestamp(w)}
        for (sku, centro, w) in skus_weeks
    ])


class TestAddReplenishmentFeatures:
    def test_matched_rows_get_replenishment_columns(self):
        weekly = _weekly([("HK1", "7501", "2026-04-20"), ("HK2", "7501", "2026-04-20")])
        rep = pd.DataFrame([{
            "sku": "HK1", "centro": "7501",
            "units_in_transit": 12, "units_received_window": 45,
            "avg_transit_days": 5.3, "n_transfers": 8,
        }])
        out = add_replenishment_features(weekly, rep)
        matched = out[out["sku"] == "HK1"].iloc[0]
        unmatched = out[out["sku"] == "HK2"].iloc[0]
        assert matched["units_in_transit"] == 12.0
        assert matched["units_received_window"] == 45.0
        assert matched["avg_transit_days"] == 5.3
        assert matched["n_transfers"] == 8
        # Unmatched row gets 0 / NaN sentinels
        assert unmatched["units_in_transit"] == 0.0
        assert unmatched["units_received_window"] == 0.0
        assert pd.isna(unmatched["avg_transit_days"])
        assert unmatched["n_transfers"] == 0

    def test_historical_weeks_are_zeroed_to_prevent_leakage(self):
        weekly = _weekly([
            ("HK1", "7501", "2026-01-05"),  # historical
            ("HK1", "7501", "2026-04-20"),  # most recent
        ])
        rep = pd.DataFrame([{
            "sku": "HK1", "centro": "7501",
            "units_in_transit": 12, "units_received_window": 45,
            "avg_transit_days": 5.3, "n_transfers": 8,
        }])
        out = add_replenishment_features(weekly, rep).sort_values("week").reset_index(drop=True)
        # Historical row: zeroed + NaN transit
        assert out.iloc[0]["units_in_transit"] == 0.0
        assert out.iloc[0]["units_received_window"] == 0.0
        assert out.iloc[0]["n_transfers"] == 0
        assert pd.isna(out.iloc[0]["avg_transit_days"])
        # Most recent row: carries the signal
        assert out.iloc[1]["units_received_window"] == 45.0
        assert out.iloc[1]["n_transfers"] == 8

    def test_dupe_rows_are_summed(self):
        weekly = _weekly([("HK1", "7501", "2026-04-20")])
        rep = pd.DataFrame([
            {"sku": "HK1", "centro": "7501", "units_in_transit": 10,
             "units_received_window": 20, "avg_transit_days": 4.0, "n_transfers": 3},
            {"sku": "HK1", "centro": "7501", "units_in_transit": 5,
             "units_received_window": 15, "avg_transit_days": 6.0, "n_transfers": 2},
        ])
        out = add_replenishment_features(weekly, rep)
        assert out.iloc[0]["units_in_transit"] == 15.0  # summed
        assert out.iloc[0]["units_received_window"] == 35.0  # summed
        assert out.iloc[0]["n_transfers"] == 5  # summed
        # avg_transit_days: unweighted mean of the two dupe rows
        assert out.iloc[0]["avg_transit_days"] == 5.0

    def test_empty_replenishment_merges_cleanly(self):
        weekly = _weekly([("HK1", "7501", "2026-04-20")])
        rep = pd.DataFrame(columns=["sku", "centro", "units_in_transit",
                                      "units_received_window", "avg_transit_days", "n_transfers"])
        out = add_replenishment_features(weekly, rep)
        assert out.iloc[0]["units_in_transit"] == 0.0
        assert out.iloc[0]["n_transfers"] == 0
        assert pd.isna(out.iloc[0]["avg_transit_days"])
