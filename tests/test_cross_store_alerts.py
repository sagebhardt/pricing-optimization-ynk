"""Tests for cross-store pricing consistency alerts."""

import pandas as pd
import numpy as np
import pytest

from src.features.cross_store_alerts_brand import build_cross_store_alerts


def _make_features(rows):
    """Helper to build a features DataFrame from a list of dicts."""
    df = pd.DataFrame(rows)
    df["week"] = pd.Timestamp("2026-03-23")
    for col in ["avg_precio_final", "discount_rate", "velocity_4w"]:
        if col not in df.columns:
            df[col] = 0.0
    return df


class TestCrossStoreAlerts:
    def test_uniform_prices_no_alerts(self):
        """Same price across stores should produce no alerts."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 100000, "discount_rate": 0.15, "velocity_4w": 2.0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 100000, "discount_rate": 0.15, "velocity_4w": 1.5},
            {"codigo_padre": "HK001", "centro": "AB75", "avg_precio_final": 100000, "discount_rate": 0.15, "velocity_4w": 3.0},
        ])
        alerts = build_cross_store_alerts(df)
        assert len(alerts) == 0

    def test_bm_price_spread_triggers_alert(self):
        """B&M price spread >10% should trigger price_inconsistency_bm."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 2.0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 120000, "discount_rate": 0.20, "velocity_4w": 1.5},
        ])
        alerts = build_cross_store_alerts(df)
        assert len(alerts) > 0
        assert "price_inconsistency_bm" in alerts["alert_reasons"].iloc[0]

    def test_single_store_no_alerts(self):
        """Parents in only one store should never trigger alerts."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 2.0},
            {"codigo_padre": "HK002", "centro": "7502", "avg_precio_final": 120000, "discount_rate": 0.20, "velocity_4w": 1.5},
        ])
        alerts = build_cross_store_alerts(df)
        assert len(alerts) == 0

    def test_ecomm_excluded_from_bm_spread(self):
        """Ecomm price diff should not trigger bm_price_inconsistency when B&M is uniform."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 2.0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 1.5},
            {"codigo_padre": "HK001", "centro": "AB75", "avg_precio_final": 100000, "discount_rate": 0.33, "velocity_4w": 5.0},
        ])
        alerts = build_cross_store_alerts(df)
        # Should NOT have bm price inconsistency (B&M stores are identical)
        if len(alerts) > 0:
            bm_reasons = alerts[alerts["alert_reasons"].str.contains("price_inconsistency_bm")]
            assert len(bm_reasons) == 0
            # But should have ecomm_gap and/or markdown_split
            assert any(alerts["alert_reasons"].str.contains("ecomm_gap|markdown_split"))

    def test_markdown_split_triggers(self):
        """Some stores discounted, others not, should trigger markdown_split."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 2.0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 127500, "discount_rate": 0.15, "velocity_4w": 3.0},
        ])
        alerts = build_cross_store_alerts(df)
        assert len(alerts) > 0
        assert any(alerts["alert_reasons"].str.contains("markdown_split"))

    def test_discount_spread_triggers(self):
        """Discount range >10pp should trigger discount_spread."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 2.0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 120000, "discount_rate": 0.20, "velocity_4w": 3.0},
        ])
        alerts = build_cross_store_alerts(df)
        assert len(alerts) > 0
        assert any(alerts["alert_reasons"].str.contains("discount_spread"))

    def test_stock_imbalance_triggers(self):
        """Stockout at one store + excess at another should trigger stock_imbalance."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 150000, "discount_rate": 0.0,
             "velocity_4w": 0.0, "stock_on_hand": 0, "weeks_of_cover": 0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 150000, "discount_rate": 0.0,
             "velocity_4w": 1.0, "stock_on_hand": 50, "weeks_of_cover": 20},
        ])
        alerts = build_cross_store_alerts(df)
        assert len(alerts) > 0
        assert any(alerts["alert_reasons"].str.contains("stock_imbalance"))

    def test_nan_stock_no_crash(self):
        """Missing stock data should not crash."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 2.0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 120000, "discount_rate": 0.20, "velocity_4w": 3.0},
        ])
        # No stock columns at all
        alerts = build_cross_store_alerts(df)
        assert isinstance(alerts, pd.DataFrame)

    def test_velocity_weighted_sync_price(self):
        """Sync price should weight toward the higher-velocity store."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 100000, "discount_rate": 0.0, "velocity_4w": 10.0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 0.5},
        ])
        alerts = build_cross_store_alerts(df, threshold_price_spread=0.01)
        assert len(alerts) > 0
        sync = alerts["sync_price"].iloc[0]
        # Should be much closer to 100000 (high velocity) than 150000
        assert sync < 120000

    def test_empty_dataframe(self):
        """Empty input should return empty output."""
        df = pd.DataFrame(columns=["codigo_padre", "centro", "week", "avg_precio_final"])
        df["week"] = pd.to_datetime(df["week"])
        alerts = build_cross_store_alerts(df)
        assert len(alerts) == 0

    def test_output_columns(self):
        """Output should contain expected columns."""
        df = _make_features([
            {"codigo_padre": "HK001", "centro": "7501", "avg_precio_final": 150000, "discount_rate": 0.0, "velocity_4w": 2.0},
            {"codigo_padre": "HK001", "centro": "7502", "avg_precio_final": 120000, "discount_rate": 0.20, "velocity_4w": 3.0},
        ])
        alerts = build_cross_store_alerts(df)
        assert len(alerts) > 0
        expected = {"codigo_padre", "centro", "week", "channel", "avg_precio_final",
                    "discount_rate", "n_stores", "median_price", "price_spread",
                    "discount_spread", "sync_price", "alert_reasons"}
        assert expected.issubset(set(alerts.columns))
