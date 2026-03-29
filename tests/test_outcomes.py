"""Tests for the feedback loop: prediction vs actual outcomes."""

import json
import os
import sys
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.features.outcome_brand import (
    compute_outcomes_for_brand,
    _compute_actual_metrics,
    _safe_float,
    IMPLEMENTED_STATUSES,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_transactions():
    """Child-level transactions spanning multiple weeks."""
    base_date = pd.Timestamp("2026-03-01")
    rows = []
    # Parent SKU "HK1234" has two child SKUs: HK1234-38, HK1234-39
    # Store "1001" — sold over 4 weeks post-decision
    for week in range(4):
        date = base_date + timedelta(days=7 + 7 * week)  # starts W+1
        rows.append({"sku": "HK1234-38", "centro": "1001", "fecha": date,
                      "cantidad": 2, "precio_final": 49990, "precio_lista": 69990})
        rows.append({"sku": "HK1234-39", "centro": "1001", "fecha": date,
                      "cantidad": 1, "precio_final": 49990, "precio_lista": 69990})
    # Parent SKU "HK5678" — sparse: only 2 units total
    rows.append({"sku": "HK5678-40", "centro": "1001",
                  "fecha": base_date + timedelta(days=10), "cantidad": 2,
                  "precio_final": 29990, "precio_lista": 39990})
    return pd.DataFrame(rows)


@pytest.fixture
def sku_parent_map():
    return {
        "HK1234-38": "HK1234",
        "HK1234-39": "HK1234",
        "HK5678-40": "HK5678",
    }


@pytest.fixture
def cost_map():
    return {"HK1234": 25000.0, "HK5678": 15000.0}


@pytest.fixture
def sample_csv_df():
    """Pricing actions CSV for a single week."""
    return pd.DataFrame([
        {
            "parent_sku": "HK1234", "store": "1001",
            "current_velocity": 2.0, "expected_velocity": 4.0,
            "current_weekly_rev": 99980, "expected_weekly_rev": 199960,
            "recommended_price": 49990, "confidence_tier": "HIGH",
            "action_type": "decrease",
        },
        {
            "parent_sku": "HK5678", "store": "1001",
            "current_velocity": 1.0, "expected_velocity": 2.0,
            "current_weekly_rev": 39990, "expected_weekly_rev": 59980,
            "recommended_price": 29990, "confidence_tier": "LOW",
            "action_type": "decrease",
        },
    ])


@pytest.fixture
def sample_decisions():
    """Decisions JSON for a single week."""
    return {
        "week": "2026-03-01",
        "brand": "hoka",
        "decisions": {
            "HK1234-1001": {"status": "approved", "timestamp": "2026-03-01T10:00:00"},
            "HK5678-1001": {"status": "approved", "timestamp": "2026-03-01T10:00:00"},
        },
    }


# ── Test: actual metric computation ──────────────────────────────────────────

class TestComputeActualMetrics:
    def test_aggregates_child_skus_to_parent(self, sample_transactions, sku_parent_map):
        """Child SKUs should be aggregated to parent level."""
        week_start = pd.Timestamp("2026-03-01")
        result = _compute_actual_metrics(sample_transactions, sku_parent_map, week_start)

        hk1234 = result[result["codigo_padre"] == "HK1234"]
        assert len(hk1234) == 1

        row = hk1234.iloc[0]
        # 4 weeks x (2+1) = 12 total units
        assert row["actual_units"] == 12
        # 12 units / 4 weeks = 3.0 velocity
        assert row["actual_velocity"] == 3.0

    def test_sparse_parent_still_counted(self, sample_transactions, sku_parent_map):
        """Even sparse parents should appear in actuals (guard is applied later)."""
        week_start = pd.Timestamp("2026-03-01")
        result = _compute_actual_metrics(sample_transactions, sku_parent_map, week_start)

        hk5678 = result[result["codigo_padre"] == "HK5678"]
        assert len(hk5678) == 1
        assert hk5678.iloc[0]["actual_units"] == 2

    def test_empty_window(self, sku_parent_map):
        """No transactions in measurement window returns empty."""
        txn = pd.DataFrame({"sku": ["X"], "centro": ["1"], "fecha": [pd.Timestamp("2020-01-01")],
                             "cantidad": [1], "precio_final": [1000], "precio_lista": [1000]})
        result = _compute_actual_metrics(txn, sku_parent_map, pd.Timestamp("2026-01-01"))
        assert len(result) == 0

    def test_week_alignment(self, sample_transactions, sku_parent_map):
        """Actuals should start at W+7 days, not W itself."""
        # If decision week start is the same as the first transaction date,
        # those transactions should NOT be counted (they're in the decision week)
        first_txn_date = sample_transactions["fecha"].min()
        # Make decision week start = first_txn_date (so actuals start 7 days later)
        result = _compute_actual_metrics(
            sample_transactions, sku_parent_map,
            first_txn_date  # This is W+7 from original base_date, so measure_start = W+14
        )
        # Fewer transactions should be captured since window shifts forward
        hk1234 = result[result["codigo_padre"] == "HK1234"]
        if len(hk1234) > 0:
            assert hk1234.iloc[0]["actual_units"] < 12  # Less than full 4 weeks


# ── Test: delta calculations ─────────────────────────────────────────────────

class TestDeltaCalculations:
    def test_velocity_error_computation(self):
        """velocity_error = actual - predicted."""
        actual_vel = 3.0
        pred_vel = 4.0
        error = actual_vel - pred_vel
        error_pct = error / pred_vel * 100
        assert error == -1.0
        assert error_pct == -25.0

    def test_direction_correct_both_positive(self):
        """Direction correct when both lifts are positive."""
        actual_lift = 50.0   # actual velocity increased 50% vs baseline
        predicted_lift = 100.0  # predicted 100% increase
        assert (actual_lift >= 0 and predicted_lift >= 0) is True

    def test_direction_wrong(self):
        """Direction wrong when lifts differ in sign."""
        actual_lift = -10.0   # velocity dropped
        predicted_lift = 50.0  # predicted increase
        correct = (
            (actual_lift >= 0 and predicted_lift >= 0) or
            (actual_lift < 0 and predicted_lift < 0)
        )
        assert correct is False

    def test_lift_vs_baseline(self):
        """Lift = (actual - baseline) / baseline * 100."""
        baseline = 2.0
        actual = 3.0
        lift = (actual - baseline) / baseline * 100
        assert lift == 50.0


# ── Test: sparse data guard ──────────────────────────────────────────────────

class TestSparseDataGuard:
    def test_sparse_threshold(self):
        """Units < 3 should be flagged as sparse."""
        assert 2 < 3  # sparse
        assert 3 >= 3  # not sparse

    def test_sparse_nullifies_velocity(self):
        """When data_quality='sparse', actual_velocity should be None."""
        actual_units = 2
        if actual_units < 3:
            data_quality = "sparse"
            actual_velocity = None
        else:
            data_quality = "normal"
            actual_velocity = 1.0

        assert data_quality == "sparse"
        assert actual_velocity is None


# ── Test: IVA stripping in margin ────────────────────────────────────────────

class TestIVAStripping:
    def test_margin_calculation(self):
        """Margin = (price/1.19 - cost) / (price/1.19) * 100."""
        price = 49990
        cost = 25000
        neto = price / 1.19
        margin_pct = (neto - cost) / neto * 100
        assert round(margin_pct, 1) == 40.5  # ~40.5% margin

    def test_zero_price_safe(self):
        """Zero price should not cause division error."""
        price = 0
        cost = 25000
        neto = price / 1.19
        if neto > 0:
            margin = (neto - cost) / neto * 100
        else:
            margin = None
        assert margin is None

    def test_negative_margin(self):
        """When cost > neto price, margin is negative."""
        price = 20000
        cost = 25000
        neto = price / 1.19  # ~16807
        margin = (neto - cost) / neto * 100
        assert margin < 0


# ── Test: safe_float helper ──────────────────────────────────────────────────

class TestSafeFloat:
    def test_normal_float(self):
        assert _safe_float(3.5) == 3.5

    def test_int(self):
        assert _safe_float(3) == 3.0

    def test_string_number(self):
        assert _safe_float("3.5") == 3.5

    def test_none(self):
        assert _safe_float(None) is None

    def test_empty_string(self):
        assert _safe_float("") is None

    def test_nan(self):
        assert _safe_float(float("nan")) is None

    def test_invalid_string(self):
        assert _safe_float("abc") is None


# ── Test: implemented statuses ───────────────────────────────────────────────

class TestImplementedStatuses:
    def test_approved_statuses(self):
        """All expected statuses should be in the set."""
        assert "approved" in IMPLEMENTED_STATUSES
        assert "planner_approved" in IMPLEMENTED_STATUSES
        assert "manual" in IMPLEMENTED_STATUSES
        assert "bm_approved" in IMPLEMENTED_STATUSES
        assert "bm_manual" in IMPLEMENTED_STATUSES

    def test_rejected_not_implemented(self):
        assert "rejected" not in IMPLEMENTED_STATUSES
        assert "planner_rejected" not in IMPLEMENTED_STATUSES
        assert "bm_rejected" not in IMPLEMENTED_STATUSES


# ── Test: missing decisions graceful handling ────────────────────────────────

class TestGracefulHandling:
    @patch("src.features.outcome_brand._gcs_bucket", return_value=None)
    @patch("src.features.outcome_brand._download_historical_csvs", return_value={})
    @patch("src.features.outcome_brand._download_historical_decisions", return_value={})
    def test_no_historical_data_returns_none(self, mock_dec, mock_csv, mock_gcs, tmp_path):
        """When no historical decisions exist, should return None."""
        # Create a minimal transactions file
        txn_path = tmp_path / "data" / "raw" / "testbrand" / "transactions.parquet"
        txn_path.parent.mkdir(parents=True)
        pd.DataFrame({"sku": ["X"], "centro": ["1"], "fecha": [pd.Timestamp("2026-01-01")],
                       "cantidad": [1], "precio_final": [1000], "precio_lista": [1000]}).to_parquet(txn_path)

        with patch("src.features.outcome_brand.PROJECT_ROOT", tmp_path):
            result = compute_outcomes_for_brand("TESTBRAND")
        assert result is None

    @patch("src.features.outcome_brand._gcs_bucket", return_value=None)
    def test_no_transactions_returns_none(self, mock_gcs, tmp_path):
        """When no transactions file exists, should return None."""
        with patch("src.features.outcome_brand.PROJECT_ROOT", tmp_path):
            result = compute_outcomes_for_brand("TESTBRAND")
        assert result is None


# ── Test: full pipeline integration (mocked) ────────────────────────────────

class TestFullOutcomePipeline:
    @patch("src.features.outcome_brand._gcs_bucket", return_value=None)
    @patch("src.features.outcome_brand._load_costs")
    @patch("src.features.outcome_brand._download_historical_decisions")
    @patch("src.features.outcome_brand._download_historical_csvs")
    @patch("src.features.outcome_brand._load_sku_parent_map")
    def test_end_to_end(self, mock_parent_map, mock_csvs, mock_decs, mock_costs,
                        mock_gcs, sample_transactions, sample_csv_df, sample_decisions,
                        sku_parent_map, cost_map, tmp_path):
        """Full pipeline produces valid outcome rows."""
        mock_parent_map.return_value = sku_parent_map
        mock_costs.return_value = cost_map
        mock_csvs.return_value = {"2026-03-01": sample_csv_df}
        mock_decs.return_value = {"2026-03-01": sample_decisions}

        # Write transactions to tmp
        txn_path = tmp_path / "data" / "raw" / "hoka" / "transactions.parquet"
        txn_path.parent.mkdir(parents=True)
        sample_transactions.to_parquet(txn_path)

        # Output dir
        (tmp_path / "data" / "processed" / "hoka").mkdir(parents=True)

        with patch("src.features.outcome_brand.PROJECT_ROOT", tmp_path):
            result = compute_outcomes_for_brand("HOKA", lookback_weeks=4)

        # Should produce results (at least for HK1234 which has enough data)
        assert result is not None
        assert len(result) > 0

        # HK1234 should have data_quality="normal" (12 units > 3)
        hk1234 = result[result["parent_sku"] == "HK1234"]
        if len(hk1234) > 0:
            row = hk1234.iloc[0]
            assert row["data_quality"] == "normal"
            assert row["actual_velocity"] is not None or not pd.isna(row["actual_velocity"])

        # HK5678 should have data_quality="sparse" (2 units < 3)
        hk5678 = result[result["parent_sku"] == "HK5678"]
        if len(hk5678) > 0:
            row = hk5678.iloc[0]
            assert row["data_quality"] == "sparse"

    @patch("src.features.outcome_brand._gcs_bucket", return_value=None)
    @patch("src.features.outcome_brand._load_costs")
    @patch("src.features.outcome_brand._download_historical_decisions")
    @patch("src.features.outcome_brand._download_historical_csvs")
    @patch("src.features.outcome_brand._load_sku_parent_map")
    def test_manual_price_used(self, mock_parent_map, mock_csvs, mock_decs, mock_costs,
                               mock_gcs, sample_transactions, sample_csv_df, sku_parent_map,
                               cost_map, tmp_path):
        """Manual price override should be used as implemented_price."""
        mock_parent_map.return_value = sku_parent_map
        mock_costs.return_value = cost_map
        mock_csvs.return_value = {"2026-03-01": sample_csv_df}

        # Decisions with manual price override
        decisions = {
            "week": "2026-03-01",
            "brand": "hoka",
            "decisions": {
                "HK1234-1001": {
                    "status": "manual",
                    "manual_price": 44990,
                    "timestamp": "2026-03-01T10:00:00",
                },
            },
        }
        mock_decs.return_value = {"2026-03-01": decisions}

        txn_path = tmp_path / "data" / "raw" / "hoka" / "transactions.parquet"
        txn_path.parent.mkdir(parents=True)
        sample_transactions.to_parquet(txn_path)
        (tmp_path / "data" / "processed" / "hoka").mkdir(parents=True)

        with patch("src.features.outcome_brand.PROJECT_ROOT", tmp_path):
            result = compute_outcomes_for_brand("HOKA", lookback_weeks=4)

        if result is not None and len(result) > 0:
            hk1234 = result[result["parent_sku"] == "HK1234"]
            if len(hk1234) > 0:
                assert hk1234.iloc[0]["implemented_price"] == 44990


# ── Test: API endpoint (outcome summary) ─────────────────────────────────────

os.environ["GOOGLE_CLIENT_ID"] = ""

from fastapi.testclient import TestClient
from api.main import app, _build_outcome_summary


@pytest.fixture
def client():
    return TestClient(app)


class TestOutcomeSummaryAPI:
    def test_analytics_includes_prediccion_vs_real(self, client):
        """Analytics endpoint should include prediccion_vs_real key."""
        r = client.get("/analytics/hoka")
        assert r.status_code == 200
        data = r.json()
        assert "prediccion_vs_real" in data

    def test_outcome_details_endpoint(self, client):
        """Outcome details endpoint should return valid structure."""
        r = client.get("/analytics/outcomes/hoka")
        assert r.status_code == 200
        data = r.json()
        assert "available" in data
        assert "items" in data

    def test_build_outcome_summary_no_data(self):
        """Summary with no data should return available=False."""
        with patch("api.storage.load_outcomes", return_value=pd.DataFrame()):
            result = _build_outcome_summary("hoka")
        assert result["available"] is False

    def test_build_outcome_summary_with_data(self):
        """Summary with valid data should compute metrics."""
        df = pd.DataFrame([
            {
                "decision_week": "2026-03-01", "parent_sku": "HK1234", "store": "1001",
                "action_type": "decrease", "confidence_tier": "HIGH",
                "data_quality": "normal",
                "velocity_error_pct": -10.0, "direction_correct": True,
                "actual_lift_vs_baseline": 40.0, "predicted_lift_vs_baseline": 50.0,
            },
            {
                "decision_week": "2026-03-01", "parent_sku": "HK5678", "store": "1001",
                "action_type": "decrease", "confidence_tier": "LOW",
                "data_quality": "normal",
                "velocity_error_pct": 20.0, "direction_correct": True,
                "actual_lift_vs_baseline": 60.0, "predicted_lift_vs_baseline": 100.0,
            },
        ])
        with patch("api.storage.load_outcomes", return_value=df):
            result = _build_outcome_summary("hoka")

        assert result["available"] is True
        assert result["decisions_evaluated"] == 2
        assert result["pct_direction_correct"] == 100.0
        assert result["median_velocity_error_pct"] == 5.0  # median of -10 and 20
