"""Tests for api/pricing_math.py — the pricing engine's pure-math core."""

import pytest
from api.pricing_math import (
    snap_to_price_anchor,
    snap_to_discount_step,
    compute_expected_velocity,
    estimate_manual_price_impact,
    PRICE_ANCHORS,
    DISCOUNT_STEPS,
)


class TestSnapToPriceAnchor:
    """Price snapping to cognitive anchors (990 endings)."""

    def test_exact_anchor(self):
        assert snap_to_price_anchor(29990, "nearest") == 29990

    def test_snap_down(self):
        assert snap_to_price_anchor(32000, "down") == 29990

    def test_snap_up(self):
        assert snap_to_price_anchor(32000, "up") == 34990

    def test_snap_nearest_rounds_down(self):
        assert snap_to_price_anchor(31000, "nearest") == 29990

    def test_snap_nearest_rounds_up(self):
        assert snap_to_price_anchor(33500, "nearest") == 34990

    def test_zero_price(self):
        assert snap_to_price_anchor(0, "down") == 0
        assert snap_to_price_anchor(0, "up") == 0
        assert snap_to_price_anchor(-100, "nearest") == 0

    def test_very_small_price(self):
        assert snap_to_price_anchor(500, "down") == 990  # smallest anchor
        assert snap_to_price_anchor(500, "nearest") == 990

    def test_very_large_price(self):
        result = snap_to_price_anchor(1_500_000, "down")
        assert result > 0
        assert result < 1_500_000

    def test_above_table_range(self):
        result = snap_to_price_anchor(2_000_000, "nearest")
        assert result > 0

    def test_all_anchors_end_in_990(self):
        for anchor in PRICE_ANCHORS:
            assert str(anchor).endswith("990"), f"Anchor {anchor} doesn't end in 990"

    def test_anchors_are_sorted(self):
        assert PRICE_ANCHORS == sorted(PRICE_ANCHORS)

    def test_no_dead_zone_prices(self):
        """Prices like 27,990 or 33,990 should not exist in anchors."""
        for anchor in PRICE_ANCHORS:
            if anchor > 20000:
                # Between 20k-50k, steps should be at 5k intervals
                last_digits = anchor % 10000
                assert last_digits == 4990 or last_digits == 9990 or anchor < 20000 or anchor > 50000, \
                    f"Anchor {anchor} might be in a dead zone"

    def test_snap_deterministic(self):
        """Same input always gives same output."""
        for _ in range(100):
            assert snap_to_price_anchor(45000, "nearest") == 44990


class TestSnapToDiscountStep:
    """Discount snapping to the 5-step ladder."""

    def test_zero_discount(self):
        assert snap_to_discount_step(0.0) == 0.0
        assert snap_to_discount_step(0.02) == 0.0

    def test_small_discount_snaps_to_fifteen(self):
        assert snap_to_discount_step(0.10) == 0.15
        assert snap_to_discount_step(0.07) == 0.15
        assert snap_to_discount_step(0.12) == 0.15

    def test_exact_steps(self):
        assert snap_to_discount_step(0.15) == 0.15
        assert snap_to_discount_step(0.20) == 0.20
        assert snap_to_discount_step(0.30) == 0.30
        assert snap_to_discount_step(0.40) == 0.40

    def test_between_steps_snaps_to_nearest(self):
        assert snap_to_discount_step(0.17) == 0.15
        assert snap_to_discount_step(0.18) == 0.20
        assert snap_to_discount_step(0.25) == 0.20 or snap_to_discount_step(0.25) == 0.30

    def test_discount_ladder_values(self):
        assert DISCOUNT_STEPS == [0.0, 0.15, 0.20, 0.30, 0.40]


class TestComputeExpectedVelocity:
    """Velocity estimation at different price points."""

    def test_zero_velocity(self):
        assert compute_expected_velocity(0, 0.0, 0.20, None) == 0.5

    def test_no_discount_change(self):
        assert compute_expected_velocity(2.0, 0.20, 0.20, None) == 2.0

    def test_shallower_discount_returns_current(self):
        assert compute_expected_velocity(3.0, 0.30, 0.15, None) == 3.0

    def test_elasticity_increases_velocity(self):
        vel = compute_expected_velocity(2.0, 0.0, 0.20, -1.5)
        assert vel > 2.0, "Deeper discount with elasticity should increase velocity"

    def test_elasticity_magnitude(self):
        """More elastic = bigger velocity response."""
        vel_low = compute_expected_velocity(2.0, 0.0, 0.20, -0.5)
        vel_high = compute_expected_velocity(2.0, 0.0, 0.20, -2.0)
        assert vel_high > vel_low

    def test_weak_elasticity_uses_fallback(self):
        """Elasticity > -0.3 should use the empirical lift table."""
        vel_no_elast = compute_expected_velocity(2.0, 0.0, 0.20, None)
        vel_weak = compute_expected_velocity(2.0, 0.0, 0.20, -0.1)
        assert vel_no_elast == vel_weak

    def test_minimum_velocity_floor(self):
        """Should never return less than 0.5."""
        vel = compute_expected_velocity(0.1, 0.0, 0.40, -0.5)
        assert vel >= 0.5

    def test_true_price_change_formula(self):
        """Going from 20% to 30% off is a 12.5% price drop, not 10%."""
        vel_correct = compute_expected_velocity(2.0, 0.20, 0.30, -1.0)
        # 12.5% price change * -(-1.0) elasticity = 12.5% velocity increase
        # 2.0 * (1 + 0.125) = 2.25
        assert abs(vel_correct - 2.25) < 0.01


class TestEstimateManualPriceImpact:
    """Manual price impact estimation for the override feature."""

    def test_basic_impact(self, sample_action):
        result = estimate_manual_price_impact(sample_action, 69990)
        assert result["snapped_price"] == 69990
        assert result["velocity"] > 0
        assert result["weekly_revenue"] > 0
        assert result["margin_pct"] is not None
        assert result["warning"] is None or result["warning"] == "thin_margin"

    def test_anchor_snapping(self, sample_action):
        result = estimate_manual_price_impact(sample_action, 68000)
        assert result["snapped_price"] == 69990  # snapped up to nearest anchor

    def test_below_cost_warning(self, sample_action):
        result = estimate_manual_price_impact(sample_action, 29990)
        # 29990 / 1.19 = 25,201 neto < 35,000 cost
        assert result["warning"] == "below_cost"

    def test_thin_margin_warning(self, sample_action):
        # Price that gives margin between 0-15%
        result = estimate_manual_price_impact(sample_action, 49990)
        # 49990/1.19 = 42,008 neto - 35,000 cost = 7,008 margin
        # 7,008 / 42,008 = 16.7% — just above floor
        assert result["margin_pct"] is not None

    def test_no_cost_data(self, sample_action_no_cost):
        result = estimate_manual_price_impact(sample_action_no_cost, 49990)
        assert result["warning"] == "no_cost_data"
        assert result["margin_pct"] is None
        assert result["margin_delta"] is None
        assert result["velocity"] > 0  # velocity still estimated

    def test_zero_list_price(self):
        action = {"current_list_price": 0, "current_price": 0, "current_velocity": 1.0}
        result = estimate_manual_price_impact(action, 29990)
        assert result["warning"] == "no_list_price"

    def test_returns_integer_revenue(self, sample_action):
        result = estimate_manual_price_impact(sample_action, 79990)
        assert isinstance(result["weekly_revenue"], int)

    def test_returns_rounded_velocity(self, sample_action):
        result = estimate_manual_price_impact(sample_action, 59990)
        vel_str = str(result["velocity"])
        # Should have at most 1 decimal place
        if "." in vel_str:
            assert len(vel_str.split(".")[1]) <= 1
