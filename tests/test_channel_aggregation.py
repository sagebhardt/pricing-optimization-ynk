"""Tests for the parent x channel aggregation step."""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.pricing_simulation import (
    DISCOUNT_STEPS,
    MIN_MARGIN_PCT,
    IVA,
    find_profit_maximizing_step,
    expected_velocity_bidirectional,
    snap_to_price_anchor,
    snap_to_discount_step,
)
from src.models import channel_pricing_brand as cpb


class TestFindProfitMaximizingStep:
    """Core: picks the step that maximizes weekly profit under constraints."""

    def test_holds_when_current_is_optimal(self):
        # Full price, healthy velocity, strong elasticity → no markdown is worth it.
        # Contract: sim returns a dict with action_type="hold" so callers can
        # tell "hold" apart from "no feasible step" (which returns None).
        r = find_profit_maximizing_step(
            list_price=100000, current_price=100000, current_discount=0.0,
            velocity=5.0, unit_cost=40000, elasticity=-1.5,
        )
        assert r is not None, "Feasible steps existed — should return a result dict"
        assert r["action_type"] == "hold"

    def test_recommends_markdown_when_velocity_weak(self):
        # Very elastic demand (-3.0): a small discount creates outsized volume gain,
        # the sim should pick a deeper step than the current hold.
        r = find_profit_maximizing_step(
            list_price=100000, current_price=100000, current_discount=0.0,
            velocity=1.0, unit_cost=30000, elasticity=-3.0,
        )
        assert r is not None
        assert r["chosen_step"] > 0.0
        assert r["action_type"] == "decrease"

    def test_recommends_increase_when_overdiscounted(self):
        # Currently at 30% off but with low velocity — may recommend raising price
        # for margin recovery (depends on elasticity).
        r = find_profit_maximizing_step(
            list_price=100000, current_price=70000, current_discount=0.30,
            velocity=1.0, unit_cost=40000, elasticity=-1.5,
        )
        assert r is not None
        # Either raises the price or picks something material; must not be None
        assert r["action_type"] in ("increase", "decrease")
        assert r["chosen_step"] != 0.30 or abs(r["chosen_price"] - snap_to_price_anchor(70000, "nearest")) >= 2000

    def test_respects_cost_floor(self):
        # Cost so high that no discount stays profitable at list — only 0% works
        r = find_profit_maximizing_step(
            list_price=50000, current_price=50000, current_discount=0.0,
            velocity=2.0, unit_cost=40000, elasticity=-2.0,
        )
        # Any chosen step must keep net price >= cost
        if r is not None:
            net = r["chosen_price"] / (1 + IVA)
            assert net >= 40000, f"Cost floor breached: net={net}, cost=40000"

    def test_respects_margin_floor(self):
        # Cost at 60% of net price @ 0% → margin floor (15%) rules out deep discounts
        r = find_profit_maximizing_step(
            list_price=100000, current_price=100000, current_discount=0.0,
            velocity=1.0, unit_cost=35000, elasticity=-3.0,
        )
        if r is not None:
            assert r["margin_pct"] is not None
            assert r["margin_pct"] >= MIN_MARGIN_PCT, \
                f"Margin floor breached: {r['margin_pct']}% < {MIN_MARGIN_PCT}%"

    def test_no_cost_maximizes_revenue(self):
        # Without cost, sim maximizes revenue (weekly_profit == weekly_revenue).
        r = find_profit_maximizing_step(
            list_price=100000, current_price=100000, current_discount=0.0,
            velocity=1.0, unit_cost=None, elasticity=-2.0,
        )
        if r is not None:
            assert r["margin_pct"] is None
            assert r["margin_unit"] is None
            assert abs(r["weekly_profit"] - r["weekly_revenue"]) < 1e-6

    def test_allow_increase_false_restricts_steps(self):
        # With allow_increase=False, sim cannot recommend lower discount than current
        r = find_profit_maximizing_step(
            list_price=100000, current_price=70000, current_discount=0.30,
            velocity=1.0, unit_cost=40000, elasticity=-1.5,
            allow_increase=False,
        )
        if r is not None:
            assert r["chosen_step"] >= 0.30 - 1e-9

    def test_material_change_threshold(self):
        # If the "best" step matches current AND the price delta is below
        # min_price_delta, action_type is "hold" (callers should skip).
        r = find_profit_maximizing_step(
            list_price=100000, current_price=100000, current_discount=0.0,
            velocity=5.0, unit_cost=40000, elasticity=-1.5,
            min_price_delta=10_000_000,  # force immateriality
        )
        assert r is not None and r["action_type"] == "hold"

    def test_feasible_steps_reported(self):
        r = find_profit_maximizing_step(
            list_price=100000, current_price=100000, current_discount=0.0,
            velocity=1.0, unit_cost=30000, elasticity=-3.0,
        )
        if r is not None:
            assert isinstance(r["feasible_steps"], list)
            assert all(s in DISCOUNT_STEPS for s in r["feasible_steps"])

    def test_hold_is_explicit_not_none(self):
        # If best step is same as current and price delta is below material threshold,
        # sim should return a dict with action_type="hold" (not None), so the caller
        # can distinguish this from "no feasible" (and not trigger a fallback).
        r = find_profit_maximizing_step(
            list_price=100000, current_price=100000, current_discount=0.0,
            velocity=5.0, unit_cost=40000, elasticity=-1.5,
        )
        # Either a hold dict or None if there was truly nothing feasible
        if r is not None:
            assert r["action_type"] == "hold", \
                f"When best step matches current and price is immaterial, action_type must be 'hold', got {r['action_type']}"

    def test_no_feasible_returns_none(self):
        # Cost above list price → no step has positive margin → None (caller can fall back)
        r = find_profit_maximizing_step(
            list_price=50000, current_price=50000, current_discount=0.0,
            velocity=1.0, unit_cost=60000, elasticity=-2.0,
        )
        assert r is None, "No feasible step should return None so caller can decide to fall back"


class TestExpectedVelocityBidirectional:
    """Velocity calculator handles both markdowns and price raises."""

    def test_same_discount_returns_current(self):
        assert expected_velocity_bidirectional(2.0, 0.20, 0.20, -1.5) == 2.0

    def test_deeper_discount_increases_velocity(self):
        v = expected_velocity_bidirectional(2.0, 0.0, 0.20, -1.5)
        assert v > 2.0

    def test_shallower_discount_decreases_velocity(self):
        # Currently at 30% off → going to 15% off should DROP velocity
        v = expected_velocity_bidirectional(2.0, 0.30, 0.15, -1.5)
        assert v < 2.0, f"Velocity should drop when price rises (got {v})"

    def test_elasticity_strength_matters(self):
        # More elastic → more volume movement per price change
        v_low = expected_velocity_bidirectional(2.0, 0.0, 0.20, -0.5)
        v_high = expected_velocity_bidirectional(2.0, 0.0, 0.20, -2.5)
        assert v_high > v_low

    def test_fallback_when_no_elasticity(self):
        # Without elasticity, fallback tables still produce sensible lifts
        v = expected_velocity_bidirectional(2.0, 0.0, 0.20, None)
        assert v > 2.0
        # Shallowing with no elasticity still drops velocity
        v2 = expected_velocity_bidirectional(2.0, 0.30, 0.15, None)
        assert v2 < 2.0

    def test_floor_never_negative(self):
        # Extreme case: raising price a lot with very elastic demand
        v = expected_velocity_bidirectional(0.5, 0.40, 0.0, -3.0)
        assert v >= 0.3, f"Velocity floor breached: {v}"


class TestChannelClassification:
    def test_ecomm_prefix_is_ecomm(self):
        assert cpb._channel_of("AB10") == "ecomm"
        assert cpb._channel_of("AB75") == "ecomm"

    def test_non_ecomm_is_bm(self):
        assert cpb._channel_of("B002") == "bm"
        assert cpb._channel_of("7501") == "bm"
        assert cpb._channel_of("D002") == "bm"


class TestParsePctString:
    def test_parses_percent_string(self):
        assert cpb._parse_pct_string("30%") == 0.30
        assert cpb._parse_pct_string("0%") == 0.0
        assert cpb._parse_pct_string("15%") == 0.15

    def test_full_price_is_zero(self):
        assert cpb._parse_pct_string("Full price") == 0.0

    def test_empty_and_nan(self):
        assert cpb._parse_pct_string("") == 0.0
        assert cpb._parse_pct_string(np.nan) == 0.0

    def test_numeric_passthrough(self):
        assert cpb._parse_pct_string(0.15) == 0.15

    def test_format_discount_string(self):
        assert cpb._format_discount_string(0.30) == "30%"
        assert cpb._format_discount_string(0.0) == "Full price"


class TestChannelAggregationIntegration:
    """End-to-end on synthetic brand data: ensure the step produces
    the expected aggregated outputs (row count, variance flag, stats JSON)."""

    def _make_brand_dir(self, tmp_path: Path, brand: str):
        """Build a minimal fake brand directory tree expected by the step."""
        brand_lower = brand.lower()
        raw = tmp_path / "data" / "raw" / brand_lower
        processed = tmp_path / "data" / "processed" / brand_lower
        actions = tmp_path / "weekly_actions" / brand_lower
        raw.mkdir(parents=True)
        processed.mkdir(parents=True)
        actions.mkdir(parents=True)
        return raw, processed, actions

    def test_aggregates_parent_to_channel_and_writes_outputs(self, tmp_path, monkeypatch):
        brand = "BOLD"
        raw, processed, actions = self._make_brand_dir(tmp_path, brand)

        # Point the module at the tmp root
        monkeypatch.setattr(cpb, "PROJECT_ROOT", tmp_path)

        week = pd.Timestamp("2026-04-20")

        # 3 B&M stores (2002, 2003, 2004) + 1 ecomm (AB02), 2 parents
        feat_rows = []
        parents = ["NI1111111111", "NI2222222222"]
        stores_bm = ["2002", "2003", "2004"]
        stores_ecomm = ["AB02"]
        for parent in parents:
            for store in stores_bm + stores_ecomm:
                feat_rows.append({
                    "codigo_padre": parent,
                    "centro": store,
                    "week": week,
                    "velocity_4w": 3.0 if store != "AB02" else 2.0,
                    "avg_precio_lista": 100000.0,
                    "avg_precio_final": 100000.0,  # full price
                    "discount_rate": 0.0,
                    "stock_on_hand": 20.0,
                    "weeks_of_cover": 6.0,
                })
        pd.DataFrame(feat_rows).to_parquet(processed / "features_parent.parquet")

        # Per-store CSV: recommends 15% at 2002 and 2003 only. 2004 holds.
        store_actions = pd.DataFrame([
            {
                "parent_sku": "NI1111111111", "store": "2002", "store_name": "Bold A",
                "product": "Shoe A", "category": "Footwear", "subcategory": "Running",
                "current_list_price": 100000, "current_price": 99990, "current_discount": "0%",
                "current_velocity": 3.0, "recommended_price": 84990, "recommended_discount": "15%",
                "expected_velocity": 4.5, "current_weekly_rev": 299970, "expected_weekly_rev": 382455,
                "rev_delta": 82485, "unit_cost": 30000, "margin_pct": 42.0, "margin_delta": 20000,
                "urgency": "MEDIUM", "reasons": "Velocity declining; Size curve breaking",
                "model_confidence": 0.9, "confidence_tier": "HIGH", "action_type": "decrease",
                "vendor_brand": "Nike",
            },
            {
                "parent_sku": "NI1111111111", "store": "2003", "store_name": "Bold B",
                "product": "Shoe A", "category": "Footwear", "subcategory": "Running",
                "current_list_price": 100000, "current_price": 99990, "current_discount": "0%",
                "current_velocity": 3.0, "recommended_price": 84990, "recommended_discount": "15%",
                "expected_velocity": 4.5, "current_weekly_rev": 299970, "expected_weekly_rev": 382455,
                "rev_delta": 82485, "unit_cost": 30000, "margin_pct": 42.0, "margin_delta": 20000,
                "urgency": "MEDIUM", "reasons": "Velocity declining",
                "model_confidence": 0.9, "confidence_tier": "HIGH", "action_type": "decrease",
                "vendor_brand": "Nike",
            },
        ])
        store_actions.to_csv(actions / f"pricing_actions_{week.date()}.csv", index=False)

        # Costs: low enough that volume gain from a 15% markdown clearly
        # beats holding at full price under the strong elasticity below
        pd.DataFrame([
            {"sku": "NI1111111111", "cost": 18000},
            {"sku": "NI2222222222", "cost": 18000},
        ]).to_parquet(raw / "costs.parquet")

        # Products catalog (minimal)
        pd.DataFrame([
            {"codigo_padre": p, "material": p + "_01",
             "material_descripcion": "Shoe",
             "primera_jerarquia": "Footwear", "segunda_jerarquia": "Running",
             "grupo_articulos_descripcion": "", "genero": "M", "grupo_etario": "A"}
            for p in parents
        ]).to_parquet(raw / "products.parquet")

        # Elasticity: strongly elastic, so the channel sim has clear room
        # to recommend the 15% markdown that the per-store CSV recommended.
        pd.DataFrame([
            {"codigo_padre": p, "elasticity": -3.0, "confidence": "high", "r_squared": 0.8}
            for p in parents
        ]).to_parquet(processed / "elasticity_by_sku.parquet")

        # Run
        result = cpb.generate_channel_actions_for_brand(brand, target_week=week)
        assert result is not None, "Expected channel actions to be produced"

        # Both parents should get a B&M row (3 stores all at full price, 2 recommend 15%)
        bm_rows = result[result["channel"] == "bm"]
        assert len(bm_rows) >= 1, "Expected at least one B&M channel row"

        # Output files exist
        out_csv = tmp_path / "weekly_actions_channel" / brand.lower() / f"pricing_actions_channel_{week.date()}.csv"
        assert out_csv.exists()

        stats_json = tmp_path / "weekly_actions_channel" / brand.lower() / f"channel_aggregation_stats_{week.date()}.json"
        assert stats_json.exists()
        with open(stats_json) as f:
            stats = json.load(f)
        assert "summary" in stats
        assert "per_parent" in stats
        assert stats["summary"]["n_channel_actions_written"] == len(result)

        # New variance contract: only counts disagreement BETWEEN actioned stores,
        # not actioned-vs-non-actioned. Parent NI1111... has 2 actioned B&M stores
        # both recommending 15% → modal=15%, no disagreement → variance=0.0.
        parent1_bm = result[(result["parent_sku"] == "NI1111111111") & (result["channel"] == "bm")]
        if len(parent1_bm) > 0:
            row = parent1_bm.iloc[0]
            assert 0.0 <= row["per_store_variance_pct"] <= 1.0
            assert row["per_store_variance_pct"] == 0.0, \
                "Both actioned stores agree on 15% → variance should be 0"
            # n_stores reflects ALL stores in the channel (incl. the holding one)
            assert row["n_stores"] == 3

        # NI2222... has NO per-store actions in the fixture → classifier gate
        # filters it out entirely, regardless of what the channel sim would
        # have recommended.
        parent2 = result[result["parent_sku"] == "NI2222222222"]
        assert len(parent2) == 0, "Classifier gate must skip parents with no per-store actions"

    def test_skips_brand_not_in_channel_grain(self, tmp_path, monkeypatch):
        # HOKA is not in CHANNEL_GRAIN_BRANDS → step no-ops
        monkeypatch.setattr(cpb, "PROJECT_ROOT", tmp_path)
        result = cpb.generate_channel_actions_for_brand("HOKA")
        assert result is None
