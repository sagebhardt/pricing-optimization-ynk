"""Tests for the precio_normal-aware elasticity path.

The auxiliar.precio_normal table lets us label markdown vs normal-price weeks
so the elasticity regression can exclude markdown-event noise.
"""

import numpy as np
import pandas as pd
import pytest

from src.features.price_elasticity_brand import estimate_elasticity_sku


def _synthetic_panel(n_normal=20, n_markdown=10, true_elasticity=-1.5,
                     markdown_event_boost=2.0, list_price=100_000):
    """Build a fake (sku, store, week) panel with known elasticity.

    Normal weeks: small natural price variation around list_price → demand
    follows ln_units = -1.5 * ln_price.
    Markdown weeks: 30% off + a 2x DEMAND BOOST that's NOT explained by price.
    A naive regression on all data overestimates |elasticity| because the
    markdown event correlates with price drops; isolating normal-price weeks
    recovers the true coefficient.
    """
    rng = np.random.default_rng(42)
    rows = []
    for w in range(n_normal):
        # "Normal" weeks still have meaningful cross-store / time variation —
        # 12% spread keeps CV ~7%, comfortably above the 5% gate even when
        # markdown weeks (which contribute most of the variation) are filtered out.
        price = list_price * rng.uniform(0.88, 1.12)
        units = price ** true_elasticity * (list_price ** (-true_elasticity)) * 100
        units = max(1, int(units * rng.uniform(0.9, 1.1)))
        rows.append({
            "codigo_padre": "P1", "sku": "P1_01", "centro": "S1",
            "week": pd.Timestamp("2026-01-01") + pd.Timedelta(weeks=w),
            "avg_price": price, "avg_list_price": list_price,
            "units": units, "ln_price": np.log(price), "ln_units": np.log(units),
            "month": 1, "week_num": w,
            "primera_jerarquia": "Footwear", "segunda_jerarquia": "Running",
            "is_normal_price": True,
        })
    for w in range(n_markdown):
        # Markdown: 30% off list AND extra event-driven demand
        price = list_price * 0.70
        # True price response would give: units * 0.70^(-1.5) ≈ units * 1.71
        # But event-driven boost adds another 2x on top → looks like elasticity ~-3.5
        baseline = price ** true_elasticity * (list_price ** (-true_elasticity)) * 100
        units = max(1, int(baseline * markdown_event_boost * rng.uniform(0.9, 1.1)))
        rows.append({
            "codigo_padre": "P1", "sku": "P1_01", "centro": "S1",
            "week": pd.Timestamp("2026-06-01") + pd.Timedelta(weeks=w),
            "avg_price": price, "avg_list_price": list_price,
            "units": units, "ln_price": np.log(price), "ln_units": np.log(units),
            "month": 6, "week_num": n_normal + w,
            "primera_jerarquia": "Footwear", "segunda_jerarquia": "Running",
            "is_normal_price": False,
        })
    return pd.DataFrame(rows)


class TestExcludeMarkdownPath:
    def test_naive_overstates_elasticity(self):
        """Without the filter, the regression conflates markdown-event boost
        with the true price-elasticity coefficient → overestimates |β|."""
        panel = _synthetic_panel()
        result = estimate_elasticity_sku(panel, exclude_markdown=False)
        assert len(result) == 1
        beta_naive = result.iloc[0]["elasticity"]
        # Markdown weeks have 2x event boost on top of the price response,
        # so the naive coefficient should be more negative than the true -1.5.
        assert beta_naive < -1.5, f"Expected overestimate, got {beta_naive:.3f}"

    def test_filter_recovers_true_elasticity(self):
        """With exclude_markdown=True the regression isolates normal-price
        weeks and the coefficient should land near the true elasticity."""
        panel = _synthetic_panel()
        result = estimate_elasticity_sku(panel, exclude_markdown=True)
        assert len(result) == 1
        beta_clean = result.iloc[0]["elasticity"]
        # Clean estimate should be within ~0.4 of the true -1.5
        # (synthetic data has noise; not asking for tight precision)
        assert -2.0 < beta_clean < -1.0, \
            f"Clean elasticity should be near -1.5, got {beta_clean:.3f}"

    def test_filter_preserves_skus_with_no_markdown(self):
        """A SKU that has only normal-price weeks should be unaffected by the filter."""
        panel = _synthetic_panel(n_markdown=0)
        result_filtered = estimate_elasticity_sku(panel, exclude_markdown=True)
        result_naive = estimate_elasticity_sku(panel, exclude_markdown=False)
        assert len(result_filtered) == 1 and len(result_naive) == 1
        # Same data → identical results
        assert abs(result_filtered.iloc[0]["elasticity"] -
                   result_naive.iloc[0]["elasticity"]) < 1e-6

    def test_filter_falls_through_when_too_few_normal_weeks(self):
        """If exclusion would leave fewer than min_observations, the SKU
        falls through to the legacy path (uses all data) instead of being
        dropped entirely. Validates the safety guard."""
        # 8 normal + 30 markdown — filtering leaves 8 < min_obs=10
        panel = _synthetic_panel(n_normal=8, n_markdown=30)
        result = estimate_elasticity_sku(panel, exclude_markdown=True, min_observations=10)
        # Legacy path should still produce an estimate (possibly biased) since
        # the filter guard kicks in. Better to have an estimate than nothing.
        assert len(result) == 1, "Filter must fall through, not drop the SKU"

    def test_no_is_normal_price_column_is_safe(self):
        """When the data lacks `is_normal_price` (e.g., precio_normal.parquet
        wasn't written), exclude_markdown=True is a no-op."""
        panel = _synthetic_panel().drop(columns=["is_normal_price"])
        result = estimate_elasticity_sku(panel, exclude_markdown=True)
        assert len(result) == 1
        assert pd.notna(result.iloc[0]["elasticity"])
