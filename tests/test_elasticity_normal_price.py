"""Tests for the precio_normal-aware elasticity path (markdown_dummy option).

Status: markdown_dummy is DEFAULT OFF as of 2026-04-25. Both filter-out and
dummy approaches collapse in real data because precio_normal is a binary
flag and ln_price is highly collinear with is_markdown. The dummy code path
remains for future experimentation with multi-tier markdown labels (e.g.,
discount-depth indicators) that would have within-markdown price variation.

These tests still validate:
  - default (no dummy) produces the same naive estimate as before
  - dummy=True flag exists and runs without error
  - dummy is a no-op when no markdown weeks are present
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


class TestMarkdownDummyPath:
    def test_naive_overstates_elasticity(self):
        """Without the markdown dummy, the regression conflates markdown-event
        boost with the price-elasticity coefficient → overestimates |β|."""
        panel = _synthetic_panel()
        result = estimate_elasticity_sku(panel, markdown_dummy=False)
        assert len(result) == 1
        beta_naive = result.iloc[0]["elasticity"]
        # Markdown weeks have 2x event boost on top of the price response,
        # so the naive coefficient should be more negative than the true -1.5.
        assert beta_naive < -1.5, f"Expected overestimate, got {beta_naive:.3f}"

    def test_dummy_runs_without_error(self):
        """markdown_dummy=True path stays functional for future experimentation
        — even though it's known to be unstable on real data due to collinearity
        between ln_price and is_markdown when precio_normal is a binary flag."""
        panel = _synthetic_panel()
        result = estimate_elasticity_sku(panel, markdown_dummy=True)
        assert len(result) == 1
        assert pd.notna(result.iloc[0]["elasticity"])

    def test_dummy_no_op_when_no_markdown_weeks(self):
        """A SKU with only normal-price weeks has a constant is_markdown
        column → the dummy path skips it (collinear with intercept). The
        regression runs as in the naive path, producing identical results."""
        panel = _synthetic_panel(n_markdown=0)
        result_dummy = estimate_elasticity_sku(panel, markdown_dummy=True)
        result_naive = estimate_elasticity_sku(panel, markdown_dummy=False)
        assert len(result_dummy) == 1 and len(result_naive) == 1
        assert abs(result_dummy.iloc[0]["elasticity"] -
                   result_naive.iloc[0]["elasticity"]) < 1e-6

    def test_dummy_keeps_all_data(self):
        """The dummy path uses ALL observations (no filtering) — validates
        that we don't reproduce the over-aggressive starvation behavior."""
        panel = _synthetic_panel(n_normal=20, n_markdown=10)
        result = estimate_elasticity_sku(panel, markdown_dummy=True)
        assert len(result) == 1
        # n_observations recorded in the result reflects the data actually
        # used in the fit. With markdown_dummy we keep all 30 rows.
        assert result.iloc[0]["n_observations"] == 30, \
            f"Expected all 30 rows used, got {result.iloc[0]['n_observations']}"

    def test_no_is_normal_price_column_is_safe(self):
        """When the data lacks `is_normal_price` (e.g., precio_normal.parquet
        wasn't written), markdown_dummy=True is a no-op."""
        panel = _synthetic_panel().drop(columns=["is_normal_price"])
        result = estimate_elasticity_sku(panel, markdown_dummy=True)
        assert len(result) == 1
        assert pd.notna(result.iloc[0]["elasticity"])
