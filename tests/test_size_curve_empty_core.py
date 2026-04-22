"""Regression test for size_curve_brand.build_size_availability falling over
when the brand has footwear but no CORE_SIZES observed (e.g., a brand that
sells only unusual sizes, or was observed in production when stock_bamers was
empty and the pipeline fell back to the sales-proxy path).

The bug: the empty-core fallback Series lacked the MultiIndex that pd.concat
needed to align with `active_counts`. Result: reset_index produced level_0 /
level_1 columns instead of codigo_padre / centro / week, and the next merge
failed with KeyError: 'codigo_padre'.
"""

import pandas as pd

from src.features.size_curve_brand import build_size_availability


def test_footwear_with_no_core_sizes_does_not_raise():
    # Sizes that are NOT in CORE_SIZES (US footwear: 7.5, 8, 8.5, ..., 11 per the module).
    # Using "XXL" as an obviously-non-core clothing-style size.
    txn = pd.DataFrame({
        "sku": ["SKU_ODD", "SKU_ODD", "SKU_ODD"],
        "centro": ["7501", "7501", "7501"],
        "fecha": pd.to_datetime(["2026-04-01", "2026-04-08", "2026-04-15"]),
        "cantidad": [1, 2, 3],
    })
    products = pd.DataFrame({
        "material": ["SKU_ODD"],
        "codigo_padre": ["PARENT"],
        "talla": ["XXL"],  # non-core
        "primera_jerarquia": ["Footwear"],
    })
    # Should return a DataFrame with codigo_padre/centro/week columns — not raise
    out = build_size_availability(txn, products)
    assert isinstance(out, pd.DataFrame)
    if len(out) > 0:
        assert "codigo_padre" in out.columns
        assert "centro" in out.columns
        assert "week" in out.columns
