"""Competitor pricing features for ML model integration.

Derives features from competitor_prices.parquet at the parent-SKU level.
These features are added to the feature table in build_enhanced_brand.py
and auto-discovered by the ML model during training.
"""

import pandas as pd
import numpy as np


def add_competitor_features(weekly: pd.DataFrame, comp: pd.DataFrame) -> pd.DataFrame:
    """
    Add competitor pricing features to the weekly feature table.

    Parameters
    ----------
    weekly : DataFrame
        Feature table with at least `codigo_padre` and `avg_precio_final`.
    comp : DataFrame
        Competitor prices with `codigo_padre`, `competitor`, `comp_price`,
        `comp_list_price`, `comp_discount`, `comp_in_stock`.

    Returns
    -------
    DataFrame with additional competitor feature columns.
    """
    if comp is None or len(comp) == 0:
        weekly["comp_count"] = 0
        return weekly

    # Aggregate competitor data per parent SKU
    valid = comp[comp["comp_price"] > 0].copy()
    if len(valid) == 0:
        weekly["comp_count"] = 0
        return weekly

    parent_comp = valid.groupby("codigo_padre").agg(
        comp_min_price=("comp_price", "min"),
        comp_avg_price=("comp_price", "mean"),
        comp_max_price=("comp_price", "max"),
        comp_count=("competitor", "nunique"),
        comp_any_discount=("comp_discount", lambda x: int((x > 0.05).any())),
        comp_max_discount=("comp_discount", "max"),
        comp_in_stock_count=("comp_in_stock", "sum"),
    ).reset_index()

    # Round prices to int
    for col in ["comp_min_price", "comp_avg_price", "comp_max_price"]:
        parent_comp[col] = parent_comp[col].round(0).astype(int)

    # Merge onto feature table
    weekly = weekly.merge(parent_comp, on="codigo_padre", how="left")

    # Fill NaN for products with no competitor data
    weekly["comp_count"] = weekly["comp_count"].fillna(0).astype(int)
    weekly["comp_any_discount"] = weekly["comp_any_discount"].fillna(0).astype(int)
    weekly["comp_max_discount"] = weekly["comp_max_discount"].fillna(0)
    weekly["comp_in_stock_count"] = weekly["comp_in_stock_count"].fillna(0).astype(int)

    # Derived features (relative to our price)
    has_our_price = weekly["avg_precio_final"].notna() & (weekly["avg_precio_final"] > 0)
    has_comp = weekly["comp_avg_price"].notna() & (weekly["comp_avg_price"] > 0)
    both = has_our_price & has_comp

    # Price index: our_price / competitor avg (>1 = we're more expensive)
    weekly["comp_price_index"] = np.where(
        both,
        weekly["avg_precio_final"] / weekly["comp_avg_price"],
        np.nan,
    )

    # Price gap: (our - comp_min) / our * 100 (positive = we're cheaper)
    weekly["comp_price_gap_pct"] = np.where(
        both & weekly["comp_min_price"].notna(),
        (weekly["avg_precio_final"] - weekly["comp_min_price"]) / weekly["avg_precio_final"] * 100,
        np.nan,
    )

    # Undercut: 1 if any competitor is cheaper than us
    weekly["comp_undercut"] = np.where(
        both & weekly["comp_min_price"].notna(),
        (weekly["comp_min_price"] < weekly["avg_precio_final"]).astype(int),
        0,
    )

    # Discount pressure: comp_max_discount - our discount (positive = competitors more aggressive)
    if "discount_rate" in weekly.columns:
        weekly["comp_discount_pressure"] = np.where(
            has_comp,
            weekly["comp_max_discount"] - weekly["discount_rate"],
            0,
        )

    return weekly
