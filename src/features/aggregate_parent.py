"""
Aggregate child-SKU features to parent-SKU-store-week level.

The model should learn at the level where decisions are made: parent SKU per store per week.
A parent SKU = all sizes of the same model/colorway (e.g., "Bondi 9 Black" across sizes 6-13).

Aggregation rules:
- Velocity/units: SUM (parent sells the total of all its sizes)
- Prices: MEDIAN (all sizes share the same list price)
- Discount: MEDIAN across sizes
- Product age: MAX (determined by the parent's first receipt)
- Size metrics: computed fresh (active sizes / total in catalog)
- Targets: MAX (if ANY size gets discounted, parent is considered marked down)
- Categorical: inherit from first child (all share the same category)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent


def aggregate_to_parent(brand: str):
    """Aggregate child-level features to parent-store-week."""
    brand_lower = brand.lower()
    processed = PROJECT_ROOT / "data" / "processed" / brand_lower
    raw = PROJECT_ROOT / "data" / "raw" / brand_lower

    print(f"[{brand}] Loading child-level features...")
    try:
        child = pd.read_parquet(processed / "features_v2.parquet")
    except FileNotFoundError:
        child = pd.read_parquet(processed / "features.parquet")

    products = pd.read_parquet(raw / "products.parquet")
    print(f"  {len(child):,} child rows, {child['sku'].nunique():,} SKUs")

    # Ensure codigo_padre exists
    if "codigo_padre" not in child.columns:
        sku_to_parent = products.set_index("material")["codigo_padre"].to_dict()
        child["codigo_padre"] = child["sku"].map(sku_to_parent)

    # Total sizes per parent in catalog
    total_sizes = products.groupby("codigo_padre")["material"].nunique().rename("total_sizes_catalog")

    # Parent product name (clean, without size suffix)
    parent_names = (
        products.groupby("codigo_padre")
        .first()[["material_descripcion", "primera_jerarquia", "segunda_jerarquia",
                   "grupo_articulos_descripcion", "genero", "grupo_etario"]]
        .reset_index()
        .rename(columns={"material_descripcion": "product_name"})
    )
    parent_names["product_name"] = (
        parent_names["product_name"]
        .str.replace(r'\s*N[°º]?\s*[\d,\.]+\s*$', '', regex=True)
        .str.replace(r'\s*T/[A-Z]+\s*$', '', regex=True)
        .str.strip()
    )

    print(f"[{brand}] Aggregating to parent-store-week...")

    # Numeric aggregation
    parent = (
        child.groupby(["codigo_padre", "centro", "week"])
        .agg(
            # Velocity & volume: SUM across sizes
            units_sold=("units_sold", "sum"),
            net_units=("net_units", "sum"),
            gross_revenue=("gross_revenue", "sum"),
            velocity_1w=("velocity_1w", "sum"),
            velocity_2w=("velocity_2w", "sum"),
            velocity_4w=("velocity_4w", "sum"),
            velocity_8w=("velocity_8w", "sum"),
            cumulative_units=("cumulative_units", "sum"),
            units_returned=("units_returned", "sum"),

            # Prices: MEDIAN (same across sizes)
            avg_precio_lista=("avg_precio_lista", "median"),
            avg_precio_final=("avg_precio_final", "median"),
            min_precio_lista=("min_precio_lista", "min"),
            max_precio_lista=("max_precio_lista", "max"),
            total_list_value=("total_list_value", "sum"),
            total_discount=("total_discount", "sum"),
            txn_count=("txn_count", "sum"),

            # Discount: MEDIAN
            discount_rate=("discount_rate", "median"),
            has_discount=("has_discount", "max"),  # 1 if any size discounted
            max_discount_rate=("max_discount_rate", "max"),
            cumulative_disc_weeks=("cumulative_disc_weeks", "max"),
            weeks_since_discount=("weeks_since_discount", "min"),  # most recent discount
            disc_exposure_rate=("disc_exposure_rate", "mean"),

            # Product age: MAX
            product_age_weeks=("product_age_weeks", "max"),
            first_sale_date=("first_sale_date", "min"),

            # Price features
            price_changed=("price_changed", "max"),
            price_change_pct=("price_change_pct", "mean"),

            # Size metrics: compute from child data
            sizes_active=("units_sold", lambda x: (x > 0).sum()),
            sizes_in_data=("sku", "nunique"),

            # Foot traffic (store-level, same for all sizes)
            weekly_entries=("weekly_entries", "first"),
            avg_dwell_time=("avg_dwell_time", "first"),
            conversion_rate=("conversion_rate", "first"),

            # Enhanced features
            price_elasticity=("price_elasticity", "first"),
            lifecycle_stage_code=("lifecycle_stage_code", "first"),
            lifecycle_position=("lifecycle_position", "first"),
            attrition_rate=("attrition_rate", "first"),
            core_completeness=("core_completeness", "first"),
            fragmentation_index=("fragmentation_index", "first"),
            is_fall_winter=("is_fall_winter", "first"),

            # Seasonality (same for all sizes in same week)
            month=("month", "first"),
            week_of_year=("week_of_year", "first"),
            quarter=("quarter", "first"),
            month_sin=("month_sin", "first"),
            month_cos=("month_cos", "first"),
            week_sin=("week_sin", "first"),
            week_cos=("week_cos", "first"),

            # Targets: MAX (parent marked down if any size is)
            will_discount_4w=("will_discount_4w", "max"),
            future_max_disc_4w=("future_max_disc_4w", "max"),
            future_velocity_2w=("future_velocity_2w", "sum"),
        )
        .reset_index()
    )

    # Stock features (conditional — only if stock data was available)
    stock_cols = ["stock_on_hand", "stock_in_transit", "stock_total",
                  "weeks_of_cover", "stock_out_days", "stock_to_sales_ratio"]
    has_stock = all(c in child.columns for c in stock_cols[:3])
    if has_stock:
        print(f"[{brand}] Aggregating stock features to parent level...")
        stock_parent = (
            child.groupby(["codigo_padre", "centro", "week"])
            .agg(
                stock_on_hand=("stock_on_hand", "sum"),
                stock_in_transit=("stock_in_transit", "sum"),
                stock_total=("stock_total", "sum"),
                stock_out_days=("stock_out_days", "max"),  # worst case across sizes
                _sizes_with_stock=("stock_on_hand", lambda x: (x > 0).sum()),
                _sizes_total_stock=("stock_on_hand", "count"),
            )
            .reset_index()
        )
        stock_parent["pct_sizes_in_stock"] = (
            stock_parent["_sizes_with_stock"] / stock_parent["_sizes_total_stock"].clip(lower=1)
        ).clip(upper=1.0)
        stock_parent.drop(columns=["_sizes_with_stock", "_sizes_total_stock"], inplace=True)
        parent = parent.merge(stock_parent, on=["codigo_padre", "centro", "week"], how="left")

        # Recompute weeks_of_cover at parent level
        parent["weeks_of_cover"] = np.where(
            parent["velocity_4w"] > 0,
            parent["stock_on_hand"] / parent["velocity_4w"],
            np.where(parent["stock_on_hand"] > 0, 52.0, 0.0),
        )
        parent["weeks_of_cover"] = parent["weeks_of_cover"].clip(upper=52.0)

        # Stock-to-sales at parent level
        parent["stock_to_sales_ratio"] = np.where(
            parent["units_sold"] > 0,
            parent["stock_on_hand"] / parent["units_sold"],
            np.where(parent["stock_on_hand"] > 0, 99.0, 0.0),
        )
        parent["stock_to_sales_ratio"] = parent["stock_to_sales_ratio"].clip(upper=99.0)

        stock_matched = parent["stock_on_hand"].notna().sum()
        print(f"  Stock coverage: {stock_matched:,} / {len(parent):,} rows ({stock_matched/len(parent):.1%})")

    # Recompute velocity trend at parent level
    parent["velocity_trend"] = np.where(
        parent["velocity_4w"] > 0,
        parent["velocity_2w"] / parent["velocity_4w"],
        1.0,
    )

    # Recompute conversion at parent level
    parent["conversion_rate"] = np.where(
        parent["weekly_entries"] > 0,
        parent["units_sold"] / parent["weekly_entries"],
        np.nan,
    )

    # Velocity lift
    parent["velocity_lift"] = np.where(
        parent["velocity_2w"] > 0,
        parent["future_velocity_2w"] / parent["velocity_2w"],
        np.nan,
    )

    # Size curve completeness at parent level
    parent = parent.merge(total_sizes, on="codigo_padre", how="left")
    parent["total_sizes_catalog"] = parent["total_sizes_catalog"].fillna(parent["sizes_in_data"])
    parent["size_curve_completeness"] = (
        parent["sizes_active"] / parent["total_sizes_catalog"]
    ).clip(upper=1.0).fillna(0)

    # Merge categorical product attributes
    parent = parent.merge(
        parent_names[["codigo_padre", "primera_jerarquia", "segunda_jerarquia",
                       "genero", "grupo_etario"]],
        on="codigo_padre", how="left",
    )

    # Drop helper columns
    parent.drop(columns=["sizes_in_data", "total_sizes_catalog", "sizes_active"], inplace=True, errors="ignore")

    # Save
    parent.to_parquet(processed / "features_parent.parquet", index=False)

    print(f"\n--- [{brand}] Parent Feature Table ---")
    print(f"  Rows:     {len(parent):,}")
    print(f"  Parents:  {parent['codigo_padre'].nunique():,}")
    print(f"  Stores:   {parent['centro'].nunique()}")
    print(f"  Weeks:    {parent['week'].nunique()}")
    print(f"  Columns:  {len(parent.columns)}")
    print(f"  Pos rate: {parent['will_discount_4w'].mean():.3f}")

    return parent


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("brand", type=str)
    args = parser.parse_args()
    aggregate_to_parent(args.brand.upper())
