"""
Feature engineering for HOKA markdown optimization.

Builds a weekly feature table at the SKU-store-week grain with:
- Sell-through velocity (7d, 14d, 28d rolling)
- Product age (weeks since first sale)
- Discount history (discount rate, weeks since last discount, cumulative discount depth)
- Price lifecycle (current price, price changes detected, markdown events)
- Return rate
- Size curve completeness (per parent SKU)
- Weekly/monthly seasonality
- Foot traffic conversion (where available)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"


def load_raw_data():
    """Load all raw parquet files."""
    txn = pd.read_parquet(RAW_DIR / "hoka_transactions.parquet")
    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")
    stores = pd.read_parquet(RAW_DIR / "hoka_stores.parquet")
    traffic = pd.read_parquet(RAW_DIR / "hoka_foot_traffic.parquet")
    calendar = pd.read_parquet(RAW_DIR / "calendar.parquet")
    return txn, products, stores, traffic, calendar


def build_weekly_sales(txn: pd.DataFrame) -> pd.DataFrame:
    """Aggregate transactions to SKU-store-week level."""
    txn = txn.copy()
    txn["week"] = txn["fecha"].dt.to_period("W").dt.start_time

    # Separate sales and returns
    sales = txn[txn["cantidad"] > 0].copy()
    returns = txn[txn["cantidad"] < 0].copy()

    # Weekly sales aggregation
    weekly = (
        sales.groupby(["sku", "centro", "week"])
        .agg(
            units_sold=("cantidad", "sum"),
            gross_revenue=("precio_final", "sum"),
            total_discount=("descuento", "sum"),
            total_list_value=("precio_lista", "sum"),
            txn_count=("folio", "nunique"),
            avg_precio_lista=("precio_lista", "mean"),
            avg_precio_final=("precio_final", "mean"),
            min_precio_lista=("precio_lista", "min"),
            max_precio_lista=("precio_lista", "max"),
        )
        .reset_index()
    )

    # Weekly returns
    weekly_returns = (
        returns.groupby(["sku", "centro", "week"])
        .agg(units_returned=("cantidad", lambda x: abs(x.sum())))
        .reset_index()
    )

    weekly = weekly.merge(weekly_returns, on=["sku", "centro", "week"], how="left")
    weekly["units_returned"] = weekly["units_returned"].fillna(0).astype(int)
    weekly["net_units"] = weekly["units_sold"] - weekly["units_returned"]

    # Discount rate for the week
    weekly["discount_rate"] = np.where(
        weekly["total_list_value"] > 0,
        weekly["total_discount"] / weekly["total_list_value"],
        0,
    )

    return weekly


def add_velocity_features(weekly: pd.DataFrame) -> pd.DataFrame:
    """Add rolling sell-through velocity features."""
    weekly = weekly.sort_values(["sku", "centro", "week"]).copy()

    # We need to fill gaps (weeks with no sales) to compute rolling correctly
    # First, create a complete week spine per SKU-store
    all_weeks = pd.date_range(weekly["week"].min(), weekly["week"].max(), freq="W-MON")
    sku_store_pairs = weekly[["sku", "centro"]].drop_duplicates()

    spine = sku_store_pairs.merge(
        pd.DataFrame({"week": all_weeks}), how="cross"
    )

    weekly = spine.merge(weekly, on=["sku", "centro", "week"], how="left")
    weekly["units_sold"] = weekly["units_sold"].fillna(0)
    weekly["net_units"] = weekly["net_units"].fillna(0)
    weekly["gross_revenue"] = weekly["gross_revenue"].fillna(0)

    # Rolling velocities (in units per week)
    for window, suffix in [(1, "1w"), (2, "2w"), (4, "4w"), (8, "8w")]:
        weekly[f"velocity_{suffix}"] = (
            weekly.groupby(["sku", "centro"])["units_sold"]
            .transform(lambda x: x.rolling(window, min_periods=1).mean())
        )

    # Velocity trend: 2w vs 4w (acceleration/deceleration)
    weekly["velocity_trend"] = np.where(
        weekly["velocity_4w"] > 0,
        weekly["velocity_2w"] / weekly["velocity_4w"],
        1.0,
    )

    # Cumulative units sold
    weekly["cumulative_units"] = (
        weekly.groupby(["sku", "centro"])["units_sold"].cumsum()
    )

    return weekly


def add_product_age(weekly: pd.DataFrame, txn: pd.DataFrame) -> pd.DataFrame:
    """Add product age (weeks since first sale) per SKU."""
    first_sale = (
        txn[txn["cantidad"] > 0]
        .groupby("sku")["fecha"]
        .min()
        .rename("first_sale_date")
    )
    weekly = weekly.merge(first_sale, on="sku", how="left")
    weekly["product_age_weeks"] = (
        (weekly["week"] - weekly["first_sale_date"]).dt.days / 7
    ).clip(lower=0)
    return weekly


def add_price_features(weekly: pd.DataFrame) -> pd.DataFrame:
    """Add price lifecycle features."""
    weekly = weekly.sort_values(["sku", "centro", "week"]).copy()

    # Detect list price changes (potential markdown events)
    weekly["prev_avg_price"] = weekly.groupby(["sku", "centro"])["avg_precio_lista"].shift(1)

    weekly["price_changed"] = (
        (weekly["avg_precio_lista"].notna())
        & (weekly["prev_avg_price"].notna())
        & (weekly["avg_precio_lista"] != weekly["prev_avg_price"])
    ).astype(int)

    weekly["price_change_pct"] = np.where(
        weekly["prev_avg_price"] > 0,
        (weekly["avg_precio_lista"] - weekly["prev_avg_price"]) / weekly["prev_avg_price"],
        0,
    )

    # Was there a discount this week?
    weekly["has_discount"] = (weekly["discount_rate"] > 0.01).astype(int)

    # Weeks since last discount
    weekly["_disc_group"] = weekly.groupby(["sku", "centro"])["has_discount"].cumsum()
    weekly["weeks_since_discount"] = weekly.groupby(["sku", "centro", "_disc_group"]).cumcount()
    weekly.loc[weekly["has_discount"] == 1, "weeks_since_discount"] = 0
    weekly.drop(columns=["_disc_group"], inplace=True)

    # Cumulative discount exposure (what fraction of selling weeks had discounts)
    weekly["cumulative_disc_weeks"] = weekly.groupby(["sku", "centro"])["has_discount"].cumsum()
    weekly["_week_count"] = weekly.groupby(["sku", "centro"]).cumcount() + 1
    weekly["disc_exposure_rate"] = weekly["cumulative_disc_weeks"] / weekly["_week_count"]
    weekly.drop(columns=["_week_count"], inplace=True)

    # Max discount depth seen so far
    weekly["max_discount_rate"] = (
        weekly.groupby(["sku", "centro"])["discount_rate"]
        .transform(lambda x: x.expanding().max())
    )

    weekly.drop(columns=["prev_avg_price"], inplace=True)
    return weekly


def add_size_curve_features(weekly: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """Add size curve completeness per parent SKU-store-week."""
    # Map SKU to parent
    sku_parent = products[["material", "codigo_padre", "talla"]].rename(
        columns={"material": "sku"}
    )
    # Count total sizes per parent
    total_sizes = sku_parent.groupby("codigo_padre")["talla"].nunique().rename("total_sizes")

    weekly = weekly.merge(sku_parent[["sku", "codigo_padre"]], on="sku", how="left")

    # For each parent-store-week, count how many distinct sizes had sales
    parent_weekly = (
        weekly[weekly["units_sold"] > 0]
        .merge(sku_parent[["sku", "talla"]], on="sku", how="left")
        .groupby(["codigo_padre", "centro", "week"])["talla"]
        .nunique()
        .rename("sizes_selling")
        .reset_index()
    )
    parent_weekly = parent_weekly.merge(total_sizes, on="codigo_padre", how="left")
    parent_weekly["size_curve_completeness"] = (
        parent_weekly["sizes_selling"] / parent_weekly["total_sizes"]
    ).clip(upper=1.0)

    weekly = weekly.merge(
        parent_weekly[["codigo_padre", "centro", "week", "size_curve_completeness"]],
        on=["codigo_padre", "centro", "week"],
        how="left",
    )
    weekly["size_curve_completeness"] = weekly["size_curve_completeness"].fillna(0)

    return weekly


def add_seasonality_features(weekly: pd.DataFrame) -> pd.DataFrame:
    """Add temporal/seasonality features."""
    weekly["month"] = weekly["week"].dt.month
    weekly["week_of_year"] = weekly["week"].dt.isocalendar().week.astype(int)
    weekly["quarter"] = weekly["week"].dt.quarter

    # Month-level cyclical encoding
    weekly["month_sin"] = np.sin(2 * np.pi * weekly["month"] / 12)
    weekly["month_cos"] = np.cos(2 * np.pi * weekly["month"] / 12)
    weekly["week_sin"] = np.sin(2 * np.pi * weekly["week_of_year"] / 52)
    weekly["week_cos"] = np.cos(2 * np.pi * weekly["week_of_year"] / 52)

    return weekly


def add_foot_traffic_features(weekly: pd.DataFrame, traffic: pd.DataFrame) -> pd.DataFrame:
    """Add store-level foot traffic and conversion features."""
    traffic = traffic.copy()
    traffic["week"] = traffic["fecha"].dt.to_period("W").dt.start_time

    # Map tienda_id to centro (store code)
    # Costanera = 7501, Marina = 7502
    traffic_weekly = (
        traffic.groupby(["tienda_id", "week"])
        .agg(
            weekly_entries=("entradas", "sum"),
            weekly_exits=("salidas", "sum"),
            avg_dwell_time=("tiempo_permanencia_prom", "mean"),
        )
        .reset_index()
    )

    # Map tienda_id to centro format
    id_to_centro = {
        "7501": "7501",
        "7502": "7502",
    }
    # Extract centro from tienda_id
    traffic_weekly["centro"] = traffic_weekly["tienda_id"].map(id_to_centro)
    traffic_weekly = traffic_weekly.dropna(subset=["centro"])

    weekly = weekly.merge(
        traffic_weekly[["centro", "week", "weekly_entries", "avg_dwell_time"]],
        on=["centro", "week"],
        how="left",
    )

    # Conversion rate = units sold / foot traffic
    weekly["conversion_rate"] = np.where(
        weekly["weekly_entries"] > 0,
        weekly["units_sold"] / weekly["weekly_entries"],
        np.nan,
    )

    return weekly


def add_product_attributes(weekly: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """Join static product attributes."""
    product_attrs = products[
        ["material", "primera_jerarquia", "segunda_jerarquia", "tercera_jerarquia",
         "genero", "grupo_etario", "color1"]
    ].rename(columns={"material": "sku"})

    # Deduplicate (shouldn't have dupes but just in case)
    product_attrs = product_attrs.drop_duplicates(subset=["sku"])

    weekly = weekly.merge(product_attrs, on="sku", how="left")
    return weekly


def build_target_variable(weekly: pd.DataFrame) -> pd.DataFrame:
    """
    Build proxy target variable for markdown optimization.

    Without inventory data, we use a revenue-based proxy:
    - For each SKU-store, identify markdown events (discount_rate > threshold)
    - Target = was this SKU marked down within the next N weeks?
    - Secondary target = discount depth applied

    This will be replaced with margin-based targets once cost data is available.
    """
    weekly = weekly.sort_values(["sku", "centro", "week"]).copy()

    # Target 1: Will this SKU be discounted in the next 4 weeks?
    weekly["will_discount_4w"] = (
        weekly.groupby(["sku", "centro"])["has_discount"]
        .transform(lambda x: x.shift(-1).rolling(4, min_periods=1).max())
    ).fillna(0).astype(int)

    # Target 2: Max discount depth in next 4 weeks
    weekly["future_max_disc_4w"] = (
        weekly.groupby(["sku", "centro"])["discount_rate"]
        .transform(lambda x: x.shift(-1).rolling(4, min_periods=1).max())
    ).fillna(0)

    # Target 3: Velocity change after markdown (did the markdown accelerate sales?)
    # Useful for evaluating markdown effectiveness
    weekly["future_velocity_2w"] = (
        weekly.groupby(["sku", "centro"])["velocity_2w"]
        .shift(-2)
    )
    weekly["velocity_lift"] = np.where(
        weekly["velocity_2w"] > 0,
        weekly["future_velocity_2w"] / weekly["velocity_2w"],
        np.nan,
    )

    return weekly


def filter_active_rows(weekly: pd.DataFrame) -> pd.DataFrame:
    """Remove rows before first sale and after last sale per SKU-store."""
    weekly = weekly.sort_values(["sku", "centro", "week"]).copy()

    # Find first and last week with actual sales per SKU-store
    active_ranges = (
        weekly[weekly["units_sold"] > 0]
        .groupby(["sku", "centro"])
        .agg(first_active_week=("week", "min"), last_active_week=("week", "max"))
        .reset_index()
    )

    weekly = weekly.merge(active_ranges, on=["sku", "centro"], how="inner")
    weekly = weekly[
        (weekly["week"] >= weekly["first_active_week"])
        & (weekly["week"] <= weekly["last_active_week"])
    ]
    weekly.drop(columns=["first_active_week", "last_active_week"], inplace=True)

    return weekly


def build_all_features():
    """Main feature engineering pipeline."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading raw data...")
    txn, products, stores, traffic, calendar = load_raw_data()

    print("Building weekly sales aggregation...")
    weekly = build_weekly_sales(txn)
    print(f"  {len(weekly):,} SKU-store-week rows")

    print("Adding velocity features...")
    weekly = add_velocity_features(weekly)

    print("Adding product age...")
    weekly = add_product_age(weekly, txn)

    print("Adding price features...")
    weekly = add_price_features(weekly)

    print("Adding size curve features...")
    weekly = add_size_curve_features(weekly, products)

    print("Adding seasonality features...")
    weekly = add_seasonality_features(weekly)

    print("Adding foot traffic features...")
    weekly = add_foot_traffic_features(weekly, traffic)

    print("Adding product attributes...")
    weekly = add_product_attributes(weekly, products)

    print("Building target variables...")
    weekly = build_target_variable(weekly)

    print("Filtering to active periods...")
    weekly = filter_active_rows(weekly)

    # Drop temp columns
    drop_cols = [c for c in weekly.columns if c.startswith("_")]
    weekly.drop(columns=drop_cols, inplace=True, errors="ignore")

    # Save
    weekly.to_parquet(PROCESSED_DIR / "hoka_features.parquet", index=False)

    print(f"\n--- Feature Table Summary ---")
    print(f"  Rows:          {len(weekly):>10,}")
    print(f"  Columns:       {len(weekly.columns):>10}")
    print(f"  SKUs:          {weekly['sku'].nunique():>10,}")
    print(f"  Stores:        {weekly['centro'].nunique():>10}")
    print(f"  Week range:    {weekly['week'].min().date()} to {weekly['week'].max().date()}")
    print(f"  Weeks w/ disc: {weekly['has_discount'].sum():>10,} ({weekly['has_discount'].mean():.1%})")

    print(f"\nFeature columns:")
    for col in sorted(weekly.columns):
        print(f"  {col:40s}  {weekly[col].dtype}  nulls={weekly[col].isna().sum():,}")

    return weekly


if __name__ == "__main__":
    build_all_features()
