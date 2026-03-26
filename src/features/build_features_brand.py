"""
Brand-agnostic feature engineering for markdown optimization.

Thin wrapper around the HOKA-specific logic in build_features.py,
parameterized by brand name. Reads from data/raw/{brand}/ and
writes to data/processed/{brand}/.

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

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _raw_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "raw" / brand.lower()


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def load_raw_data(brand: str):
    """Load all raw parquet files for the given brand."""
    raw = _raw_dir(brand)
    txn = pd.read_parquet(raw / "transactions.parquet")
    products = pd.read_parquet(raw / "products.parquet")
    stores = pd.read_parquet(raw / "stores.parquet")
    traffic = pd.read_parquet(raw / "foot_traffic.parquet")
    # Calendar is shared across brands
    cal_path = raw / "calendar.parquet"
    if not cal_path.exists():
        cal_path = PROJECT_ROOT / "data" / "raw" / "calendar.parquet"
    calendar = pd.read_parquet(cal_path)
    # Stock (optional — only available for brands where planning uploaded it)
    stock_path = raw / "stock.parquet"
    stock = pd.read_parquet(stock_path) if stock_path.exists() else None
    return txn, products, stores, traffic, calendar, stock


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
    """Add rolling sell-through velocity features.

    Uses vectorized groupby().rolling() instead of .transform(lambda ...)
    to handle large datasets (Bold: 44K+ SKU-store groups).
    """
    weekly = weekly.sort_values(["sku", "centro", "week"]).reset_index(drop=True)
    print(f"    Computing rolling velocities on {len(weekly):,} rows...")

    # Vectorized rolling: groupby().rolling() is C-level, no Python lambda per group
    gb = weekly.groupby(["sku", "centro"], sort=False)["units_sold"]
    for window, suffix in [(1, "1w"), (2, "2w"), (4, "4w"), (8, "8w")]:
        rolled = gb.rolling(window, min_periods=1).mean()
        weekly[f"velocity_{suffix}"] = rolled.droplevel([0, 1]).sort_index().values
        print(f"      velocity_{suffix} done")

    weekly["velocity_trend"] = np.where(
        weekly["velocity_4w"] > 0,
        weekly["velocity_2w"] / weekly["velocity_4w"],
        1.0,
    )

    weekly["cumulative_units"] = (
        weekly.groupby(["sku", "centro"], sort=False)["units_sold"].cumsum()
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

    # Max discount depth seen so far (vectorized expanding max)
    exp_max = (
        weekly.groupby(["sku", "centro"], sort=False)["discount_rate"]
        .expanding().max()
    )
    weekly["max_discount_rate"] = exp_max.droplevel([0, 1]).sort_index().values

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


def add_stock_features(weekly: pd.DataFrame, stock: pd.DataFrame) -> pd.DataFrame:
    """Add inventory-based features from daily stock snapshots.

    Features added:
    - stock_on_hand: end-of-week stock units
    - stock_in_transit: end-of-week in-transit units
    - stock_total: on-hand + in-transit
    - weeks_of_cover: stock_on_hand / velocity_4w (how many weeks until stockout)
    - stock_out_days: number of days during the week with zero stock
    - stock_to_sales_ratio: stock_on_hand / units_sold (inventory efficiency)
    """
    stock = stock.copy()

    # Map store_id ("7501-Hoka Costanera") to centro ("7501")
    stock["centro"] = stock["store_id"].str.split("-", n=1).str[0]
    stock["week"] = stock["fecha"].dt.to_period("W").dt.start_time

    # End-of-week snapshot (last day of each week per SKU-store)
    stock_sorted = stock.sort_values(["sku", "centro", "fecha"])
    eow = stock_sorted.groupby(["sku", "centro", "week"]).last().reset_index()

    # Days with zero stock per week
    stock["is_stockout"] = (stock["stock_on_hand_units"] == 0).astype(int)
    stockout_days = (
        stock.groupby(["sku", "centro", "week"])["is_stockout"]
        .sum()
        .rename("stock_out_days")
        .reset_index()
    )

    stock_weekly = eow[["sku", "centro", "week",
                         "stock_on_hand_units", "stock_in_transit_units",
                         "total_stock_position_units"]].rename(columns={
        "stock_on_hand_units": "stock_on_hand",
        "stock_in_transit_units": "stock_in_transit",
        "total_stock_position_units": "stock_total",
    })
    stock_weekly = stock_weekly.merge(stockout_days, on=["sku", "centro", "week"], how="left")

    weekly = weekly.merge(stock_weekly, on=["sku", "centro", "week"], how="left")

    # Weeks of cover: how many weeks of stock left at current sell rate
    weekly["weeks_of_cover"] = np.where(
        weekly["velocity_4w"] > 0,
        weekly["stock_on_hand"] / weekly["velocity_4w"],
        np.where(weekly["stock_on_hand"] > 0, 52.0, 0.0),  # cap at 52 if no sales
    )
    weekly["weeks_of_cover"] = weekly["weeks_of_cover"].clip(upper=52.0)

    # Stock-to-sales ratio
    weekly["stock_to_sales_ratio"] = np.where(
        weekly["units_sold"] > 0,
        weekly["stock_on_hand"] / weekly["units_sold"],
        np.where(weekly["stock_on_hand"] > 0, 99.0, 0.0),
    )
    weekly["stock_to_sales_ratio"] = weekly["stock_to_sales_ratio"].clip(upper=99.0)

    return weekly


def add_size_sales_share(weekly: pd.DataFrame, txn: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """Add each child SKU's historical volume share within its parent.

    This enables volume-weighted size availability at parent level during aggregation.
    A child that accounts for 20% of parent sales matters 10x more than one at 2%.
    """
    sku_parent = products[["material", "codigo_padre"]].rename(
        columns={"material": "sku"}
    ).drop_duplicates("sku")

    # All-time sales per child SKU
    sales = txn[txn["cantidad"] > 0].groupby("sku")["cantidad"].sum().rename("sku_total_units")
    sales = sales.reset_index().merge(sku_parent, on="sku", how="left")

    # Parent total
    parent_total = sales.groupby("codigo_padre")["sku_total_units"].transform("sum")
    sales["size_sales_share"] = sales["sku_total_units"] / parent_total.clip(lower=1)

    # Rank within parent (1 = best seller)
    sales["size_rank"] = sales.groupby("codigo_padre")["sku_total_units"].rank(
        ascending=False, method="min"
    ).astype(int)

    weekly = weekly.merge(
        sales[["sku", "size_sales_share", "size_rank"]],
        on="sku", how="left",
    )
    weekly["size_sales_share"] = weekly["size_sales_share"].fillna(0)
    weekly["size_rank"] = weekly["size_rank"].fillna(99).astype(int)

    top3_pct = (weekly["size_rank"] <= 3).mean()
    print(f"  Top-3 sizes: {top3_pct:.1%} of child rows, avg share: {weekly.loc[weekly['size_rank'] <= 3, 'size_sales_share'].mean():.1%}")

    return weekly


def add_foot_traffic_features(weekly: pd.DataFrame, traffic: pd.DataFrame) -> pd.DataFrame:
    """Add store-level foot traffic and conversion features."""
    traffic = traffic.copy()
    traffic["week"] = traffic["fecha"].dt.to_period("W").dt.start_time

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
    # Use unique tienda_ids present in data as identity mapping
    unique_ids = traffic_weekly["tienda_id"].unique()
    id_to_centro = {str(tid): str(tid) for tid in unique_ids}
    traffic_weekly["centro"] = traffic_weekly["tienda_id"].astype(str).map(id_to_centro)
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
    Build target variables for markdown optimization.

    Targets:
    - will_discount_4w: binary — was this SKU marked down within the next 4 weeks?
    - future_max_disc_4w: continuous — max discount depth in next 4 weeks
    - velocity_lift: ratio — did the markdown accelerate sales?
    - needs_markdown: binary (when stock available) — low WoC + declining velocity
    """
    weekly = weekly.sort_values(["sku", "centro", "week"]).reset_index(drop=True)
    gb = weekly.groupby(["sku", "centro"], sort=False)

    # Target 1: Will this SKU be discounted in the next 4 weeks?
    # Vectorized: shift(-1) then rolling(4).max(), no lambda
    weekly["_shifted_disc"] = gb["has_discount"].shift(-1)
    rolled = weekly.groupby(["sku", "centro"], sort=False)["_shifted_disc"].rolling(4, min_periods=1).max()
    weekly["will_discount_4w"] = rolled.droplevel([0, 1]).sort_index().values
    weekly["will_discount_4w"] = weekly["will_discount_4w"].fillna(0).astype(int)
    weekly.drop(columns=["_shifted_disc"], inplace=True)

    # Target 2: Max discount depth in next 4 weeks
    weekly["_shifted_rate"] = gb["discount_rate"].shift(-1)
    rolled2 = weekly.groupby(["sku", "centro"], sort=False)["_shifted_rate"].rolling(4, min_periods=1).max()
    weekly["future_max_disc_4w"] = rolled2.droplevel([0, 1]).sort_index().values
    weekly["future_max_disc_4w"] = weekly["future_max_disc_4w"].fillna(0)
    weekly.drop(columns=["_shifted_rate"], inplace=True)

    # Target 3: Velocity change after markdown (did the markdown accelerate sales?)
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


DISCOUNT_STEPS = [0.0, 0.15, 0.20, 0.30, 0.40]
EMPIRICAL_LIFT = {0.0: 1.0, 0.15: 1.8, 0.20: 2.2, 0.30: 3.0, 0.40: 4.0}


def add_margin_targets(weekly: pd.DataFrame, brand: str) -> pd.DataFrame:
    """
    Compute margin-optimal discount targets for each SKU-store-week.

    For each row, simulates gross profit at each discount step:
        profit(step) = (list_price * (1 - step) - cost) * velocity(step)

    Velocity at each step is estimated via elasticity (if available)
    or empirical lift table.

    Adds columns:
        optimal_disc_margin  — the discount step that maximizes weekly gross profit
        should_reprice       — 1 if current discount != optimal, 0 otherwise
        optimal_profit       — estimated gross profit at optimal discount
    """
    raw = _raw_dir(brand)
    costs_df = pd.read_parquet(raw / "costs.parquet")
    costs_df = costs_df[costs_df["cost"] > 0].dropna(subset=["cost"])
    cost_map = costs_df.set_index("sku")["cost"].to_dict()
    sorted_keys = sorted(cost_map.keys(), key=len, reverse=True)

    # Load elasticity if available
    processed = _processed_dir(brand)
    elast_map = {}
    try:
        elast_df = pd.read_parquet(processed / "elasticity_by_sku.parquet")
        elast_map = elast_df[elast_df["confidence"].isin(["high", "medium"])].set_index("codigo_padre")["elasticity"].to_dict()
    except FileNotFoundError:
        pass

    def _get_cost(sku):
        if sku in cost_map:
            return cost_map[sku]
        for k in sorted_keys:
            if sku.startswith(k):
                return cost_map[k]
        return None

    def _snap_disc(d):
        if d < 0.07:
            return 0.0
        return min(DISCOUNT_STEPS, key=lambda s: abs(s - d))

    def _estimate_velocity(base_vel, base_disc, target_disc, elasticity):
        if base_vel <= 0:
            return 0.1
        if target_disc <= base_disc:
            return base_vel  # not deepening discount
        disc_change = target_disc - base_disc
        if elasticity is not None and elasticity < -0.3:
            vol_change = -disc_change * elasticity
            return max(base_vel * (1 + vol_change), 0.1)
        base_lift = EMPIRICAL_LIFT.get(_snap_disc(base_disc), 1.0)
        target_lift = EMPIRICAL_LIFT.get(target_disc, 1 + target_disc * 5)
        return max(base_vel * target_lift / max(base_lift, 0.1), 0.1)

    # Vectorized would be ideal but the logic is complex — use apply
    results = []
    parent_col = "codigo_padre" if "codigo_padre" in weekly.columns else "sku"

    for _, row in weekly.iterrows():
        sku = row.get(parent_col, row.get("sku", ""))
        list_price = row.get("avg_precio_lista", 0)
        actual_disc = row.get("discount_rate", 0)
        actual_vel = row.get("velocity_4w", 0)
        cost = _get_cost(sku)

        if not cost or pd.isna(list_price) or list_price <= 0 or pd.isna(actual_vel) or actual_vel <= 0:
            results.append((np.nan, np.nan, np.nan))
            continue

        actual_disc = actual_disc if pd.notna(actual_disc) else 0
        elasticity = elast_map.get(sku)

        best_profit = -float("inf")
        best_step = _snap_disc(actual_disc)

        for step in DISCOUNT_STEPS:
            price = list_price * (1 - step)
            margin_per_unit = price - cost
            if margin_per_unit <= 0 and step > 0:
                continue  # skip unprofitable discount levels

            if step <= actual_disc:
                # Going up (reducing discount) — assume 25% volume loss per 10pp
                vol_factor = max(1 - (actual_disc - step) * 2.5, 0.3)
                est_vel = actual_vel * vol_factor
            else:
                est_vel = _estimate_velocity(actual_vel, actual_disc, step, elasticity)

            profit = margin_per_unit * est_vel
            if profit > best_profit:
                best_profit = profit
                best_step = step

        current_step = _snap_disc(actual_disc)
        should_reprice = 1 if best_step != current_step else 0
        results.append((best_step, should_reprice, best_profit))

    margin_df = pd.DataFrame(results, columns=["optimal_disc_margin", "should_reprice", "optimal_profit"],
                              index=weekly.index)
    weekly = pd.concat([weekly, margin_df], axis=1)

    n_valid = margin_df["optimal_disc_margin"].notna().sum()
    n_reprice = (margin_df["should_reprice"] == 1).sum()
    print(f"  Margin targets: {n_valid:,} rows with cost data, {n_reprice:,} should reprice ({n_reprice/max(n_valid,1):.0%})")

    # Distribution of optimal discounts
    dist = margin_df["optimal_disc_margin"].dropna().value_counts().sort_index()
    for step, count in dist.items():
        print(f"    {step:5.0%}: {count:>8,} rows ({count/n_valid:.0%})")

    return weekly


def build_features_for_brand(brand: str):
    """Main feature engineering pipeline for a given brand."""
    processed = _processed_dir(brand)
    processed.mkdir(parents=True, exist_ok=True)

    print(f"[{brand}] Loading raw data...")
    txn, products, stores, traffic, calendar, stock = load_raw_data(brand)

    print(f"[{brand}] Building weekly sales aggregation...")
    weekly = build_weekly_sales(txn)
    print(f"  {len(weekly):,} SKU-store-week rows")

    # Override list prices with official price list when available
    official_path = _raw_dir(brand) / "official_prices.parquet"
    if official_path.exists():
        official = pd.read_parquet(official_path)
        official = official[official["list_price"] > 0].dropna(subset=["list_price"])
        price_map = official.set_index("sku")["list_price"].to_dict()

        # Try direct match first, then prefix match (official SKUs may be parent-level)
        direct = weekly["sku"].map(price_map)
        if direct.notna().sum() == 0 and len(price_map) > 0:
            # Prefix match: longest matching official SKU wins
            sorted_keys = sorted(price_map.keys(), key=len, reverse=True)
            def _prefix_lookup(child_sku):
                for parent_sku in sorted_keys:
                    if child_sku.startswith(parent_sku):
                        return price_map[parent_sku]
                return None
            direct = weekly["sku"].map(_prefix_lookup)

        matched = direct.notna().sum()
        weekly["avg_precio_lista"] = direct.fillna(weekly["avg_precio_lista"])
        print(f"[{brand}] Official prices: {len(price_map)} SKUs, applied to {matched:,} rows ({matched/len(weekly)*100:.0f}%)")
    else:
        print(f"[{brand}] No official price list — using transaction-derived list prices")

    print(f"[{brand}] Adding velocity features...")
    weekly = add_velocity_features(weekly)

    print(f"[{brand}] Adding product age...")
    weekly = add_product_age(weekly, txn)

    print(f"[{brand}] Adding price features...")
    weekly = add_price_features(weekly)

    print(f"[{brand}] Adding size curve features...")
    weekly = add_size_curve_features(weekly, products)

    print(f"[{brand}] Adding seasonality features...")
    weekly = add_seasonality_features(weekly)

    if stock is not None:
        print(f"[{brand}] Adding stock/inventory features...")
        weekly = add_stock_features(weekly, stock)
        stock_cols = ["stock_on_hand", "stock_in_transit", "stock_total",
                      "weeks_of_cover", "stock_out_days", "stock_to_sales_ratio"]
        matched = weekly[stock_cols[0]].notna().sum()
        print(f"  Stock matched: {matched:,} / {len(weekly):,} rows ({matched/len(weekly):.1%})")

        print(f"[{brand}] Adding size sales share (volume weight per child SKU)...")
        weekly = add_size_sales_share(weekly, txn, products)
    else:
        print(f"[{brand}] No stock data available — skipping inventory features")

    print(f"[{brand}] Adding foot traffic features...")
    weekly = add_foot_traffic_features(weekly, traffic)

    print(f"[{brand}] Adding product attributes...")
    weekly = add_product_attributes(weekly, products)

    print(f"[{brand}] Building target variables...")
    weekly = build_target_variable(weekly)

    print(f"[{brand}] Filtering to active periods...")
    weekly = filter_active_rows(weekly)

    # Margin-optimal targets (when costs available)
    costs_path = _raw_dir(brand) / "costs.parquet"
    if costs_path.exists():
        print(f"[{brand}] Computing margin-optimal targets...")
        weekly = add_margin_targets(weekly, brand)
    else:
        print(f"[{brand}] No cost data — margin targets unavailable")

    # Drop temp columns
    drop_cols = [c for c in weekly.columns if c.startswith("_")]
    weekly.drop(columns=drop_cols, inplace=True, errors="ignore")

    # Save
    weekly.to_parquet(processed / "features.parquet", index=False)

    print(f"\n--- [{brand}] Feature Table Summary ---")
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, required=True, help="Brand name (e.g. HOKA, BOLD)")
    args = parser.parse_args()
    build_features_for_brand(args.brand)
