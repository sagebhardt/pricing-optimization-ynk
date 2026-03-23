"""
Price elasticity estimation for HOKA products.

Estimates demand sensitivity to price using natural price variation in
transaction data. Uses log-log regression:

    ln(Q) = α + β·ln(P) + controls + ε

Where β is the price elasticity of demand.

Estimation levels:
1. Per parent SKU (most granular, needs enough price variation)
2. Per subcategory (fallback when SKU-level data is sparse)
3. Per category (broadest fallback)

Controls for: store fixed effects, month seasonality, product age, trend.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"


def prepare_elasticity_data():
    """Prepare weekly SKU-store data for elasticity estimation."""
    txn = pd.read_parquet(RAW_DIR / "hoka_transactions.parquet")
    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")

    # Only positive sales
    sales = txn[txn["cantidad"] > 0].copy()
    sales["week"] = sales["fecha"].dt.to_period("W").dt.start_time

    # Effective price per unit
    sales["effective_price"] = sales["precio_final"] / sales["cantidad"]

    # Weekly aggregation per SKU-store
    weekly = (
        sales.groupby(["sku", "centro", "week"])
        .agg(
            units=("cantidad", "sum"),
            avg_price=("effective_price", "mean"),
            avg_list_price=("precio_lista", "mean"),
            txn_count=("folio", "nunique"),
        )
        .reset_index()
    )

    # Add product hierarchy
    product_map = products[["material", "codigo_padre", "primera_jerarquia",
                            "segunda_jerarquia"]].rename(columns={"material": "sku"})
    product_map = product_map.drop_duplicates(subset=["sku"])
    weekly = weekly.merge(product_map, on="sku", how="left")

    # Add controls
    weekly["month"] = weekly["week"].dt.month
    weekly["week_num"] = (weekly["week"] - weekly["week"].min()).dt.days / 7

    # Log transforms (filter zeros)
    weekly = weekly[(weekly["units"] > 0) & (weekly["avg_price"] > 0)]
    weekly["ln_units"] = np.log(weekly["units"])
    weekly["ln_price"] = np.log(weekly["avg_price"])

    # Discount depth as fraction
    weekly["discount_depth"] = np.where(
        weekly["avg_list_price"] > 0,
        1 - (weekly["avg_price"] / weekly["avg_list_price"]),
        0,
    )

    return weekly


def estimate_elasticity_sku(data, min_price_variation=0.05, min_observations=10):
    """
    Estimate price elasticity per parent SKU using log-log OLS.
    Returns elasticity only when there's enough price variation.
    """
    results = []

    for parent, group in data.groupby("codigo_padre"):
        if len(group) < min_observations:
            continue

        # Check price variation (coefficient of variation)
        price_cv = group["avg_price"].std() / group["avg_price"].mean()
        if price_cv < min_price_variation:
            continue

        # Build regression: ln(Q) = β0 + β1·ln(P) + β2·month + β3·trend + store dummies
        X_cols = ["ln_price"]

        # Month dummies
        month_dummies = pd.get_dummies(group["month"], prefix="m", drop_first=True)
        X = pd.concat([group[X_cols].reset_index(drop=True), month_dummies.reset_index(drop=True)], axis=1)

        # Store dummies
        if group["centro"].nunique() > 1:
            store_dummies = pd.get_dummies(group["centro"], prefix="s", drop_first=True)
            X = pd.concat([X, store_dummies.reset_index(drop=True)], axis=1)

        # Trend
        X["trend"] = group["week_num"].values

        y = group["ln_units"].values

        # Drop any NaN
        mask = ~(X.isna().any(axis=1) | np.isnan(y))
        X = X[mask]
        y = y[mask]

        if len(X) < min_observations:
            continue

        try:
            reg = LinearRegression().fit(X, y)
            elasticity = reg.coef_[0]  # Coefficient on ln_price
            r2 = reg.score(X, y)

            # Confidence: based on R², sample size, and price variation
            confidence = "high" if (r2 > 0.3 and len(X) > 30 and price_cv > 0.1) else \
                         "medium" if (r2 > 0.15 and len(X) > 15) else "low"

            results.append({
                "codigo_padre": parent,
                "elasticity": elasticity,
                "r2": r2,
                "n_observations": len(X),
                "price_cv": price_cv,
                "avg_price": group["avg_price"].mean(),
                "avg_units_week": group["units"].mean(),
                "confidence": confidence,
                "primera_jerarquia": group["primera_jerarquia"].iloc[0],
                "segunda_jerarquia": group["segunda_jerarquia"].iloc[0],
            })
        except Exception:
            continue

    return pd.DataFrame(results)


def estimate_elasticity_category(data, min_observations=30):
    """Estimate elasticity at the subcategory level as fallback."""
    results = []

    for (cat, subcat), group in data.groupby(["primera_jerarquia", "segunda_jerarquia"]):
        if len(group) < min_observations:
            continue

        X_cols = ["ln_price"]
        month_dummies = pd.get_dummies(group["month"], prefix="m", drop_first=True)
        X = pd.concat([group[X_cols].reset_index(drop=True), month_dummies.reset_index(drop=True)], axis=1)

        if group["centro"].nunique() > 1:
            store_dummies = pd.get_dummies(group["centro"], prefix="s", drop_first=True)
            X = pd.concat([X, store_dummies.reset_index(drop=True)], axis=1)

        X["trend"] = group["week_num"].values
        y = group["ln_units"].values

        mask = ~(X.isna().any(axis=1) | np.isnan(y))
        X, y = X[mask], y[mask]

        if len(X) < min_observations:
            continue

        try:
            reg = LinearRegression().fit(X, y)
            results.append({
                "primera_jerarquia": cat,
                "segunda_jerarquia": subcat,
                "elasticity": reg.coef_[0],
                "r2": reg.score(X, y),
                "n_observations": len(X),
                "price_cv": group["avg_price"].std() / group["avg_price"].mean(),
            })
        except Exception:
            continue

    return pd.DataFrame(results)


def build_elasticity_features(features_df, sku_elasticity, cat_elasticity):
    """
    Add elasticity to the feature table.
    Uses SKU-level when available (high/medium confidence), falls back to category.
    """
    features_df = features_df.copy()

    # Map SKU to parent
    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")
    sku_to_parent = products.set_index("material")["codigo_padre"].to_dict()
    features_df["_parent"] = features_df["sku"].map(sku_to_parent)

    # SKU-level elasticity (high/medium confidence)
    reliable_sku = sku_elasticity[sku_elasticity["confidence"].isin(["high", "medium"])]
    sku_elast_map = reliable_sku.set_index("codigo_padre")["elasticity"].to_dict()
    features_df["elasticity_sku"] = features_df["_parent"].map(sku_elast_map)

    # Category fallback
    cat_elast_map = cat_elasticity.set_index(
        ["primera_jerarquia", "segunda_jerarquia"]
    )["elasticity"].to_dict()

    features_df["elasticity_cat"] = features_df.apply(
        lambda r: cat_elast_map.get((r["primera_jerarquia"], r.get("segunda_jerarquia")), np.nan),
        axis=1,
    )

    # Combined: prefer SKU, fall back to category
    features_df["price_elasticity"] = features_df["elasticity_sku"].fillna(
        features_df["elasticity_cat"]
    )

    # Optimal discount given elasticity (simplified)
    # If elasticity = -2, a 10% price cut increases volume by ~20%
    # Revenue-maximizing discount = 1 / (1 + |elasticity|)
    features_df["optimal_discount_rev"] = np.where(
        features_df["price_elasticity"].notna() & (features_df["price_elasticity"] < -0.5),
        1 / (1 + np.abs(features_df["price_elasticity"])),
        np.nan,
    )

    features_df.drop(columns=["_parent", "elasticity_sku", "elasticity_cat"], inplace=True)

    return features_df


def run_elasticity_analysis():
    """Main elasticity estimation pipeline."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("Preparing elasticity data...")
    data = prepare_elasticity_data()
    print(f"  {len(data):,} weekly SKU-store observations")

    print("\nEstimating SKU-level elasticity...")
    sku_elast = estimate_elasticity_sku(data)
    sku_elast.to_parquet(PROCESSED_DIR / "elasticity_by_sku.parquet", index=False)

    if len(sku_elast) > 0:
        reliable = sku_elast[sku_elast["confidence"].isin(["high", "medium"])]
        print(f"  {len(sku_elast)} parent SKUs estimated")
        print(f"  {len(reliable)} with high/medium confidence")
        print(f"  Median elasticity: {sku_elast['elasticity'].median():.2f}")
        print(f"  Mean elasticity: {sku_elast['elasticity'].mean():.2f}")

        print(f"\n  Elasticity distribution:")
        bins = [(-10, -3), (-3, -2), (-2, -1.5), (-1.5, -1), (-1, -0.5), (-0.5, 0), (0, 10)]
        labels = ["Very elastic (<-3)", "Elastic (-3 to -2)", "Moderately elastic (-2 to -1.5)",
                  "Unit elastic (-1.5 to -1)", "Inelastic (-1 to -0.5)",
                  "Very inelastic (-0.5 to 0)", "Positive (Giffen/error)"]
        for (lo, hi), label in zip(bins, labels):
            count = ((sku_elast["elasticity"] >= lo) & (sku_elast["elasticity"] < hi)).sum()
            if count > 0:
                print(f"    {label:40s} {count:>4} ({count/len(sku_elast):.1%})")

        print(f"\n  Top 10 most elastic (price-sensitive) parent SKUs:")
        most_elastic = reliable.nsmallest(10, "elasticity")
        for _, row in most_elastic.iterrows():
            print(f"    {row['codigo_padre']:<25s} e={row['elasticity']:>6.2f} R²={row['r2']:.2f} n={row['n_observations']:>4} {row['segunda_jerarquia']}")

        print(f"\n  Top 10 most inelastic (price-insensitive) parent SKUs:")
        most_inelastic = reliable.nlargest(10, "elasticity")
        for _, row in most_inelastic.iterrows():
            print(f"    {row['codigo_padre']:<25s} e={row['elasticity']:>6.2f} R²={row['r2']:.2f} n={row['n_observations']:>4} {row['segunda_jerarquia']}")

    print("\nEstimating category-level elasticity...")
    cat_elast = estimate_elasticity_category(data)
    cat_elast.to_parquet(PROCESSED_DIR / "elasticity_by_category.parquet", index=False)

    if len(cat_elast) > 0:
        print(f"  {len(cat_elast)} subcategories estimated")
        for _, row in cat_elast.sort_values("elasticity").iterrows():
            print(f"    {row['primera_jerarquia']}/{row['segunda_jerarquia']:<20s} e={row['elasticity']:>6.2f} R²={row['r2']:.2f} n={row['n_observations']:>5}")

    return sku_elast, cat_elast


if __name__ == "__main__":
    run_elasticity_analysis()
