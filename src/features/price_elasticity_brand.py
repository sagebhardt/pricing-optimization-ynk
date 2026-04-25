"""
Brand-agnostic price elasticity estimation.

Thin wrapper around the HOKA-specific logic in price_elasticity.py,
parameterized by brand name. Reads from data/raw/{brand}/ and
writes to data/processed/{brand}/.

Estimates demand sensitivity to price using natural price variation in
transaction data. Uses log-log regression:

    ln(Q) = alpha + beta * ln(P) + controls + epsilon

Where beta is the price elasticity of demand.

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

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _raw_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "raw" / brand.lower()


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def prepare_elasticity_data(brand: str):
    """Prepare weekly SKU-store data for elasticity estimation."""
    raw = _raw_dir(brand)
    txn = pd.read_parquet(raw / "transactions.parquet")
    products = pd.read_parquet(raw / "products.parquet")

    # Only positive sales
    sales = txn[txn["cantidad"] > 0].copy()
    sales["week"] = sales["fecha"].dt.to_period("W").dt.start_time

    # Tag click & collect + coupon transactions — exclude from price calculations
    # (C&C = ecomm pricing; coupons = influencer/employee/Entel discounts — not store price sensitivity)
    sales["is_cc"] = (
        (sales["tipo_entrega"] == "Retiro en Tienda") if "tipo_entrega" in sales.columns else False
    )
    sales["has_coupon"] = (
        sales["codigo_descuento"].notna() & (sales["codigo_descuento"] != "")
    ) if "codigo_descuento" in sales.columns else False
    sales["is_credit_note"] = (
        sales["tipo_documento"].str.contains("NOTA DE CREDITO", case=False, na=False)
    ) if "tipo_documento" in sales.columns else False
    sales["is_extreme_discount"] = False
    mask_has_list = sales["precio_lista"] > 0
    if mask_has_list.any():
        disc_pct = sales.loc[mask_has_list, "descuento"] / sales.loc[mask_has_list, "precio_lista"]
        sales.loc[mask_has_list, "is_extreme_discount"] = disc_pct > 0.50
    # Markdown regime via datawarehouse lista_precio — conflates elasticity if included
    sales["is_markdown_list"] = (
        sales["list_category"].isin(["liquidacion", "outlet"])
    ) if "list_category" in sales.columns else False
    retail = sales[~sales["is_cc"] & ~sales["has_coupon"] & ~sales["is_credit_note"]
                   & ~sales["is_extreme_discount"] & ~sales["is_markdown_list"]]

    # Effective price per unit (retail-only to avoid ecomm discount contamination)
    retail = retail.copy()
    retail["effective_price"] = retail["precio_final"] / retail["cantidad"]

    # Weekly volume from ALL channels (demand is real regardless of source)
    vol = (
        sales.groupby(["sku", "centro", "week"])
        .agg(units=("cantidad", "sum"), txn_count=("folio", "nunique"))
        .reset_index()
    )

    # Weekly prices from RETAIL-ONLY
    prices = (
        retail.groupby(["sku", "centro", "week"])
        .agg(avg_price=("effective_price", "mean"), avg_list_price=("precio_lista", "mean"))
        .reset_index()
    )

    # Left join: markdown-only SKU-store-weeks have volume but no retail price;
    # they get dropped by the avg_price>0 filter below. Left join makes that intent
    # explicit (vs. inner, which silently drops them at the join step).
    weekly = vol.merge(prices, on=["sku", "centro", "week"], how="left")

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

    # Flag normal-price weeks using YNK's curated reference price
    # (auxiliar.precio_normal). Markdown weeks have an additional event-driven
    # demand boost on top of the price-elasticity response — including them
    # inflates elasticity estimates. By tagging them we can let
    # estimate_elasticity_sku exclude them from the regression.
    pn_path = raw / "precio_normal.parquet"
    if pn_path.exists():
        pn_df = pd.read_parquet(pn_path)[["sku", "precio_normal"]].rename(
            columns={"sku": "codigo_padre"}
        )
        weekly = weekly.merge(pn_df, on="codigo_padre", how="left")
        # Within 5% of reference = normal-price week. SKUs without coverage
        # in precio_normal default to normal=True (no filtering applied).
        weekly["is_normal_price"] = np.where(
            weekly["precio_normal"].notna() & (weekly["precio_normal"] > 0),
            weekly["avg_price"] >= weekly["precio_normal"] * 0.95,
            True,
        )
        n_normal = int(weekly["is_normal_price"].sum())
        n_total = len(weekly)
        print(f"  precio_normal coverage: {weekly['precio_normal'].notna().sum():,}/{n_total:,} rows; "
              f"normal-price weeks: {n_normal:,} ({100*n_normal/max(n_total,1):.0f}%)")
    else:
        weekly["is_normal_price"] = True

    return weekly


def estimate_elasticity_sku(data, min_price_variation=0.05, min_observations=10,
                            exclude_markdown=True):
    """
    Estimate price elasticity per parent SKU using log-log OLS.
    Returns elasticity only when there's enough price variation.

    Uses numpy arrays and np.linalg.lstsq directly (avoids pd.get_dummies
    and sklearn overhead per iteration — critical for 6K+ parent SKUs).

    When exclude_markdown=True (default) and the data has an `is_normal_price`
    column, markdown weeks are filtered before fitting. This isolates true
    price elasticity from the markdown-event demand boost. SKUs that lose
    all price variation after filtering get skipped (they fall through to
    category-level elasticity).
    """
    results = []
    use_normal_filter = exclude_markdown and "is_normal_price" in data.columns

    # Pre-compute global month codes for one-hot encoding
    unique_months = np.sort(data["month"].unique())
    # Drop-first months for dummy encoding
    month_codes = unique_months[1:] if len(unique_months) > 1 else np.array([], dtype=int)

    for parent, group in data.groupby("codigo_padre"):
        if len(group) < min_observations:
            continue

        if use_normal_filter:
            normal_group = group[group["is_normal_price"]]
            # Only filter when the SKU still has enough observations after
            # excluding markdown weeks. Otherwise fall through to the legacy
            # path so we don't lose the SKU entirely.
            if len(normal_group) >= min_observations:
                group = normal_group

        # Check price variation (coefficient of variation)
        price_cv = group["avg_price"].std() / group["avg_price"].mean()
        if price_cv < min_price_variation:
            continue

        n = len(group)

        # Build feature matrix with numpy (no pd.get_dummies / pd.concat)
        features = [group["ln_price"].values.reshape(-1, 1)]

        # Month dummies (one-hot, drop first)
        g_months = group["month"].values
        for m in month_codes:
            features.append((g_months == m).astype(np.float64).reshape(-1, 1))

        # Store dummies (one-hot, drop first — only for stores in this group)
        stores = group["centro"].values
        unique_stores = np.unique(stores)
        if len(unique_stores) > 1:
            for s in unique_stores[1:]:
                features.append((stores == s).astype(np.float64).reshape(-1, 1))

        # Trend
        features.append(group["week_num"].values.reshape(-1, 1))

        X = np.hstack(features)
        y = group["ln_units"].values

        # Drop any NaN
        mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X = X[mask]
        y = y[mask]

        if len(X) < min_observations:
            continue

        try:
            # Add intercept and solve with numpy (avoids sklearn overhead)
            X_i = np.column_stack([np.ones(len(X)), X])
            coefs, _, _, _ = np.linalg.lstsq(X_i, y, rcond=None)
            elasticity = coefs[1]  # First feature after intercept = ln_price

            # R-squared
            y_pred = X_i @ coefs
            ss_res = ((y - y_pred) ** 2).sum()
            ss_tot = ((y - y.mean()) ** 2).sum()
            r2 = float(1 - ss_res / max(ss_tot, 1e-10))

            # Confidence: based on R2, sample size, and price variation
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


def build_elasticity_features(features_df, sku_elasticity, cat_elasticity, brand: str):
    """
    Add elasticity to the feature table.
    Uses SKU-level when available (high/medium confidence), falls back to category.
    """
    raw = _raw_dir(brand)
    features_df = features_df.copy()

    # Map SKU to parent
    products = pd.read_parquet(raw / "products.parquet")
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
    features_df["optimal_discount_rev"] = np.where(
        features_df["price_elasticity"].notna() & (features_df["price_elasticity"] < -0.5),
        1 / (1 + np.abs(features_df["price_elasticity"])),
        np.nan,
    )

    features_df.drop(columns=["_parent", "elasticity_sku", "elasticity_cat"], inplace=True)

    return features_df


def run_elasticity_for_brand(brand: str):
    """Main elasticity estimation pipeline for a given brand."""
    processed = _processed_dir(brand)
    processed.mkdir(parents=True, exist_ok=True)

    print(f"[{brand}] Preparing elasticity data...")
    data = prepare_elasticity_data(brand)
    print(f"  {len(data):,} weekly SKU-store observations")

    print(f"\n[{brand}] Estimating SKU-level elasticity...")
    sku_elast = estimate_elasticity_sku(data)
    sku_elast.to_parquet(processed / "elasticity_by_sku.parquet", index=False)

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
            print(f"    {row['codigo_padre']:<25s} e={row['elasticity']:>6.2f} R2={row['r2']:.2f} n={row['n_observations']:>4} {row['segunda_jerarquia']}")

        print(f"\n  Top 10 most inelastic (price-insensitive) parent SKUs:")
        most_inelastic = reliable.nlargest(10, "elasticity")
        for _, row in most_inelastic.iterrows():
            print(f"    {row['codigo_padre']:<25s} e={row['elasticity']:>6.2f} R2={row['r2']:.2f} n={row['n_observations']:>4} {row['segunda_jerarquia']}")

    print(f"\n[{brand}] Estimating category-level elasticity...")
    cat_elast = estimate_elasticity_category(data)
    cat_elast.to_parquet(processed / "elasticity_by_category.parquet", index=False)

    if len(cat_elast) > 0:
        print(f"  {len(cat_elast)} subcategories estimated")
        for _, row in cat_elast.sort_values("elasticity").iterrows():
            print(f"    {row['primera_jerarquia']}/{row['segunda_jerarquia']:<20s} e={row['elasticity']:>6.2f} R2={row['r2']:.2f} n={row['n_observations']:>5}")

    return sku_elast, cat_elast


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, required=True, help="Brand name (e.g. HOKA, BOLD)")
    args = parser.parse_args()
    run_elasticity_for_brand(args.brand)
