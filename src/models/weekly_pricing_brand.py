"""
Brand-agnostic Weekly Pricing Action List.

Thin wrapper around the HOKA-specific logic in weekly_pricing.py,
parameterized by brand name. Reads from data/processed/{brand}/,
models/{brand}/, and data/raw/{brand}/. Writes to
weekly_actions/{brand}/.

Generates the actual weekly output the commercial team needs:
- Which parent SKUs to reprice this week
- Current price -> recommended price (in CLP, rounded to xxx,990)
- Expected unit lift at new price
- Urgency level and reason
- Grouped by parent SKU (not individual sizes)

Respects business reality:
- Prices change once per week
- Discount ladder: 0% -> 15% -> 20% -> 30% -> 40%
- Prices end in ,990 or ,000
- Markdown decisions are per parent SKU, not per size
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent.parent

# Discount ladder (same across brands)
DISCOUNT_STEPS = [0.0, 0.15, 0.20, 0.30, 0.40]

CATEGORICAL_COLS = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]
EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
]


def _raw_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "raw" / brand.lower()


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def _model_dir(brand: str) -> Path:
    return PROJECT_ROOT / "models" / brand.lower()


def _output_dir(brand: str) -> Path:
    return PROJECT_ROOT / "weekly_actions" / brand.lower()


def _load_store_names(brand: str) -> dict:
    """Dynamically load store names from the brand's stores.parquet."""
    raw = _raw_dir(brand)
    try:
        stores = pd.read_parquet(raw / "stores.parquet")
        # Try common column patterns for store code -> name mapping
        if "centro" in stores.columns and "nombre" in stores.columns:
            return stores.set_index("centro")["nombre"].to_dict()
        elif "centro" in stores.columns and "nombre_tienda" in stores.columns:
            return stores.set_index("centro")["nombre_tienda"].to_dict()
        elif "store_code" in stores.columns and "store_name" in stores.columns:
            return stores.set_index("store_code")["store_name"].to_dict()
        else:
            # Fallback: use first two columns as code/name
            cols = stores.columns.tolist()
            if len(cols) >= 2:
                return stores.set_index(cols[0])[cols[1]].to_dict()
            else:
                return {}
    except (FileNotFoundError, KeyError):
        return {}


def round_to_clp(price):
    """Round price to Chilean retail convention (ending in ,990)."""
    if price <= 0:
        return 0
    # Round to nearest 1000, then subtract 10
    rounded = round(price / 1000) * 1000 - 10
    if rounded < 990:
        rounded = 990
    return int(rounded)


def snap_to_discount_step(discount_pct):
    """Snap a continuous discount % to the actual discount ladder."""
    if discount_pct < 0.07:
        return 0.0
    best = min(DISCOUNT_STEPS, key=lambda s: abs(s - discount_pct))
    # Don't snap down to 0 if model suggests meaningful discount
    if best == 0.0 and discount_pct >= 0.07:
        return 0.15
    return best


def compute_expected_velocity(current_vel, current_disc, new_disc, elasticity):
    """
    Estimate weekly units at new price point.
    Uses elasticity where available, falls back to historical lift patterns.
    """
    if current_vel <= 0:
        return 0.5  # Minimum expected

    if new_disc <= current_disc:
        return current_vel  # Not deepening discount, no lift expected

    # Discount change
    disc_change = new_disc - current_disc

    if elasticity is not None and elasticity < -0.3:
        # Use elasticity: price change % -> volume change %
        price_change_pct = -disc_change  # Negative = price decrease
        volume_change = price_change_pct * elasticity  # Elasticity is negative, so double negative = positive
        return max(current_vel * (1 + volume_change), 0.5)
    else:
        # Empirical fallback: observed lift patterns
        lift_by_step = {
            0.15: 1.8,   # 15% discount -> ~80% more units
            0.20: 2.2,   # 20% -> ~120% more
            0.30: 3.0,   # 30% -> ~200% more
            0.40: 4.0,   # 40% -> ~300% more
        }
        lift = lift_by_step.get(new_disc, 1 + disc_change * 5)
        return max(current_vel * lift, 0.5)


def classify_urgency(row):
    """
    Determine urgency level and reason for markdown.

    Returns (urgency, reasons) where urgency is HIGH/MEDIUM/LOW.
    """
    reasons = []
    urgency_score = 0

    # Velocity collapse
    vel_trend = row.get("velocity_trend", 1.0)
    if vel_trend < 0.5:
        reasons.append("Velocity collapsed (trend < 0.5)")
        urgency_score += 3
    elif vel_trend < 0.7:
        reasons.append("Velocity declining")
        urgency_score += 2

    # Lifecycle stage
    lifecycle = row.get("lifecycle_stage_code")
    if lifecycle == 5:  # clearance
        reasons.append("In clearance stage")
        urgency_score += 3
    elif lifecycle == 4:  # decline
        reasons.append("In decline stage")
        urgency_score += 2

    # Size curve breaking
    attrition = row.get("attrition_rate", 0)
    if attrition > 0.5:
        reasons.append(f"Size curve critical ({attrition:.0%} attrition)")
        urgency_score += 3
    elif attrition > 0.3:
        reasons.append(f"Size curve breaking ({attrition:.0%} attrition)")
        urgency_score += 1

    # Product age (older = more urgent)
    age = row.get("product_age_weeks", 0)
    if age > 40:
        reasons.append(f"Product is {int(age)} weeks old")
        urgency_score += 2
    elif age > 26:
        reasons.append(f"Product is {int(age)} weeks old")
        urgency_score += 1

    # Already discounted elsewhere (price consistency)
    if row.get("max_discount_rate", 0) > 0.1 and row.get("discount_rate", 0) < 0.05:
        reasons.append("Discounted in other channels")
        urgency_score += 1

    # Model confidence
    prob = row.get("markdown_probability", 0)
    if prob > 0.9:
        urgency_score += 1

    if urgency_score >= 5:
        urgency = "HIGH"
    elif urgency_score >= 3:
        urgency = "MEDIUM"
    else:
        urgency = "LOW"

    if not reasons:
        reasons.append("Model predicts markdown based on overall patterns")

    return urgency, reasons


def generate_weekly_actions_for_brand(brand: str, target_week=None):
    """
    Generate the weekly pricing action list for a given brand.
    This is what the commercial team reviews every Monday.
    """
    output_dir = _output_dir(brand)
    output_dir.mkdir(parents=True, exist_ok=True)
    processed = _processed_dir(brand)
    model_dir = _model_dir(brand)
    raw = _raw_dir(brand)

    # Load store names dynamically
    store_names = _load_store_names(brand)

    print(f"[{brand}] Loading models and data...")

    # Load model
    try:
        with open(model_dir / "markdown_classifier.pkl", "rb") as f:
            cls_model = pickle.load(f)
        with open(model_dir / "depth_regressor.pkl", "rb") as f:
            reg_model = pickle.load(f)
    except FileNotFoundError:
        print(f"  ERROR: Models not found for {brand} in {model_dir}")
        return None

    # Prefer parent-level features; fall back to child-level
    parent_path = processed / "features_parent.parquet"
    if parent_path.exists():
        features = pd.read_parquet(parent_path)
        print(f"  Using parent-level features: {len(features):,} rows")
    else:
        try:
            features = pd.read_parquet(processed / "features_v2.parquet")
        except FileNotFoundError:
            features = pd.read_parquet(processed / "features.parquet")
        print(f"  Using child-level features: {len(features):,} rows")

    products = pd.read_parquet(raw / "products.parquet")

    # Load elasticity
    try:
        sku_elast = pd.read_parquet(processed / "elasticity_by_sku.parquet")
        elast_map = sku_elast[sku_elast["confidence"].isin(["high", "medium"])].set_index("codigo_padre")["elasticity"].to_dict()
    except FileNotFoundError:
        elast_map = {}

    # Determine target week
    if target_week is None:
        target_week = features["week"].max()
    else:
        target_week = pd.Timestamp(target_week)

    print(f"[{brand}] Generating weekly pricing actions for: {target_week.date()}")

    # Get current week data
    week_data = features[features["week"] == target_week].copy()
    if len(week_data) == 0:
        print(f"  No data for {target_week.date()}")
        return None

    # Score at whatever level the features are (parent or child)
    df_prep = week_data.copy()
    for col in CATEGORICAL_COLS:
        if col in df_prep.columns:
            df_prep[col] = df_prep[col].astype("category").cat.codes
    feat_cols = [c for c in df_prep.columns if c not in EXCLUDE_COLS]
    X = df_prep[feat_cols].values

    week_data["markdown_probability"] = cls_model.predict_proba(X)[:, 1]
    week_data["raw_depth"] = np.clip(reg_model.predict(X), 0, 0.60)

    # If already at parent level, use directly. Otherwise aggregate.
    is_parent_level = "codigo_padre" in week_data.columns and "sku" not in week_data.columns

    if is_parent_level:
        parent_agg = week_data.copy()
        # Get product names from product master
        parent_names = (
            products.groupby("codigo_padre")
            .first()[["material_descripcion", "primera_jerarquia", "segunda_jerarquia"]]
            .reset_index()
            .rename(columns={"material_descripcion": "product_name"})
        )
        parent_names["product_name"] = (
            parent_names["product_name"]
            .str.replace(r'\s*N[°º]?\s*[\d,\.]+\s*$', '', regex=True)
            .str.replace(r'\s*T/[A-Z]+\s*$', '', regex=True).str.strip()
        )
        total_sizes = products.groupby("codigo_padre")["material"].nunique().rename("total_sizes_catalog")

        if "product_name" not in parent_agg.columns:
            parent_agg = parent_agg.merge(parent_names[["codigo_padre", "product_name"]], on="codigo_padre", how="left")
        if "total_sizes_catalog" not in parent_agg.columns:
            parent_agg = parent_agg.merge(total_sizes, on="codigo_padre", how="left")
        if "sizes_selling" not in parent_agg.columns:
            parent_agg["sizes_selling"] = parent_agg.get("size_curve_completeness", 0) * parent_agg.get("total_sizes_catalog", 1)
        print(f"  {len(parent_agg):,} parent-store rows (already parent level)")
    else:
        # Child level — aggregate up (legacy path)
        prod_info = products[["material", "material_descripcion", "codigo_padre",
                               "primera_jerarquia", "segunda_jerarquia",
                               "grupo_articulos_descripcion"]].rename(
            columns={"material": "sku", "material_descripcion": "product_name"}
        ).drop_duplicates(subset=["sku"])

        parent_names = (
            prod_info.groupby("codigo_padre")
            .first()[["product_name", "primera_jerarquia", "segunda_jerarquia"]]
            .reset_index()
        )
        parent_names["product_name"] = parent_names["product_name"].str.replace(
            r'\s*N[°º]?\s*[\d,\.]+\s*$', '', regex=True
        ).str.replace(r'\s*T/[A-Z]+\s*$', '', regex=True).str.strip()

        total_sizes = prod_info.groupby("codigo_padre")["sku"].nunique().rename("total_sizes_catalog")

        parent_agg = (
            week_data.groupby(["codigo_padre", "centro"])
            .agg(
                velocity_4w=("velocity_4w", "sum"),
                velocity_2w=("velocity_2w", "sum"),
                velocity_8w=("velocity_8w", "sum"),
                units_sold=("units_sold", "sum"),
                avg_precio_lista=("avg_precio_lista", "median"),
                avg_precio_final=("avg_precio_final", "median"),
                discount_rate=("discount_rate", "median"),
                max_discount_rate=("max_discount_rate", "max"),
                markdown_probability=("markdown_probability", "mean"),
                raw_depth=("raw_depth", "mean"),
                sizes_selling=("units_sold", lambda x: (x > 0).sum()),
                product_age_weeks=("product_age_weeks", "max"),
                lifecycle_stage_code=("lifecycle_stage_code", "median"),
                attrition_rate=("attrition_rate", "first"),
                velocity_trend=("velocity_trend", "mean"),
                cumulative_units=("cumulative_units", "sum"),
            )
            .reset_index()
        )
        parent_agg = parent_agg.merge(parent_names, on="codigo_padre", how="left")
        parent_agg = parent_agg.merge(total_sizes, on="codigo_padre", how="left")
        parent_agg["velocity_trend"] = np.where(
            parent_agg["velocity_8w"] > 0,
            parent_agg["velocity_4w"] / parent_agg["velocity_8w"],
            1.0,
        )
        print(f"  Aggregated {len(week_data):,} child rows -> {len(parent_agg):,} parent-store rows")

    parent_agg["total_sizes_catalog"] = parent_agg.get("total_sizes_catalog", pd.Series(dtype=float)).fillna(1)

    # ================================================================
    # PASS 1: Price INCREASES (recover margin on over-discounted SKUs)
    # ================================================================
    parent_actions = []

    for _, row in parent_agg.iterrows():
        disc_rate = row["discount_rate"] if pd.notna(row["discount_rate"]) else 0
        if disc_rate <= 0.05:
            continue

        current_step = snap_to_discount_step(disc_rate)
        if current_step == 0:
            continue

        list_price = row["avg_precio_lista"]
        final_price = row["avg_precio_final"]
        if pd.isna(list_price) or list_price <= 0:
            continue

        velocity = row["velocity_4w"]
        vel_trend = row["velocity_trend"]
        lifecycle = row.get("lifecycle_stage_code")
        attrition = row.get("attrition_rate", 0)
        attrition = attrition if pd.notna(attrition) else 0
        age = row.get("product_age_weeks", 0)

        should_increase = (
            velocity >= 1.0
            and vel_trend >= 1.0
            and lifecycle not in (4, 5)
            and attrition < 0.3
            and age < 35
        )
        if not should_increase:
            continue

        current_idx = DISCOUNT_STEPS.index(current_step) if current_step in DISCOUNT_STEPS else len(DISCOUNT_STEPS) - 1
        if current_idx <= 0:
            continue
        recommended_step = DISCOUNT_STEPS[current_idx - 1]
        recommended_price = round_to_clp(list_price * (1 - recommended_step))
        current_final_rounded = round_to_clp(final_price) if pd.notna(final_price) else 0

        if recommended_price <= current_final_rounded + 2000:
            continue

        vol_loss = 0.25
        expected_velocity = max(velocity * (1 - vol_loss), 0.3)
        current_weekly_rev = velocity * (final_price if pd.notna(final_price) else list_price)
        expected_weekly_rev = expected_velocity * recommended_price

        reasons = [f"Selling {velocity:.1f} u/sem at {current_step:.0%} off -- test higher price"]
        if vel_trend > 1.1:
            reasons.append("Sales accelerating")
        n_sell = int(row["sizes_selling"])
        n_total = int(row["total_sizes_catalog"])
        if n_sell >= n_total * 0.7:
            reasons.append(f"Size curve healthy ({n_sell}/{n_total})")

        parent_actions.append({
            "parent_sku": row["codigo_padre"],
            "store": row["centro"],
            "store_name": store_names.get(row["centro"], row["centro"]),
            "product": str(row.get("product_name", row["codigo_padre"]))[:50],
            "category": row.get("primera_jerarquia", ""),
            "subcategory": row.get("segunda_jerarquia", ""),
            "sizes_selling": n_sell,
            "sizes_total": n_total,
            "product_age_weeks": int(age),
            "current_list_price": int(list_price),
            "current_price": current_final_rounded,
            "current_discount": f"{current_step:.0%}",
            "current_velocity": round(velocity, 1),
            "recommended_price": recommended_price,
            "recommended_discount": f"{recommended_step:.0%}" if recommended_step > 0 else "Full price",
            "expected_velocity": round(expected_velocity, 1),
            "current_weekly_rev": int(current_weekly_rev) if pd.notna(current_weekly_rev) else 0,
            "expected_weekly_rev": int(expected_weekly_rev),
            "rev_delta": int(expected_weekly_rev - current_weekly_rev) if pd.notna(current_weekly_rev) else 0,
            "urgency": "INCREASE",
            "reasons": "; ".join(reasons),
            "model_confidence": round(row["markdown_probability"], 3),
            "action_type": "increase",
        })

    increase_keys = {(a["parent_sku"], a["store"]) for a in parent_actions}

    # ================================================================
    # PASS 2: Price DECREASES (markdowns)
    # ================================================================
    actionable = parent_agg[parent_agg["markdown_probability"] >= 0.50].copy()

    for _, row in actionable.iterrows():
        parent = row["codigo_padre"]
        store = row["centro"]

        if (parent, store) in increase_keys:
            continue

        list_price = row["avg_precio_lista"]
        final_price = row["avg_precio_final"]
        disc_rate = row["discount_rate"] if pd.notna(row["discount_rate"]) else 0

        if pd.isna(list_price) or list_price <= 0:
            continue

        velocity = row["velocity_4w"]
        avg_prob = row["markdown_probability"]
        avg_raw_depth = row["raw_depth"]
        age = row.get("product_age_weeks", 0)

        recommended_step = snap_to_discount_step(avg_raw_depth)
        current_step = snap_to_discount_step(disc_rate)

        if recommended_step <= current_step:
            if avg_prob > 0.8 and current_step < 0.40:
                current_idx = DISCOUNT_STEPS.index(current_step) if current_step in DISCOUNT_STEPS else 0
                if current_idx + 1 < len(DISCOUNT_STEPS):
                    recommended_step = DISCOUNT_STEPS[current_idx + 1]
                else:
                    continue
            else:
                continue

        recommended_price = round_to_clp(list_price * (1 - recommended_step))
        current_final_rounded = round_to_clp(final_price) if pd.notna(final_price) else 0
        if abs(recommended_price - current_final_rounded) < 2000:
            continue

        elasticity = elast_map.get(parent)
        expected_velocity = compute_expected_velocity(velocity, disc_rate, recommended_step, elasticity)

        urgency, reasons = classify_urgency(row)

        current_weekly_rev = velocity * (final_price if pd.notna(final_price) else list_price)
        expected_weekly_rev = expected_velocity * recommended_price

        n_sell = int(row["sizes_selling"])
        n_total = int(row["total_sizes_catalog"])

        parent_actions.append({
            "parent_sku": parent,
            "store": store,
            "store_name": store_names.get(store, store),
            "product": str(row.get("product_name", parent))[:50],
            "category": row.get("primera_jerarquia", ""),
            "subcategory": row.get("segunda_jerarquia", ""),
            "sizes_selling": n_sell,
            "sizes_total": n_total,
            "product_age_weeks": int(age),
            "current_list_price": int(list_price),
            "current_price": current_final_rounded,
            "current_discount": f"{current_step:.0%}",
            "current_velocity": round(velocity, 1),
            "recommended_price": recommended_price,
            "recommended_discount": f"{recommended_step:.0%}",
            "expected_velocity": round(expected_velocity, 1),
            "current_weekly_rev": int(current_weekly_rev) if pd.notna(current_weekly_rev) else 0,
            "expected_weekly_rev": int(expected_weekly_rev),
            "rev_delta": int(expected_weekly_rev - current_weekly_rev) if pd.notna(current_weekly_rev) else 0,
            "urgency": urgency,
            "reasons": "; ".join(reasons),
            "model_confidence": round(avg_prob, 3),
            "action_type": "decrease",
        })

    actions_df = pd.DataFrame(parent_actions)

    if len(actions_df) == 0:
        print(f"  [{brand}] No pricing actions recommended this week.")
        return None

    # Sort: increases first (margin recovery), then markdowns by urgency
    urgency_order = {"INCREASE": -1, "HIGH": 0, "MEDIUM": 1, "LOW": 2}
    actions_df["_urgency_sort"] = actions_df["urgency"].map(urgency_order)
    actions_df = actions_df.sort_values(["_urgency_sort", "rev_delta"], ascending=[True, False])
    actions_df.drop(columns=["_urgency_sort"], inplace=True)

    # Save
    filename = f"pricing_actions_{target_week.date()}"
    actions_df.to_csv(output_dir / f"{filename}.csv", index=False)

    # Print the action list
    n_decreases = len(actions_df[actions_df["action_type"] == "decrease"])
    n_increases = len(actions_df[actions_df["action_type"] == "increase"])

    print(f"\n{'=' * 100}")
    print(f"[{brand}] WEEKLY PRICING ACTIONS -- {target_week.date()}")
    print(f"{'=' * 100}")
    print(f"Total actions: {len(actions_df)} ({n_increases} price increases, {n_decreases} markdowns)\n")

    # Price increases first
    increases = actions_df[actions_df["action_type"] == "increase"]
    if len(increases) > 0:
        print(f"\n--- PRICE INCREASES ({len(increases)} -- margin recovery) ---")
        print(f"{'Parent SKU':<22} {'Store':<12} {'Current':>12} {'-> Rec Price':>12} {'Disc':>8} {'Vel Now':>8} {'Vel Exp':>8} {'Reason'}")
        print("-" * 100)
        for _, row in increases.iterrows():
            print(
                f"{row['parent_sku']:<22} "
                f"{row['store_name']:<12} "
                f"${row['current_price']:>10,} "
                f"-> ${row['recommended_price']:>9,} "
                f"{'^ ' + row['recommended_discount']:>7} "
                f"{row['current_velocity']:>7.1f} "
                f"{row['expected_velocity']:>7.1f} "
                f"{row['reasons'][:40]}"
            )

    # Then markdowns by urgency
    markdowns = actions_df[actions_df["action_type"] == "decrease"]
    for urgency in ["HIGH", "MEDIUM", "LOW"]:
        subset = markdowns[markdowns["urgency"] == urgency]
        if len(subset) == 0:
            continue

        print(f"\n--- {urgency} URGENCY ({len(subset)} actions) ---")
        print(f"{'Parent SKU':<22} {'Store':<12} {'Current':>12} {'-> Rec Price':>12} {'Disc':>6} {'Vel Now':>8} {'Vel Exp':>8} {'Reason'}")
        print("-" * 100)

        for _, row in subset.iterrows():
            print(
                f"{row['parent_sku']:<22} "
                f"{row['store_name']:<12} "
                f"${row['current_price']:>10,} "
                f"-> ${row['recommended_price']:>9,} "
                f"{row['recommended_discount']:>5} "
                f"{row['current_velocity']:>7.1f} "
                f"{row['expected_velocity']:>7.1f} "
                f"{row['reasons'][:40]}"
            )

    # Summary
    print(f"\n{'=' * 100}")
    print(f"[{brand}] SUMMARY")
    print(f"  Total actions:     {len(actions_df)}")
    print(f"  HIGH urgency:      {(actions_df['urgency'] == 'HIGH').sum()}")
    print(f"  MEDIUM urgency:    {(actions_df['urgency'] == 'MEDIUM').sum()}")
    print(f"  LOW urgency:       {(actions_df['urgency'] == 'LOW').sum()}")
    print(f"  Stores affected:   {actions_df['store'].nunique()}")
    total_rev_delta = actions_df["rev_delta"].sum()
    print(f"  Expected weekly rev delta: ${total_rev_delta:+,.0f} CLP")
    print(f"\n  Saved to: {output_dir / filename}.csv")

    return actions_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, required=True, help="Brand name (e.g. HOKA, BOLD)")
    parser.add_argument("--week", type=str, default=None)
    args = parser.parse_args()
    generate_weekly_actions_for_brand(args.brand, target_week=args.week)
