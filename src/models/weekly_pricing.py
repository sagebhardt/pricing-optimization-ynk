"""
Weekly Pricing Action List for HOKA.

Generates the actual weekly output the commercial team needs:
- Which parent SKUs to reprice this week
- Current price → recommended price (in CLP, rounded to xxx,990)
- Expected unit lift at new price
- Urgency level and reason
- Grouped by parent SKU (not individual sizes)

Respects business reality:
- Prices change once per week
- Discount ladder: 0% → 15% → 20% → 30% → 40%
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

PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
MODEL_DIR = Path(__file__).parent.parent.parent / "models"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "weekly_actions"

# HOKA's actual discount ladder
DISCOUNT_STEPS = [0.0, 0.15, 0.20, 0.30, 0.40]

STORE_NAMES = {
    "7501": "Costanera",
    "7502": "Marina",
    "AB75": "E-commerce",
    "7599": "Eventos",
}

CATEGORICAL_COLS = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]
EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
]


# Cognitive price anchors — each sits just below a round psychological threshold.
PRICE_ANCHORS = [
    990, 1990, 2990, 3990, 4990, 5990, 6990, 7990, 8990, 9990,
    12990, 14990, 16990, 19990,
    24990, 29990, 34990, 39990, 44990, 49990,
    54990, 59990, 69990, 79990, 89990, 99990,
    109990, 119990, 129990, 139990, 149990, 169990, 199990,
    249990, 299990, 399990, 499990, 599990, 799990, 999990,
]


def snap_to_price_anchor(price, direction="down"):
    """
    Snap price to the nearest cognitive price anchor.
    direction: "down" (markdowns), "up" (increases), "nearest" (display).
    """
    if price <= 0:
        return 0
    if price > PRICE_ANCHORS[-1]:
        step = 10000
        if direction == "down":
            return int(price // step) * step - 10
        elif direction == "up":
            return (int(price // step) + 1) * step - 10
        else:
            return int(round(price / step) * step - 10)
    if direction == "down":
        valid = [a for a in PRICE_ANCHORS if a <= price]
        return valid[-1] if valid else PRICE_ANCHORS[0]
    elif direction == "up":
        valid = [a for a in PRICE_ANCHORS if a >= price]
        return valid[0] if valid else PRICE_ANCHORS[-1]
    else:
        return min(PRICE_ANCHORS, key=lambda a: abs(a - price))


def snap_to_discount_step(discount_pct):
    """Snap a continuous discount % to HOKA's actual discount ladder."""
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
        # Use elasticity: price change % → volume change %
        price_change_pct = -disc_change  # Negative = price decrease
        volume_change = price_change_pct * elasticity  # Elasticity is negative, so double negative = positive
        return max(current_vel * (1 + volume_change), 0.5)
    else:
        # Empirical fallback: observed lift patterns from HOKA data
        # Based on the markdown ladder analysis above
        lift_by_step = {
            0.15: 1.8,   # 15% discount → ~80% more units
            0.20: 2.2,   # 20% → ~120% more
            0.30: 3.0,   # 30% → ~200% more
            0.40: 4.0,   # 40% → ~300% more
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


def generate_weekly_actions(target_week=None):
    """
    Generate the weekly pricing action list.
    This is what the commercial team reviews every Monday.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading models and data...")

    # Load v2 model
    try:
        with open(MODEL_DIR / "markdown_classifier_v2.pkl", "rb") as f:
            cls_model = pickle.load(f)
        with open(MODEL_DIR / "depth_regressor_v2.pkl", "rb") as f:
            reg_model = pickle.load(f)
    except FileNotFoundError:
        with open(MODEL_DIR / "markdown_classifier.pkl", "rb") as f:
            cls_model = pickle.load(f)
        with open(MODEL_DIR / "depth_regressor.pkl", "rb") as f:
            reg_model = pickle.load(f)

    # Load features
    try:
        features = pd.read_parquet(PROCESSED_DIR / "hoka_features_v2.parquet")
    except FileNotFoundError:
        features = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")

    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")

    # Load elasticity
    try:
        sku_elast = pd.read_parquet(PROCESSED_DIR / "elasticity_by_sku.parquet")
        elast_map = sku_elast[sku_elast["confidence"].isin(["high", "medium"])].set_index("codigo_padre")["elasticity"].to_dict()
    except FileNotFoundError:
        elast_map = {}

    # Determine target week
    if target_week is None:
        target_week = features["week"].max()
    else:
        target_week = pd.Timestamp(target_week)

    print(f"Generating weekly pricing actions for: {target_week.date()}")

    # Get current week data
    week_data = features[features["week"] == target_week].copy()
    if len(week_data) == 0:
        print(f"  No data for {target_week.date()}")
        return None

    # Score
    df_prep = week_data.copy()
    for col in CATEGORICAL_COLS:
        if col in df_prep.columns:
            df_prep[col] = df_prep[col].astype("category").cat.codes
    feat_cols = [c for c in df_prep.columns if c not in EXCLUDE_COLS]
    X = df_prep[feat_cols].values

    week_data["markdown_probability"] = cls_model.predict_proba(X)[:, 1]
    week_data["raw_depth"] = np.clip(reg_model.predict(X), 0, 0.60)

    # Product info
    prod_info = products[["material", "material_descripcion", "codigo_padre",
                           "primera_jerarquia", "segunda_jerarquia", "talla"]].rename(
        columns={"material": "sku", "material_descripcion": "product_name"}
    ).drop_duplicates(subset=["sku"])
    week_data = week_data.merge(prod_info[["sku", "product_name"]], on="sku", how="left")

    # ================================================================
    # PASS 1: Price INCREASES (recover margin on over-discounted SKUs)
    # ================================================================
    currently_discounted = week_data[
        (week_data["discount_rate"].notna()) & (week_data["discount_rate"] > 0.05)
    ].copy()

    parent_actions = []

    for (parent, store), group in currently_discounted.groupby(["codigo_padre", "centro"]):
        current_list_price = group["avg_precio_lista"].median()
        current_final_price = group["avg_precio_final"].median()
        current_disc_rate = group["discount_rate"].median()
        if pd.isna(current_disc_rate) or pd.isna(current_list_price) or current_list_price <= 0:
            continue

        current_step = snap_to_discount_step(current_disc_rate)
        if current_step == 0:
            continue

        current_velocity = group["velocity_4w"].mean()
        vel_trend = group["velocity_trend"].mean()
        avg_prob = group["markdown_probability"].mean()
        first_row = group.iloc[0]
        product_age = first_row.get("product_age_weeks", 0)
        lifecycle = first_row.get("lifecycle_stage_code")
        n_sizes_selling = (group["units_sold"] > 0).sum()
        n_sizes_total = len(group)

        # Criteria for price increase (demand-based, not model-based):
        # The classifier is circular on discounted items, so use direct signals:
        # 1. Velocity is strong (selling well despite/because of discount)
        # 2. Sales are not decelerating
        # 3. Not in decline/clearance lifecycle
        # 4. Size curve is healthy (not breaking apart)
        attrition = first_row.get("attrition_rate", 0)
        attrition = attrition if pd.notna(attrition) else 0

        should_increase = (
            current_velocity >= 1.0
            and vel_trend >= 1.0  # Stable or accelerating
            and lifecycle not in (4, 5)  # Not decline or clearance
            and attrition < 0.3  # Size curve not breaking
            and product_age < 35  # Not too old
        )

        if not should_increase:
            continue

        # Step UP the ladder (reduce discount)
        current_idx = DISCOUNT_STEPS.index(current_step) if current_step in DISCOUNT_STEPS else len(DISCOUNT_STEPS) - 1
        if current_idx <= 0:
            continue
        recommended_step = DISCOUNT_STEPS[current_idx - 1]

        recommended_price = snap_to_price_anchor(current_list_price * (1 - recommended_step), direction="up")
        current_final_rounded = snap_to_price_anchor(current_final_price, direction="nearest") if pd.notna(current_final_price) else 0

        if recommended_price <= current_final_rounded + 2000:
            continue

        # Estimate velocity at higher price (will drop somewhat)
        # Conservative: assume 20-30% volume loss per discount step removed
        vol_loss = 0.25
        expected_velocity = max(current_velocity * (1 - vol_loss), 0.3)

        # Revenue: higher price × lower volume — usually net positive for margin
        current_weekly_rev = current_velocity * (current_final_price if pd.notna(current_final_price) else current_list_price)
        expected_weekly_rev = expected_velocity * recommended_price

        reasons = []
        reasons.append(f"Selling {current_velocity:.1f} u/sem at {current_step:.0%} off — test higher price")
        if vel_trend > 1.1:
            reasons.append("Sales accelerating")
        if n_sizes_selling >= n_sizes_total * 0.7:
            reasons.append(f"Size curve healthy ({n_sizes_selling}/{n_sizes_total})")
        if product_age < 20:
            reasons.append(f"Young product ({int(product_age)} weeks)")

        parent_actions.append({
            "parent_sku": parent,
            "store": store,
            "store_name": STORE_NAMES.get(store, store),
            "product": first_row.get("product_name", "")[:50] if pd.notna(first_row.get("product_name")) else parent,
            "category": first_row.get("primera_jerarquia", ""),
            "subcategory": first_row.get("segunda_jerarquia", ""),
            "sizes_selling": n_sizes_selling,
            "sizes_total": n_sizes_total,
            "product_age_weeks": int(product_age),
            "current_list_price": int(current_list_price),
            "current_price": current_final_rounded,
            "current_discount": f"{current_step:.0%}",
            "current_velocity": round(current_velocity, 1),
            "recommended_price": recommended_price,
            "recommended_discount": f"{recommended_step:.0%}" if recommended_step > 0 else "Full price",
            "expected_velocity": round(expected_velocity, 1),
            "current_weekly_rev": int(current_weekly_rev) if pd.notna(current_weekly_rev) else 0,
            "expected_weekly_rev": int(expected_weekly_rev),
            "rev_delta": int(expected_weekly_rev - current_weekly_rev) if pd.notna(current_weekly_rev) else 0,
            "urgency": "INCREASE",
            "reasons": "; ".join(reasons),
            "model_confidence": round(avg_prob, 3),
            "action_type": "increase",
        })

    n_increases = len(parent_actions)

    # ================================================================
    # PASS 2: Price DECREASES (markdowns)
    # ================================================================
    actionable = week_data[week_data["markdown_probability"] >= 0.50].copy()

    # Track which parent-stores already got an increase recommendation
    increase_keys = {(a["parent_sku"], a["store"]) for a in parent_actions}

    for (parent, store), group in actionable.groupby(["codigo_padre", "centro"]):
        if (parent, store) in increase_keys:
            continue  # Already recommending increase for this one
        # Current state
        current_list_price = group["avg_precio_lista"].median()
        current_final_price = group["avg_precio_final"].median()
        current_disc_rate = group["discount_rate"].median() if group["discount_rate"].notna().any() else 0
        current_disc_rate = current_disc_rate if pd.notna(current_disc_rate) else 0

        current_velocity = group["velocity_4w"].mean()
        n_sizes_selling = (group["units_sold"] > 0).sum()
        n_sizes_total = len(group)
        avg_prob = group["markdown_probability"].mean()
        avg_raw_depth = group["raw_depth"].mean()

        # Get product info from first row
        first_row = group.iloc[0]
        product_age = first_row.get("product_age_weeks", 0)

        # Skip if no valid list price
        if pd.isna(current_list_price) or current_list_price <= 0:
            continue

        # Snap model recommendation to discount ladder
        recommended_step = snap_to_discount_step(avg_raw_depth)

        # Don't recommend going backward (reducing discount)
        current_step = snap_to_discount_step(current_disc_rate)
        if recommended_step <= current_step:
            # Model thinks current level is fine or should go deeper?
            if avg_prob > 0.8 and current_step < 0.40:
                # Nudge to next step
                current_idx = DISCOUNT_STEPS.index(current_step) if current_step in DISCOUNT_STEPS else 0
                if current_idx + 1 < len(DISCOUNT_STEPS):
                    recommended_step = DISCOUNT_STEPS[current_idx + 1]
                else:
                    continue  # Already at max discount
            else:
                continue  # No action needed

        # Calculate recommended price
        recommended_price = snap_to_price_anchor(current_list_price * (1 - recommended_step), direction="nearest")

        # If recommended price equals current final price, skip
        current_final_rounded = snap_to_price_anchor(current_final_price, direction="nearest") if pd.notna(current_final_price) else 0
        if abs(recommended_price - current_final_rounded) < 2000:
            continue

        # Expected velocity at new price
        elasticity = elast_map.get(parent)
        expected_velocity = compute_expected_velocity(
            current_velocity, current_disc_rate, recommended_step, elasticity
        )

        # Urgency
        urgency, reasons = classify_urgency(first_row)

        # Revenue comparison (weekly)
        current_weekly_rev = current_velocity * (current_final_price if pd.notna(current_final_price) else current_list_price)
        expected_weekly_rev = expected_velocity * recommended_price

        parent_actions.append({
            "parent_sku": parent,
            "store": store,
            "store_name": STORE_NAMES.get(store, store),
            "product": first_row.get("product_name", "")[:50] if pd.notna(first_row.get("product_name")) else parent,
            "category": first_row.get("primera_jerarquia", ""),
            "subcategory": first_row.get("segunda_jerarquia", ""),
            "sizes_selling": n_sizes_selling,
            "sizes_total": n_sizes_total,
            "product_age_weeks": int(product_age),
            # Current state
            "current_list_price": int(current_list_price),
            "current_price": current_final_rounded,
            "current_discount": f"{current_step:.0%}",
            "current_velocity": round(current_velocity, 1),
            # Recommendation
            "recommended_price": recommended_price,
            "recommended_discount": f"{recommended_step:.0%}",
            "expected_velocity": round(expected_velocity, 1),
            # Impact
            "current_weekly_rev": int(current_weekly_rev) if pd.notna(current_weekly_rev) else 0,
            "expected_weekly_rev": int(expected_weekly_rev),
            "rev_delta": int(expected_weekly_rev - current_weekly_rev) if pd.notna(current_weekly_rev) else 0,
            # Urgency
            "urgency": urgency,
            "reasons": "; ".join(reasons),
            "model_confidence": round(avg_prob, 3),
            "action_type": "decrease",
        })

    actions_df = pd.DataFrame(parent_actions)

    if len(actions_df) == 0:
        print("  No pricing actions recommended this week.")
        return None

    # Sort: increases first (margin recovery), then markdowns by urgency
    urgency_order = {"INCREASE": -1, "HIGH": 0, "MEDIUM": 1, "LOW": 2}
    actions_df["_urgency_sort"] = actions_df["urgency"].map(urgency_order)
    actions_df = actions_df.sort_values(["_urgency_sort", "rev_delta"], ascending=[True, False])
    actions_df.drop(columns=["_urgency_sort"], inplace=True)

    # Save
    filename = f"pricing_actions_{target_week.date()}"
    actions_df.to_csv(OUTPUT_DIR / f"{filename}.csv", index=False)

    # Print the action list
    n_decreases = len(actions_df[actions_df["action_type"] == "decrease"])
    n_increases = len(actions_df[actions_df["action_type"] == "increase"])

    print(f"\n{'=' * 100}")
    print(f"WEEKLY PRICING ACTIONS — {target_week.date()}")
    print(f"{'=' * 100}")
    print(f"Total actions: {len(actions_df)} ({n_increases} price increases, {n_decreases} markdowns)\n")

    # Price increases first
    increases = actions_df[actions_df["action_type"] == "increase"]
    if len(increases) > 0:
        print(f"\n--- PRICE INCREASES ({len(increases)} — margin recovery) ---")
        print(f"{'Parent SKU':<22} {'Store':<12} {'Current':>12} {'→ Rec Price':>12} {'Disc':>8} {'Vel Now':>8} {'Vel Exp':>8} {'Reason'}")
        print("-" * 100)
        for _, row in increases.iterrows():
            print(
                f"{row['parent_sku']:<22} "
                f"{row['store_name']:<12} "
                f"${row['current_price']:>10,} "
                f"→ ${row['recommended_price']:>9,} "
                f"{'↑ ' + row['recommended_discount']:>7} "
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
        print(f"{'Parent SKU':<22} {'Store':<12} {'Current':>12} {'→ Rec Price':>12} {'Disc':>6} {'Vel Now':>8} {'Vel Exp':>8} {'Reason'}")
        print("-" * 100)

        for _, row in subset.iterrows():
            print(
                f"{row['parent_sku']:<22} "
                f"{row['store_name']:<12} "
                f"${row['current_price']:>10,} "
                f"→ ${row['recommended_price']:>9,} "
                f"{row['recommended_discount']:>5} "
                f"{row['current_velocity']:>7.1f} "
                f"{row['expected_velocity']:>7.1f} "
                f"{row['reasons'][:40]}"
            )

    # Summary
    print(f"\n{'=' * 100}")
    print(f"SUMMARY")
    print(f"  Total actions:     {len(actions_df)}")
    print(f"  HIGH urgency:      {(actions_df['urgency'] == 'HIGH').sum()}")
    print(f"  MEDIUM urgency:    {(actions_df['urgency'] == 'MEDIUM').sum()}")
    print(f"  LOW urgency:       {(actions_df['urgency'] == 'LOW').sum()}")
    print(f"  Stores affected:   {actions_df['store'].nunique()}")
    total_rev_delta = actions_df["rev_delta"].sum()
    print(f"  Expected weekly rev delta: ${total_rev_delta:+,.0f} CLP")
    print(f"\n  Saved to: {OUTPUT_DIR / filename}.csv")

    return actions_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", type=str, default=None)
    args = parser.parse_args()
    generate_weekly_actions(target_week=args.week)
