"""
Brand-agnostic Weekly Pricing Action List.

Thin wrapper around the HOKA-specific logic in weekly_pricing.py,
parameterized by brand name. Reads from data/processed/{brand}/,
models/{brand}/, and data/raw/{brand}/. Writes to
weekly_actions/{brand}/.

Generates the actual weekly output the commercial team needs:
- Which parent SKUs to reprice this week
- Current price -> recommended price (snapped to cognitive price anchors)
- Expected unit lift at new price
- Urgency level and reason
- Grouped by parent SKU (not individual sizes)

Respects business reality:
- Prices change once per week
- Discount ladder: 0% -> 15% -> 20% -> 30% -> 40%
- Prices snap to cognitive anchors (just below round thresholds: 9990, 14990, 19990, 24990, etc.)
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
from config.database import BRANDS, EXCLUDE_STORES_PRICING
from src.models.pricing_simulation import (
    DISCOUNT_STEPS,
    MIN_MARGIN_PCT,
    PRICE_ANCHORS,
    snap_to_price_anchor,
    snap_to_discount_step,
    compute_expected_velocity,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent

CATEGORICAL_COLS = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]
EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
    "should_reprice", "optimal_disc_margin", "optimal_profit",
    "click_collect_units", "instore_units", "instore_velocity_4w", "click_collect_ratio",
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
        if "centro" in stores.columns and "sucursal" in stores.columns:
            # sucursal format: "B050-Bamers Curico II" -> extract name after the dash
            mapping = {}
            for _, row in stores.iterrows():
                code = row["centro"]
                suc = str(row["sucursal"])
                name = suc.split("-", 1)[1].strip() if "-" in suc else suc
                mapping[code] = name
            return mapping
        elif "centro" in stores.columns and "nombre" in stores.columns:
            return stores.set_index("centro")["nombre"].to_dict()
        else:
            return {}
    except (FileNotFoundError, KeyError):
        return {}


def classify_urgency(row):
    """
    Determine urgency level and reason for markdown.

    Returns (urgency, reasons) where urgency is HIGH/MEDIUM/LOW.
    """
    reasons = []
    urgency_score = 0

    # Click & collect context: flag C&C ratio so brand managers see the channel mix.
    # Note: C&C sales DO respond to pricing (ecomm price ≈ store price 54% of the time),
    # so total velocity is the correct signal for impact projections.
    cc_ratio = row.get("click_collect_ratio", 0)
    cc_ratio = cc_ratio if pd.notna(cc_ratio) else 0
    if cc_ratio > 0.3:
        instore_vel = row.get("instore_velocity_4w", 0)
        instore_vel = instore_vel if pd.notna(instore_vel) else 0
        reasons.append(f"Click&collect {cc_ratio:.0%} of sales (in-store: {instore_vel:.1f} u/sem)")

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

    # Inventory pressure (when stock data available)
    woc = row.get("weeks_of_cover")
    stock_on_hand = row.get("stock_on_hand")
    if pd.notna(woc) and pd.notna(stock_on_hand) and stock_on_hand > 0:
        if woc > 20 and vel_trend <= 1.0:
            reasons.append(f"Overstocked ({woc:.0f} weeks of cover)")
            urgency_score += 3
        elif woc > 12 and vel_trend < 0.8:
            reasons.append(f"High stock ({woc:.0f} WoC) + slowing sales")
            urgency_score += 2
        elif woc > 8:
            reasons.append(f"Elevated stock ({woc:.0f} WoC)")
            urgency_score += 1

    # Volume-weighted size availability (best sellers out of stock = critical)
    weighted_avail = row.get("weighted_size_avail")
    top3_in_stock = row.get("top3_sizes_in_stock")
    rev_at_risk = row.get("revenue_at_risk_pct")

    if pd.notna(weighted_avail) and pd.notna(stock_on_hand) and stock_on_hand > 0:
        if weighted_avail < 0.3:
            reasons.append(f"Best sellers out of stock ({weighted_avail:.0%} vol-weighted avail)")
            urgency_score += 3
        elif weighted_avail < 0.5:
            reasons.append(f"Key sizes depleting ({weighted_avail:.0%} vol-weighted avail)")
            urgency_score += 2
        elif weighted_avail < 0.7:
            reasons.append(f"Size run weakening ({weighted_avail:.0%} vol-weighted avail)")
            urgency_score += 1
    elif pd.notna(row.get("pct_sizes_in_stock")):
        # Fallback to unweighted if weighted not available
        pct_sizes = row.get("pct_sizes_in_stock")
        if pct_sizes < 0.4 and pd.notna(stock_on_hand) and stock_on_hand > 0:
            reasons.append(f"Broken size run ({pct_sizes:.0%} sizes in stock)")
            urgency_score += 2

    if pd.notna(top3_in_stock) and top3_in_stock < 0.5:
        reasons.append(f"Top sellers stockout ({top3_in_stock:.0%} of top 3 in stock)")
        urgency_score += 2

    # Already discounted elsewhere (price consistency)
    if row.get("max_discount_rate", 0) > 0.1 and row.get("discount_rate", 0) < 0.05:
        reasons.append("Discounted in other channels")
        urgency_score += 1

    # Competitor pricing (only adds urgency when our velocity is already weak)
    comp_undercut = row.get("comp_undercut", 0)
    velocity = row.get("velocity_4w", 0)
    if comp_undercut and pd.notna(comp_undercut) and comp_undercut > 0:
        comp_gap = row.get("comp_price_gap_pct")
        gap_str = f"{comp_gap:+.0f}%" if pd.notna(comp_gap) else ""
        if velocity < 0.5 or vel_trend < 0.7:
            # Losing sales AND competitor is cheaper → real pressure
            reasons.append(f"Competitor cheaper {gap_str} + velocity weak")
            urgency_score += 2
        else:
            # Competitor cheaper but we're selling fine → informational only, no urgency
            reasons.append(f"Competitor cheaper {gap_str} — velocity healthy, hold price")

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


def compute_confidence_tier(prob, velocity, age, has_elasticity, action_type):
    """
    Rate how trustworthy a recommendation is.

    HIGH:        Strong demand signals + elasticity data
    MEDIUM:      Decent signals, some gaps
    LOW:         Weak signals, high uncertainty
    SPECULATIVE: Fabricated velocity estimates (increases w/o elasticity)
    """
    if action_type == "increase" and not has_elasticity:
        return "SPECULATIVE"

    score = 0
    if prob > 0.85:
        score += 3
    elif prob > 0.70:
        score += 2
    elif prob > 0.50:
        score += 1

    if has_elasticity:
        score += 2
    if velocity >= 2.0:
        score += 2
    elif velocity >= 1.0:
        score += 1

    if age >= 8:
        score += 1  # enough history

    if score >= 6:
        return "HIGH"
    if score >= 4:
        return "MEDIUM"
    return "LOW"


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

    # Load costs (prefix match — costs are typically at parent-SKU level)
    cost_map = {}
    costs_path = raw / "costs.parquet"
    if costs_path.exists():
        costs_df = pd.read_parquet(costs_path)
        costs_df = costs_df[costs_df["cost"] > 0].dropna(subset=["cost"])
        cost_map = costs_df.set_index("sku")["cost"].to_dict()
        print(f"  Costs loaded: {len(cost_map)} SKUs")
    else:
        print(f"  No cost data — margin analysis unavailable")

    cost_sorted_keys = sorted(cost_map.keys(), key=len, reverse=True) if cost_map else []

    def _get_cost(sku):
        """Lookup unit cost by exact or prefix match."""
        if sku in cost_map:
            return cost_map[sku]
        for k in cost_sorted_keys:
            if sku.startswith(k):
                return cost_map[k]
        return None

    # Determine target week — use most recent week with sufficient data
    if target_week is None:
        # Count rows per week, pick latest with >= 10 parent-store rows
        week_counts = features.groupby("week").size()
        viable_weeks = week_counts[week_counts >= 10].index
        if len(viable_weeks) > 0:
            target_week = viable_weeks.max()
        else:
            target_week = features["week"].max()
    else:
        target_week = pd.Timestamp(target_week)

    print(f"[{brand}] Generating weekly pricing actions for: {target_week.date()}")

    # Get current week data
    week_data = features[features["week"] == target_week].copy()
    if len(week_data) == 0:
        print(f"  No data for {target_week.date()}")
        return None

    # Filter out non-retail stores (logistics, digital, internal)
    store_col = "centro" if "centro" in week_data.columns else None
    if store_col:
        excluded = [s for s in EXCLUDE_STORES_PRICING if week_data[store_col].str.contains(s, na=False).any()]
        if excluded:
            n_before = len(week_data)
            week_data = week_data[~week_data[store_col].str.startswith(tuple(EXCLUDE_STORES_PRICING))]
            print(f"  Filtered {n_before - len(week_data)} rows from non-retail stores: {excluded}")

        # Also filter to active stores if configured
        brand_cfg = BRANDS.get(brand.upper(), {})
        active_stores = brand_cfg.get("stores_active")
        if active_stores:
            n_before = len(week_data)
            week_data = week_data[week_data[store_col].isin(active_stores)]
            print(f"  Filtered to {len(week_data)} rows across {len(active_stores)} active stores (from {n_before})")

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

        # Don't increase price if overstocked (WoC > 15)
        woc = row.get("weeks_of_cover")
        overstocked = pd.notna(woc) and woc > 15

        should_increase = (
            velocity >= 1.0
            and vel_trend >= 1.0
            and lifecycle not in (4, 5)
            and attrition < 0.3
            and age < 35
            and not overstocked
        )
        if not should_increase:
            continue

        current_idx = DISCOUNT_STEPS.index(current_step) if current_step in DISCOUNT_STEPS else len(DISCOUNT_STEPS) - 1
        if current_idx <= 0:
            continue
        recommended_step = DISCOUNT_STEPS[current_idx - 1]
        raw_rec = list_price * (1 - recommended_step)
        recommended_price = snap_to_price_anchor(raw_rec, direction="up")
        list_price_anchor = snap_to_price_anchor(list_price, direction="nearest")

        # If snap pushed above list price, only allow if velocity justifies premium
        is_premium = recommended_price > list_price
        if is_premium and (pd.isna(velocity) or velocity < 2.0):
            recommended_price = list_price_anchor
        current_final_rounded = snap_to_price_anchor(final_price, direction="nearest") if pd.notna(final_price) else 0

        if recommended_price <= current_final_rounded + 2000:
            continue

        elasticity = elast_map.get(row["codigo_padre"])
        has_elast = elasticity is not None and elasticity < -0.3

        if has_elast:
            price_increase_pct = (recommended_price - current_final_rounded) / max(current_final_rounded, 1)
            vol_change = price_increase_pct * elasticity  # elasticity is negative → vol drops
            expected_velocity = max(velocity * (1 + vol_change), 0.3)
        else:
            vol_loss = 0.25
            expected_velocity = max(velocity * (1 - vol_loss), 0.3)

        current_weekly_rev = velocity * current_final_rounded
        expected_weekly_rev = expected_velocity * recommended_price

        if is_premium:
            confidence_tier = "SPECULATIVE"
        else:
            confidence_tier = compute_confidence_tier(
                row["markdown_probability"], velocity, age, has_elast, "increase"
            )

        reasons = [f"Selling {velocity:.1f} u/sem at {current_step:.0%} off -- test higher price"]
        if is_premium:
            reasons.append(f"PREMIUM: above list price ({list_price_anchor:,.0f} -> {recommended_price:,.0f})")
        if not has_elast:
            reasons.append("Sin elasticidad — volumen estimado")
        if vel_trend > 1.1:
            reasons.append("Sales accelerating")
        n_sell = int(row["sizes_selling"])
        n_total = int(row["total_sizes_catalog"])
        if n_sell >= n_total * 0.7:
            reasons.append(f"Size curve healthy ({n_sell}/{n_total})")

        # Stock + cost info
        _stock = int(row["stock_on_hand"]) if pd.notna(row.get("stock_on_hand")) else None
        _woc = round(row["weeks_of_cover"], 1) if pd.notna(row.get("weeks_of_cover")) else None
        unit_cost = _get_cost(row["codigo_padre"])

        # Margin calculations (strip IVA 19% — prices include tax, costs are net)
        if unit_cost:
            current_neto = current_final_rounded / 1.19
            rec_neto = recommended_price / 1.19
            current_margin_unit = current_neto - unit_cost
            rec_margin_unit = rec_neto - unit_cost
            current_margin_weekly = current_margin_unit * velocity
            expected_margin_weekly = rec_margin_unit * expected_velocity
            margin_delta = int(expected_margin_weekly - current_margin_weekly)
            rec_margin_pct = round(rec_margin_unit / rec_neto * 100, 1) if rec_neto > 0 else 0
            reasons.append(f"Margin recovery: {rec_margin_pct:.0f}% at new price")
            if rec_margin_pct < 20:
                reasons.append(f"THIN MARGIN: {rec_margin_pct:.0f}% at recommended price")
        else:
            margin_delta = None
            rec_margin_pct = None

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
            "stock_on_hand": _stock,
            "weeks_of_cover": _woc,
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
            "unit_cost": int(unit_cost) if unit_cost else None,
            "margin_pct": rec_margin_pct,
            "margin_delta": margin_delta,
            "urgency": "INCREASE",
            "reasons": "; ".join(reasons),
            "model_confidence": round(row["markdown_probability"], 3),
            "confidence_tier": confidence_tier,
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

        recommended_price = snap_to_price_anchor(list_price * (1 - recommended_step), direction="nearest")
        current_final_rounded = snap_to_price_anchor(final_price, direction="nearest") if pd.notna(final_price) else 0
        if abs(recommended_price - current_final_rounded) < 2000:
            continue

        elasticity = elast_map.get(parent)
        has_elast = elasticity is not None and elasticity < -0.3
        expected_velocity = compute_expected_velocity(velocity, disc_rate, recommended_step, elasticity)

        urgency, reasons = classify_urgency(row)
        confidence_tier = compute_confidence_tier(avg_prob, velocity, age, has_elast, "decrease")

        current_weekly_rev = velocity * current_final_rounded
        expected_weekly_rev = expected_velocity * recommended_price

        n_sell = int(row["sizes_selling"])
        n_total = int(row["total_sizes_catalog"])

        # Stock + cost info
        _stock = int(row["stock_on_hand"]) if pd.notna(row.get("stock_on_hand")) else None
        _woc = round(row["weeks_of_cover"], 1) if pd.notna(row.get("weeks_of_cover")) else None
        unit_cost = _get_cost(parent)

        # Margin calculations
        if unit_cost:
            # Block below-cost recommendations — step back to a shallower discount
            # Compare net (excl IVA) price against net cost
            if recommended_price / 1.19 < unit_cost:
                # Find the shallowest discount that stays above cost (net)
                for step in DISCOUNT_STEPS:
                    candidate = snap_to_price_anchor(list_price * (1 - step), direction="nearest")
                    if candidate / 1.19 >= unit_cost:
                        recommended_step = step
                        recommended_price = candidate
                        break
                else:
                    continue  # Can't find a price above cost — skip

                if abs(recommended_price - current_final_rounded) < 2000:
                    continue

                reasons.append(f"Discount capped to protect margin (cost={unit_cost:,.0f})")
                expected_velocity = compute_expected_velocity(velocity, disc_rate, recommended_step, elasticity)
                expected_weekly_rev = expected_velocity * recommended_price

            # Margin floor: step back to shallower discount if margin < MIN_MARGIN_PCT
            rec_neto = recommended_price / 1.19
            rec_margin_unit = rec_neto - unit_cost
            rec_margin_pct = round(rec_margin_unit / rec_neto * 100, 1) if rec_neto > 0 else 0

            if rec_margin_pct < MIN_MARGIN_PCT:
                original_step = recommended_step
                for step in DISCOUNT_STEPS:
                    candidate = snap_to_price_anchor(list_price * (1 - step), direction="nearest")
                    cand_neto = candidate / 1.19
                    cand_margin = (cand_neto - unit_cost) / cand_neto * 100 if cand_neto > 0 else 0
                    if cand_margin >= MIN_MARGIN_PCT:
                        recommended_step = step
                        recommended_price = candidate
                        break
                else:
                    continue  # Can't meet margin floor at any discount — skip

                # Margin floor may step back past current discount — no longer a markdown
                if recommended_step <= current_step:
                    continue

                if abs(recommended_price - current_final_rounded) < 2000:
                    continue

                if recommended_step != original_step:
                    reasons.append(f"Discount capped at {recommended_step:.0%} to protect margin (floor={MIN_MARGIN_PCT}%)")
                    expected_velocity = compute_expected_velocity(velocity, disc_rate, recommended_step, elasticity)
                    expected_weekly_rev = expected_velocity * recommended_price

            current_neto = current_final_rounded / 1.19
            rec_neto = recommended_price / 1.19
            current_margin_unit = current_neto - unit_cost
            rec_margin_unit = rec_neto - unit_cost
            current_margin_weekly = current_margin_unit * velocity
            expected_margin_weekly = rec_margin_unit * expected_velocity
            margin_delta = int(expected_margin_weekly - current_margin_weekly)
            rec_margin_pct = round(rec_margin_unit / rec_neto * 100, 1) if rec_neto > 0 else 0

            if rec_margin_pct < 20:
                reasons.append(f"THIN MARGIN: {rec_margin_pct:.0f}% at recommended price")
        else:
            margin_delta = None
            rec_margin_pct = None

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
            "stock_on_hand": _stock,
            "weeks_of_cover": _woc,
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
            "unit_cost": int(unit_cost) if unit_cost else None,
            "margin_pct": rec_margin_pct,
            "margin_delta": margin_delta,
            "urgency": urgency,
            "reasons": "; ".join(reasons),
            "model_confidence": round(avg_prob, 3),
            "confidence_tier": confidence_tier,
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

    # Add vendor brand for multi-brand banners
    from config.vendor_brands import get_vendor_brand, is_ecomm_store
    actions_df["vendor_brand"] = actions_df["parent_sku"].apply(lambda s: get_vendor_brand(s, brand))

    # Attach ecommerce price + true online velocity per parent SKU
    # (ecomm is a pricing channel that overlays all stores, not just another store)
    ecomm_rows = parent_agg[parent_agg["centro"].apply(is_ecomm_store)]
    if len(ecomm_rows) > 0:
        ecomm_prices = ecomm_rows.groupby("codigo_padre").agg(
            ecomm_price=("avg_precio_final", "median"),
            ecomm_discount=("discount_rate", "median"),
            ecomm_velocity=("velocity_4w", "sum"),
        ).reset_index().rename(columns={"codigo_padre": "parent_sku"})

        # True online velocity = ecomm delivery + C&C at all stores
        if "click_collect_units" in parent_agg.columns:
            cc_velocity = (
                parent_agg[~parent_agg["centro"].apply(is_ecomm_store)]
                .groupby("codigo_padre")["click_collect_units"]
                .sum()
                .rename("cc_velocity_total")
                .reset_index()
                .rename(columns={"codigo_padre": "parent_sku"})
            )
            ecomm_prices = ecomm_prices.merge(cc_velocity, on="parent_sku", how="left")
            ecomm_prices["ecomm_velocity"] = ecomm_prices["ecomm_velocity"] + ecomm_prices["cc_velocity_total"].fillna(0)
            ecomm_prices.drop(columns=["cc_velocity_total"], inplace=True)

        actions_df = actions_df.merge(ecomm_prices, on="parent_sku", how="left")
        # Price gap: positive = store is more expensive than ecomm
        actions_df["ecomm_price_gap_pct"] = np.where(
            actions_df["ecomm_price"].notna() & (actions_df["ecomm_price"] > 0),
            ((actions_df["current_price"] - actions_df["ecomm_price"]) / actions_df["current_price"] * 100).round(1),
            np.nan,
        )

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
