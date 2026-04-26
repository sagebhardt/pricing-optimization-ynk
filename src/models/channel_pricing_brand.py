"""
Channel-level pricing aggregation (parent SKU x channel).

Rolls up per-store pricing recommendations into per-channel actions
(B&M vs ecomm), re-running the same 9-step profit simulation that
weekly_pricing_brand.py uses per store — but on channel-aggregated
velocity, stock, and cost. Produces one row per parent x channel instead
of one per parent x store.

Outputs:
- weekly_actions_channel/{brand}/pricing_actions_channel_{week}.csv
- weekly_actions_channel/{brand}/channel_aggregation_stats_{week}.json

Reads:
- weekly_actions/{brand}/pricing_actions_{week}.csv  (per-store recs)
- data/processed/{brand}/features_parent.parquet     (per-store inputs)
- data/raw/{brand}/costs.parquet                     (unit costs)
- data/raw/{brand}/products.parquet                  (catalog names)
- data/processed/{brand}/elasticity_by_sku.parquet   (demand curve)

Channel classification:
- ecomm = stores where is_ecomm_store(centro) is True (AB* prefix)
- bm    = all other stores

Variance & mandatory review:
- per_store_variance_pct = fraction of stores in channel whose
  individual recommended step differs from the channel recommendation.
- mandatory_review flag set when variance_pct > 0.50 — UI forces BMs
  to drill in before approving.

Gap stats (in channel_aggregation_stats_{week}.json):
- chain_uniform_profit vs sum_per_store_profit per parent x channel.
  Lets us empirically judge whether the uniform-price constraint costs
  material profit before committing to Phase 2+.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import json
from pathlib import Path
from collections import Counter

import numpy as np
import pandas as pd

from config.database import BRANDS, EXCLUDE_STORES_PRICING, CHANNEL_GRAIN_BRANDS
from config.vendor_brands import get_vendor_brand, is_ecomm_store
from src.models.pricing_simulation import (
    DISCOUNT_STEPS,
    MIN_MARGIN_PCT,
    IVA,
    snap_to_price_anchor,
    snap_to_discount_step,
    find_profit_maximizing_step,
    expected_velocity_bidirectional,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent

# Mandatory-review threshold for intra-channel variance.
# 0.50 = "majority of actioned stores disagree with the modal step."
# Calibrated empirically against BAMERS (2026-04-25): with the prior 0.30
# threshold 67% of rows fired (small actioned-store sets quantize ~33%
# from a single outlier — N=3 with 1 disagreement = 33%, false positive).
# At 0.50 only ~15-20% fire, capturing genuine intra-channel disagreement.
# Stockout-driven outliers (1-in-3 patterns) get surfaced via the existing
# stock_imbalance / ecomm_gap cross-store alerts, not via this flag.
MANDATORY_REVIEW_VARIANCE = 0.50


def _raw_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "raw" / brand.lower()


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def _store_actions_dir(brand: str) -> Path:
    return PROJECT_ROOT / "weekly_actions" / brand.lower()


def _channel_output_dir(brand: str) -> Path:
    return PROJECT_ROOT / "weekly_actions_channel" / brand.lower()


def _parse_pct_string(s) -> float:
    """Parse '30%' -> 0.30. Empty / 'Full price' -> 0.0."""
    if pd.isna(s):
        return 0.0
    text = str(s).strip()
    if not text or text.lower() == "full price":
        return 0.0
    if text.endswith("%"):
        try:
            return float(text[:-1]) / 100.0
        except ValueError:
            return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _channel_of(store_code: str) -> str:
    return "ecomm" if is_ecomm_store(store_code) else "bm"


def _build_cost_lookup(brand: str) -> dict:
    """Load parent-SKU costs with prefix-match fallback (same as weekly_pricing_brand)."""
    costs_path = _raw_dir(brand) / "costs.parquet"
    if not costs_path.exists():
        return {}
    df = pd.read_parquet(costs_path)
    df = df[df["cost"] > 0].dropna(subset=["cost"])
    return df.set_index("sku")["cost"].to_dict()


def _cost_for(parent_sku: str, cost_map: dict, sorted_keys: list):
    if parent_sku in cost_map:
        return float(cost_map[parent_sku])
    for k in sorted_keys:
        if parent_sku.startswith(k):
            return float(cost_map[k])
    return None


def _latest_actions_csv(brand: str, target_week=None) -> Path:
    d = _store_actions_dir(brand)
    if target_week is not None:
        want = d / f"pricing_actions_{pd.Timestamp(target_week).date()}.csv"
        if want.exists():
            return want
    csvs = sorted(d.glob("pricing_actions_*.csv"))
    return csvs[-1] if csvs else None


def _format_discount_string(step: float) -> str:
    """Match the 'NN%' format used in the per-store CSV."""
    if step <= 0:
        return "Full price"
    return f"{step:.0%}"


def generate_channel_actions_for_brand(brand: str, target_week=None):
    """
    Aggregate per-store recommendations into channel-level actions.

    If CHANNEL_GRAIN_BRANDS is configured and the brand is not in it,
    logs a skip message and returns None.
    """
    brand_upper = brand.upper()
    if CHANNEL_GRAIN_BRANDS and brand_upper not in CHANNEL_GRAIN_BRANDS:
        print(f"[{brand_upper}] channel_aggregate: brand not in CHANNEL_GRAIN_BRANDS — skipping")
        return None

    print(f"[{brand_upper}] Loading per-store recommendations and features...")

    # 1. Per-store pricing actions (already filtered to active, retail stores)
    actions_csv = _latest_actions_csv(brand, target_week=target_week)
    if actions_csv is None:
        print(f"  No per-store pricing actions CSV found — did 'pricing' step run?")
        return None
    store_actions = pd.read_csv(actions_csv)
    print(f"  Loaded {len(store_actions):,} per-store actions from {actions_csv.name}")

    # Target week from the filename if not given
    if target_week is None:
        stem = actions_csv.stem  # pricing_actions_2026-04-20
        date_part = stem.split("_")[-1]
        target_week = pd.Timestamp(date_part)
    else:
        target_week = pd.Timestamp(target_week)

    # 2. Features for the target week (all retail stores, all parents)
    feat_path = _processed_dir(brand) / "features_parent.parquet"
    if not feat_path.exists():
        print(f"  features_parent.parquet missing — did 'aggregate' step run?")
        return None
    features = pd.read_parquet(feat_path)
    features = features[features["week"] == target_week].copy()
    if features.empty:
        print(f"  No feature rows for {target_week.date()}")
        return None

    # Apply the same store filters weekly_pricing_brand.py applies
    features = features[~features["centro"].astype(str).str.startswith(tuple(EXCLUDE_STORES_PRICING))]
    brand_cfg = BRANDS.get(brand_upper, {})
    active_stores = brand_cfg.get("stores_active")
    if active_stores:
        features = features[features["centro"].isin(active_stores)]
    print(f"  Feature rows after store filter: {len(features):,}")

    # 3. Costs
    cost_map = _build_cost_lookup(brand)
    cost_sorted = sorted(cost_map.keys(), key=len, reverse=True) if cost_map else []

    # 4. Elasticity
    try:
        sku_elast = pd.read_parquet(_processed_dir(brand) / "elasticity_by_sku.parquet")
        elast_map = sku_elast[sku_elast["confidence"].isin(["high", "medium"])]\
            .set_index("codigo_padre")["elasticity"].to_dict()
    except FileNotFoundError:
        elast_map = {}

    # 5. Product names
    products_path = _raw_dir(brand) / "products.parquet"
    if not products_path.exists():
        print(f"  products.parquet missing at {products_path} — aborting")
        return None
    try:
        products = pd.read_parquet(products_path)
    except Exception as e:
        print(f"  Failed to read products.parquet ({e}) — aborting")
        return None
    parent_names = (
        products.groupby("codigo_padre")
        .first()[["material_descripcion", "primera_jerarquia", "segunda_jerarquia"]]
        .reset_index()
        .rename(columns={"material_descripcion": "product_name"})
    )
    parent_names["product_name"] = (
        parent_names["product_name"]
        .str.replace(r'\s*N[°º]?\s*[\d,\.]+\s*$', '', regex=True)
        .str.replace(r'\s*T/[A-Z]+\s*$', '', regex=True)
        .str.strip()
    )
    name_map = parent_names.set_index("codigo_padre").to_dict(orient="index")

    # 6. Per-store recommendation map: (parent, store) -> row from pricing_actions CSV.
    # CRITICAL: cast both keys to str. pd.read_csv auto-casts purely-numeric store
    # codes (like BOLD's "2002") to int while features_parent.parquet keeps centro
    # as str. Mismatched types silently miss every lookup, which dropped BOLD into
    # a 0-row channel output during BAMERS's first successful run.
    store_action_map = {
        (str(r["parent_sku"]), str(r["store"])): r
        for _, r in store_actions.iterrows()
    }

    # Tag channel on features
    features["channel"] = features["centro"].apply(_channel_of)

    out_rows = []
    gap_stats = []

    # Process each parent x channel
    grouped = features.groupby(["codigo_padre", "channel"], sort=False)
    n_groups = 0
    n_written = 0
    n_no_feasible = 0
    n_mandatory = 0
    n_skipped_no_classifier_signal = 0

    for (parent, channel), grp in grouped:
        n_groups += 1
        # Basic aggregates
        n_stores = grp["centro"].nunique()
        vel = grp["velocity_4w"].fillna(0)
        channel_velocity = float(vel.sum())
        if channel_velocity <= 0:
            # Everyone at zero velocity this week — not a meaningful channel signal
            continue

        # CLASSIFIER GATE: only emit a channel-level action if the per-store
        # classifier flagged at least one store in this channel as worth
        # acting on. Mirrors the markdown_probability >= 0.50 threshold the
        # per-store path uses (weekly_pricing_brand.py PASS 2). Without
        # this gate the channel sim ungates the model and recommends
        # markdowns on noise — first BAMERS run had 99% mandatory_review
        # because the channel sim was systematically more aggressive
        # than the trained classifier wanted.
        channel_has_classifier_signal = any(
            (str(parent), str(store)) in store_action_map for store in grp["centro"].unique()
        )
        if not channel_has_classifier_signal:
            n_skipped_no_classifier_signal += 1
            continue

        # Stock (null if all stores lack stock data for this parent)
        if "stock_on_hand" in grp.columns and grp["stock_on_hand"].notna().any():
            channel_stock = float(grp["stock_on_hand"].sum(min_count=1))
            has_stock_data = True
        else:
            channel_stock = None
            has_stock_data = False

        list_prices = grp["avg_precio_lista"].dropna()
        if list_prices.empty:
            continue
        list_price = float(list_prices.median())

        # Velocity-weighted current price + current discount
        fp = grp["avg_precio_final"].fillna(0).values
        dr = grp["discount_rate"].fillna(0).values
        w = vel.values
        if w.sum() > 0:
            channel_current_price = float(np.average(fp, weights=np.where(w > 0, w, 1e-9)))
            channel_current_disc = float(np.average(dr, weights=np.where(w > 0, w, 1e-9)))
        else:
            channel_current_price = float(np.median(fp)) if len(fp) else 0.0
            channel_current_disc = float(np.median(dr)) if len(dr) else 0.0

        # Cost (per parent SKU)
        unit_cost = _cost_for(parent, cost_map, cost_sorted)
        elasticity = elast_map.get(parent)

        # 7a. Profit-maximizing channel step
        sim = find_profit_maximizing_step(
            list_price=list_price,
            current_price=channel_current_price,
            current_discount=channel_current_disc,
            velocity=channel_velocity,
            unit_cost=unit_cost,
            elasticity=elasticity,
            min_margin_pct=MIN_MARGIN_PCT,
            min_price_delta=2000.0,
            allow_increase=True,
        )

        aggregation_method = "profit_simulation"

        # Contract with find_profit_maximizing_step:
        # - sim is None         → no feasible step under cost/margin floors, fallback allowed
        # - sim["action_type"] == "hold" → the sim explicitly said "no material change";
        #                                  we MUST NOT override via fallback (that would
        #                                  contradict its verdict on the same inputs).
        if sim is not None and sim["action_type"] == "hold":
            continue

        if sim is None:
            # 7b. Fallback: velocity-weighted average of per-store recommended steps.
            # Only fires when the profit sim had ZERO feasible candidates (typically
            # missing cost data so cost/margin floors can't be evaluated). Not fired
            # on "hold" — the sim already evaluated the candidates and said do nothing.
            store_recs = []
            store_vels = []
            for store in grp["centro"].unique():
                key = (str(parent), str(store))
                if key in store_action_map:
                    r = store_action_map[key]
                    step_val = _parse_pct_string(r.get("recommended_discount"))
                    store_recs.append(step_val)
                    store_vels.append(max(float(r.get("current_velocity", 0) or 0), 0.01))
            if not store_recs:
                n_no_feasible += 1
                continue
            weighted_step = float(np.average(store_recs, weights=store_vels))
            snapped_step = snap_to_discount_step(weighted_step)
            candidate_price = snap_to_price_anchor(list_price * (1 - snapped_step), direction="nearest")
            current_rounded = snap_to_price_anchor(channel_current_price, direction="nearest")
            # Skip if no material change
            if abs(candidate_price - current_rounded) < 2000 and snapped_step == snap_to_discount_step(channel_current_disc):
                continue
            exp_vel = expected_velocity_bidirectional(
                channel_velocity, channel_current_disc, snapped_step, elasticity,
            )
            chosen_step = snapped_step
            chosen_price = int(candidate_price)
            expected_velocity_val = exp_vel
            net = chosen_price / (1 + IVA)
            if unit_cost is not None and unit_cost > 0:
                margin_unit = net - unit_cost
                margin_pct_val = round((margin_unit / net) * 100.0, 1) if net > 0 else 0.0
                weekly_profit_val = margin_unit * exp_vel
            else:
                margin_unit = None
                margin_pct_val = None
                weekly_profit_val = chosen_price * exp_vel
            action_type = "decrease" if chosen_step > channel_current_disc + 1e-9 else ("increase" if chosen_step < channel_current_disc - 1e-9 else "hold")
            if action_type == "hold":
                continue
            aggregation_method = "velocity_weighted_fallback"
        else:
            chosen_step = sim["chosen_step"]
            chosen_price = sim["chosen_price"]
            expected_velocity_val = sim["expected_velocity"]
            margin_pct_val = sim["margin_pct"]
            margin_unit = sim["margin_unit"]
            weekly_profit_val = sim["weekly_profit"]
            action_type = sim["action_type"]

        # 8. Intra-channel variance: do the actioned stores within this channel
        # AGREE with each other on the right step? Old definition compared each
        # store's step to the channel's chosen step — but the channel sim is
        # ungated relative to per-store, so it routinely chose deeper steps
        # than per-store recommended, producing 99% "disagrees with channel."
        # New definition: of the stores that the per-store classifier flagged,
        # how many recommend a different step than the modal store-level step?
        # Captures real intra-channel disagreement (override candidates).
        actioned_steps = []
        for store in grp["centro"].unique():
            key = (str(parent), str(store))
            if key in store_action_map:
                r = store_action_map[key]
                actioned_steps.append(_parse_pct_string(r.get("recommended_discount")))
        if len(actioned_steps) >= 2:
            modal_step = Counter(actioned_steps).most_common(1)[0][0]
            non_modal = sum(1 for s in actioned_steps if abs(s - modal_step) > 1e-6)
            variance_pct = non_modal / len(actioned_steps)
        else:
            # Single actioned store (or none) — no intra-channel disagreement possible
            variance_pct = 0.0
        mandatory_review = variance_pct > MANDATORY_REVIEW_VARIANCE
        if mandatory_review:
            n_mandatory += 1

        # 9. Gap stats — apples-to-apples: chain-uniform profit vs the sum of
        # per-store profit-maximizing recommendations. Both sides run
        # find_profit_maximizing_step on the SAME inputs (per-store
        # velocity/stock/cost vs channel-aggregated velocity/stock/cost) so
        # the gap genuinely measures the cost of the uniform-price constraint.
        # Earlier version used the gated CSV recommendation, which compared
        # ungated channel sim to gated per-store sim and produced systematic
        # negative gaps (channel sim more aggressive than the gate allowed).
        chain_uniform_profit = float(weekly_profit_val) if weekly_profit_val is not None else 0.0
        sum_per_store_profit = 0.0
        for store in grp["centro"].unique():
            store_row_feat = grp[grp["centro"] == store].iloc[0]
            store_vel = float(store_row_feat.get("velocity_4w", 0) or 0)
            store_cur_price = float(store_row_feat.get("avg_precio_final", channel_current_price) or 0)
            store_cur_disc = float(store_row_feat.get("discount_rate", 0) or 0)
            if store_vel <= 0:
                continue
            store_sim = find_profit_maximizing_step(
                list_price=list_price,
                current_price=store_cur_price,
                current_discount=store_cur_disc,
                velocity=store_vel,
                unit_cost=unit_cost,
                elasticity=elasticity,
                min_margin_pct=MIN_MARGIN_PCT,
                min_price_delta=2000.0,
                allow_increase=True,
            )
            if store_sim is None:
                # No feasible step for this store (cost/margin floor)
                continue
            sum_per_store_profit += float(store_sim["weekly_profit"])
        gap_abs = sum_per_store_profit - chain_uniform_profit
        gap_pct = (gap_abs / sum_per_store_profit * 100.0) if sum_per_store_profit > 0 else 0.0
        gap_stats.append({
            "parent_sku": parent,
            "channel": channel,
            "n_stores": int(n_stores),
            "chain_uniform_profit": round(chain_uniform_profit, 2),
            "sum_per_store_profit": round(sum_per_store_profit, 2),
            "gap_abs_clp": round(gap_abs, 2),
            "gap_pct": round(gap_pct, 2),
            "variance_pct": round(variance_pct, 3),
            "has_stock_data": has_stock_data,
            "aggregation_method": aggregation_method,
        })

        # 10. Collect urgency, reasons, confidence from actioned stores in this channel
        channel_store_actions = [
            store_action_map[(str(parent), str(s))]
            for s in grp["centro"].unique()
            if (str(parent), str(s)) in store_action_map
        ]
        # Rebate aggregation: max across stores (rebate is parent-level so all
        # stores see the same amount, but max is defensive against ragged data).
        # Action rows already carry raw_cost / rebate_amount per store; pull max.
        rebate_amount_channel = 0
        if channel_store_actions:
            try:
                rebate_amount_channel = int(max(
                    (float(a.get("rebate_amount", 0) or 0) for a in channel_store_actions),
                    default=0,
                ))
            except (TypeError, ValueError):
                rebate_amount_channel = 0
        if channel_store_actions:
            # Urgency must align with the channel-level action_type: when the
            # channel chose a markdown, INCREASE is irrelevant noise from
            # individual stores that happened to want a price raise. Filter
            # accordingly so the badge text + section assignment stay consistent.
            if action_type == "increase":
                urgency = "INCREASE"
            else:
                severity = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
                relevant = [a.get("urgency", "LOW") for a in channel_store_actions
                            if a.get("urgency") != "INCREASE"]
                urgency = max(relevant, key=lambda u: severity.get(str(u), 0)) if relevant else "MEDIUM"
            reason_counter = Counter()
            for a in channel_store_actions:
                raw = str(a.get("reasons", ""))
                for piece in raw.split(";"):
                    piece = piece.strip()
                    if piece:
                        reason_counter[piece] += 1
            top_reasons = [r for r, _ in reason_counter.most_common(3)]
            reasons_str = "; ".join(top_reasons)
            conf_order = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "SPECULATIVE": 0}
            confidence_tier = max(
                (a.get("confidence_tier", "LOW") for a in channel_store_actions),
                key=lambda c: conf_order.get(str(c), 0),
            )
        else:
            urgency = "MEDIUM" if action_type == "decrease" else ("INCREASE" if action_type == "increase" else "LOW")
            reasons_str = f"Channel-level recommendation — no individual store triggered; channel profit maximized at {chosen_step:.0%}"
            confidence_tier = "MEDIUM"

        if mandatory_review:
            reasons_str = (reasons_str + "; " if reasons_str else "") + \
                f"Revisar por tienda: {variance_pct:.0%} de las tiendas difieren del canal"

        if aggregation_method == "velocity_weighted_fallback":
            reasons_str = (reasons_str + "; " if reasons_str else "") + "Stock data missing — velocity-weighted fallback"

        # 11. Revenue + margin impact
        current_weekly_rev = channel_velocity * channel_current_price
        expected_weekly_rev = expected_velocity_val * chosen_price
        rev_delta = expected_weekly_rev - current_weekly_rev

        if unit_cost is not None and unit_cost > 0:
            cur_net = channel_current_price / (1 + IVA)
            cur_margin_unit = cur_net - unit_cost
            cur_margin_weekly = cur_margin_unit * channel_velocity
            rec_margin_weekly = (margin_unit if margin_unit is not None else 0) * expected_velocity_val
            margin_delta = int(rec_margin_weekly - cur_margin_weekly)
        else:
            margin_delta = None

        name_info = name_map.get(parent, {})
        product = str(name_info.get("product_name", parent))[:50]
        category = name_info.get("primera_jerarquia", "") or ""
        subcategory = name_info.get("segunda_jerarquia", "") or ""

        out_rows.append({
            "parent_sku": parent,
            "channel": channel,
            "product": product,
            "category": category,
            "subcategory": subcategory,
            "vendor_brand": get_vendor_brand(parent, brand_upper),
            "n_stores": int(n_stores),
            "stores_in_channel": ",".join(sorted(grp["centro"].unique().tolist())),
            "has_stock_data": has_stock_data,
            "channel_stock": int(channel_stock) if channel_stock is not None else None,
            "current_list_price": int(list_price),
            "current_price": int(snap_to_price_anchor(channel_current_price, direction="nearest")),
            "current_discount": _format_discount_string(snap_to_discount_step(channel_current_disc)),
            "current_velocity": round(channel_velocity, 1),
            "recommended_price": int(chosen_price),
            "recommended_discount": _format_discount_string(chosen_step),
            "expected_velocity": round(expected_velocity_val, 1),
            "current_weekly_rev": int(current_weekly_rev),
            "expected_weekly_rev": int(expected_weekly_rev),
            "rev_delta": int(rev_delta),
            "unit_cost": int(unit_cost) if unit_cost is not None else None,
            "rebate_amount": rebate_amount_channel,
            "margin_pct": margin_pct_val,
            "margin_delta": margin_delta,
            "urgency": urgency,
            "reasons": reasons_str,
            "confidence_tier": confidence_tier,
            "action_type": action_type,
            "per_store_variance_pct": round(variance_pct, 3),
            "mandatory_review": bool(mandatory_review),
            "aggregation_method": aggregation_method,
        })
        n_written += 1

    if not out_rows:
        print(f"  [{brand_upper}] No channel-level actions recommended.")
        return None

    out_df = pd.DataFrame(out_rows)
    # Sort: decreases first by rev_delta desc, then increases
    urgency_order = {"INCREASE": -1, "HIGH": 0, "MEDIUM": 1, "LOW": 2}
    out_df["_u"] = out_df["urgency"].map(urgency_order).fillna(3).astype(int)
    out_df = out_df.sort_values(["_u", "rev_delta"], ascending=[True, False]).drop(columns=["_u"])

    # Write outputs
    out_dir = _channel_output_dir(brand)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"pricing_actions_channel_{target_week.date()}.csv"
    out_df.to_csv(out_path, index=False)

    # Gap stats summary — mean/median gap_pct across parents, variance distribution
    gap_df = pd.DataFrame(gap_stats) if gap_stats else pd.DataFrame()
    summary = {
        "brand": brand_upper,
        "week": str(target_week.date()),
        "n_parents_processed": int(n_groups),
        "n_channel_actions_written": int(n_written),
        "n_mandatory_review": int(n_mandatory),
        "n_no_feasible": int(n_no_feasible),
        "n_skipped_no_classifier_signal": int(n_skipped_no_classifier_signal),
        "gap_pct_mean": round(float(gap_df["gap_pct"].mean()), 2) if not gap_df.empty else None,
        "gap_pct_median": round(float(gap_df["gap_pct"].median()), 2) if not gap_df.empty else None,
        "gap_pct_p75": round(float(gap_df["gap_pct"].quantile(0.75)), 2) if not gap_df.empty else None,
        "gap_pct_p95": round(float(gap_df["gap_pct"].quantile(0.95)), 2) if not gap_df.empty else None,
        "total_chain_uniform_profit": round(float(gap_df["chain_uniform_profit"].sum()), 2) if not gap_df.empty else 0.0,
        "total_sum_per_store_profit": round(float(gap_df["sum_per_store_profit"].sum()), 2) if not gap_df.empty else 0.0,
        "variance_pct_mean": round(float(gap_df["variance_pct"].mean()), 3) if not gap_df.empty else None,
        "pct_fallback": round(float((gap_df["aggregation_method"] == "velocity_weighted_fallback").mean()) * 100, 1) if not gap_df.empty else 0.0,
    }
    stats_path = out_dir / f"channel_aggregation_stats_{target_week.date()}.json"
    with open(stats_path, "w") as f:
        json.dump({"summary": summary, "per_parent": gap_stats}, f, indent=2)

    # Console summary
    n_bm = int((out_df["channel"] == "bm").sum())
    n_ecomm = int((out_df["channel"] == "ecomm").sum())
    total_rev_delta = int(out_df["rev_delta"].sum())
    print(f"\n{'=' * 80}")
    print(f"[{brand_upper}] CHANNEL-LEVEL PRICING ACTIONS — {target_week.date()}")
    print(f"{'=' * 80}")
    print(f"  Channel actions written: {n_written} ({n_bm} B&M, {n_ecomm} ecomm)")
    print(f"  Mandatory review (variance > {MANDATORY_REVIEW_VARIANCE:.0%}): {n_mandatory}")
    print(f"  Total expected rev delta: ${total_rev_delta:+,} CLP/week")
    if summary["gap_pct_mean"] is not None:
        print(f"  Gap vs per-store optimum: mean {summary['gap_pct_mean']}%, p75 {summary['gap_pct_p75']}%, p95 {summary['gap_pct_p95']}%")
    print(f"  -> {out_path}")
    print(f"  -> {stats_path}")

    return out_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("brand", type=str)
    parser.add_argument("--week", type=str, default=None)
    args = parser.parse_args()
    generate_channel_actions_for_brand(args.brand, target_week=args.week)
