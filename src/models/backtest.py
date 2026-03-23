"""
Backtesting framework for HOKA markdown optimization.

Simulates model-in-production: for each week in the test period, generate
recommendations using only data available up to that point, then compare
against what actually happened.

Key metrics:
- Timing delta: did the model recommend markdown earlier or later than actual?
- Depth delta: was the model's recommended depth shallower or deeper?
- Revenue impact estimate: what would have changed if model was followed?
- Precision at top-K: of the top-K recommendations per week, how many were correct?
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import xgboost as xgb
import pickle
import json
from pathlib import Path

PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
MODEL_DIR = Path(__file__).parent.parent.parent / "models"
REPORT_DIR = Path(__file__).parent.parent.parent / "reports"

# Same exclusions as train.py
EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
]

CATEGORICAL_COLS = [
    "primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario",
]


def load_models():
    with open(MODEL_DIR / "markdown_classifier.pkl", "rb") as f:
        cls_model = pickle.load(f)
    with open(MODEL_DIR / "depth_regressor.pkl", "rb") as f:
        reg_model = pickle.load(f)
    return cls_model, reg_model


def prepare_features(df):
    """Prepare feature matrix (same logic as train.py)."""
    df = df.copy()
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df[col] = df[col].astype("category").cat.codes
    feature_cols = [c for c in df.columns if c not in EXCLUDE_COLS]
    return df[feature_cols], feature_cols


def identify_actual_markdown_events(df):
    """
    Identify actual markdown events from historical data.
    A markdown event = first week where discount_rate > threshold for a SKU-store
    after a period of no/low discounting.
    """
    df = df.sort_values(["sku", "centro", "week"]).copy()

    # A markdown event: transition from no-discount to discount
    df["prev_has_discount"] = df.groupby(["sku", "centro"])["has_discount"].shift(1).fillna(0)
    df["markdown_event"] = ((df["has_discount"] == 1) & (df["prev_has_discount"] == 0)).astype(int)

    # Also track first-ever markdown per SKU-store
    first_markdown = (
        df[df["has_discount"] == 1]
        .groupby(["sku", "centro"])["week"]
        .min()
        .rename("first_markdown_week")
        .reset_index()
    )

    df = df.merge(first_markdown, on=["sku", "centro"], how="left")
    df.drop(columns=["prev_has_discount"], inplace=True)

    return df


def run_walk_forward_backtest(df, cls_model, reg_model, test_start="2025-06-01", top_k=50):
    """
    Walk-forward backtest: for each week in test period, score all SKU-stores,
    generate top-K recommendations, compare to actuals.
    """
    df = df.copy()
    test_start = pd.Timestamp(test_start)
    test_weeks = sorted(df[df["week"] >= test_start]["week"].unique())

    print(f"  Backtest period: {test_weeks[0].date()} to {test_weeks[-1].date()} ({len(test_weeks)} weeks)")

    X_all, feature_cols = prepare_features(df)

    weekly_results = []

    for week in test_weeks:
        week_mask = df["week"] == week
        week_data = df[week_mask].copy()

        if len(week_data) == 0:
            continue

        X_week = X_all[week_mask]

        # Score
        prob = cls_model.predict_proba(X_week)[:, 1]
        depth = reg_model.predict(X_week)

        week_data = week_data.copy()
        week_data["model_prob"] = prob
        week_data["model_depth"] = np.clip(depth, 0, 1)

        # Rank by probability
        week_data = week_data.sort_values("model_prob", ascending=False)

        # Top-K recommendations
        top_k_recs = week_data.head(top_k)

        # Metrics for this week
        # 1. How many of top-K actually got discounted in next 4 weeks?
        actual_pos_in_top_k = top_k_recs["will_discount_4w"].sum()
        precision_at_k = actual_pos_in_top_k / min(top_k, len(top_k_recs))

        # 2. Of all actual markdown events this week, how many were in top-K?
        actual_markdown_skus = set(
            week_data[week_data["will_discount_4w"] == 1]["sku"].values
        )
        recommended_skus = set(top_k_recs["sku"].values)
        if len(actual_markdown_skus) > 0:
            recall_at_k = len(actual_markdown_skus & recommended_skus) / len(actual_markdown_skus)
        else:
            recall_at_k = np.nan

        # 3. Depth accuracy for recommended items that actually got discounted
        matched = top_k_recs[top_k_recs["will_discount_4w"] == 1]
        if len(matched) > 0:
            depth_mae = (matched["model_depth"] - matched["future_max_disc_4w"]).abs().mean()
        else:
            depth_mae = np.nan

        # 4. Average model probability for actual positives vs negatives
        pos_avg_prob = week_data[week_data["will_discount_4w"] == 1]["model_prob"].mean()
        neg_avg_prob = week_data[week_data["will_discount_4w"] == 0]["model_prob"].mean()

        # 5. Revenue at risk in top-K (sum of gross_revenue for recommended items)
        revenue_at_risk = top_k_recs["gross_revenue"].sum()

        weekly_results.append({
            "week": week,
            "total_skus": len(week_data),
            "actual_positives": len(actual_markdown_skus),
            "precision_at_k": precision_at_k,
            "recall_at_k": recall_at_k,
            "depth_mae": depth_mae,
            "pos_avg_prob": pos_avg_prob,
            "neg_avg_prob": neg_avg_prob,
            "revenue_at_risk": revenue_at_risk,
            "top_k": min(top_k, len(top_k_recs)),
        })

    return pd.DataFrame(weekly_results)


def analyze_timing_delta(df, cls_model):
    """
    For each SKU-store, compare when the model would have first flagged
    markdown risk (prob > threshold) vs when markdown actually happened.
    """
    df = df.sort_values(["sku", "centro", "week"]).copy()
    X_all, feature_cols = prepare_features(df)

    df["model_prob"] = cls_model.predict_proba(X_all)[:, 1]

    # Threshold for "model recommends markdown"
    threshold = 0.5

    # First week model flags risk
    model_flag = (
        df[df["model_prob"] >= threshold]
        .groupby(["sku", "centro"])["week"]
        .min()
        .rename("model_first_flag")
        .reset_index()
    )

    # First actual markdown
    actual_flag = (
        df[df["has_discount"] == 1]
        .groupby(["sku", "centro"])["week"]
        .min()
        .rename("actual_first_markdown")
        .reset_index()
    )

    timing = model_flag.merge(actual_flag, on=["sku", "centro"], how="inner")
    timing["delta_weeks"] = (
        (timing["actual_first_markdown"] - timing["model_first_flag"]).dt.days / 7
    ).round(1)
    # Positive = model flagged earlier (good), negative = model flagged later (bad)

    return timing


def analyze_depth_accuracy(df, cls_model, reg_model):
    """
    For SKU-stores that were actually marked down, compare model-recommended
    depth vs actual depth.
    """
    df = df.sort_values(["sku", "centro", "week"]).copy()
    X_all, _ = prepare_features(df)

    df["model_prob"] = cls_model.predict_proba(X_all)[:, 1]
    df["model_depth"] = np.clip(reg_model.predict(X_all), 0, 1)

    # Look at weeks where markdown actually happened
    actual_mkdown = df[(df["has_discount"] == 1) & (df["discount_rate"] > 0.01)].copy()

    actual_mkdown["depth_delta"] = actual_mkdown["model_depth"] - actual_mkdown["discount_rate"]
    # Positive = model recommends deeper, negative = model recommends shallower

    return actual_mkdown[["sku", "centro", "week", "discount_rate", "model_depth",
                          "depth_delta", "model_prob", "units_sold", "gross_revenue"]]


def estimate_revenue_impact(df, timing_df, depth_df):
    """
    Estimate revenue impact of following model recommendations.

    Scenarios:
    1. Earlier markdown → more units at shallower discount (saves margin)
    2. Later markdown → more full-price sales before discount kicks in
    3. Shallower depth → higher per-unit revenue on discounted sales
    """
    # Merge product data for avg price
    avg_prices = (
        df[df["units_sold"] > 0]
        .groupby("sku")
        .agg(
            avg_list_price=("avg_precio_lista", "mean"),
            avg_final_price=("avg_precio_final", "mean"),
            total_units=("units_sold", "sum"),
        )
        .reset_index()
    )

    # Scenario: Shallower discounts on marked-down items
    if len(depth_df) > 0:
        depth_with_price = depth_df.merge(avg_prices[["sku", "avg_list_price"]], on="sku", how="left")

        # Revenue delta per unit = price * (actual_disc - model_disc) * units
        # If model_depth < actual discount_rate → shallower → positive impact
        depth_with_price["per_unit_savings"] = (
            depth_with_price["avg_list_price"]
            * (depth_with_price["discount_rate"] - depth_with_price["model_depth"])
        )
        depth_with_price["weekly_savings"] = (
            depth_with_price["per_unit_savings"] * depth_with_price["units_sold"]
        )

        total_depth_savings = depth_with_price["weekly_savings"].sum()
        avg_per_week = depth_with_price.groupby("week")["weekly_savings"].sum().mean()
    else:
        total_depth_savings = 0
        avg_per_week = 0

    # Timing impact
    if len(timing_df) > 0:
        early_flags = timing_df[timing_df["delta_weeks"] > 0]
        late_flags = timing_df[timing_df["delta_weeks"] < 0]
        avg_early_weeks = early_flags["delta_weeks"].mean() if len(early_flags) > 0 else 0
        avg_late_weeks = late_flags["delta_weeks"].abs().mean() if len(late_flags) > 0 else 0
    else:
        avg_early_weeks = 0
        avg_late_weeks = 0

    return {
        "depth_total_savings_clp": total_depth_savings,
        "depth_avg_weekly_savings_clp": avg_per_week,
        "timing_skus_flagged_early": len(timing_df[timing_df["delta_weeks"] > 0]) if len(timing_df) > 0 else 0,
        "timing_skus_flagged_late": len(timing_df[timing_df["delta_weeks"] < 0]) if len(timing_df) > 0 else 0,
        "timing_avg_early_weeks": avg_early_weeks,
        "timing_avg_late_weeks": avg_late_weeks,
    }


def run_backtest():
    """Main backtesting pipeline."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading data and models...")
    df = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")
    cls_model, reg_model = load_models()

    # ================================================================
    # 1. Walk-forward weekly backtest
    # ================================================================
    print("\n" + "=" * 60)
    print("WALK-FORWARD BACKTEST (Jun 2025 – Mar 2026)")
    print("=" * 60)

    weekly_metrics = run_walk_forward_backtest(df, cls_model, reg_model, test_start="2025-06-01", top_k=50)
    weekly_metrics.to_csv(REPORT_DIR / "backtest_weekly_metrics.csv", index=False)

    print(f"\n  Weekly Precision@50:  {weekly_metrics['precision_at_k'].mean():.3f} (avg)")
    print(f"  Weekly Recall@50:     {weekly_metrics['recall_at_k'].mean():.3f} (avg)")
    print(f"  Depth MAE:            {weekly_metrics['depth_mae'].mean():.4f} (avg)")
    print(f"  Prob separation:      pos={weekly_metrics['pos_avg_prob'].mean():.3f} vs neg={weekly_metrics['neg_avg_prob'].mean():.3f}")

    # ================================================================
    # 2. Timing analysis
    # ================================================================
    print("\n" + "=" * 60)
    print("TIMING ANALYSIS")
    print("=" * 60)

    timing_df = analyze_timing_delta(df, cls_model)
    timing_df.to_csv(REPORT_DIR / "backtest_timing_delta.csv", index=False)

    print(f"\n  SKU-stores analyzed: {len(timing_df):,}")
    print(f"  Model flagged earlier: {(timing_df['delta_weeks'] > 0).sum():,} ({(timing_df['delta_weeks'] > 0).mean():.1%})")
    print(f"  Model flagged same week: {(timing_df['delta_weeks'] == 0).sum():,} ({(timing_df['delta_weeks'] == 0).mean():.1%})")
    print(f"  Model flagged later: {(timing_df['delta_weeks'] < 0).sum():,} ({(timing_df['delta_weeks'] < 0).mean():.1%})")
    print(f"  Avg weeks early (when early): {timing_df[timing_df['delta_weeks'] > 0]['delta_weeks'].mean():.1f}")
    print(f"  Avg weeks late (when late): {timing_df[timing_df['delta_weeks'] < 0]['delta_weeks'].abs().mean():.1f}")

    print("\n  Timing delta distribution:")
    bins = [-100, -8, -4, -0.5, 0.5, 4, 8, 100]
    labels = ["8+ wks late", "4-8 wks late", "1-4 wks late",
              "Same week", "1-4 wks early", "4-8 wks early", "8+ wks early"]
    timing_df["timing_bucket"] = pd.cut(timing_df["delta_weeks"], bins=bins, labels=labels)
    dist = timing_df["timing_bucket"].value_counts().sort_index()
    for bucket, count in dist.items():
        pct = count / len(timing_df) * 100
        bar = "█" * int(pct / 2)
        print(f"    {bucket:20s} {count:>5,} ({pct:5.1f}%) {bar}")

    # ================================================================
    # 3. Depth accuracy
    # ================================================================
    print("\n" + "=" * 60)
    print("DEPTH ACCURACY ANALYSIS")
    print("=" * 60)

    depth_df = analyze_depth_accuracy(df, cls_model, reg_model)
    depth_df.to_csv(REPORT_DIR / "backtest_depth_accuracy.csv", index=False)

    print(f"\n  Marked-down SKU-weeks analyzed: {len(depth_df):,}")
    print(f"  Avg actual discount:  {depth_df['discount_rate'].mean():.1%}")
    print(f"  Avg model recommended: {depth_df['model_depth'].mean():.1%}")
    print(f"  Avg depth delta:      {depth_df['depth_delta'].mean():.1%} ({'deeper' if depth_df['depth_delta'].mean() > 0 else 'shallower'})")
    print(f"  Depth MAE:            {depth_df['depth_delta'].abs().mean():.1%}")

    print("\n  Model vs actual discount depth:")
    shallower = (depth_df["depth_delta"] < -0.01).sum()
    similar = ((depth_df["depth_delta"] >= -0.01) & (depth_df["depth_delta"] <= 0.01)).sum()
    deeper = (depth_df["depth_delta"] > 0.01).sum()
    total = len(depth_df)
    print(f"    Model recommends shallower: {shallower:>5,} ({shallower/total:.1%})")
    print(f"    Similar (±1pp):             {similar:>5,} ({similar/total:.1%})")
    print(f"    Model recommends deeper:    {deeper:>5,} ({deeper/total:.1%})")

    # ================================================================
    # 4. Revenue impact estimate
    # ================================================================
    print("\n" + "=" * 60)
    print("ESTIMATED REVENUE IMPACT")
    print("=" * 60)

    impact = estimate_revenue_impact(df, timing_df, depth_df)

    print(f"\n  Depth optimization:")
    print(f"    Total savings from shallower discounts: ${impact['depth_total_savings_clp']:,.0f} CLP")
    print(f"    Average weekly savings:                 ${impact['depth_avg_weekly_savings_clp']:,.0f} CLP")
    print(f"    Annualized:                             ${impact['depth_avg_weekly_savings_clp'] * 52:,.0f} CLP")

    print(f"\n  Timing optimization:")
    print(f"    SKUs flagged earlier:     {impact['timing_skus_flagged_early']:,} (avg {impact['timing_avg_early_weeks']:.1f} weeks)")
    print(f"    SKUs flagged later:       {impact['timing_skus_flagged_late']:,} (avg {impact['timing_avg_late_weeks']:.1f} weeks)")

    # Save full impact report
    impact["annualized_depth_savings_clp"] = impact["depth_avg_weekly_savings_clp"] * 52
    with open(REPORT_DIR / "backtest_impact_summary.json", "w") as f:
        json.dump(impact, f, indent=2, default=str)

    # ================================================================
    # 5. Per-category breakdown
    # ================================================================
    print("\n" + "=" * 60)
    print("PER-CATEGORY BREAKDOWN")
    print("=" * 60)

    # Merge category info into depth_df
    cat_info = df[["sku", "primera_jerarquia", "segunda_jerarquia"]].drop_duplicates(subset=["sku"])
    depth_with_cat = depth_df.merge(cat_info, on="sku", how="left")

    cat_summary = (
        depth_with_cat.groupby(["primera_jerarquia", "segunda_jerarquia"])
        .agg(
            n_skuweeks=("sku", "count"),
            avg_actual_disc=("discount_rate", "mean"),
            avg_model_disc=("model_depth", "mean"),
            avg_depth_delta=("depth_delta", "mean"),
            total_revenue=("gross_revenue", "sum"),
        )
        .sort_values("total_revenue", ascending=False)
        .reset_index()
    )
    cat_summary.to_csv(REPORT_DIR / "backtest_category_breakdown.csv", index=False)

    print(f"\n  {'Category':<35s} {'N':>6} {'Actual':>8} {'Model':>8} {'Delta':>8} {'Revenue':>15}")
    print("  " + "-" * 85)
    for _, row in cat_summary.head(10).iterrows():
        cat = f"{row['primera_jerarquia']}/{row['segunda_jerarquia']}"
        print(f"  {cat:<35s} {row['n_skuweeks']:>6,} {row['avg_actual_disc']:>7.1%} {row['avg_model_disc']:>7.1%} {row['avg_depth_delta']:>+7.1%} {row['total_revenue']:>14,.0f}")

    # ================================================================
    # 6. Per-store breakdown
    # ================================================================
    print("\n" + "=" * 60)
    print("PER-STORE BREAKDOWN")
    print("=" * 60)

    store_summary = (
        depth_df.groupby("centro")
        .agg(
            n_skuweeks=("sku", "count"),
            avg_actual_disc=("discount_rate", "mean"),
            avg_model_disc=("model_depth", "mean"),
            avg_depth_delta=("depth_delta", "mean"),
            total_revenue=("gross_revenue", "sum"),
        )
        .sort_values("total_revenue", ascending=False)
        .reset_index()
    )
    store_summary.to_csv(REPORT_DIR / "backtest_store_breakdown.csv", index=False)

    print(f"\n  {'Store':>10} {'N':>6} {'Actual':>8} {'Model':>8} {'Delta':>8} {'Revenue':>15}")
    print("  " + "-" * 60)
    for _, row in store_summary.iterrows():
        print(f"  {row['centro']:>10} {row['n_skuweeks']:>6,} {row['avg_actual_disc']:>7.1%} {row['avg_model_disc']:>7.1%} {row['avg_depth_delta']:>+7.1%} {row['total_revenue']:>14,.0f}")

    print(f"\nReports saved to: {REPORT_DIR}")
    return weekly_metrics, timing_df, depth_df, impact


if __name__ == "__main__":
    run_backtest()
