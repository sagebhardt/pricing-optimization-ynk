"""
Recommendation engine for HOKA markdown optimization.

Generates weekly ranked markdown recommendations per SKU-store:
- Markdown probability (should we discount?)
- Recommended discount depth (how much?)
- SHAP-based rationale (why this recommendation?)
- Priority ranking by estimated revenue impact
- Actionable output for commercial team review
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import xgboost as xgb
import shap
import pickle
import json
from pathlib import Path
from datetime import datetime

PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
MODEL_DIR = Path(__file__).parent.parent.parent / "models"
RECS_DIR = Path(__file__).parent.parent.parent / "recommendations"

EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
]

CATEGORICAL_COLS = [
    "primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario",
]

STORE_NAMES = {
    "7501": "Hoka Costanera",
    "7502": "Hoka Marina",
    "AB75": "CD Hoka (E-commerce)",
    "7599": "Hoka Eventos",
}

CONFIDENCE_THRESHOLDS = {
    "high": 0.75,
    "medium": 0.50,
    "low": 0.30,
}


def load_models_and_data():
    with open(MODEL_DIR / "markdown_classifier.pkl", "rb") as f:
        cls_model = pickle.load(f)
    with open(MODEL_DIR / "depth_regressor.pkl", "rb") as f:
        reg_model = pickle.load(f)

    df = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")
    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")

    return cls_model, reg_model, df, products


def prepare_features(df):
    df = df.copy()
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df[col] = df[col].astype("category").cat.codes
    feature_cols = [c for c in df.columns if c not in EXCLUDE_COLS]
    return df[feature_cols], feature_cols


def compute_shap_rationale(model, X_row, feature_cols, top_n=5):
    """Generate human-readable SHAP rationale for a single prediction."""
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_row)

    if len(shap_values.shape) == 1:
        sv = shap_values
    else:
        sv = shap_values[0]

    # Top contributing features
    abs_shap = np.abs(sv)
    top_idx = abs_shap.argsort()[-top_n:][::-1]

    rationale = []
    for idx in top_idx:
        feat_name = feature_cols[idx]
        feat_val = X_row.iloc[0, idx] if hasattr(X_row, 'iloc') else X_row[idx]
        shap_val = sv[idx]
        direction = "increases" if shap_val > 0 else "decreases"

        # Human-readable feature descriptions
        readable = _feature_to_readable(feat_name, feat_val, direction)
        if readable:
            rationale.append(readable)

    return rationale


def _feature_to_readable(feat_name, feat_val, direction):
    """Convert feature name + value to human-readable rationale."""
    templates = {
        "weeks_since_discount": lambda v, d: f"{'No' if v > 8 else str(int(v))} weeks since last discount → {d} markdown likelihood",
        "velocity_4w": lambda v, d: f"4-week velocity at {v:.1f} units/week → {d} markdown pressure",
        "velocity_trend": lambda v, d: f"Sales {'accelerating' if v > 1 else 'decelerating'} (trend={v:.2f}) → {d} urgency",
        "product_age_weeks": lambda v, d: f"Product is {int(v)} weeks old → {d} markdown risk",
        "size_curve_completeness": lambda v, d: f"Size availability at {v:.0%} → {d} markdown signal",
        "has_discount": lambda v, d: f"Currently {'on' if v else 'not on'} discount → {d} continuation probability",
        "disc_exposure_rate": lambda v, d: f"{v:.0%} of selling weeks had discounts → {d} markdown pattern",
        "max_discount_rate": lambda v, d: f"Deepest historical discount at {v:.0%} → {d} depth expectation",
        "cumulative_units": lambda v, d: f"{int(v)} total units sold → {d} sell-through assessment",
        "weekly_entries": lambda v, d: f"Store traffic at {int(v)} entries/week → {d} demand signal",
        "conversion_rate": lambda v, d: f"Conversion rate at {v:.1%} → {d} pricing effectiveness",
        "month_sin": lambda v, d: f"Seasonal timing → {d} markdown likelihood",
        "month_cos": lambda v, d: f"Seasonal timing → {d} markdown likelihood",
    }

    if feat_name in templates:
        try:
            return templates[feat_name](feat_val, direction)
        except (ValueError, TypeError):
            return None
    return None


def generate_recommendations(target_week=None, min_confidence="low", top_n=100):
    """
    Generate markdown recommendations for a specific week.
    If target_week is None, uses the most recent week in the data.
    """
    RECS_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading models and data...")
    cls_model, reg_model, df, products = load_models_and_data()

    # Determine target week
    if target_week is None:
        target_week = df["week"].max()
    else:
        target_week = pd.Timestamp(target_week)

    print(f"Generating recommendations for week of {target_week.date()}")

    # Get data for target week
    week_data = df[df["week"] == target_week].copy()

    if len(week_data) == 0:
        print(f"  No data for week {target_week.date()}")
        return None

    # Prepare features
    X_week, feature_cols = prepare_features(week_data)

    # Score (store probabilities and depths as arrays aligned to week_data)
    probs = cls_model.predict_proba(X_week.values)[:, 1]
    depths = np.clip(reg_model.predict(X_week.values), 0, 0.60)
    week_data["markdown_probability"] = probs
    week_data["recommended_depth"] = depths

    # Store feature matrix as numpy for SHAP later
    X_week_np = X_week.values

    # Confidence level
    prob_col = "markdown_probability"
    week_data["confidence"] = pd.cut(
        week_data[prob_col],
        bins=[0, CONFIDENCE_THRESHOLDS["low"], CONFIDENCE_THRESHOLDS["medium"],
              CONFIDENCE_THRESHOLDS["high"], 1.0],
        labels=["skip", "low", "medium", "high"],
    )

    # Filter by minimum confidence
    min_prob = CONFIDENCE_THRESHOLDS.get(min_confidence, 0.3)
    recs = week_data[week_data[prob_col] >= min_prob].copy()

    # Enrich with product info
    product_info = products[["material", "material_descripcion", "primera_jerarquia",
                             "segunda_jerarquia", "codigo_padre", "talla", "color1"]].rename(
        columns={"material": "sku", "material_descripcion": "product_name"}
    ).drop_duplicates(subset=["sku"])
    recs = recs.merge(product_info, on="sku", how="left")

    # Estimated revenue impact (simplified: discount savings if shallower)
    recs["current_avg_price"] = recs["avg_precio_lista"]
    recs["estimated_weekly_revenue"] = recs["velocity_4w"] * recs["current_avg_price"]

    # Priority score: probability × estimated revenue × (1 + velocity_trend indicator)
    recs["priority_score"] = (
        recs[prob_col]
        * recs["estimated_weekly_revenue"].fillna(0).clip(lower=0)
        * np.where(recs["velocity_trend"] < 0.8, 1.2, 1.0)  # Boost decelerating products
    )

    # Sort by priority
    recs = recs.sort_values("priority_score", ascending=False).head(top_n)

    # Generate SHAP rationale for top recommendations
    n_rationale = min(20, len(recs))
    print(f"  Generating SHAP rationale for top {n_rationale} recommendations...")
    explainer = shap.TreeExplainer(cls_model)

    # Score all top recs at once for SHAP
    top_recs_idx = recs.head(n_rationale).index.tolist()
    # Map back to position in week_data to get correct X rows
    week_data_reset = week_data.reset_index(drop=True)
    recs_reset = recs.reset_index(drop=True)

    rationales = []
    for i in range(n_rationale):
        row = recs_reset.iloc[i]
        # Find this SKU-store in the original week_data
        mask = (week_data_reset["sku"] == row["sku"]) & (week_data_reset["centro"] == row["centro"])
        pos = mask.idxmax()
        X_row = X_week_np[pos:pos+1]

        shap_vals = explainer.shap_values(X_row)[0]
        abs_shap = np.abs(shap_vals)
        top_feat_idx = abs_shap.argsort()[-3:][::-1]

        reasons = []
        for fi in top_feat_idx:
            readable = _feature_to_readable(
                feature_cols[fi],
                X_row[0, fi],
                "increases" if shap_vals[fi] > 0 else "decreases"
            )
            if readable:
                reasons.append(readable)
        rationales.append("; ".join(reasons[:3]) if reasons else "Multiple factors")

    recs = recs_reset
    recs.loc[recs.head(n_rationale).index, "rationale"] = rationales

    # Build output
    output_cols = [
        "sku", "product_name", "centro", "codigo_padre", "talla", "color1_y",
        "primera_jerarquia_y", "segunda_jerarquia_y",
        "markdown_probability", "confidence", "recommended_depth",
        "priority_score", "velocity_4w", "velocity_trend",
        "product_age_weeks", "size_curve_completeness",
        "has_discount", "discount_rate", "max_discount_rate",
        "current_avg_price", "estimated_weekly_revenue",
        "rationale",
    ]
    # Only include columns that exist
    output_cols = [c for c in output_cols if c in recs.columns]
    output = recs[output_cols].copy()

    # Rename for readability
    rename_map = {
        "centro": "store_code",
        "codigo_padre": "parent_sku",
        "color1_y": "color",
        "primera_jerarquia_y": "category",
        "segunda_jerarquia_y": "subcategory",
        "velocity_4w": "weekly_velocity",
        "has_discount": "currently_discounted",
        "discount_rate": "current_discount_pct",
        "max_discount_rate": "max_historical_discount",
    }
    output.rename(columns={k: v for k, v in rename_map.items() if k in output.columns}, inplace=True)

    # Add store name
    if "store_code" in output.columns:
        output["store_name"] = output["store_code"].map(STORE_NAMES)

    # Save
    filename = f"recommendations_{target_week.date()}"
    output.to_csv(RECS_DIR / f"{filename}.csv", index=False)
    output.to_parquet(RECS_DIR / f"{filename}.parquet", index=False)

    # Print summary
    print(f"\n{'=' * 70}")
    print(f"MARKDOWN RECOMMENDATIONS — Week of {target_week.date()}")
    print(f"{'=' * 70}")
    print(f"\n  Total SKU-stores scored:    {len(week_data):,}")
    print(f"  Recommendations generated:  {len(output):,}")
    print(f"  High confidence:            {(output['confidence'] == 'high').sum()}")
    print(f"  Medium confidence:          {(output['confidence'] == 'medium').sum()}")
    print(f"  Low confidence:             {(output['confidence'] == 'low').sum()}")

    print(f"\n  {'Rank':>4} {'SKU':<25} {'Store':<15} {'Prob':>6} {'Depth':>6} {'Vel':>5} {'Age':>4} {'Rationale'}")
    print("  " + "-" * 110)
    for rank, (_, row) in enumerate(output.head(20).iterrows(), 1):
        store = row.get("store_name", row.get("store_code", "?"))
        if isinstance(store, str) and len(store) > 12:
            store = store[:12] + "…"
        rationale = str(row.get("rationale", ""))[:50]
        print(f"  {rank:>4} {row['sku']:<25} {store:<15} {row['markdown_probability']:>5.1%} {row['recommended_depth']:>5.1%} {row.get('weekly_velocity', 0):>5.1f} {row.get('product_age_weeks', 0):>4.0f} {rationale}")

    # Aggregate by parent SKU for commercial team view
    if "parent_sku" in output.columns:
        parent_view = (
            output.groupby("parent_sku")
            .agg(
                n_variants=("sku", "count"),
                avg_probability=("markdown_probability", "mean"),
                avg_depth=("recommended_depth", "mean"),
                max_priority=("priority_score", "max"),
            )
            .sort_values("max_priority", ascending=False)
            .head(15)
        )
        parent_view.to_csv(RECS_DIR / f"{filename}_by_parent.csv")

        print(f"\n  Top Parent SKUs for Review:")
        print(f"  {'Parent SKU':<25} {'Variants':>8} {'Avg Prob':>8} {'Avg Depth':>9}")
        print("  " + "-" * 55)
        for parent, row in parent_view.iterrows():
            print(f"  {parent:<25} {row['n_variants']:>8} {row['avg_probability']:>7.1%} {row['avg_depth']:>8.1%}")

    print(f"\n  Saved to: {RECS_DIR / filename}.csv")

    return output


def generate_historical_recommendations(weeks_back=12):
    """Generate recommendations for each of the last N weeks for backtesting comparison."""
    RECS_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading data...")
    df = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")
    all_weeks = sorted(df["week"].unique())

    target_weeks = all_weeks[-weeks_back:]
    print(f"Generating recommendations for {len(target_weeks)} weeks")

    all_recs = []
    for week in target_weeks:
        recs = generate_recommendations(target_week=week, min_confidence="medium", top_n=50)
        if recs is not None:
            all_recs.append(recs)

    if all_recs:
        combined = pd.concat(all_recs, ignore_index=True)
        combined.to_parquet(RECS_DIR / "historical_recommendations.parquet", index=False)
        print(f"\n  Combined {len(combined):,} recommendations across {len(target_weeks)} weeks")

    return combined if all_recs else None


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", type=str, default=None, help="Target week (YYYY-MM-DD)")
    parser.add_argument("--historical", type=int, default=0, help="Generate for last N weeks")
    parser.add_argument("--top-n", type=int, default=100, help="Max recommendations")
    parser.add_argument("--confidence", type=str, default="low", choices=["low", "medium", "high"])
    args = parser.parse_args()

    if args.historical > 0:
        generate_historical_recommendations(weeks_back=args.historical)
    else:
        generate_recommendations(
            target_week=args.week,
            min_confidence=args.confidence,
            top_n=args.top_n,
        )
