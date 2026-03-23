"""
Reporting module for HOKA markdown optimization.

Generates a comprehensive analysis report combining:
- Data quality summary
- Model performance metrics
- Backtest results
- Current recommendations summary
- Key insights and action items
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
MODEL_DIR = Path(__file__).parent.parent.parent / "models"
REPORT_DIR = Path(__file__).parent.parent.parent / "reports"
RECS_DIR = Path(__file__).parent.parent.parent / "recommendations"


def data_quality_report(features_df, txn_df, products_df):
    """Assess data quality and coverage."""
    report = []
    report.append("## 1. Data Quality Summary\n")

    # Transaction coverage
    report.append("### Transaction Data")
    report.append(f"- Total transactions: {len(txn_df):,}")
    report.append(f"- Date range: {txn_df['fecha'].min().date()} to {txn_df['fecha'].max().date()}")
    report.append(f"- Unique SKUs: {txn_df['sku'].nunique():,}")
    report.append(f"- Unique stores: {txn_df['centro'].nunique()}")

    # Null analysis on features
    report.append("\n### Feature Completeness")
    null_rates = features_df.isnull().mean().sort_values(ascending=False)
    high_null = null_rates[null_rates > 0.1]
    if len(high_null) > 0:
        report.append("Features with >10% nulls:")
        for feat, rate in high_null.items():
            report.append(f"  - {feat}: {rate:.1%} null")
    else:
        report.append("All features have <10% null rate.")

    # Feature table
    report.append(f"\n### Feature Table")
    report.append(f"- Rows: {len(features_df):,}")
    report.append(f"- Columns: {len(features_df.columns)}")
    report.append(f"- SKUs: {features_df['sku'].nunique():,}")
    report.append(f"- Stores: {features_df['centro'].nunique()}")
    report.append(f"- Week range: {features_df['week'].min().date()} to {features_df['week'].max().date()}")

    # SKU coverage check
    txn_skus = set(txn_df["sku"].unique())
    product_skus = set(products_df["material"].unique())
    in_txn_not_product = txn_skus - product_skus
    if in_txn_not_product:
        report.append(f"\n**Warning:** {len(in_txn_not_product)} SKUs in transactions but not in product master")

    return "\n".join(report)


def model_performance_report():
    """Summarize model training results."""
    report = []
    report.append("## 2. Model Performance\n")

    try:
        with open(MODEL_DIR / "training_metadata.json") as f:
            meta = json.load(f)
    except FileNotFoundError:
        report.append("*No training metadata found. Run train.py first.*")
        return "\n".join(report)

    # Classifier
    report.append("### Markdown Probability Classifier")
    cls = meta["classifier"]
    report.append(f"- Features: {cls['n_features']}")
    report.append(f"- Training samples: {cls['n_samples']:,}")
    report.append(f"- **Average AUC: {cls['avg_auc']:.3f}**")
    report.append(f"- **Average Precision: {cls['avg_precision']:.3f}**")
    report.append("\nCross-validation folds:")
    for fold in cls["cv_results"]:
        report.append(f"  - {fold['val_weeks']}: AUC={fold['auc']:.3f}, P={fold['precision']:.3f}, R={fold['recall']:.3f}")

    # Regressor
    report.append("\n### Discount Depth Regressor")
    reg = meta["regressor"]
    report.append(f"- Features: {reg['n_features']}")
    report.append(f"- Training samples: {reg['n_samples']:,}")
    report.append(f"- **Average MAE: {reg['avg_mae']:.4f} ({reg['avg_mae']*100:.1f} pp)**")
    report.append(f"- **Average R²: {reg['avg_r2']:.3f}**")

    # SHAP importance
    report.append("\n### Top Feature Drivers (SHAP)")
    try:
        cls_shap = pd.read_csv(MODEL_DIR / "classifier_shap_importance.csv")
        report.append("\nClassifier top 10:")
        for _, row in cls_shap.head(10).iterrows():
            report.append(f"  - {row['feature']}: {row['mean_abs_shap']:.4f}")
    except FileNotFoundError:
        pass

    try:
        reg_shap = pd.read_csv(MODEL_DIR / "regressor_shap_importance.csv")
        report.append("\nRegressor top 10:")
        for _, row in reg_shap.head(10).iterrows():
            report.append(f"  - {row['feature']}: {row['mean_abs_shap']:.6f}")
    except FileNotFoundError:
        pass

    report.append(f"\n*Note: {meta.get('note', '')}*")

    return "\n".join(report)


def backtest_report():
    """Summarize backtesting results."""
    report = []
    report.append("## 3. Backtest Results\n")

    # Weekly metrics
    try:
        weekly = pd.read_csv(REPORT_DIR / "backtest_weekly_metrics.csv")
        report.append("### Walk-Forward Weekly Metrics")
        report.append(f"- Test period: {weekly['week'].min()} to {weekly['week'].max()} ({len(weekly)} weeks)")
        report.append(f"- **Precision@50: {weekly['precision_at_k'].mean():.1%}** (avg)")
        report.append(f"- **Recall@50: {weekly['recall_at_k'].mean():.1%}** (avg)")
        report.append(f"- **Depth MAE: {weekly['depth_mae'].mean():.4f}** ({weekly['depth_mae'].mean()*100:.1f}pp avg)")
        report.append(f"- Probability separation: positives={weekly['pos_avg_prob'].mean():.3f} vs negatives={weekly['neg_avg_prob'].mean():.3f}")
    except FileNotFoundError:
        report.append("*No weekly backtest data. Run backtest.py first.*")

    # Timing
    try:
        timing = pd.read_csv(REPORT_DIR / "backtest_timing_delta.csv")
        report.append("\n### Timing Analysis")
        report.append(f"- SKU-stores analyzed: {len(timing):,}")
        early = timing[timing["delta_weeks"] > 0]
        same = timing[(timing["delta_weeks"] >= -0.5) & (timing["delta_weeks"] <= 0.5)]
        late = timing[timing["delta_weeks"] < -0.5]
        report.append(f"- Model flagged earlier: {len(early):,} ({len(early)/len(timing):.1%}), avg {early['delta_weeks'].mean():.1f} weeks early")
        report.append(f"- Same timing: {len(same):,} ({len(same)/len(timing):.1%})")
        report.append(f"- Model flagged later: {len(late):,} ({len(late)/len(timing):.1%})")
    except FileNotFoundError:
        pass

    # Impact
    try:
        with open(REPORT_DIR / "backtest_impact_summary.json") as f:
            impact = json.load(f)
        report.append("\n### Estimated Revenue Impact")
        annual = impact.get("annualized_depth_savings_clp", 0)
        report.append(f"- Annualized depth optimization: ${annual:,.0f} CLP")
        report.append(f"- SKUs flagged earlier: {impact.get('timing_skus_flagged_early', 0):,}")
        report.append(f"- SKUs flagged later: {impact.get('timing_skus_flagged_late', 0):,}")
    except FileNotFoundError:
        pass

    # Category breakdown
    try:
        cats = pd.read_csv(REPORT_DIR / "backtest_category_breakdown.csv")
        report.append("\n### Category Breakdown")
        report.append(f"| Category | Sub-category | SKU-weeks | Actual Disc | Model Disc | Delta |")
        report.append(f"|----------|-------------|-----------|-------------|------------|-------|")
        for _, row in cats.head(8).iterrows():
            report.append(
                f"| {row['primera_jerarquia']} | {row['segunda_jerarquia']} | "
                f"{row['n_skuweeks']:,} | {row['avg_actual_disc']:.1%} | "
                f"{row['avg_model_disc']:.1%} | {row['avg_depth_delta']:+.1%} |"
            )
    except FileNotFoundError:
        pass

    return "\n".join(report)


def recommendations_report():
    """Summarize current recommendations."""
    report = []
    report.append("## 4. Current Recommendations\n")

    # Find most recent recommendation file
    try:
        rec_files = sorted(RECS_DIR.glob("recommendations_*.csv"))
        if not rec_files:
            report.append("*No recommendations generated yet.*")
            return "\n".join(report)

        latest = pd.read_csv(rec_files[-1])
        week = rec_files[-1].stem.replace("recommendations_", "")
        report.append(f"### Week of {week}")
        report.append(f"- Total recommendations: {len(latest)}")

        if "confidence" in latest.columns:
            conf_counts = latest["confidence"].value_counts()
            for conf, count in conf_counts.items():
                report.append(f"- {conf.title()} confidence: {count}")

        if "markdown_probability" in latest.columns:
            report.append(f"- Avg markdown probability: {latest['markdown_probability'].mean():.1%}")
        if "recommended_depth" in latest.columns:
            report.append(f"- Avg recommended depth: {latest['recommended_depth'].mean():.1%}")

    except Exception as e:
        report.append(f"*Error loading recommendations: {e}*")

    return "\n".join(report)


def key_insights():
    """Generate key insights and action items."""
    report = []
    report.append("## 5. Key Insights & Action Items\n")

    report.append("### What the Model Tells Us")
    report.append("1. **Discount history is the strongest predictor** — `weeks_since_discount` dominates. "
                  "Once a SKU enters a discount cycle, it tends to stay discounted.")
    report.append("2. **Size curve health matters** — SKUs with broken size curves (missing sizes) "
                  "are more likely to need deeper markdowns.")
    report.append("3. **Velocity deceleration is a leading indicator** — sell-through slowdowns "
                  "precede markdown events by 2-4 weeks on average.")
    report.append("4. **Store traffic improves predictions** — foot traffic data from Costanera and Marina "
                  "adds signal for conversion-based markdown timing.")

    report.append("\n### Data Gaps That Limit the Model")
    report.append("1. **No inventory/stock data** — Cannot calculate weeks-of-cover, the most important "
                  "signal for markdown timing. Currently using velocity as a proxy.")
    report.append("2. **No cost data** — Model optimizes revenue, not margin. With costs, "
                  "recommendations would protect gross margin directly.")
    report.append("3. **No season assignments** — Cannot determine end-of-season urgency. "
                  "Using product age as proxy.")

    report.append("\n### Recommended Next Steps")
    report.append("1. **Priority:** Get `ynk.stock` populated with daily inventory snapshots (Jacques)")
    report.append("2. **Priority:** Get `ynk.costos` populated with HOKA unit costs (Jacques)")
    report.append("3. **Quick win:** Add season/temporada mapping for HOKA SKUs")
    report.append("4. **Validation:** Review top 20 recommendations with HOKA commercial lead")
    report.append("5. **A/B design:** Define control vs. treatment store split for live pilot")

    return "\n".join(report)


def generate_full_report():
    """Generate complete analysis report."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating full report...")

    # Load data
    features = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")
    txn = pd.read_parquet(RAW_DIR / "hoka_transactions.parquet")
    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")

    sections = [
        f"# HOKA Markdown Optimization — POC Report",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Data range:** {features['week'].min().date()} to {features['week'].max().date()}",
        "",
        "---",
        "",
        data_quality_report(features, txn, products),
        "",
        "---",
        "",
        model_performance_report(),
        "",
        "---",
        "",
        backtest_report(),
        "",
        "---",
        "",
        recommendations_report(),
        "",
        "---",
        "",
        key_insights(),
    ]

    report_text = "\n".join(sections)

    # Save
    report_path = REPORT_DIR / "poc_report.md"
    with open(report_path, "w") as f:
        f.write(report_text)

    print(f"\nReport saved to: {report_path}")
    print(f"Report length: {len(report_text):,} characters")

    return report_text


if __name__ == "__main__":
    report = generate_full_report()
    print("\n" + "=" * 60)
    print(report)
