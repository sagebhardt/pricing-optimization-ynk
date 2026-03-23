"""
FastAPI recommendation API for HOKA markdown optimization.

Endpoints:
- GET  /health              → Health check
- GET  /recommendations     → Current week's recommendations
- GET  /recommendations/{week} → Recommendations for a specific week
- GET  /sku/{sku_id}        → Recommendation detail for a specific SKU
- GET  /alerts              → Size curve depletion alerts
- GET  /model/info          → Model metadata and performance
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import pandas as pd
import numpy as np
import xgboost as xgb
import shap
import pickle
import json
from pathlib import Path
from datetime import date, datetime
from typing import Optional

app = FastAPI(
    title="HOKA Markdown Optimization API",
    version="0.1.0",
    description="ML-driven markdown timing and depth recommendations for HOKA",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths
MODEL_DIR = Path(__file__).parent.parent / "models"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

# Global state (loaded on startup)
state = {}

EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
]
CATEGORICAL_COLS = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]

STORE_NAMES = {
    "7501": "Hoka Costanera",
    "7502": "Hoka Marina",
    "AB75": "CD Hoka (E-commerce)",
    "7599": "Hoka Eventos",
}


class Recommendation(BaseModel):
    sku: str
    store_code: str
    store_name: str
    product_name: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    markdown_probability: float
    confidence: str
    recommended_depth: float
    priority_score: float
    weekly_velocity: float
    product_age_weeks: float
    lifecycle_stage: Optional[str] = None
    size_curve_health: Optional[float] = None
    price_elasticity: Optional[float] = None
    rationale: list[str]


class ModelInfo(BaseModel):
    version: str
    classifier_auc: float
    classifier_avg_precision: float
    regressor_mae: float
    regressor_r2: float
    data_range: str
    n_features: int
    note: str


@app.on_event("startup")
def load_models():
    """Load models and data on startup."""
    # Try v2 first, fall back to v1
    try:
        with open(MODEL_DIR / "markdown_classifier_v2.pkl", "rb") as f:
            state["cls_model"] = pickle.load(f)
        with open(MODEL_DIR / "depth_regressor_v2.pkl", "rb") as f:
            state["reg_model"] = pickle.load(f)
        state["model_version"] = "v2"

        with open(MODEL_DIR / "training_metadata_v2.json") as f:
            state["metadata"] = json.load(f)
    except FileNotFoundError:
        with open(MODEL_DIR / "markdown_classifier.pkl", "rb") as f:
            state["cls_model"] = pickle.load(f)
        with open(MODEL_DIR / "depth_regressor.pkl", "rb") as f:
            state["reg_model"] = pickle.load(f)
        state["model_version"] = "v1"

        with open(MODEL_DIR / "training_metadata.json") as f:
            state["metadata"] = json.load(f)

    # Load feature data
    try:
        state["features"] = pd.read_parquet(PROCESSED_DIR / "hoka_features_v2.parquet")
    except FileNotFoundError:
        state["features"] = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")

    # Load product master
    state["products"] = pd.read_parquet(RAW_DIR / "hoka_products.parquet")

    # Load alerts
    try:
        state["alerts"] = pd.read_parquet(PROCESSED_DIR / "size_curve_alerts.parquet")
    except FileNotFoundError:
        state["alerts"] = pd.DataFrame()

    state["explainer"] = shap.TreeExplainer(state["cls_model"])
    print(f"Loaded model {state['model_version']} with {len(state['features']):,} feature rows")


def score_week(target_week=None):
    """Score all SKU-stores for a given week."""
    df = state["features"]
    if target_week is None:
        target_week = df["week"].max()
    else:
        target_week = pd.Timestamp(target_week)

    week_data = df[df["week"] == target_week].copy()
    if len(week_data) == 0:
        return None, target_week

    # Prepare features
    df_prep = week_data.copy()
    for col in CATEGORICAL_COLS:
        if col in df_prep.columns:
            df_prep[col] = df_prep[col].astype("category").cat.codes

    feat_cols = [c for c in df_prep.columns if c not in EXCLUDE_COLS]
    X = df_prep[feat_cols].values

    # Score
    week_data["markdown_probability"] = state["cls_model"].predict_proba(X)[:, 1]
    week_data["recommended_depth"] = np.clip(state["reg_model"].predict(X), 0, 0.60)

    # Confidence
    week_data["confidence"] = week_data["markdown_probability"].apply(
        lambda p: "high" if p >= 0.75 else "medium" if p >= 0.50 else "low" if p >= 0.30 else "skip"
    )

    # Priority score
    week_data["priority_score"] = (
        week_data["markdown_probability"]
        * week_data["velocity_4w"].fillna(0).clip(lower=0)
        * week_data["avg_precio_lista"].fillna(0).clip(lower=0)
    )

    # Lifecycle stage name
    stage_map = {0: "launch", 1: "growth", 2: "peak", 3: "steady", 4: "decline", 5: "clearance"}
    if "lifecycle_stage_code" in week_data.columns:
        week_data["lifecycle_stage"] = week_data["lifecycle_stage_code"].map(stage_map)

    # Product info
    prod = state["products"][["material", "material_descripcion", "primera_jerarquia",
                               "segunda_jerarquia"]].rename(
        columns={"material": "sku", "material_descripcion": "product_name"}
    ).drop_duplicates(subset=["sku"])
    week_data = week_data.merge(prod, on="sku", how="left", suffixes=("", "_prod"))

    return week_data, target_week


def build_rationale(X_row, feat_cols):
    """Build SHAP-based rationale for a single prediction."""
    sv = state["explainer"].shap_values(X_row)[0]
    top_idx = np.abs(sv).argsort()[-3:][::-1]

    rationale = []
    for idx in top_idx:
        feat = feat_cols[idx]
        val = X_row[0, idx]
        direction = "increases" if sv[idx] > 0 else "decreases"
        rationale.append(f"{feat}={val:.2f} → {direction} markdown likelihood")

    return rationale


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_version": state.get("model_version", "unknown"),
        "data_rows": len(state.get("features", [])),
    }


@app.get("/recommendations")
def get_recommendations(
    min_confidence: str = Query("medium", enum=["low", "medium", "high"]),
    top_n: int = Query(50, ge=1, le=500),
    store: Optional[str] = Query(None),
):
    """Get current week's markdown recommendations."""
    scored, week = score_week()
    if scored is None:
        raise HTTPException(404, "No data for current week")

    conf_thresholds = {"low": 0.30, "medium": 0.50, "high": 0.75}
    min_prob = conf_thresholds[min_confidence]
    recs = scored[scored["markdown_probability"] >= min_prob]

    if store:
        recs = recs[recs["centro"] == store]

    recs = recs.sort_values("priority_score", ascending=False).head(top_n)

    results = []
    for _, row in recs.iterrows():
        results.append({
            "sku": row["sku"],
            "store_code": row["centro"],
            "store_name": STORE_NAMES.get(row["centro"], row["centro"]),
            "product_name": row.get("product_name"),
            "category": row.get("primera_jerarquia_prod") or row.get("primera_jerarquia"),
            "subcategory": row.get("segunda_jerarquia_prod") or row.get("segunda_jerarquia"),
            "markdown_probability": round(row["markdown_probability"], 4),
            "confidence": row["confidence"],
            "recommended_depth": round(row["recommended_depth"], 4),
            "priority_score": round(row["priority_score"], 2),
            "weekly_velocity": round(row.get("velocity_4w", 0), 2),
            "product_age_weeks": round(row.get("product_age_weeks", 0), 1),
            "lifecycle_stage": row.get("lifecycle_stage"),
            "size_curve_health": round(row["size_curve_completeness"], 2) if pd.notna(row.get("size_curve_completeness")) else None,
            "price_elasticity": round(row["price_elasticity"], 2) if pd.notna(row.get("price_elasticity")) else None,
        })

    return {
        "week": str(week.date()),
        "total_scored": len(scored),
        "recommendations": len(results),
        "items": results,
    }


@app.get("/recommendations/{week}")
def get_recommendations_for_week(
    week: str,
    min_confidence: str = Query("medium", enum=["low", "medium", "high"]),
    top_n: int = Query(50, ge=1, le=500),
):
    """Get recommendations for a specific week."""
    scored, target_week = score_week(week)
    if scored is None:
        raise HTTPException(404, f"No data for week {week}")

    conf_thresholds = {"low": 0.30, "medium": 0.50, "high": 0.75}
    min_prob = conf_thresholds[min_confidence]
    recs = scored[scored["markdown_probability"] >= min_prob].sort_values("priority_score", ascending=False).head(top_n)

    results = []
    for _, row in recs.iterrows():
        results.append({
            "sku": row["sku"],
            "store_code": row["centro"],
            "store_name": STORE_NAMES.get(row["centro"], row["centro"]),
            "markdown_probability": round(row["markdown_probability"], 4),
            "confidence": row["confidence"],
            "recommended_depth": round(row["recommended_depth"], 4),
            "priority_score": round(row["priority_score"], 2),
            "weekly_velocity": round(row.get("velocity_4w", 0), 2),
        })

    return {
        "week": str(target_week.date()),
        "recommendations": len(results),
        "items": results,
    }


@app.get("/sku/{sku_id}")
def get_sku_detail(sku_id: str):
    """Get detailed recommendation and history for a specific SKU."""
    df = state["features"]
    sku_data = df[df["sku"] == sku_id].sort_values("week")

    if len(sku_data) == 0:
        raise HTTPException(404, f"SKU {sku_id} not found")

    # Product info
    prod = state["products"]
    prod_info = prod[prod["material"] == sku_id].iloc[0] if len(prod[prod["material"] == sku_id]) > 0 else None

    # Weekly history
    history = []
    for _, row in sku_data.tail(12).iterrows():
        history.append({
            "week": str(row["week"].date()),
            "store": row["centro"],
            "units_sold": int(row["units_sold"]),
            "velocity_4w": round(row["velocity_4w"], 2),
            "discount_rate": round(row.get("discount_rate", 0) or 0, 3),
            "has_discount": bool(row["has_discount"]),
        })

    return {
        "sku": sku_id,
        "product_name": prod_info["material_descripcion"] if prod_info is not None else None,
        "category": prod_info["primera_jerarquia"] if prod_info is not None else None,
        "parent_sku": prod_info["codigo_padre"] if prod_info is not None else None,
        "total_units_sold": int(sku_data["cumulative_units"].max()),
        "weeks_active": len(sku_data[sku_data["units_sold"] > 0]),
        "history": history,
    }


@app.get("/alerts")
def get_size_alerts(min_attrition: float = Query(0.3)):
    """Get size curve depletion alerts."""
    alerts = state.get("alerts", pd.DataFrame())
    if len(alerts) == 0:
        return {"alerts": []}

    latest = alerts[alerts["week"] == alerts["week"].max()]
    latest = latest[latest["attrition_rate"] >= min_attrition].sort_values("attrition_rate", ascending=False)

    results = []
    for _, row in latest.head(30).iterrows():
        results.append({
            "parent_sku": row["codigo_padre"],
            "store": row["centro"],
            "active_sizes": int(row["active_sizes_4w"]),
            "total_sizes": int(row["total_sizes_ever"]),
            "attrition_rate": round(row["attrition_rate"], 3),
            "core_completeness": round(row["core_completeness"], 3),
            "alert_reasons": row.get("alert_reasons", ""),
        })

    return {
        "week": str(alerts["week"].max().date()),
        "total_alerts": len(latest),
        "items": results,
    }


@app.get("/model/info")
def get_model_info(brand: Optional[str] = Query(None)):
    """Get model metadata and performance metrics, optionally per brand."""
    if brand:
        meta_path = Path(__file__).parent.parent / "models" / brand.lower() / "training_metadata.json"
        try:
            with open(meta_path) as f:
                meta = json.load(f)
        except FileNotFoundError:
            meta = state.get("metadata", {})
    else:
        meta = state.get("metadata", {})

    cls = meta.get("classifier", {})
    reg = meta.get("regressor", {})

    return {
        "version": "parent",
        "brand": brand or "default",
        "classifier": {
            "avg_auc": cls.get("avg_auc"),
            "avg_precision": cls.get("avg_precision"),
            "n_features": cls.get("n_features"),
        },
        "regressor": {
            "avg_mae": reg.get("avg_mae"),
            "avg_r2": reg.get("avg_r2"),
        },
        "note": meta.get("note", ""),
    }


@app.get("/pricing-actions")
def get_pricing_actions(brand: Optional[str] = Query(None)):
    """Get the weekly pricing action list (CSV-backed). Optionally filter by brand."""
    base = Path(__file__).parent.parent / "weekly_actions"
    if brand:
        actions_dir = base / brand.lower()
    else:
        actions_dir = base
    try:
        files = sorted(actions_dir.glob("pricing_actions_*.csv"))
        if not files:
            return {"items": [], "week": None, "total": 0}
        latest = pd.read_csv(files[-1]).fillna("")
        week = files[-1].stem.replace("pricing_actions_", "")
        items = latest.to_dict(orient="records")
        return {"week": week, "total": len(items), "items": items}
    except Exception as e:
        raise HTTPException(500, str(e))


# Serve React dashboard (must be after all API routes)
STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    def serve_spa(full_path: str):
        """Serve React SPA for any non-API route."""
        file_path = STATIC_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(STATIC_DIR / "index.html")
