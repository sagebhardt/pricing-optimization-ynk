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

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, PlainTextResponse, JSONResponse
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


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Protect API endpoints with Google OAuth2 token verification."""
    path = request.url.path

    # Public paths — no auth
    if path in ("/health", "/auth/config") or path.startswith("/assets/") or path in ("/favicon.svg", "/favicon.ico"):
        return await call_next(request)

    from config.auth import GOOGLE_CLIENT_ID, get_user_role

    # Dev mode — no auth configured
    if not GOOGLE_CLIENT_ID:
        request.state.user = {
            "email": "dev@local", "name": "Developer", "picture": "",
            "role": "admin", "permissions": ["approve", "audit", "export", "manage", "read"],
            "brands": None,
        }
        return await call_next(request)

    # Non-API routes — serve SPA (frontend handles login)
    api_prefixes = ("/pricing", "/decisions", "/export", "/alerts", "/model",
                    "/recommendations", "/sku/", "/audit", "/auth/me", "/admin", "/feedback")
    if not any(path.startswith(p) for p in api_prefixes):
        return await call_next(request)

    # Verify Google OAuth2 token
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"detail": "Authentication required"})

    try:
        from google.oauth2 import id_token
        from google.auth.transport import requests as google_requests
        idinfo = id_token.verify_oauth2_token(
            auth_header[7:], google_requests.Request(), GOOGLE_CLIENT_ID
        )
    except Exception:
        return JSONResponse(status_code=401, content={"detail": "Invalid or expired token"})

    email = idinfo.get("email", "")
    role_info = get_user_role(email)
    if role_info is None:
        return JSONResponse(status_code=403, content={"detail": f"Access denied for {email}"})

    request.state.user = {
        "email": email,
        "name": idinfo.get("name", email),
        "picture": idinfo.get("picture", ""),
        **role_info,
    }
    return await call_next(request)


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


class DecisionPayload(BaseModel):
    brand: str
    week: str
    key: str
    status: Optional[str] = None


class BulkDecisionPayload(BaseModel):
    brand: str
    week: str
    keys: list[str]
    status: str


class FeedbackPayload(BaseModel):
    brand: str
    week: str
    key: str
    implemented: bool
    actual_price: Optional[int] = None
    note: Optional[str] = ""


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

    # Load alerts from all brands
    alert_frames = []
    for brand_dir in (Path(__file__).parent.parent / "data" / "processed").iterdir():
        if brand_dir.is_dir():
            alert_path = brand_dir / "size_curve_alerts.parquet"
            if alert_path.exists():
                df = pd.read_parquet(alert_path)
                df["brand"] = brand_dir.name
                alert_frames.append(df)
    state["alerts"] = pd.concat(alert_frames, ignore_index=True) if alert_frames else pd.DataFrame()

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
def get_size_alerts(brand: Optional[str] = Query(None), min_attrition: float = Query(0.3)):
    """Get size curve depletion alerts. Optionally filter by brand."""
    alerts = state.get("alerts", pd.DataFrame())
    if len(alerts) == 0:
        return {"alerts": [], "week": None, "total_alerts": 0}

    if brand and "brand" in alerts.columns:
        alerts = alerts[alerts["brand"] == brand.lower()]
        if len(alerts) == 0:
            return {"alerts": [], "week": None, "total_alerts": 0}

    latest = alerts[alerts["week"] == alerts["week"].max()]
    latest = latest[latest["attrition_rate"] >= min_attrition].sort_values("attrition_rate", ascending=False)

    results = []
    for _, row in latest.head(30).iterrows():
        results.append({
            "parent_sku": row["codigo_padre"],
            "store": row["centro"],
            "brand": row.get("brand", ""),
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


# ── Authentication helpers ────────────────────────────────────────────────────

def _get_user(request: Request) -> dict:
    """Get authenticated user from request state (set by middleware)."""
    return getattr(request.state, "user", {
        "email": "unknown", "name": "Unknown", "role": "viewer",
        "permissions": [], "brands": None,
    })


def _check_brand_access(user: dict, brand: str):
    """Raise 403 if user doesn't have access to this brand."""
    user_brands = user.get("brands")
    if user_brands is not None and brand.lower() not in user_brands:
        raise HTTPException(403, f"No access to brand {brand}")


@app.get("/auth/config")
def auth_config():
    """Auth configuration (public — needed before login)."""
    from config.auth import GOOGLE_CLIENT_ID
    return {"client_id": GOOGLE_CLIENT_ID, "required": bool(GOOGLE_CLIENT_ID)}


@app.get("/auth/me")
def auth_me(request: Request):
    """Current user info."""
    return _get_user(request)


# ── Admin: user management ────────────────────────────────────────────────────

class UserPayload(BaseModel):
    email: str
    role: str
    brands: Optional[list[str]] = None
    name: Optional[str] = ""


@app.get("/admin/users")
def admin_list_users(request: Request):
    """List all configured users."""
    from api import storage
    user = _get_user(request)
    if "manage" not in user.get("permissions", []):
        raise HTTPException(403, "Admin access required")
    cfg = storage.load_user_config()
    return cfg


@app.post("/admin/users")
def admin_set_user(payload: UserPayload, request: Request):
    """Add or update a user's role."""
    from api import storage
    user = _get_user(request)
    if "manage" not in user.get("permissions", []):
        raise HTTPException(403, "Admin access required")
    if payload.role not in ("admin", "brand_manager", "viewer"):
        raise HTTPException(400, "Invalid role")

    cfg = storage.load_user_config()
    cfg.setdefault("users", {})[payload.email.lower().strip()] = {
        "role": payload.role,
        "brands": payload.brands if payload.role == "brand_manager" else None,
        "name": payload.name or "",
    }
    storage.save_user_config(cfg)

    storage.append_audit({
        "brand": "_system",
        "user_email": user["email"],
        "user_name": user["name"],
        "action": "set_role",
        "key": payload.email.lower(),
        "detail": payload.role,
    })
    return {"ok": True, "total_users": len(cfg["users"])}


@app.delete("/admin/users")
def admin_delete_user(email: str = Query(...), request: Request = None):
    """Remove a user."""
    from api import storage
    user = _get_user(request)
    if "manage" not in user.get("permissions", []):
        raise HTTPException(403, "Admin access required")

    cfg = storage.load_user_config()
    removed = cfg.get("users", {}).pop(email.lower().strip(), None)
    if not removed:
        raise HTTPException(404, "User not found")
    storage.save_user_config(cfg)

    storage.append_audit({
        "brand": "_system",
        "user_email": user["email"],
        "user_name": user["name"],
        "action": "remove_user",
        "key": email.lower(),
    })
    return {"ok": True}


@app.put("/admin/domains")
def admin_set_domains(request: Request, domains: list[str]):
    """Update allowed email domains."""
    from api import storage
    user = _get_user(request)
    if "manage" not in user.get("permissions", []):
        raise HTTPException(403, "Admin access required")
    cfg = storage.load_user_config()
    cfg["allowed_domains"] = [d.lower().strip() for d in domains]
    storage.save_user_config(cfg)
    return {"ok": True}


# ── Decisions (storage-backed) ────────────────────────────────────────────────

@app.get("/decisions")
def get_decisions(brand: str = Query(...), week: Optional[str] = Query(None)):
    """Get decisions for a brand (latest week by default)."""
    from api import storage
    return storage.load_decisions(brand, week)


@app.post("/decisions")
def save_decision(payload: DecisionPayload, request: Request):
    """Save a single approve/reject decision."""
    from api import storage
    user = _get_user(request)
    if "approve" not in user.get("permissions", []):
        raise HTTPException(403, "Permission 'approve' required")
    _check_brand_access(user, payload.brand)

    data = storage.load_decisions(payload.brand, payload.week)
    data["week"] = payload.week
    data["brand"] = payload.brand.lower()

    if payload.status is None or payload.status == "":
        data["decisions"].pop(payload.key, None)
        action = "undo"
    else:
        data["decisions"][payload.key] = {
            "status": payload.status,
            "timestamp": datetime.now().isoformat(),
            "user": user["email"],
        }
        action = payload.status

    storage.save_decisions(data)
    storage.append_audit({
        "brand": payload.brand.lower(),
        "user_email": user["email"],
        "user_name": user["name"],
        "action": action,
        "key": payload.key,
        "week": payload.week,
    })
    return {"ok": True, "total": len(data["decisions"])}


@app.post("/decisions/bulk")
def bulk_decisions(payload: BulkDecisionPayload, request: Request):
    """Bulk approve/reject (only sets keys not already decided)."""
    from api import storage
    user = _get_user(request)
    if "approve" not in user.get("permissions", []):
        raise HTTPException(403, "Permission 'approve' required")
    _check_brand_access(user, payload.brand)

    data = storage.load_decisions(payload.brand, payload.week)
    data["week"] = payload.week
    data["brand"] = payload.brand.lower()

    changed = 0
    for key in payload.keys:
        if key not in data["decisions"]:
            data["decisions"][key] = {
                "status": payload.status,
                "timestamp": datetime.now().isoformat(),
                "user": user["email"],
            }
            changed += 1

    storage.save_decisions(data)
    if changed:
        storage.append_audit({
            "brand": payload.brand.lower(),
            "user_email": user["email"],
            "user_name": user["name"],
            "action": f"bulk_{payload.status}",
            "count": changed,
            "week": payload.week,
        })
    return {"ok": True, "total": len(data["decisions"])}


# ── Audit log ─────────────────────────────────────────────────────────────────

@app.get("/audit")
def get_audit(request: Request, brand: str = Query(...), limit: int = Query(100, ge=1, le=500)):
    """Get recent audit log entries for a brand."""
    from api import storage
    user = _get_user(request)
    if "audit" not in user.get("permissions", []):
        raise HTTPException(403, "Permission 'audit' required")
    return {"items": storage.load_audit(brand, limit)}


# ── Feedback (ops implementation tracking) ────────────────────────────────────

@app.get("/feedback")
def get_feedback(brand: str = Query(...), week: Optional[str] = Query(None)):
    """Get ops implementation feedback for a brand."""
    from api import storage
    return storage.load_feedback(brand, week)


@app.post("/feedback")
def save_feedback_item(payload: FeedbackPayload, request: Request):
    """Report whether a price change was implemented by ops."""
    from api import storage
    user = _get_user(request)

    data = storage.load_feedback(payload.brand, payload.week)
    data["week"] = payload.week
    data["brand"] = payload.brand.lower()
    data.setdefault("items", {})[payload.key] = {
        "implemented": payload.implemented,
        "actual_price": payload.actual_price,
        "note": payload.note or "",
        "reported_by": user["email"],
        "reported_at": datetime.now().isoformat(),
    }
    storage.save_feedback(data)

    storage.append_audit({
        "brand": payload.brand.lower(),
        "user_email": user["email"],
        "user_name": user["name"],
        "action": "feedback_implemented" if payload.implemented else "feedback_skipped",
        "key": payload.key,
        "week": payload.week,
    })
    return {"ok": True}


# ── Export ────────────────────────────────────────────────────────────────────

def _format_clp(n) -> str:
    """Format number as CLP: $36.990"""
    try:
        v = int(round(float(n)))
        formatted = f"{abs(v):,}".replace(",", ".")
        return f"-${formatted}" if v < 0 else f"${formatted}"
    except (ValueError, TypeError):
        return str(n)


@app.get("/export/price-changes")
def export_price_changes(
    request: Request,
    brand: str = Query(...),
    format: str = Query("excel", enum=["excel", "text"]),
):
    """Export approved price changes as Excel or plain text."""
    from api import storage
    user = _get_user(request)
    if "export" not in user.get("permissions", []):
        raise HTTPException(403, "Permission 'export' required")
    _check_brand_access(user, brand)

    actions_dir = Path(__file__).parent.parent / "weekly_actions" / brand.lower()
    files = sorted(actions_dir.glob("pricing_actions_*.csv"))
    if not files:
        raise HTTPException(404, "No pricing actions found")

    df = pd.read_csv(files[-1]).fillna("")
    week = files[-1].stem.replace("pricing_actions_", "")

    dec_data = storage.load_decisions(brand, week)
    dec_map = dec_data.get("decisions", {})

    df["_key"] = df["parent_sku"].astype(str) + "-" + df["store"].astype(str)
    approved = df[df["_key"].apply(
        lambda k: dec_map.get(k, {}).get("status") == "approved"
    )].copy()

    if len(approved) == 0:
        raise HTTPException(400, "No hay acciones aprobadas para exportar")

    increases = approved[approved["action_type"] == "increase"].copy()
    markdowns = approved[approved["action_type"] != "increase"].copy()

    storage.append_audit({
        "brand": brand.lower(),
        "user_email": user["email"],
        "user_name": user["name"],
        "action": f"export_{format}",
        "count": len(approved),
        "week": week,
    })

    if format == "text":
        return _export_text(brand, week, increases, markdowns)
    return _export_excel(brand, week, increases, markdowns)


def _export_text(brand, week, increases, markdowns):
    """Plain text export for copy-paste into messaging."""
    lines = []
    total = len(increases) + len(markdowns)
    impact = int(
        (increases["rev_delta"].astype(float).sum() if len(increases) else 0)
        + (markdowns["rev_delta"].astype(float).sum() if len(markdowns) else 0)
    )

    lines.append(f"CAMBIOS DE PRECIO — {brand.upper()}")
    lines.append(f"Semana: {week}  |  Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"Aprobados: {total} cambios  |  Impacto estimado: {_format_clp(impact)}/semana")
    lines.append("")

    if len(increases) > 0:
        lines.append(f"{'═' * 70}")
        lines.append(f"  SUBIR PRECIO ({len(increases)} productos)")
        lines.append(f"{'═' * 70}")
        lines.append("")
        lines.append(f"{'SKU':<16} {'PRODUCTO':<32} {'ANTES':>12} {'NUEVO':>12}")
        lines.append(f"{'─' * 16} {'─' * 32} {'─' * 12} {'─' * 12}")
        for _, r in increases.iterrows():
            lines.append(
                f"{str(r['parent_sku']):<16} "
                f"{str(r['product'])[:32]:<32} "
                f"{_format_clp(r['current_price']):>12} "
                f"{_format_clp(r['recommended_price']):>12}"
            )
        lines.append("")

    if len(markdowns) > 0:
        lines.append(f"{'═' * 70}")
        lines.append(f"  REBAJAS ({len(markdowns)} productos)")
        lines.append(f"{'═' * 70}")
        lines.append("")
        lines.append(f"{'SKU':<16} {'PRODUCTO':<28} {'ANTES':>12} {'NUEVO':>12} {'DCTO':>8}")
        lines.append(f"{'─' * 16} {'─' * 28} {'─' * 12} {'─' * 12} {'─' * 8}")
        for _, r in markdowns.iterrows():
            lines.append(
                f"{str(r['parent_sku']):<16} "
                f"{str(r['product'])[:28]:<28} "
                f"{_format_clp(r['current_price']):>12} "
                f"{_format_clp(r['recommended_price']):>12} "
                f"{str(r['recommended_discount']):>8}"
            )
        lines.append("")

    return PlainTextResponse("\n".join(lines))


def _export_excel(brand, week, increases, markdowns):
    """Excel export with formatted sheets."""
    import io
    from api import storage
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter
    except ImportError:
        raise HTTPException(500, "openpyxl no instalado — pip install openpyxl")

    wb = Workbook()
    wb.remove(wb.active)

    hdr_font = Font(bold=True, size=11, color="FFFFFF")
    hdr_fill = PatternFill(start_color="1F2937", end_color="1F2937", fill_type="solid")
    money_fmt = '#,##0'
    row_border = Border(bottom=Side(style='thin', color='E5E7EB'))

    def build_sheet(ws, title, rows, columns, col_widths):
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(columns))
        c = ws.cell(row=1, column=1, value=title)
        c.font = Font(bold=True, size=14)
        ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=len(columns))
        ws.cell(
            row=2, column=1,
            value=f"Semana: {week}  |  Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  {len(rows)} productos"
        ).font = Font(size=10, color="6B7280")
        for ci, (col_name, _) in enumerate(columns, 1):
            cell = ws.cell(row=4, column=ci, value=col_name)
            cell.font = hdr_font
            cell.fill = hdr_fill
            cell.alignment = Alignment(horizontal='center')
        money_keys = {'current_price', 'recommended_price', 'rev_delta', 'current_list_price',
                      'current_weekly_rev', 'expected_weekly_rev'}
        for ri, row_data in enumerate(rows, 5):
            for ci, (_, key) in enumerate(columns, 1):
                val = row_data.get(key, "")
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.border = row_border
                if key in money_keys:
                    try:
                        cell.value = int(float(val))
                        cell.number_format = money_fmt
                    except (ValueError, TypeError):
                        pass
        for ci, w in enumerate(col_widths, 1):
            ws.column_dimensions[get_column_letter(ci)].width = w

    if len(increases) > 0:
        ws = wb.create_sheet("Subir Precio")
        cols = [
            ("SKU", "parent_sku"), ("Producto", "product"), ("Tienda", "store_name"),
            ("Precio Actual", "current_price"), ("Precio Nuevo", "recommended_price"),
            ("Delta Rev/Sem", "rev_delta"),
        ]
        build_sheet(ws, f"SUBIR PRECIO — {brand.upper()}", increases.to_dict('records'),
                    cols, [18, 35, 25, 15, 15, 15])

    if len(markdowns) > 0:
        ws = wb.create_sheet("Rebajas")
        cols = [
            ("SKU", "parent_sku"), ("Producto", "product"), ("Tienda", "store_name"),
            ("Descuento", "recommended_discount"), ("Precio Actual", "current_price"),
            ("Precio Nuevo", "recommended_price"), ("Urgencia", "urgency"),
            ("Delta Rev/Sem", "rev_delta"),
        ]
        build_sheet(ws, f"REBAJAS — {brand.upper()}", markdowns.to_dict('records'),
                    cols, [18, 30, 25, 12, 15, 15, 12, 15])

    buffer = io.BytesIO()
    wb.save(buffer)

    filename = f"cambios_precio_{brand.lower()}_{week}.xlsx"
    storage.save_export(brand, filename, buffer.getvalue())

    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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
