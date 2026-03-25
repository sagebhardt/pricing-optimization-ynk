"""
YNK Pricing Optimization API.

Serves the pricing dashboard: weekly actions, decisions, export, alerts,
audit log, feedback, admin panel. Authenticates via Google SSO.
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
import json
from pathlib import Path
from datetime import datetime
from typing import Optional

app = FastAPI(
    title="YNK Pricing Optimization API",
    version="2.0",
    description="ML-driven pricing actions for Yaneken Retail Group",
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


BASE_DIR = Path(__file__).parent.parent




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


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/alerts")
def get_size_alerts(brand: Optional[str] = Query(None), min_attrition: float = Query(0.3)):
    """Get size curve depletion alerts. Optionally filter by brand."""
    from api import storage
    alerts = storage.load_alerts()
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
    """Get model metadata and performance metrics per brand."""
    from api import storage
    meta = storage.load_model_info(brand or "hoka")

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
    """Get the weekly pricing action list."""
    from api import storage
    if not brand:
        return {"items": [], "week": None, "total": 0}
    return storage.load_pricing_actions(brand)


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

    ad = storage.load_pricing_actions(brand)
    if not ad.get("items"):
        raise HTTPException(404, "No pricing actions found")
    df = pd.DataFrame(ad["items"])
    week = ad["week"]

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
