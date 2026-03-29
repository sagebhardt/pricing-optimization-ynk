"""Persistence layer: GCS-backed with local file fallback for dev."""

import json
import os
import time
from datetime import datetime
from pathlib import Path

GCS_BUCKET = os.getenv("GCS_BUCKET", "")
_BASE_DIR = Path(__file__).parent.parent  # project root

# ── GCS helpers ───────────────────────────────────────────────────────────────

_gcs_client = None


def _get_bucket():
    global _gcs_client
    from google.cloud import storage
    if _gcs_client is None:
        _gcs_client = storage.Client()
    return _gcs_client.bucket(GCS_BUCKET)


def _use_gcs() -> bool:
    return bool(GCS_BUCKET)


# ── In-memory cache (TTL-based) ──────────────────────────────────────────────

_cache = {}
CACHE_TTL = 300  # 5 minutes


def _cached(key: str, loader, ttl: int = CACHE_TTL):
    """Return cached value or call loader. Thread-safe enough for single-worker uvicorn."""
    now = time.time()
    entry = _cache.get(key)
    if entry and now - entry["ts"] < ttl:
        return entry["val"]
    val = loader()
    _cache[key] = {"val": val, "ts": now}
    return val


def cache_clear(prefix: str = ""):
    """Clear cache entries matching prefix."""
    keys = [k for k in _cache if k.startswith(prefix)]
    for k in keys:
        _cache.pop(k, None)


# ── Pricing actions (read-only, written by pipeline) ──────────────────────────

def load_pricing_actions(brand: str) -> dict:
    """Load latest pricing actions CSV. Returns {week, total, items}."""
    return _cached(f"actions:{brand}", lambda: _load_pricing_actions_impl(brand))


def _load_pricing_actions_impl(brand: str) -> dict:
    import pandas as pd

    def _ensure_vendor_brand(df, brand):
        """Add vendor_brand column if missing (for CSVs generated before the column was added)."""
        if "vendor_brand" not in df.columns and "parent_sku" in df.columns:
            from config.vendor_brands import get_vendor_brand
            df["vendor_brand"] = df["parent_sku"].apply(lambda s: get_vendor_brand(s, brand))
        return df

    # Try GCS first
    if _use_gcs():
        try:
            bucket = _get_bucket()
            prefix = f"weekly_actions/{brand.lower()}/pricing_actions_"
            blobs = sorted([b for b in bucket.list_blobs(prefix=prefix)], key=lambda b: b.name)
            if blobs:
                import io
                content = blobs[-1].download_as_text()
                df = pd.read_csv(io.StringIO(content)).fillna("")
                df = _ensure_vendor_brand(df, brand)
                week = blobs[-1].name.split("pricing_actions_")[1].replace(".csv", "")
                return {"week": week, "total": len(df), "items": df.to_dict(orient="records")}
        except Exception:
            pass  # fall through to local

    # Local fallback
    actions_dir = _BASE_DIR / "weekly_actions" / brand.lower()
    try:
        files = sorted(actions_dir.glob("pricing_actions_*.csv"))
        if not files:
            return {"items": [], "week": None, "total": 0}
        df = pd.read_csv(files[-1]).fillna("")
        df = _ensure_vendor_brand(df, brand)
        week = files[-1].stem.replace("pricing_actions_", "")
        return {"week": week, "total": len(df), "items": df.to_dict(orient="records")}
    except Exception:
        return {"items": [], "week": None, "total": 0}


# ── Alerts (read-only, written by pipeline) ───────────────────────────────────

def load_alerts() -> "pd.DataFrame":
    """Load size curve alerts for all brands."""
    return _cached("alerts:all", _load_alerts_impl, ttl=600)


def _load_alerts_impl():
    import pandas as pd

    frames = []

    # Try GCS first
    if _use_gcs():
        try:
            bucket = _get_bucket()
            for blob in bucket.list_blobs(prefix="alerts/"):
                if blob.name.endswith(".parquet"):
                    import io
                    content = blob.download_as_bytes()
                    df = pd.read_parquet(io.BytesIO(content))
                    brand = blob.name.split("/")[1]
                    df["brand"] = brand
                    frames.append(df)
            if frames:
                return pd.concat(frames, ignore_index=True)
        except Exception:
            pass  # fall through to local

    # Local fallback
    processed = _BASE_DIR / "data" / "processed"
    if processed.exists():
        for brand_dir in processed.iterdir():
            if brand_dir.is_dir():
                ap = brand_dir / "size_curve_alerts.parquet"
                if ap.exists():
                    df = pd.read_parquet(ap)
                    df["brand"] = brand_dir.name
                    frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Model metadata (read-only) ───────────────────────────────────────────────

def load_model_info(brand: str) -> dict:
    """Load training metadata for a brand."""
    return _cached(f"model_info:{brand}", lambda: _load_model_info_impl(brand), ttl=600)


def _load_model_info_impl(brand: str) -> dict:
    # Try GCS first
    if _use_gcs():
        try:
            bucket = _get_bucket()
            blob = bucket.blob(f"models/{brand.lower()}/training_metadata.json")
            if blob.exists():
                return json.loads(blob.download_as_text())
        except Exception:
            pass

    # Local fallback
    fp = _BASE_DIR / "models" / brand.lower() / "training_metadata.json"
    try:
        with open(fp) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


# ── SHAP features (read-only, for analytics panel) ───────────────────────────

def load_shap_features(brand: str, kind: str = "classifier") -> list:
    """Load SHAP feature importance. kind: 'classifier' or 'regressor'."""
    return _cached(f"shap:{brand}:{kind}", lambda: _load_shap_impl(brand, kind), ttl=3600)


def _load_shap_impl(brand: str, kind: str) -> list:
    import pandas as pd
    filename = f"{kind}_shap.csv"

    if _use_gcs():
        try:
            bucket = _get_bucket()
            blob = bucket.blob(f"models/{brand.lower()}/{filename}")
            if blob.exists():
                import io
                df = pd.read_csv(io.StringIO(blob.download_as_text()))
                return df.head(10).to_dict(orient="records")
        except Exception:
            pass

    fp = _BASE_DIR / "models" / brand.lower() / filename
    try:
        df = pd.read_csv(fp)
        return df.head(10).to_dict(orient="records")
    except FileNotFoundError:
        return []


# ── Elasticity summary (read-only, for analytics panel) ──────────────────────

def load_elasticity_summary(brand: str) -> dict:
    """Load elasticity data grouped by subcategory."""
    return _cached(f"elasticity:{brand}", lambda: _load_elasticity_impl(brand), ttl=3600)


def _load_elasticity_impl(brand: str) -> dict:
    import pandas as pd

    # Try SKU-level parquet (richer data)
    df = None
    if _use_gcs():
        try:
            bucket = _get_bucket()
            blob = bucket.blob(f"models/{brand.lower()}/elasticity_by_sku.parquet")
            if blob.exists():
                import io
                df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        except Exception:
            pass

    if df is None:
        fp = _BASE_DIR / "data" / "processed" / brand.lower() / "elasticity_by_sku.parquet"
        try:
            df = pd.read_parquet(fp)
        except FileNotFoundError:
            return {"total": 0, "by_subcategory": []}

    if len(df) == 0:
        return {"total": 0, "by_subcategory": []}

    # Summary stats
    elas = df["elasticity"].dropna()
    total = len(elas)
    summary = {
        "total": total,
        "median": round(float(elas.median()), 3) if total > 0 else None,
        "elastic_count": int((elas < -1).sum()),
        "inelastic_count": int((elas > -0.5).sum()),
        "by_confidence": df["confidence"].value_counts().to_dict() if "confidence" in df.columns else {},
    }

    # By subcategory
    by_sub = []
    if "segunda_jerarquia" in df.columns:
        for sub, group in df.groupby("segunda_jerarquia"):
            e = group["elasticity"].dropna()
            if len(e) >= 3:
                by_sub.append({
                    "subcategory": str(sub),
                    "median_elasticity": round(float(e.median()), 3),
                    "sku_count": len(e),
                    "high_confidence": int((group["confidence"] == "high").sum()) if "confidence" in group.columns else 0,
                })
        by_sub.sort(key=lambda x: x["median_elasticity"])

    summary["by_subcategory"] = by_sub

    # By vendor brand (for multi-brand banners)
    by_vendor = []
    if "codigo_padre" in df.columns:
        from config.vendor_brands import get_vendor_brand
        df["_vendor"] = df["codigo_padre"].apply(lambda s: get_vendor_brand(s, brand))
        for vb, group in df.groupby("_vendor"):
            e = group["elasticity"].dropna()
            if len(e) >= 3:
                by_vendor.append({
                    "vendor_brand": str(vb),
                    "median_elasticity": round(float(e.median()), 3),
                    "sku_count": len(e),
                    "high_confidence": int((group["confidence"] == "high").sum()) if "confidence" in group.columns else 0,
                })
        by_vendor.sort(key=lambda x: x["median_elasticity"])
        df.drop(columns=["_vendor"], inplace=True)
    summary["by_vendor_brand"] = by_vendor

    return summary


# ── Outcome results (prediction vs actual) ────────────────────────────────────

def load_outcomes(brand: str):
    """Load outcome comparison data for a brand."""
    return _cached(f"outcomes:{brand}", lambda: _load_outcomes_impl(brand), ttl=600)


def _load_outcomes_impl(brand: str):
    import pandas as pd

    if _use_gcs():
        try:
            bucket = _get_bucket()
            blob = bucket.blob(f"outcomes/{brand.lower()}/outcome_results.parquet")
            if blob.exists():
                import io
                return pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        except Exception:
            pass

    # Local fallback
    fp = _BASE_DIR / "data" / "processed" / brand.lower() / "outcome_results.parquet"
    try:
        return pd.read_parquet(fp)
    except FileNotFoundError:
        return pd.DataFrame()


# ── Decisions ─────────────────────────────────────────────────────────────────

def load_decisions(brand: str, week: str = None) -> dict:
    if _use_gcs():
        return _gcs_load_decisions(brand, week)
    return _local_load_decisions(brand, week)


def save_decisions(data: dict):
    data["updated_at"] = datetime.now().isoformat()
    if _use_gcs():
        _gcs_save_decisions(data)
    else:
        _local_save_decisions(data)


def _gcs_load_decisions(brand, week):
    bucket = _get_bucket()
    prefix = f"decisions/{brand.lower()}/decisions_"
    if week:
        blob = bucket.blob(f"{prefix}{week}.json")
        if blob.exists():
            return json.loads(blob.download_as_text())
        return {"week": week, "brand": brand.lower(), "decisions": {}}
    blobs = sorted(
        [b for b in bucket.list_blobs(prefix=prefix)],
        key=lambda b: b.name,
    )
    if not blobs:
        return {"week": None, "brand": brand.lower(), "decisions": {}}
    return json.loads(blobs[-1].download_as_text())


def _gcs_save_decisions(data):
    bucket = _get_bucket()
    blob = bucket.blob(f"decisions/{data['brand']}/decisions_{data['week']}.json")
    blob.upload_from_string(
        json.dumps(data, indent=2, ensure_ascii=False),
        content_type="application/json",
    )


def _local_load_decisions(brand, week):
    base = _BASE_DIR / "decisions" / brand.lower()
    if not base.exists():
        return {"week": week, "brand": brand.lower(), "decisions": {}}
    if week:
        fp = base / f"decisions_{week}.json"
        if fp.exists():
            with open(fp) as f:
                return json.load(f)
        return {"week": week, "brand": brand.lower(), "decisions": {}}
    files = sorted(base.glob("decisions_*.json"))
    if not files:
        return {"week": None, "brand": brand.lower(), "decisions": {}}
    with open(files[-1]) as f:
        return json.load(f)


def _local_save_decisions(data):
    base = _BASE_DIR / "decisions" / data["brand"]
    base.mkdir(parents=True, exist_ok=True)
    with open(base / f"decisions_{data['week']}.json", "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ── Audit log ─────────────────────────────────────────────────────────────────

def append_audit(entry: dict):
    """Append an audit log entry (brand, user_email, user_name, action, key, details)."""
    entry["timestamp"] = datetime.now().isoformat()
    brand = entry.get("brand", "unknown")
    if _use_gcs():
        _gcs_append_audit(brand, entry)
    else:
        _local_append_audit(brand, entry)


def load_audit(brand: str, limit: int = 100) -> list:
    if _use_gcs():
        return _gcs_load_audit(brand, limit)
    return _local_load_audit(brand, limit)


def _gcs_append_audit(brand, entry):
    bucket = _get_bucket()
    now = datetime.now()
    path = f"audit/{brand.lower()}/{now.strftime('%Y-%m')}.jsonl"
    blob = bucket.blob(path)
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    if blob.exists():
        existing = blob.download_as_text()
        blob.upload_from_string(existing + line, content_type="application/x-ndjson")
    else:
        blob.upload_from_string(line, content_type="application/x-ndjson")


def _gcs_load_audit(brand, limit):
    bucket = _get_bucket()
    prefix = f"audit/{brand.lower()}/"
    blobs = sorted(bucket.list_blobs(prefix=prefix), key=lambda b: b.name, reverse=True)
    entries = []
    for blob in blobs:
        for line in reversed(blob.download_as_text().strip().split("\n")):
            if line:
                entries.append(json.loads(line))
            if len(entries) >= limit:
                break
        if len(entries) >= limit:
            break
    return entries[:limit]


def _local_append_audit(brand, entry):
    base = _BASE_DIR / "audit" / brand.lower()
    base.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    with open(base / f"{now.strftime('%Y-%m')}.jsonl", "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _local_load_audit(brand, limit):
    base = _BASE_DIR / "audit" / brand.lower()
    if not base.exists():
        return []
    entries = []
    for fp in sorted(base.glob("*.jsonl"), reverse=True):
        for line in reversed(fp.read_text().strip().split("\n")):
            if line:
                entries.append(json.loads(line))
            if len(entries) >= limit:
                break
        if len(entries) >= limit:
            break
    return entries[:limit]


# ── Feedback (ops implementation tracking) ────────────────────────────────────

def load_feedback(brand: str, week: str = None) -> dict:
    if _use_gcs():
        return _gcs_load_generic(f"feedback/{brand.lower()}", "feedback", week)
    return _local_load_generic(brand, "feedback", week)


def save_feedback(data: dict):
    data["updated_at"] = datetime.now().isoformat()
    if _use_gcs():
        bucket = _get_bucket()
        blob = bucket.blob(f"feedback/{data['brand']}/feedback_{data['week']}.json")
        blob.upload_from_string(json.dumps(data, indent=2, ensure_ascii=False), content_type="application/json")
    else:
        base = _BASE_DIR / "feedback" / data["brand"]
        base.mkdir(parents=True, exist_ok=True)
        with open(base / f"feedback_{data['week']}.json", "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


def _gcs_load_generic(prefix, kind, week):
    bucket = _get_bucket()
    if week:
        blob = bucket.blob(f"{prefix}/{kind}_{week}.json")
        if blob.exists():
            return json.loads(blob.download_as_text())
        return {"week": week, "items": {}}
    blobs = sorted([b for b in bucket.list_blobs(prefix=f"{prefix}/{kind}_")], key=lambda b: b.name)
    if not blobs:
        return {"week": None, "items": {}}
    return json.loads(blobs[-1].download_as_text())


def _local_load_generic(brand, kind, week):
    base = _BASE_DIR / kind / brand.lower()
    if not base.exists():
        return {"week": week, "items": {}}
    if week:
        fp = base / f"{kind}_{week}.json"
        if fp.exists():
            with open(fp) as f:
                return json.load(f)
        return {"week": week, "items": {}}
    files = sorted(base.glob(f"{kind}_*.json"))
    if not files:
        return {"week": None, "items": {}}
    with open(files[-1]) as f:
        return json.load(f)


# ── User config ───────────────────────────────────────────────────────────────

_USER_CONFIG_PATH = "config/users.json"
_DEFAULT_USER_CONFIG = {"users": {}, "allowed_domains": ["yaneken.cl", "ynk.cl"]}


def load_user_config() -> dict:
    """Load user role config from GCS or local file."""
    if _use_gcs():
        bucket = _get_bucket()
        blob = bucket.blob(_USER_CONFIG_PATH)
        if blob.exists():
            return json.loads(blob.download_as_text())
    else:
        fp = _BASE_DIR / "decisions" / "users.json"
        if fp.exists():
            with open(fp) as f:
                return json.load(f)
    return {**_DEFAULT_USER_CONFIG}


def save_user_config(data: dict):
    """Save user role config."""
    if _use_gcs():
        bucket = _get_bucket()
        blob = bucket.blob(_USER_CONFIG_PATH)
        blob.upload_from_string(
            json.dumps(data, indent=2, ensure_ascii=False),
            content_type="application/json",
        )
    else:
        fp = _BASE_DIR / "decisions" / "users.json"
        fp.parent.mkdir(parents=True, exist_ok=True)
        with open(fp, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


# ── Export storage ────────────────────────────────────────────────────────────

def save_export(brand: str, filename: str, content: bytes):
    """Save export file for audit trail."""
    if _use_gcs():
        bucket = _get_bucket()
        blob = bucket.blob(f"exports/{brand.lower()}/{filename}")
        blob.upload_from_string(
            content,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        base = _BASE_DIR / "exports" / brand.lower()
        base.mkdir(parents=True, exist_ok=True)
        with open(base / filename, "wb") as f:
            f.write(content)
