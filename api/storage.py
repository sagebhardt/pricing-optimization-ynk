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
                if blob.name.endswith("size_curve_alerts.parquet"):
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


# ── Cross-store consistency alerts (read-only, written by pipeline) ──────────

def load_cross_store_alerts(brand: str = None):
    """Load cross-store pricing consistency alerts."""
    key = f"cross_store_alerts:{brand or 'all'}"
    return _cached(key, lambda: _load_cross_store_alerts_impl(brand), ttl=600)


def _load_cross_store_alerts_impl(brand: str = None):
    import pandas as pd

    frames = []

    if _use_gcs():
        try:
            bucket = _get_bucket()
            prefix = f"alerts/{brand.lower()}/" if brand else "alerts/"
            for blob in bucket.list_blobs(prefix=prefix):
                if blob.name.endswith("cross_store_alerts.parquet"):
                    import io
                    content = blob.download_as_bytes()
                    df = pd.read_parquet(io.BytesIO(content))
                    b = blob.name.split("/")[1]
                    df["brand"] = b
                    frames.append(df)
            if frames:
                return pd.concat(frames, ignore_index=True)
        except Exception:
            pass

    # Local fallback
    processed = _BASE_DIR / "data" / "processed"
    if processed.exists():
        dirs = [processed / brand.lower()] if brand else [d for d in processed.iterdir() if d.is_dir()]
        for brand_dir in dirs:
            if brand_dir.is_dir():
                ap = brand_dir / "cross_store_alerts.parquet"
                if ap.exists():
                    df = pd.read_parquet(ap)
                    df["brand"] = brand_dir.name
                    frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Competitor prices (read-only, written by pipeline) ───────────────────────

def load_competitor_summary(brand: str) -> dict:
    """Load competitor pricing summary for a brand."""
    return _cached(f"competitors:{brand}", lambda: _load_competitor_summary_impl(brand), ttl=3600)


def _load_competitor_summary_impl(brand: str) -> dict:
    import pandas as pd

    df = pd.DataFrame()

    if _use_gcs():
        try:
            bucket = _get_bucket()
            blob = bucket.blob(f"competitors/{brand.lower()}/competitor_prices.parquet")
            if blob.exists():
                import io
                content = blob.download_as_bytes()
                df = pd.read_parquet(io.BytesIO(content))
        except Exception:
            pass

    if len(df) == 0:
        local = _BASE_DIR / "data" / "processed" / brand.lower() / "competitor_prices.parquet"
        if local.exists():
            df = pd.read_parquet(local)

    if len(df) == 0:
        return {"coverage": {}, "items": []}

    valid = df[df["comp_price"] > 0]
    by_comp = valid.groupby("competitor")["codigo_padre"].nunique().to_dict()
    total_parents = valid["codigo_padre"].nunique()
    scraped_at = valid["scraped_at"].max() if "scraped_at" in valid.columns else None

    items = []
    for parent, group in valid.groupby("codigo_padre"):
        items.append({
            "parent_sku": parent,
            "competitors": [
                {
                    "name": row["competitor"],
                    "price": int(row["comp_price"]),
                    "list_price": int(row["comp_list_price"]) if pd.notna(row.get("comp_list_price")) else None,
                    "url": row.get("competitor_url", ""),
                    "in_stock": bool(row.get("comp_in_stock", True)),
                }
                for _, row in group.iterrows()
            ],
        })

    return {
        "scraped_at": scraped_at,
        "coverage": {
            "total_parents": total_parents,
            "by_competitor": by_comp,
        },
        "items": items[:100],
    }


def load_competitor_analytics(brand: str) -> dict:
    """Load enriched competitor analytics with price positioning."""
    return _cached(f"comp_analytics:{brand}", lambda: _load_comp_analytics_impl(brand), ttl=3600)


def _load_comp_analytics_impl(brand: str) -> dict:
    import pandas as pd
    import numpy as np

    # Load competitor prices
    summary = load_competitor_summary(brand)
    if not summary.get("items"):
        return {"available": False}

    # Load our pricing actions to cross-reference (optional — enrich if available)
    actions_data = load_pricing_actions(brand)
    our_items = actions_data.get("items", [])

    # Build our price lookup: parent_sku → median current price
    our_prices = {}
    for a in our_items:
        sku = a.get("parent_sku", "")
        price = a.get("current_price", 0)
        if sku and price > 0:
            if sku not in our_prices:
                our_prices[sku] = []
            our_prices[sku].append(price)
    our_median = {k: int(sorted(v)[len(v)//2]) for k, v in our_prices.items()}

    # Build competitor comparison per product
    products = []
    cheaper_count = 0
    parity_count = 0
    expensive_count = 0
    by_competitor = {}

    for item in summary["items"]:
        sku = item["parent_sku"]
        our_price = our_median.get(sku)
        # If no pricing action, use the max competitor price as proxy for "our" price
        if not our_price:
            list_prices = [c.get("list_price") or c["price"] for c in item["competitors"]]
            our_price = max(list_prices) if list_prices else None
        if not our_price:
            continue

        comp_min = min(c["price"] for c in item["competitors"])
        comp_avg = int(sum(c["price"] for c in item["competitors"]) / len(item["competitors"]))
        gap_pct = round((our_price - comp_min) / our_price * 100, 1)

        # Classify position
        if gap_pct > 5:
            position = "expensive"
            expensive_count += 1
        elif gap_pct < -5:
            position = "cheaper"
            cheaper_count += 1
        else:
            position = "parity"
            parity_count += 1

        cheapest_site = min(item["competitors"], key=lambda c: c["price"])["name"]

        # Track per-competitor stats
        for c in item["competitors"]:
            name = c["name"]
            if name not in by_competitor:
                by_competitor[name] = {"products": 0, "cheapest_count": 0, "avg_gap_sum": 0}
            by_competitor[name]["products"] += 1
            c_gap = (our_price - c["price"]) / our_price * 100
            by_competitor[name]["avg_gap_sum"] += c_gap
            if c["price"] <= comp_min * 1.02:
                by_competitor[name]["cheapest_count"] += 1

        products.append({
            "parent_sku": sku,
            "our_price": our_price,
            "comp_min": comp_min,
            "comp_avg": comp_avg,
            "gap_pct": gap_pct,
            "position": position,
            "cheapest_site": cheapest_site,
            "n_competitors": len(item["competitors"]),
            "competitors": [
                {"name": c["name"], "price": c["price"], "in_stock": c.get("in_stock", True), "url": c.get("url", "")}
                for c in item["competitors"]
            ],
        })

    total = len(products)
    if total == 0:
        return {"available": False}

    # Per-competitor summary
    comp_breakdown = []
    for name, stats in sorted(by_competitor.items(), key=lambda x: -x[1]["products"]):
        comp_breakdown.append({
            "name": name,
            "products": stats["products"],
            "cheapest_pct": round(stats["cheapest_count"] / stats["products"] * 100) if stats["products"] > 0 else 0,
            "avg_gap_pct": round(stats["avg_gap_sum"] / stats["products"], 1) if stats["products"] > 0 else 0,
        })

    # Sort products by gap (most overpriced first)
    products.sort(key=lambda x: -x["gap_pct"])

    return {
        "available": True,
        "scraped_at": summary.get("scraped_at"),
        "total_products": total,
        "position_summary": {
            "cheaper": cheaper_count,
            "parity": parity_count,
            "expensive": expensive_count,
            "cheaper_pct": round(cheaper_count / total * 100),
            "parity_pct": round(parity_count / total * 100),
            "expensive_pct": round(expensive_count / total * 100),
            "avg_price_index": round(sum(p["our_price"] for p in products) / sum(p["comp_avg"] for p in products), 3) if products else 1.0,
        },
        "by_competitor": comp_breakdown,
        "overpriced": products[:15],
        "underpriced": list(reversed(products[-10:])),
    }


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
    """Write audit entry as individual GCS object — O(1) per write, no race condition."""
    bucket = _get_bucket()
    now = datetime.now()
    # Individual object per entry: audit/{brand}/2026-04/{timestamp}.json
    path = f"audit/{brand.lower()}/{now.strftime('%Y-%m')}/{now.strftime('%Y%m%dT%H%M%S%f')}.json"
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(entry, ensure_ascii=False), content_type="application/json")


def _gcs_load_audit(brand, limit):
    bucket = _get_bucket()
    prefix = f"audit/{brand.lower()}/"
    blobs = sorted(bucket.list_blobs(prefix=prefix), key=lambda b: b.name, reverse=True)
    entries = []
    for blob in blobs:
        text = blob.download_as_text().strip()
        if blob.name.endswith(".json") and not blob.name.endswith(".jsonl"):
            # New format: one JSON object per file
            if text:
                entries.append(json.loads(text))
        else:
            # Old format: JSONL (multiple lines per file)
            for line in reversed(text.split("\n")):
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
