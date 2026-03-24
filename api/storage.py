"""Persistence layer: GCS-backed with local file fallback for dev."""

import json
import os
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
