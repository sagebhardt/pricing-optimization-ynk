"""Authentication and authorization configuration."""

import os

# Google OAuth2 Client ID — set via env var
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

# Role permissions
ROLE_PERMISSIONS = {
    "admin": {"read", "approve", "export", "audit", "manage", "plan"},
    "brand_manager": {"read", "approve", "export", "audit"},
    "planner": {"read", "plan", "export", "audit"},
    "viewer": {"read"},
}

# Bootstrap admin (hardcoded, can never be locked out)
_BOOTSTRAP_ADMINS = {"sgr@ynk.cl"}

# Cache for dynamic user config (reloaded every 60s)
_user_config_cache = {"data": None, "ts": 0}


def _load_dynamic_config() -> dict:
    """Load user config from GCS with 60-second cache."""
    import time
    now = time.time()
    if _user_config_cache["data"] is not None and now - _user_config_cache["ts"] < 60:
        return _user_config_cache["data"]

    try:
        gcs_bucket = os.getenv("GCS_BUCKET", "")
        if gcs_bucket:
            from google.cloud import storage
            client = storage.Client()
            blob = client.bucket(gcs_bucket).blob("config/users.json")
            if blob.exists():
                import json
                cfg = json.loads(blob.download_as_text())
                _user_config_cache["data"] = cfg
                _user_config_cache["ts"] = now
                return cfg
        else:
            # Local fallback
            import json
            from pathlib import Path
            fp = Path(__file__).parent.parent / "decisions" / "users.json"
            if fp.exists():
                with open(fp) as f:
                    cfg = json.load(f)
                    _user_config_cache["data"] = cfg
                    _user_config_cache["ts"] = now
                    return cfg
    except Exception as e:
        import sys
        print(f"[auth] WARNING: Failed to load user config: {e}", file=sys.stderr)

    default = {"users": {}, "allowed_domains": ["yaneken.cl", "ynk.cl"]}
    _user_config_cache["data"] = default
    _user_config_cache["ts"] = now
    return default


def get_user_role(email: str) -> dict | None:
    """Map a Google email to a role and permissions. Returns None if unauthorized."""
    email = email.lower().strip()

    # Bootstrap admin — always works even if GCS is down
    if email in _BOOTSTRAP_ADMINS:
        return {"role": "admin", "permissions": sorted(ROLE_PERMISSIONS["admin"]), "brands": None}

    # Check dynamic config (GCS-backed)
    cfg = _load_dynamic_config()

    # Explicit user assignment
    if email in cfg.get("users", {}):
        entry = cfg["users"][email]
        role = entry.get("role", "viewer")
        perms = ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS["viewer"])
        return {
            "role": role,
            "permissions": sorted(perms),
            "brands": entry.get("brands"),
        }

    # Domain-based default: viewer
    domain = email.split("@")[-1] if "@" in email else ""
    if domain in cfg.get("allowed_domains", []):
        return {"role": "viewer", "permissions": sorted(ROLE_PERMISSIONS["viewer"]), "brands": None}

    return None
