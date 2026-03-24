"""Authentication and authorization configuration."""

import os

# Google OAuth2 Client ID — set via env var
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

# Role permissions
ROLE_PERMISSIONS = {
    "admin": {"read", "approve", "export", "audit", "manage"},
    "brand_manager": {"read", "approve", "export", "audit"},
    "viewer": {"read"},
}

# Bootstrap admins — hardcoded fallback so you can never lock yourself out
_BOOTSTRAP_ADMINS = {"sgr@ynk.cl"}


def get_user_role(email: str) -> dict | None:
    """Map a Google email to a role and permissions. Returns None if unauthorized."""
    email = email.lower().strip()

    # Bootstrap admin — always works even if GCS is down
    if email in _BOOTSTRAP_ADMINS:
        return {"role": "admin", "permissions": sorted(ROLE_PERMISSIONS["admin"]), "brands": None}

    # Check dynamic config (GCS-backed)
    try:
        from api.storage import load_user_config
        cfg = load_user_config()
    except Exception:
        cfg = {"users": {}, "allowed_domains": ["yaneken.cl", "ynk.cl"]}

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
