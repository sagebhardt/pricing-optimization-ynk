"""
Vendor brand identification from SKU prefixes.

Multi-brand banners (BOLD, BAMERS, BELSPORT) carry products from multiple
vendor brands (Nike, Adidas, Puma, etc.). This module maps SKU prefixes
to vendor brand names for grouping and analytics.

Store channel classification is also here — AB* prefix stores are
ecommerce/logistics centers, all others are brick-and-mortar.
"""

# Longest-prefix-first matching: check 3-char before 2-char.
# Sorted by length descending at lookup time.
VENDOR_BRAND_PREFIXES = {
    # 3-char prefixes
    "CAH": "Carhartt",
    "CAB": "Bamers",
    # 2-char prefixes
    "NI": "Nike",
    "NP": "Nike",       # Nike Peru — grouped under Nike
    "AD": "Adidas",
    "PM": "Puma",
    "JR": "Jordan",      # Kept separate from Nike
    "NB": "New Balance",
    "VN": "Vans",
    "CV": "Converse",
    "NE": "New Era",
    "SK": "Skechers",
    "CR": "Crocs",
    "BM": "Bamers",
    "HK": "Hoka",
    "OK": "Oakley",
    "UA": "Under Armour",
    "RB": "Reebok",
    "AL": "Alpinestars",
    "LT": "Lacoste",
    "SH": "Shaka",
    "UM": "Umbro",
    "QS": "Quiksilver",
    "MN": "Merrell",
    "ML": "Maui and Sons",
    "SC": "Saucony",
    "BL": "Belsport",
    "KP": "Kappa",
}

# Brand-specific overrides: same prefix means different vendors in different banners
_BRAND_OVERRIDES = {
    "BELSPORT": {
        "LT": "Lotto",
        "AL": "Alphabet",
    },
}

# Pre-sorted keys for lookup (longest first)
_SORTED_PREFIXES = sorted(VENDOR_BRAND_PREFIXES.keys(), key=len, reverse=True)

# Banners that carry multiple vendor brands (show vendor grouping UI)
MULTI_VENDOR_BRANDS = {"bold", "bamers", "belsport"}


def get_vendor_brand(sku: str, brand: str = None) -> str:
    """Extract vendor brand from SKU prefix. Returns 'Other' if unknown.

    brand: optional banner name for brand-specific prefix overrides.
    """
    sku = str(sku).strip().upper()
    overrides = _BRAND_OVERRIDES.get(brand.upper(), {}) if brand else {}
    for prefix in _SORTED_PREFIXES:
        if sku.startswith(prefix):
            if prefix in overrides:
                return overrides[prefix]
            return VENDOR_BRAND_PREFIXES[prefix]
    return "Other"


def is_ecomm_store(store_code: str) -> bool:
    """AB* prefix stores are ecommerce/logistics centers."""
    return str(store_code).strip().upper().startswith("AB")
