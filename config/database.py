"""Database connection configuration."""

import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("YNK_DB_HOST", ""),
    "port": int(os.getenv("YNK_DB_PORT", "5432")),
    "dbname": os.getenv("YNK_DB_NAME", "consultas"),
    "user": os.getenv("YNK_DB_USER", ""),
    "password": os.getenv("YNK_DB_PASSWORD", ""),
}

# Service/non-product SKUs to exclude (applies to all brands)
EXCLUDE_SKUS = [
    "000000001000000031",  # Shipping charges
    "AJUSTE",              # Adjustments
    "HANDBAG01",           # Shopping bags
]

# Brand configurations
BRANDS = {
    "HOKA": {
        "banner": "HOKA",
        "brand_codes": ["HK"],
        "stores_active": ["7501", "7502", "AB75"],
        "stores_all": ["7501", "7502", "7599", "AB75"],
    },
    "BOLD": {
        "banner": "BOLD",
        "brand_codes": ["NI", "PM", "AD", "JR", "NB", "VN", "NE", "NP", "CV", "CAH"],
        "stores_active": None,  # All stores
        "stores_all": None,
    },
}

# Backwards compatibility
HOKA_STORES = BRANDS["HOKA"]["stores_active"]
HOKA_STORES_ALL = BRANDS["HOKA"]["stores_all"]
HOKA_BRAND_CODE = "HK"
HOKA_BANNER = "HOKA"
