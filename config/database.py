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

# Non-retail stores to exclude from pricing actions (logistics, digital, internal)
EXCLUDE_STORES_PRICING = [
    "DX01",   # Centro Logistico Dexim
    "B611",   # Bamers Ventas Internas
    "B997",   # Marketplace NC BM
    "514",    # Locker Virtual Dexim
    "170",    # Bamers Digital
    "508",    # Bamers Digital II
    "D005",   # Oakley Ventas Internas
    "AB10",   # Belsport E-commerce / Digital
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
    "BAMERS": {
        "banner": "BAMERS",
        "brand_codes": ["BM", "SK", "CR", "CAB"],
        "stores_active": [
            "B002", "B003", "B004", "B008", "B010", "B011", "B012",
            "B019", "B022", "B024", "B046", "B047", "B048", "B049",
            "B050", "B052", "B054", "B055", "B056", "B057", "B058",
            "B059", "B060", "B602", "B603",
        ],
        "stores_all": None,  # All stores for historical training
    },
    "OAKLEY": {
        "banner": "OAKLEY",
        "brand_codes": ["OK"],
        "stores_active": [
            "D002", "D004", "D011", "D012", "D013", "D014", "D016", "E001",
        ],
        "stores_all": None,
    },
    # Brand codes overlap with BOLD/BAMERS (NI, PM, AD, etc.) — this is fine
    # because extract_brand.py filters by banner name, not just brand codes.
    "BELSPORT": {
        "banner": "BELSPORT",
        "brand_codes": ["PM", "NI", "AD", "SK", "RB", "VN", "AL", "CV", "LT",
                         "SH", "UM", "CR", "NB", "QS", "BM", "MN", "UA", "ML",
                         "NP", "SC", "BL", "KP"],
        "stores_active": None,  # All stores
        "stores_all": None,
    },
}

# Stock table locations (team uploads may land in different schemas/tables)
STOCK_TABLES = {
    "HOKA": "public.stock_hoka",
    "BOLD": "public.stock_bold",
    "BAMERS": "public.stock_bamers",
    "OAKLEY": "public.stock_oakley",
    "BELSPORT": "public.stock_belsport",
}

# Backwards compatibility
HOKA_STORES = BRANDS["HOKA"]["stores_active"]
HOKA_STORES_ALL = BRANDS["HOKA"]["stores_all"]
HOKA_BRAND_CODE = "HK"
HOKA_BANNER = "HOKA"
