"""Database connection configuration."""

import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("YNK_DB_HOST", ""),
    "port": int(os.getenv("YNK_DB_PORT", "5432")),
    "dbname": os.getenv("YNK_DB_NAME", "dhw"),
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
    "2019",   # Bold — 99% click & collect (ecomm fulfillment point)
    "B609",   # Bamers — 100% click & collect
    "BEC2",   # Bamers — 98% click & collect (ecomm fulfillment)
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

# Stock table locations (team uploads may land in different schemas/tables).
# Set to None to fall through to DW_STOCK_BANNERS (sap_s4.stock).
# Observed on 2026-04-22 Cloud Run runs: public.stock_bold and public.stock_bamers
# no longer exist ("relation does not exist"), so BOLD + BAMERS now pull from DW.
STOCK_TABLES = {
    "HOKA": "public.stock_hoka",
    "BOLD": None,
    "BAMERS": None,
    "OAKLEY": "public.stock_oakley",
    "BELSPORT": None,
}

# Brand → sap_s4.venta_organizacion_id list, used when STOCK_TABLES[brand]
# is None. Banner IDs confirmed via sap_s4.venta_organizacion on 2026-04-22.
DW_STOCK_BANNERS = {
    "BOLD": [2],
    "BAMERS": [16],
    "BELSPORT": [1, 4],  # Belsport + Belsport Kids
}

# Brand → list of organizacion_ventas_nombre values in sap_s4.view_ventas.
# Used to pull markdown/lista_precio enrichment for transactions.
DW_BRAND_BANNERS = {
    "HOKA": ["Hoka"],
    "BOLD": ["Bold"],
    "BAMERS": ["Bamers"],
    "OAKLEY": ["Oakley"],
    "BELSPORT": ["Belsport", "Belsport Kids"],
}

# Brands that roll per-store recommendations into one action per parent x channel
# (B&M vs ecomm). Multi-store brands benefit most from the collapse; small-store
# brands (HOKA=3, OAKLEY=8) keep per-store grain to preserve model fidelity.
# Empty set → channel_aggregate step runs for all brands.
CHANNEL_GRAIN_BRANDS = {"BELSPORT", "BOLD", "BAMERS"}

# Backwards compatibility
HOKA_STORES = BRANDS["HOKA"]["stores_active"]
HOKA_STORES_ALL = BRANDS["HOKA"]["stores_all"]
HOKA_BRAND_CODE = "HK"
HOKA_BANNER = "HOKA"
