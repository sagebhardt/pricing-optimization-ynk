"""
Brand-agnostic competitor pricing scraper.

Orchestrates per-site adapters, deduplicates results,
and saves competitor_prices.parquet.

Usage:
    python src/scraping/scrape_brand.py HOKA
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import re

import pandas as pd
from pathlib import Path
from datetime import datetime

from config.competitors import BRAND_COMPETITORS

PROJECT_ROOT = Path(__file__).parent.parent.parent

# Words that look like color codes but are part of model names
_MODEL_WORDS = {"GTX", "MID", "ATR", "LOW", "RUN", "HAT", "LS", "PACK", "SOCK", "CREW", "TANK", "TEE"}


def _extract_model_name(raw_name: str) -> str:
    """Extract a clean model name from internal product name for search.

    "W BONDI 7 BFBG" → "BONDI 7"
    "M CHALLENGER 8 MSTRD" → "CHALLENGER 8"
    "SPEEDGOAT 6 TTT 11" → "SPEEDGOAT 6"
    "U ORA RECOVERY SLIDE 3 WWH 12/14" → "ORA RECOVERY SLIDE 3"
    "M MACH 5 BLACK / MULTI" → "MACH 5"
    "HOKA RUN HAT OTM N° OSFA" → "RUN HAT"
    """
    name = str(raw_name).strip()

    # Strip gender prefix
    name = re.sub(r'^[MWU]\s+', '', name)

    # Strip brand prefix
    for prefix in ("HOKA ONE ONE ", "HOKA "):
        if name.upper().startswith(prefix):
            name = name[len(prefix):]

    # Strip N° / talla suffixes
    name = re.sub(r'\s+N[°º]\s*\S+.*$', '', name)

    # Strip "/ color description" suffix
    name = re.sub(r'\s*/\s*[A-Z].*$', '', name)

    # From the right: strip trailing size, then color code, then stop.
    # Size is always AFTER color code in internal names: "BONDI 9 BFBG 7" or "MACH 6 FLV 7"
    # So strip: trailing_size → color_code → STOP (keep model number)
    tokens = name.split()

    # 1. Strip trailing letter sizes (S, M, L, XL, U, T, OSFA) and number sizes
    _LETTER_SIZES = {"S", "M", "L", "XL", "XXL", "U", "W", "T", "OSFA"}
    while tokens and (re.match(r'^\d+[,./]?\d*$', tokens[-1]) or tokens[-1] in _LETTER_SIZES):
        tokens.pop()

    # 2. Strip trailing color code (3-6 uppercase, not model words)
    while tokens and re.match(r'^[A-Z]{3,6}$', tokens[-1]) and tokens[-1] not in _MODEL_WORDS:
        tokens.pop()

    # 3. STOP — remaining tokens include the model number

    return " ".join(tokens).strip()


def _get_adapter(name: str):
    """Lazy-load a scraper adapter by name."""
    if name == "falabella":
        from src.scraping.falabella import FalabellaScraper
        return FalabellaScraper()
    elif name == "mercadolibre":
        from src.scraping.mercadolibre import MercadoLibreScraper
        return MercadoLibreScraper()
    elif name in ("hoka_cl", "nike_cl", "theline", "sparta", "marathon"):
        from src.scraping.brand_sites import get_brand_site_scraper
        return get_brand_site_scraper(name)
    elif name == "ripley":
        from src.scraping.ripley import RipleyScraper
        return RipleyScraper()
    elif name == "paris":
        from src.scraping.paris import ParisScraper
        return ParisScraper()
    else:
        print(f"  Unknown adapter: {name}")
        return None


def _build_catalog(brand: str) -> pd.DataFrame:
    """Build a deduplicated parent-level product catalog for scraping."""
    from config.database import BRANDS

    raw_dir = PROJECT_ROOT / "data" / "raw" / brand.lower()
    products = pd.read_parquet(raw_dir / "products.parquet")

    # Filter to brand products only (exclude cross-brand contamination)
    cfg = BRANDS.get(brand.upper(), {})
    brand_codes = cfg.get("brand_codes", [])
    if brand_codes and "grupo_articulos" in products.columns:
        products = products[products["grupo_articulos"].isin(brand_codes)]

    # Deduplicate to parent SKU level (one search per model, not per size)
    catalog = (
        products.groupby("codigo_padre")
        .first()
        .reset_index()
        [["codigo_padre", "material_descripcion", "grupo_articulos_descripcion",
          "primera_jerarquia", "segunda_jerarquia"]]
        .rename(columns={
            "material_descripcion": "product_name",
            "grupo_articulos_descripcion": "vendor_brand",
        })
    )

    # Extract clean model names for search queries
    catalog["product_name"] = catalog["product_name"].apply(_extract_model_name)

    # Deduplicate by model name — avoid searching "BONDI 9" once per color variant
    # Keep first parent SKU per model name (results will map back via matching)
    catalog = catalog[catalog["product_name"].str.len() > 0]
    catalog = catalog.drop_duplicates(subset="product_name", keep="first")

    # Cap at 200 products to stay within Cloud Run timeout (~4s/request × 200 = ~13min/site)
    # Prioritize footwear over apparel/accessories
    MAX_SCRAPE = 200
    if len(catalog) > MAX_SCRAPE:
        cat_priority = {"Footwear": 0, "Calzado": 0}
        catalog["_cat_sort"] = catalog.get("primera_jerarquia", pd.Series(dtype=str)).map(
            lambda x: cat_priority.get(x, 1) if pd.notna(x) else 1
        )
        catalog = catalog.sort_values("_cat_sort").head(MAX_SCRAPE).drop(columns=["_cat_sort"])
        print(f"  Capped to {MAX_SCRAPE} products (footwear first)")

    # Add EAN11 if available (take any non-null EAN per parent)
    if "ean11" in products.columns:
        ean_map = (
            products[products["ean11"].notna() & (products["ean11"] != "")]
            .groupby("codigo_padre")["ean11"]
            .first()
        )
        catalog = catalog.merge(ean_map, on="codigo_padre", how="left")

    print(f"  Catalog: {len(catalog)} parent SKUs to scrape")
    return catalog


def scrape_competitors_for_brand(brand: str):
    """Scrape competitor prices for a brand and save results."""
    brand = brand.upper()
    competitors = BRAND_COMPETITORS.get(brand, [])
    if not competitors:
        print(f"[{brand}] No competitors configured — skipping")
        return None

    print(f"[{brand}] Competitor pricing scrape")
    print(f"  Sites: {', '.join(competitors)}")

    catalog = _build_catalog(brand)
    all_results = []
    stats = {}

    for name in competitors:
        adapter = _get_adapter(name)
        if adapter is None:
            continue

        print(f"\n  [{name}] Scraping...")
        try:
            results = adapter.scrape(catalog)
            n_matches = len(results)
            all_results.append(results)
            stats[name] = n_matches
            print(f"  [{name}] Done: {n_matches} matches")
        except Exception as e:
            print(f"  [{name}] FAILED: {e}")
            stats[name] = 0

    if not all_results or all(len(r) == 0 for r in all_results):
        print(f"\n[{brand}] No competitor matches found")
        # Write empty parquet so downstream handles gracefully
        empty = pd.DataFrame(columns=[
            "codigo_padre", "ean11", "competitor", "competitor_url",
            "comp_price", "comp_list_price", "comp_discount", "comp_in_stock",
            "match_method", "match_score", "scraped_at",
        ])
        out_dir = PROJECT_ROOT / "data" / "processed" / brand.lower()
        out_dir.mkdir(parents=True, exist_ok=True)
        empty.to_parquet(out_dir / "competitor_prices.parquet", index=False)
        return empty

    combined = pd.concat(all_results, ignore_index=True)
    combined["scraped_at"] = datetime.utcnow().isoformat()

    # Deduplicate: keep best match per (parent, competitor)
    if "match_score" in combined.columns:
        combined = combined.sort_values("match_score", ascending=False)
        combined = combined.drop_duplicates(subset=["codigo_padre", "competitor"], keep="first")

    # Save
    out_dir = PROJECT_ROOT / "data" / "processed" / brand.lower()
    out_dir.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out_dir / "competitor_prices.parquet", index=False)

    # Summary
    n_parents = combined["codigo_padre"].nunique()
    coverage = n_parents / len(catalog) * 100 if len(catalog) > 0 else 0
    print(f"\n[{brand}] COMPETITOR SCRAPE SUMMARY")
    print(f"  Total matches: {len(combined)}")
    print(f"  Parents matched: {n_parents}/{len(catalog)} ({coverage:.0f}%)")
    for site, count in sorted(stats.items(), key=lambda x: -x[1]):
        print(f"    {site}: {count}")
    print(f"  Saved to: {out_dir / 'competitor_prices.parquet'}")

    return combined


if __name__ == "__main__":
    brand = sys.argv[1] if len(sys.argv) > 1 else "HOKA"
    scrape_competitors_for_brand(brand)
