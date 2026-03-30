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

import pandas as pd
from pathlib import Path
from datetime import datetime

from config.competitors import BRAND_COMPETITORS

PROJECT_ROOT = Path(__file__).parent.parent.parent


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

    # Clean product names (strip size/talla suffixes)
    catalog["product_name"] = (
        catalog["product_name"]
        .str.replace(r'\s*N[°º]?\s*[\d,\.]+\s*$', '', regex=True)
        .str.replace(r'\s*T/[A-Z0-9]+\s*$', '', regex=True)
        .str.strip()
    )

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
