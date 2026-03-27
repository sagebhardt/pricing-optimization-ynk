"""
Brand-agnostic data extraction.

Usage:
    python src/data/extract_brand.py BOLD
    python src/data/extract_brand.py HOKA
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import psycopg2
from pathlib import Path
from config.database import DB_CONFIG, BRANDS, EXCLUDE_SKUS, STOCK_TABLES


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def extract_brand(brand_name: str):
    brand_name = brand_name.upper()
    if brand_name not in BRANDS:
        print(f"Unknown brand: {brand_name}. Available: {list(BRANDS.keys())}")
        return None

    cfg = BRANDS[brand_name]
    banner = cfg["banner"]
    brand_codes = cfg["brand_codes"]
    raw_dir = Path(__file__).parent.parent.parent / "data" / "raw" / brand_name.lower()
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"{'=' * 60}")
    print(f"EXTRACTING: {brand_name}")
    print(f"  Banner: {banner}")
    print(f"  Brand codes: {brand_codes}")
    print(f"  Output: {raw_dir}")
    print(f"{'=' * 60}")

    conn = get_connection()
    excl = ",".join(f"'{s}'" for s in EXCLUDE_SKUS)

    # 1. Product master
    print("\nExtracting product master...")
    brand_filter = ",".join(f"'{c}'" for c in brand_codes)
    products = pd.read_sql(f"""
        SELECT material, material_descripcion, tipo_material, tipo_material_descripcion,
               grupo_articulos, grupo_articulos_descripcion, codigo_padre, ean11,
               talla, color1, color2, primera_jerarquia, segunda_jerarquia,
               tercera_jerarquia, cuarta_jerarquia, genero, age AS grupo_etario,
               temporada, fecha_ultima_compra, fecha_modificacion
        FROM ventas.sku_tableau
        WHERE material IN (SELECT DISTINCT sku FROM ventas.ventas_por_vendedor WHERE banner = '{banner}')
    """, conn)
    products.to_parquet(raw_dir / "products.parquet", index=False)
    print(f"  {len(products):,} products")

    # 2. Transactions (filter to brand codes)
    print("Extracting transactions...")
    txn = pd.read_sql(f"""
        SELECT folio, tipo_documento, canal, tipo AS tipo_entrega,
               fecha_pos AS fecha, banner, tienda, centro, almacen,
               sku, descripcion, cantidad, precio_lista, descuento, precio_final,
               cliente_rut, codigo_descuento, codigo_descuento_tipo, codigo_descuento_monto
        FROM ventas.ventas_por_vendedor
        WHERE banner = '{banner}'
          AND sku NOT IN ({excl})
    """, conn)

    # Filter to actual brand products
    brand_skus = set(products[products["grupo_articulos"].isin(brand_codes)]["material"])
    n_before = len(txn)
    txn = txn[txn["sku"].isin(brand_skus)]
    txn["fecha"] = pd.to_datetime(txn["fecha"])
    txn.to_parquet(raw_dir / "transactions.parquet", index=False)
    print(f"  {len(txn):,} transactions (filtered {n_before - len(txn)} non-brand SKUs)")
    print(f"  Date range: {txn['fecha'].min().date()} to {txn['fecha'].max().date()}")

    # 3. Stores
    print("Extracting stores...")
    stores = pd.read_sql(f"""
        SELECT * FROM ventas.sucursales_tableau
        WHERE LOWER(banner) = '{banner.lower()}'
    """, conn)
    stores.to_parquet(raw_dir / "stores.parquet", index=False)
    print(f"  {len(stores)} stores")

    # 4. Foot traffic
    print("Extracting foot traffic...")
    traffic = pd.read_sql(f"""
        SELECT tienda_id, tienda_nombre, fecha, hora, entradas, salidas,
               tiempo_permanencia_prom, flujo_externo
        FROM ventas.flujo_tiendas
        WHERE banner_nombre = '{banner}'
           OR banner_nombre = '{banner.title()}'
    """, conn)
    if len(traffic) > 0:
        traffic["fecha"] = pd.to_datetime(traffic["fecha"])
    traffic.to_parquet(raw_dir / "foot_traffic.parquet", index=False)
    print(f"  {len(traffic):,} traffic records")

    # 5. Markdown contribution
    print("Extracting markdown contribution 2024...")
    mkdown = pd.read_sql(f"""
        SELECT banner, sku, contribucion_valor
        FROM ventas.contribucion_mkdown_2024_sku_x_banner
        WHERE banner = '{banner}'
    """, conn)
    mkdown.to_parquet(raw_dir / "mkdown_contribution_2024.parquet", index=False)
    print(f"  {len(mkdown):,} records")

    # 6. Stock / inventory (last 16 weeks only — sufficient for size curve trailing 12w peak)
    stock = pd.DataFrame()
    stock_table = STOCK_TABLES.get(brand_name)
    if stock_table:
        print(f"Extracting stock from {stock_table}...")
        try:
            stock = pd.read_sql(
                f"SELECT * FROM {stock_table} WHERE fecha >= CURRENT_DATE - INTERVAL '16 weeks'",
                conn,
            )
            n_raw = len(stock)
            if len(brand_skus) > 0:
                stock = stock[stock["sku"].isin(brand_skus)]
            stock["fecha"] = pd.to_datetime(stock["fecha"])
            stock.to_parquet(raw_dir / "stock.parquet", index=False)
            print(f"  {len(stock):,} stock records (from {n_raw:,} raw, last 16 weeks)")
            if len(stock) > 0:
                print(f"  Date range: {stock['fecha'].min().date()} to {stock['fecha'].max().date()}")
                print(f"  Stores: {stock['store_id'].nunique()}")
        except Exception as e:
            print(f"  Stock table {stock_table} not available — skipping ({e})")
            conn.rollback()
    else:
        print(f"No stock table configured for {brand_name} — skipping")

    # 7. Calendar (shared)
    cal_path = Path(__file__).parent.parent.parent / "data" / "raw" / "calendar.parquet"
    if not cal_path.exists():
        print("Extracting calendar...")
        cal = pd.read_sql("SELECT * FROM ventas.calendario", conn)
        cal["fecha"] = pd.to_datetime(cal["fecha"])
        cal.to_parquet(cal_path, index=False)
        print(f"  {len(cal):,} records")

    conn.close()

    # 8. Supplemental files from GCS (costs, official_prices — not in DB)
    bucket_name = os.environ.get("GCS_BUCKET", "")
    if bucket_name:
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            for fname in ["costs.parquet", "official_prices.parquet"]:
                blob = bucket.blob(f"data/raw/{brand_name.lower()}/{fname}")
                if blob.exists():
                    dest = raw_dir / fname
                    blob.download_to_filename(str(dest))
                    print(f"  Downloaded {fname} from GCS")
        except Exception as e:
            print(f"  GCS supplemental download skipped: {e}")

    print(f"\n--- {brand_name} Extraction Complete ---")
    print(f"  Transactions: {len(txn):,}")
    print(f"  Products:     {len(products):,}")
    print(f"  Stores:       {len(stores)}")
    print(f"  Foot traffic: {len(traffic):,}")
    print(f"  Stock:        {len(stock):,}")

    return {"transactions": txn, "products": products, "stores": stores, "traffic": traffic, "stock": stock}


if __name__ == "__main__":
    brand = sys.argv[1] if len(sys.argv) > 1 else "HOKA"
    extract_brand(brand)
