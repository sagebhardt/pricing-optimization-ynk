"""
Extract HOKA data from PostgreSQL and save as local parquet files.

Extracts:
- Transactions (ventas_por_vendedor filtered for HOKA)
- Product master (sku_tableau for HOKA SKUs)
- Store master (sucursales_tableau for HOKA)
- Foot traffic (flujo_tiendas for HOKA stores)
- Markdown contribution 2024
- Calendar
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import psycopg2
from pathlib import Path
from config.database import DB_CONFIG, HOKA_BANNER, HOKA_BRAND_CODE, EXCLUDE_SKUS

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def extract_transactions():
    """Extract HOKA transactions, filtering out service SKUs and non-HK products."""
    print("Extracting HOKA transactions...")
    query = f"""
        SELECT
            v.folio,
            v.tipo_documento,
            v.canal,
            v.tipo AS tipo_entrega,
            v.fecha_pos AS fecha,
            v.banner,
            v.tienda,
            v.centro,
            v.almacen,
            v.sku,
            v.descripcion,
            v.cantidad,
            v.precio_lista,
            v.descuento,
            v.precio_final,
            v.cliente_rut,
            v.codigo_descuento,
            v.codigo_descuento_tipo,
            v.codigo_descuento_monto
        FROM ventas.ventas_por_vendedor v
        WHERE v.banner = '{HOKA_BANNER}'
          AND v.sku NOT IN ({','.join(f"'{s}'" for s in EXCLUDE_SKUS)})
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    # Filter to actual HOKA products using sku_tableau join
    hoka_skus = extract_product_master()
    hoka_sku_set = set(hoka_skus[hoka_skus["grupo_articulos"] == HOKA_BRAND_CODE]["material"])

    # Keep only HOKA-brand SKUs
    n_before = len(df)
    df = df[df["sku"].isin(hoka_sku_set)]
    print(f"  Filtered {n_before - len(df)} non-HOKA SKUs, {len(df)} rows remaining")

    df["fecha"] = pd.to_datetime(df["fecha"])
    df.to_parquet(RAW_DIR / "hoka_transactions.parquet", index=False)
    print(f"  Saved {len(df):,} transactions ({df['fecha'].min().date()} to {df['fecha'].max().date()})")
    return df


def extract_product_master():
    """Extract product master for all SKUs sold under HOKA banner."""
    print("Extracting product master...")
    query = """
        SELECT
            s.material,
            s.material_descripcion,
            s.tipo_material,
            s.tipo_material_descripcion,
            s.grupo_articulos,
            s.grupo_articulos_descripcion,
            s.codigo_padre,
            s.ean11,
            s.talla,
            s.color1,
            s.color2,
            s.primera_jerarquia,
            s.segunda_jerarquia,
            s.tercera_jerarquia,
            s.cuarta_jerarquia,
            s.genero,
            s.age AS grupo_etario,
            s.temporada,
            s.fecha_ultima_compra,
            s.fecha_modificacion
        FROM ventas.sku_tableau s
        WHERE s.material IN (
            SELECT DISTINCT sku
            FROM ventas.ventas_por_vendedor
            WHERE banner = 'HOKA'
        )
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df.to_parquet(RAW_DIR / "hoka_products.parquet", index=False)
    print(f"  Saved {len(df):,} product records")
    return df


def extract_stores():
    """Extract HOKA store master."""
    print("Extracting store master...")
    query = """
        SELECT *
        FROM ventas.sucursales_tableau
        WHERE LOWER(banner) = 'hoka'
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df.to_parquet(RAW_DIR / "hoka_stores.parquet", index=False)
    print(f"  Saved {len(df)} stores")
    return df


def extract_foot_traffic():
    """Extract foot traffic for HOKA stores."""
    print("Extracting foot traffic...")
    query = """
        SELECT
            tienda_id,
            tienda_nombre,
            fecha,
            hora,
            entradas,
            salidas,
            tiempo_permanencia_prom,
            flujo_externo
        FROM ventas.flujo_tiendas
        WHERE banner_nombre = 'Hoka'
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df["fecha"] = pd.to_datetime(df["fecha"])
    df.to_parquet(RAW_DIR / "hoka_foot_traffic.parquet", index=False)
    print(f"  Saved {len(df):,} traffic records ({df['fecha'].min().date()} to {df['fecha'].max().date()})")
    return df


def extract_markdown_contribution():
    """Extract 2024 markdown contribution data for HOKA."""
    print("Extracting markdown contribution 2024...")
    query = """
        SELECT banner, sku, contribucion_valor
        FROM ventas.contribucion_mkdown_2024_sku_x_banner
        WHERE banner = 'HOKA'
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df.to_parquet(RAW_DIR / "hoka_mkdown_contribution_2024.parquet", index=False)
    print(f"  Saved {len(df):,} markdown contribution records")
    return df


def extract_calendar():
    """Extract calendar table."""
    print("Extracting calendar...")
    query = "SELECT * FROM ventas.calendario"
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df["fecha"] = pd.to_datetime(df["fecha"])
    df.to_parquet(RAW_DIR / "calendar.parquet", index=False)
    print(f"  Saved {len(df):,} calendar records")
    return df


def run_full_extract():
    """Run all extractions."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {RAW_DIR}\n")

    products = extract_product_master()
    transactions = extract_transactions()
    stores = extract_stores()
    traffic = extract_foot_traffic()
    mkdown = extract_markdown_contribution()
    calendar = extract_calendar()

    print("\n--- Extraction Summary ---")
    print(f"  Transactions:     {len(transactions):>10,} rows")
    print(f"  Products:         {len(products):>10,} rows")
    print(f"  Stores:           {len(stores):>10,} rows")
    print(f"  Foot traffic:     {len(traffic):>10,} rows")
    print(f"  Mkdown contrib:   {len(mkdown):>10,} rows")
    print(f"  Calendar:         {len(calendar):>10,} rows")

    return {
        "transactions": transactions,
        "products": products,
        "stores": stores,
        "traffic": traffic,
        "mkdown_contribution": mkdown,
        "calendar": calendar,
    }


if __name__ == "__main__":
    run_full_extract()
