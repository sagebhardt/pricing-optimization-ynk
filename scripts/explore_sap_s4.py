#!/usr/bin/env python3
"""
Survey the new sap_s4 schema in the dhw database to find tables/views we don't
currently use that could improve pricing/elasticity models.

Usage:
    python scripts/explore_sap_s4.py              # full survey
    python scripts/explore_sap_s4.py <table>      # deep-dive one table
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from psycopg2.extras import RealDictCursor
from config.database import DB_CONFIG

# Tables we already pull. Anything else is potentially useful.
USED = {
    "costo", "producto", "centro", "lista_precio", "producto_precio_padre",
    "view_ventas", "factura_cabecera", "stock",
    "venta_organizacion", "venta_organizacion_id",
    "view_ordenes_compra_detalle", "view_recepcion_orden_compra_resumen",
    "traspaso_detalle", "traspaso_cabecera",
}

# Keyword filters that suggest model-relevant content.
PROMISING_KEYWORDS = (
    "oferta", "promoc", "markdown", "descuento", "campaign", "campana",
    "cliente", "customer", "loyal", "fidel",
    "devoluc", "return",
    "objetivo", "meta", "budget", "forecast",
    "trafico", "traffic", "visita",
    "marketing", "publicidad",
    "elasticidad", "precio_historic", "price_history",
    "inventario", "turnover",
)


def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"], port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"], user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


def list_schemas(conn):
    with conn.cursor() as c:
        c.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name
        """)
        return [r[0] for r in c.fetchall()]


def list_tables(conn, schema):
    with conn.cursor() as c:
        c.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """, (schema,))
        return c.fetchall()


def table_row_count(conn, schema, table):
    """Use planner estimate (pg_class.reltuples) — exact COUNT(*) on big tables would be slow."""
    with conn.cursor() as c:
        c.execute("""
            SELECT reltuples::bigint AS estimate
            FROM pg_class
            WHERE relname = %s
              AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
        """, (table, schema))
        row = c.fetchone()
        return row[0] if row else None


def table_columns(conn, schema, table):
    with conn.cursor() as c:
        c.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return c.fetchall()


def sample_row(conn, schema, table, n=1):
    """Pull 1 sample row to see realistic values. Wrapped in try/except: views may
    error or be very slow."""
    with conn.cursor(cursor_factory=RealDictCursor) as c:
        try:
            c.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT %s', (n,))
            return c.fetchall()
        except Exception as e:
            return [{"_error": str(e)[:120]}]


def is_promising(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in PROMISING_KEYWORDS)


def survey(conn, schema="sap_s4"):
    print(f"\n=== Schemas in {DB_CONFIG['dbname']} ===")
    for s in list_schemas(conn):
        marker = "  <-- target" if s == schema else ""
        print(f"  {s}{marker}")

    print(f"\n=== Tables in {schema} ===")
    tables = list_tables(conn, schema)
    if not tables:
        print(f"  (no tables — does {schema} exist?)")
        return

    new_tables = []
    promising = []
    used_present = []

    for name, kind in tables:
        if name in USED:
            used_present.append((name, kind))
        else:
            new_tables.append((name, kind))
            if is_promising(name):
                promising.append((name, kind))

    print(f"\n  Already used by pipeline: {len(used_present)}")
    for name, kind in used_present:
        print(f"    [{kind[:5]}] {name}")

    print(f"\n  New (not currently pulled): {len(new_tables)}")
    for name, kind in sorted(new_tables):
        flag = "  <PROMISING>" if is_promising(name) else ""
        print(f"    [{kind[:5]}] {name}{flag}")

    if not promising:
        print("\n(No tables matched promising keywords. Inspect 'New' list manually.)")
        return

    print(f"\n=== Promising new tables — schema + sample row ===")
    for name, kind in promising:
        rowcount = table_row_count(conn, schema, name)
        cols = table_columns(conn, schema, name)
        print(f"\n--- {schema}.{name} ({kind}, ~{rowcount:,} rows) ---")
        for cname, ctype in cols[:25]:
            print(f"    {cname:<32} {ctype}")
        if len(cols) > 25:
            print(f"    ... +{len(cols)-25} more columns")
        sample = sample_row(conn, schema, name, n=1)
        if sample and "_error" not in sample[0]:
            print(f"  Sample row:")
            for k, v in list(sample[0].items())[:12]:
                vstr = str(v)[:60]
                print(f"    {k}: {vstr}")
        elif sample:
            print(f"  (sample failed: {sample[0].get('_error', '?')})")


def deep_dive(conn, table, schema="sap_s4"):
    print(f"\n=== Deep dive: {schema}.{table} ===\n")
    rowcount = table_row_count(conn, schema, table)
    print(f"Estimated rows: {rowcount:,}\n")
    print("Columns:")
    for cname, ctype in table_columns(conn, schema, table):
        print(f"  {cname:<32} {ctype}")
    print("\nSample 5 rows:")
    samples = sample_row(conn, schema, table, n=5)
    for i, row in enumerate(samples, 1):
        print(f"\n  -- Row {i} --")
        for k, v in row.items():
            print(f"    {k}: {str(v)[:80]}")


if __name__ == "__main__":
    conn = get_connection()
    try:
        if len(sys.argv) > 1:
            deep_dive(conn, sys.argv[1])
        else:
            survey(conn)
    finally:
        conn.close()
