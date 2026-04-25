#!/usr/bin/env python3
"""
List tables in every non-system schema of the dwh database, with row count
estimates. Used for one-off exploration of the new datawarehouse.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from config.database import DB_CONFIG


def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"], port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"], user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


def list_user_schemas(conn):
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


def schema_tables(conn, schema):
    with conn.cursor() as c:
        c.execute("""
            SELECT
                t.table_name,
                t.table_type,
                COALESCE(c.reltuples, 0)::bigint AS rowcount
            FROM information_schema.tables t
            LEFT JOIN pg_namespace n ON n.nspname = t.table_schema
            LEFT JOIN pg_class c ON c.relname = t.table_name AND c.relnamespace = n.oid
            WHERE t.table_schema = %s
            ORDER BY rowcount DESC, t.table_name
        """, (schema,))
        return c.fetchall()


def main():
    conn = get_connection()
    try:
        schemas = list_user_schemas(conn)
        print(f"=== {len(schemas)} schemas in {DB_CONFIG['dbname']} ===\n")
        for s in schemas:
            tables = schema_tables(conn, s)
            print(f"---  {s}  ({len(tables)} tables/views)  ---")
            if not tables:
                print("    (empty)")
                continue
            for name, kind in [(t[0], t[1]) for t in tables[:50]]:
                rc = next((t[2] for t in tables if t[0] == name), 0)
                kind_short = "VIEW" if "VIEW" in kind else "TBL "
                rc_str = f"{rc:>13,}" if rc > 0 else "    (no stats)"
                print(f"  {kind_short}  {rc_str}  {name}")
            if len(tables) > 50:
                print(f"  ... +{len(tables)-50} more")
            print()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
