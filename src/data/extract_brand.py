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
from config.database import (
    DB_CONFIG, BRANDS, EXCLUDE_SKUS, STOCK_TABLES,
    DW_STOCK_BANNERS, DW_BRAND_BANNERS,
)
from config.price_lists import classify_price_list


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def _extract_costs_from_dw(parent_skus, lookback_days: int = 90):
    """Pull per-parent CLP cost from datawarehouse.costo.

    Costs live at child (sized) SKU level. For each parent: take the latest cost
    per (producto_id, centro_id) within lookback_days, then average across
    children and centros. Returns a DataFrame with columns ['sku', 'cost'] or
    None if the DW is unreachable. Parents with no recent cost rows are omitted
    (caller falls back to ti.productos for those).
    """
    if not parent_skus:
        return None
    conn = None
    try:
        conn = get_connection()
        placeholders = ",".join(["%s"] * len(parent_skus))
        query = f"""
            WITH brand_children AS (
                SELECT producto_id, sku_padre_sap
                FROM datawarehouse.producto
                WHERE sku_padre_sap IN ({placeholders})
                  AND sku_padre_sap IS NOT NULL
            ),
            latest_cost AS (
                SELECT DISTINCT ON (c.producto_id, c.centro_id)
                       c.producto_id, c.centro_id, c.costo
                FROM datawarehouse.costo c
                JOIN brand_children bc ON c.producto_id = bc.producto_id
                WHERE c.fecha >= CURRENT_DATE - INTERVAL '{int(lookback_days)} days'
                  AND c.costo > 0
                ORDER BY c.producto_id, c.centro_id, c.fecha DESC
            )
            SELECT bc.sku_padre_sap AS sku,
                   ROUND(AVG(lc.costo))::bigint AS cost,
                   COUNT(DISTINCT lc.producto_id) AS n_children_with_cost,
                   (SELECT COUNT(*) FROM brand_children b2
                    WHERE b2.sku_padre_sap = bc.sku_padre_sap) AS n_children_total
            FROM brand_children bc
            JOIN latest_cost lc ON bc.producto_id = lc.producto_id
            GROUP BY bc.sku_padre_sap
        """
        df = pd.read_sql(query, conn, params=tuple(parent_skus))
        if len(df) > 0:
            thin = df[df["n_children_with_cost"] < 0.3 * df["n_children_total"]]
            if len(thin) > 0:
                print(f"  datawarehouse: {len(thin)} parents have <30% child coverage (may be stale)")
        return df[["sku", "cost"]]
    except Exception as e:
        print(f"  datawarehouse.costo extraction skipped: {e}")
        return None
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


def _extract_official_prices_from_dw(parent_skus):
    """Pull per-parent list price from datawarehouse.producto_precio_padre.

    Each parent can have multiple rows across price lists (retail, outlet,
    virtual, eventos, liquidación, etc.). We take the MAX of `precio_normal`
    over rows currently within their validity window — this recovers the
    undiscounted retail list price regardless of which list it belongs to.
    Returns a DataFrame with columns ['sku', 'list_price'] or None on error.
    """
    if not parent_skus:
        return None
    conn = None
    try:
        conn = get_connection()
        placeholders = ",".join(["%s"] * len(parent_skus))
        query = f"""
            SELECT sku_padre_sap AS sku,
                   MAX(precio_normal) AS list_price
            FROM datawarehouse.producto_precio_padre
            WHERE sku_padre_sap IN ({placeholders})
              AND precio_normal > 0
              AND fecha_inicio_validez <= CURRENT_DATE
              AND (fecha_fin_validez IS NULL OR fecha_fin_validez > CURRENT_DATE)
            GROUP BY sku_padre_sap
        """
        return pd.read_sql(query, conn, params=tuple(parent_skus))
    except Exception as e:
        print(f"  datawarehouse.producto_precio_padre extraction skipped: {e}")
        return None
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


def _extract_stock_from_dw(parent_skus, banner_ids, lookback_weeks: int = 16):
    """Pull per-SKU per-store daily stock from datawarehouse.stock.

    Output schema matches the legacy `public.stock_{brand}` tables that
    downstream code (size_curve, build_features_brand) consumes:
      fecha, store_id, sku, stock_on_hand_units, stock_in_transit_units,
      total_stock_position_units.

    `banner_ids` is a list of datawarehouse.venta_organizacion_id values
    (e.g., [1, 4] for Belsport + Belsport Kids) to scope stock to the brand's
    own stores. Returns a DataFrame or None on error.
    """
    if not parent_skus or not banner_ids:
        return None
    conn = None
    try:
        conn = get_connection()
        sku_placeholders = ",".join(["%s"] * len(parent_skus))
        banner_placeholders = ",".join(["%s"] * len(banner_ids))
        query = f"""
            SELECT
                s.fecha,
                ctr.tienda_nombre AS store_id,
                p.sku_sap AS sku,
                COALESCE(s.stock, 0) AS stock_on_hand_units,
                COALESCE(s.stock_transito, 0) AS stock_in_transit_units,
                (COALESCE(s.stock, 0) + COALESCE(s.stock_transito, 0)) AS total_stock_position_units
            FROM datawarehouse.stock s
            JOIN datawarehouse.producto p ON s.producto_id = p.producto_id
            JOIN datawarehouse.centro ctr ON s.centro_id = ctr.centro_id
            WHERE ctr.venta_organizacion_id IN ({banner_placeholders})
              AND s.fecha >= CURRENT_DATE - INTERVAL '{int(lookback_weeks)} weeks'
              AND p.sku_padre_sap IN ({sku_placeholders})
        """
        params = tuple(banner_ids) + tuple(parent_skus)
        df = pd.read_sql(query, conn, params=params)
        df["fecha"] = pd.to_datetime(df["fecha"])
        return df
    except Exception as e:
        print(f"  datawarehouse.stock extraction skipped: {e}")
        return None
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


def _extract_list_names_from_dw(banner_names, lookback_years: int = 3):
    """Pull folio_sii → lista_precio.descripcion mapping from DW for given banners.

    Uses `datawarehouse.view_ventas` (a populated materialized view indexed on fecha)
    joined to `factura_cabecera` via doc_facturacion (indexed) and `lista_precio`.
    Returns a DataFrame with columns [folio, list_name, list_category] or None on error.
    The caller merges this into transactions on folio.
    """
    if not banner_names:
        return None
    conn = None
    try:
        conn = get_connection()
        placeholders = ",".join(["%s"] * len(banner_names))
        query = f"""
            SELECT DISTINCT
                vv.folio_sii AS folio,
                lp.descripcion AS list_name
            FROM datawarehouse.view_ventas vv
            JOIN datawarehouse.factura_cabecera fc ON vv.doc_facturacion = fc.doc_facturacion
            JOIN datawarehouse.lista_precio lp ON fc.lista_precio_id = lp.lista_precio_id
            WHERE vv.fecha >= CURRENT_DATE - INTERVAL '{int(lookback_years)} years'
              AND vv.organizacion_ventas_nombre IN ({placeholders})
              AND vv.folio_sii IS NOT NULL
        """
        df = pd.read_sql(query, conn, params=tuple(banner_names))
        # Always add list_category column (even on empty result) for consumer contract
        df["list_category"] = df["list_name"].apply(classify_price_list) if len(df) > 0 else pd.Series(dtype=object)
        return df
    except Exception as e:
        print(f"  datawarehouse list-name extraction skipped: {e}")
        return None
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


def _extract_backorder_from_dw(parent_skus, banner_names, lookback_months: int = 12):
    """Pull open purchase-order units per (sku, centro) from DW.

    Uses `datawarehouse.view_ordenes_compra_detalle` (PO lines) LEFT JOINed to
    aggregated `view_recepcion_orden_compra_resumen` (receipts), both keyed by
    (oc_numero, sku_sap). Computes (ordered - received) > 0 as the open/backorder
    quantity; status column values aren't inspected, so this stays robust to
    SAP status-code drift.

    Returns DataFrame [cod_padre, sku, centro, open_qty, earliest_delivery,
    n_open_pos] or None on error.
    """
    if not parent_skus or not banner_names:
        return None
    conn = None
    try:
        conn = get_connection()
        sku_ph = ",".join(["%s"] * len(parent_skus))
        ban_ph = ",".join(["%s"] * len(banner_names))
        query = f"""
            WITH po_detail AS (
                SELECT vo.oc_numero, vo.tienda_codigo_sap, vo.sku_sap,
                       vo.cantidad, vo.fecha_entrega, p.sku_padre_sap
                FROM datawarehouse.view_ordenes_compra_detalle vo
                JOIN datawarehouse.producto p ON vo.sku_sap = p.sku_sap
                WHERE vo.banner IN ({ban_ph})
                  AND vo.fecha_creacion >= CURRENT_DATE - INTERVAL '{int(lookback_months)} months'
                  AND p.sku_padre_sap IN ({sku_ph})
            ),
            received AS (
                SELECT vr.oc_numero, vr.sku_sap,
                       SUM(vr.cantidad_recibida) AS received_qty
                FROM datawarehouse.view_recepcion_orden_compra_resumen vr
                WHERE vr.oc_numero IN (SELECT DISTINCT oc_numero FROM po_detail)
                GROUP BY vr.oc_numero, vr.sku_sap
            )
            SELECT pd.sku_padre_sap AS cod_padre,
                   pd.sku_sap AS sku,
                   pd.tienda_codigo_sap AS centro,
                   SUM(pd.cantidad - COALESCE(r.received_qty, 0)) AS open_qty,
                   MIN(pd.fecha_entrega) AS earliest_delivery,
                   COUNT(*) AS n_open_pos
            FROM po_detail pd
            LEFT JOIN received r ON pd.oc_numero = r.oc_numero AND pd.sku_sap = r.sku_sap
            GROUP BY pd.sku_padre_sap, pd.sku_sap, pd.tienda_codigo_sap
            HAVING SUM(pd.cantidad - COALESCE(r.received_qty, 0)) > 0
        """
        params = tuple(banner_names) + tuple(parent_skus)
        return pd.read_sql(query, conn, params=params)
    except Exception as e:
        print(f"  datawarehouse backorder extraction skipped: {e}")
        return None
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


def _extract_replenishment_from_dw(parent_skus, banner_names, lookback_weeks: int = 12):
    """Pull inter-store transfer signal per (sku, destination centro) from DW.

    Reads raw `traspaso_cabecera` + `traspaso_detalle` tables (rather than the
    string-joined `view_traspasos_detalle`) so we can filter cleanly and use
    integer-id indexed joins. Banner scope via venta_organizacion name lookup.

    Returns DataFrame with columns:
      cod_padre, sku, centro, units_in_transit, units_received_window,
      avg_transit_days, n_transfers
    or None on error. `banner_names` same list shape as DW_BRAND_BANNERS.
    """
    if not parent_skus or not banner_names:
        return None
    conn = None
    try:
        conn = get_connection()
        sku_ph = ",".join(["%s"] * len(parent_skus))
        ban_ph = ",".join(["%s"] * len(banner_names))
        query = f"""
            WITH brand_transfers AS (
                SELECT td.producto_id,
                       td.centro_destino_id,
                       tc.fecha_creacion,
                       tc.fecha_recepcion,
                       td.unidades
                FROM datawarehouse.traspaso_detalle td
                JOIN datawarehouse.traspaso_cabecera tc
                     ON td.traspaso_cabecera_id = tc.traspaso_cabecera_id
                JOIN datawarehouse.producto p ON td.producto_id = p.producto_id
                JOIN datawarehouse.centro ctr ON td.centro_destino_id = ctr.centro_id
                JOIN datawarehouse.venta_organizacion vo
                     ON ctr.venta_organizacion_id = vo.venta_organizacion_id
                WHERE p.sku_padre_sap IN ({sku_ph})
                  AND vo.organizacion_ventas_nombre IN ({ban_ph})
                  AND tc.fecha_creacion >= CURRENT_DATE - INTERVAL '{int(lookback_weeks)} weeks'
            )
            SELECT p.sku_padre_sap AS cod_padre,
                   p.sku_sap AS sku,
                   ctr.tienda_codigo_sap AS centro,
                   SUM(CASE WHEN bt.fecha_recepcion IS NULL THEN bt.unidades ELSE 0 END)
                       AS units_in_transit,
                   SUM(CASE WHEN bt.fecha_recepcion IS NOT NULL THEN bt.unidades ELSE 0 END)
                       AS units_received_window,
                   AVG(CASE WHEN bt.fecha_recepcion IS NOT NULL
                            THEN (bt.fecha_recepcion - bt.fecha_creacion) END)
                       AS avg_transit_days,
                   COUNT(*) AS n_transfers
            FROM brand_transfers bt
            JOIN datawarehouse.producto p ON bt.producto_id = p.producto_id
            JOIN datawarehouse.centro ctr ON bt.centro_destino_id = ctr.centro_id
            GROUP BY p.sku_padre_sap, p.sku_sap, ctr.tienda_codigo_sap
            -- Keep groups where EITHER bucket has net-positive units. A pair can have
            -- non-zero in-transit even if received-minus-reversals nets to zero overall.
            HAVING SUM(CASE WHEN bt.fecha_recepcion IS NULL THEN bt.unidades ELSE 0 END) > 0
                OR SUM(CASE WHEN bt.fecha_recepcion IS NOT NULL THEN bt.unidades ELSE 0 END) > 0
        """
        params = tuple(parent_skus) + tuple(banner_names)
        return pd.read_sql(query, conn, params=params)
    except Exception as e:
        print(f"  datawarehouse replenishment extraction skipped: {e}")
        return None
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


def _extract_costs_from_ti(parent_skus):
    """Legacy fallback: cost from ti.productos with USD/CLP 1000x heuristic.

    Costs < 500 are treated as USD and multiplied by 1000 (calibrated against
    known HOKA costs). Used only for parents the datawarehouse does not cover.
    """
    if not parent_skus:
        return None
    conn = None
    try:
        conn = get_connection()
        placeholders = ",".join(["%s"] * len(parent_skus))
        df = pd.read_sql(
            f"SELECT cod_padre AS sku, reg_info AS cost FROM ti.productos WHERE cod_padre IN ({placeholders})",
            conn, params=tuple(parent_skus),
        )
        df = df[df["cost"] > 0].dropna(subset=["cost"])
        df["cost"] = df["cost"].apply(lambda c: c * 1000 if c < 500 else c)
        return df.drop_duplicates(subset=["sku"])
    except Exception as e:
        print(f"  ti.productos cost extraction skipped: {e}")
        return None
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


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

    # 2. Transactions (filter to brand codes in SQL to avoid pulling non-brand rows)
    print("Extracting transactions...")
    brand_skus = set(products[products["grupo_articulos"].isin(brand_codes)]["material"])
    txn = pd.read_sql(f"""
        SELECT folio, tipo_documento, canal, tipo AS tipo_entrega,
               fecha_pos AS fecha, banner, tienda, centro, almacen,
               sku, descripcion, cantidad, precio_lista, descuento, precio_final,
               cliente_rut, codigo_descuento, codigo_descuento_tipo, codigo_descuento_monto
        FROM ventas.ventas_por_vendedor
        WHERE banner = '{banner}'
          AND sku NOT IN ({excl})
          AND sku IN (
              SELECT DISTINCT material FROM ventas.sku_tableau
              WHERE grupo_articulos IN ({brand_filter})
          )
    """, conn)
    txn["fecha"] = pd.to_datetime(txn["fecha"])

    # 2b. Enrich with lista_precio from DW (folio == folio_sii).
    # Adds list_name + list_category columns so downstream can flag markdown periods
    # without relying on price-deviation inference.
    dw_banners = DW_BRAND_BANNERS.get(brand_name)
    if dw_banners:
        list_map = _extract_list_names_from_dw(dw_banners)
        if list_map is not None and len(list_map) > 0:
            txn["folio"] = txn["folio"].astype(str)
            list_map["folio"] = list_map["folio"].astype(str)
            txn = txn.merge(list_map, on="folio", how="left")
            matched = txn["list_name"].notna().sum()
            print(f"  Enriched {matched:,}/{len(txn):,} txns with lista_precio "
                  f"({100*matched/max(len(txn),1):.0f}%)")

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
    dw_banners = DW_STOCK_BANNERS.get(brand_name)
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
    elif dw_banners:
        parents_for_stock = list(products["codigo_padre"].dropna().unique())
        print(f"Extracting stock from datawarehouse.stock (banners={dw_banners})...")
        dw_stock = _extract_stock_from_dw(parents_for_stock, dw_banners)
        if dw_stock is not None and len(dw_stock) > 0:
            stock = dw_stock
            stock.to_parquet(raw_dir / "stock.parquet", index=False)
            print(f"  {len(stock):,} stock records, "
                  f"{stock['fecha'].min().date()}→{stock['fecha'].max().date()}, "
                  f"stores={stock['store_id'].nunique()}")
        else:
            print("  datawarehouse.stock returned no rows")
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

    parent_skus = list(products["codigo_padre"].dropna().unique())

    # 9. Generate costs.parquet if not already present (from GCS or local).
    # Source order: datawarehouse.costo (authoritative, CLP) → ti.productos (legacy, USD/CLP heuristic).
    costs_path = raw_dir / "costs.parquet"
    if not costs_path.exists() and parent_skus:
        dw_df = _extract_costs_from_dw(parent_skus)
        covered = set(dw_df["sku"]) if dw_df is not None and len(dw_df) > 0 else set()
        missing = [s for s in parent_skus if s not in covered]
        ti_df = _extract_costs_from_ti(missing) if missing else None

        parts = [df for df in (dw_df, ti_df) if df is not None and len(df) > 0]
        if parts:
            cost_df = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["sku"])
            cost_df.to_parquet(costs_path, index=False)
            n_dw = len(dw_df) if dw_df is not None else 0
            n_ti = len(ti_df) if ti_df is not None else 0
            print(f"  Generated costs.parquet: {len(cost_df):,} SKUs "
                  f"(datawarehouse={n_dw:,}, ti.productos fallback={n_ti:,})")

    # 10. Generate official_prices.parquet from datawarehouse if GCS didn't provide one.
    official_path = raw_dir / "official_prices.parquet"
    if not official_path.exists() and parent_skus:
        op_df = _extract_official_prices_from_dw(parent_skus)
        if op_df is not None and len(op_df) > 0:
            op_df.to_parquet(official_path, index=False)
            pct = 100 * len(op_df) / len(parent_skus)
            print(f"  Generated official_prices.parquet: {len(op_df):,} / {len(parent_skus):,} parents "
                  f"({pct:.0f}% coverage from datawarehouse)")

    # 11. Backorder signal — open PO units per (sku, centro) from datawarehouse.
    # Net-new feature: enables pricing to see incoming supply when stock is low.
    dw_banners_names = DW_BRAND_BANNERS.get(brand_name)
    if parent_skus and dw_banners_names:
        bo_df = _extract_backorder_from_dw(parent_skus, dw_banners_names)
        if bo_df is not None and len(bo_df) > 0:
            bo_df.to_parquet(raw_dir / "backorder_signal.parquet", index=False)
            print(f"  Generated backorder_signal.parquet: {len(bo_df):,} open lines, "
                  f"{bo_df['open_qty'].sum():,.0f} units across {bo_df['centro'].nunique()} centros")

    # 12. Replenishment signal — inter-store transfers to this brand's centros.
    # Tells the model which destinations are actively being replenished and how long transit takes.
    if parent_skus and dw_banners_names:
        rep_df = _extract_replenishment_from_dw(parent_skus, dw_banners_names)
        if rep_df is not None and len(rep_df) > 0:
            rep_df.to_parquet(raw_dir / "replenishment_signal.parquet", index=False)
            in_transit = rep_df["units_in_transit"].sum()
            received = rep_df["units_received_window"].sum()
            n_centros = rep_df["centro"].nunique()
            print(f"  Generated replenishment_signal.parquet: {len(rep_df):,} (sku,centro) pairs, "
                  f"{int(in_transit):,} units in transit + {int(received):,} received "
                  f"(12w) across {n_centros} centros")

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
