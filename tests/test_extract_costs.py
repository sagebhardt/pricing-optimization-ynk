"""Tests for cost and official-price extraction in src/data/extract_brand.py."""

from unittest.mock import patch, MagicMock
import pandas as pd

from src.data import extract_brand as eb


class TestExtractCostsFromDw:
    def test_empty_parents_returns_none(self):
        assert eb._extract_costs_from_dw([]) is None

    def test_returns_dataframe_on_success(self):
        # Query returns extra coverage columns that should be dropped before returning
        raw = pd.DataFrame({
            "sku": ["HKABC", "HKDEF"],
            "cost": [42000, 58000],
            "n_children_with_cost": [10, 12],
            "n_children_total": [10, 12],
        })
        with patch.object(eb, "get_dw_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw) as read_sql:
            out = eb._extract_costs_from_dw(["HKABC", "HKDEF"])
        assert read_sql.called
        query, _ = read_sql.call_args[0]
        assert "sap_s4.costo" in query
        assert "sap_s4.producto" in query
        assert list(out.columns) == ["sku", "cost"]
        assert list(out["cost"]) == [42000, 58000]

    def test_thin_child_coverage_is_logged(self, capsys):
        raw = pd.DataFrame({
            "sku": ["SPARSE"],
            "cost": [40000],
            "n_children_with_cost": [1],
            "n_children_total": [10],
        })
        with patch.object(eb, "get_dw_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw):
            eb._extract_costs_from_dw(["SPARSE"])
        assert "<30% child coverage" in capsys.readouterr().out

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_dw_connection", side_effect=RuntimeError("net")):
            assert eb._extract_costs_from_dw(["HKABC"]) is None


class TestExtractCostsFromTi:
    def test_empty_parents_returns_none(self):
        assert eb._extract_costs_from_ti([]) is None

    def test_usd_values_are_multiplied_by_1000(self):
        raw = pd.DataFrame({"sku": ["A", "B"], "cost": [42.0, 56000.0]})
        with patch.object(eb, "get_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw):
            out = eb._extract_costs_from_ti(["A", "B"])
        costs = dict(zip(out["sku"], out["cost"]))
        assert costs["A"] == 42000.0  # USD → CLP
        assert costs["B"] == 56000.0  # already CLP

    def test_zero_and_null_costs_are_dropped(self):
        raw = pd.DataFrame({"sku": ["A", "B", "C"], "cost": [0.0, None, 42000.0]})
        with patch.object(eb, "get_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw):
            out = eb._extract_costs_from_ti(["A", "B", "C"])
        assert list(out["sku"]) == ["C"]

    def test_duplicates_deduped(self):
        raw = pd.DataFrame({"sku": ["A", "A"], "cost": [42000.0, 43000.0]})
        with patch.object(eb, "get_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw):
            out = eb._extract_costs_from_ti(["A"])
        assert len(out) == 1

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_connection", side_effect=RuntimeError("net")):
            assert eb._extract_costs_from_ti(["A"]) is None


class TestExtractOfficialPricesFromDw:
    def test_empty_parents_returns_none(self):
        assert eb._extract_official_prices_from_dw([]) is None

    def test_returns_dataframe_on_success(self):
        raw = pd.DataFrame({"sku": ["HKABC", "HKDEF"], "list_price": [49990, 129990]})
        with patch.object(eb, "get_dw_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw) as read_sql:
            out = eb._extract_official_prices_from_dw(["HKABC", "HKDEF"])
        assert read_sql.called
        query, _ = read_sql.call_args[0]
        assert "sap_s4.producto_precio_padre" in query
        assert "MAX(precio_normal)" in query
        assert "fecha_inicio_validez <= CURRENT_DATE" in query
        assert "fecha_fin_validez" in query  # guard: validity-window filter must stay
        pd.testing.assert_frame_equal(out, raw)

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_dw_connection", side_effect=RuntimeError("net")):
            assert eb._extract_official_prices_from_dw(["HKABC"]) is None


class TestExtractStockFromDw:
    def test_empty_parents_returns_none(self):
        assert eb._extract_stock_from_dw([], [1, 4]) is None

    def test_empty_banners_returns_none(self):
        assert eb._extract_stock_from_dw(["NI123"], []) is None

    def test_returns_dataframe_on_success(self):
        raw = pd.DataFrame({
            "fecha": ["2026-04-21"],
            "store_id": ["AB10-Belsport Digital"],
            "sku": ["NI123456X"],
            "stock_on_hand_units": [5],
            "stock_in_transit_units": [2],
            "total_stock_position_units": [7],
        })
        with patch.object(eb, "get_dw_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw) as read_sql:
            out = eb._extract_stock_from_dw(["NI1234"], [1, 4])
        query, _ = read_sql.call_args[0]
        assert "sap_s4.stock" in query
        assert "sap_s4.producto" in query
        assert "sap_s4.centro" in query
        assert "venta_organizacion_id" in query
        assert "16 weeks" in query  # default lookback
        # params should be banner_ids first, then parent_skus
        assert "params" in read_sql.call_args[1], "params must be passed as kwarg"
        params = read_sql.call_args[1]["params"]
        assert params == (1, 4, "NI1234")
        # fecha must be converted to datetime
        assert out["fecha"].dtype.kind == "M"

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_dw_connection", side_effect=RuntimeError("net")):
            assert eb._extract_stock_from_dw(["NI1234"], [1, 4]) is None


class TestExtractListNamesFromDw:
    def test_empty_banners_returns_none(self):
        assert eb._extract_list_names_from_dw([]) is None

    def test_returns_dataframe_with_categories(self):
        raw = pd.DataFrame({
            "folio": ["100001", "100002", "100003"],
            "centro": ["7501", "7501", "7502"],
            "list_name": ["Hoka tiendas", "Liquidación Bold", "Bamers Outlets"],
        })
        with patch.object(eb, "get_dw_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw) as read_sql:
            out = eb._extract_list_names_from_dw(["Hoka"])
        query, _ = read_sql.call_args[0]
        assert "sap_s4.view_ventas" in query
        assert "sap_s4.factura_cabecera" in query
        assert "sap_s4.lista_precio" in query
        assert "doc_facturacion" in query
        assert "folio_sii" in query
        # Register-code prefix (e.g. "039-") must be stripped so the join key
        # matches ventas.ventas_por_vendedor.folio (which has no prefix).
        assert "SPLIT_PART" in query
        # Composite key with centro disambiguates same folio number across stores
        assert "tienda_codigo_sap AS centro" in query
        # list_category should have been added
        assert list(out["list_category"]) == ["retail", "liquidacion", "outlet"]

    def test_empty_result_still_returns_empty_df_not_none(self):
        raw = pd.DataFrame(columns=["folio", "list_name"])
        with patch.object(eb, "get_dw_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw):
            out = eb._extract_list_names_from_dw(["Hoka"])
        assert out is not None
        assert len(out) == 0
        # Consumer contract: list_category must be present even when empty
        assert "list_category" in out.columns

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_dw_connection", side_effect=RuntimeError("net")):
            assert eb._extract_list_names_from_dw(["Hoka"]) is None


class TestExtractBackorderFromDw:
    def test_empty_parents_returns_none(self):
        assert eb._extract_backorder_from_dw([], ["Hoka"]) is None

    def test_empty_banners_returns_none(self):
        assert eb._extract_backorder_from_dw(["HKABC"], []) is None

    def test_returns_dataframe_on_success(self):
        raw = pd.DataFrame({
            "cod_padre": ["HK1099673BBLC"],
            "sku": ["HK1099673BBLC080"],
            "centro": ["7501"],
            "open_qty": [12],
            "earliest_delivery": ["2026-05-15"],
            "n_open_pos": [2],
        })
        with patch.object(eb, "get_dw_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw) as read_sql:
            out = eb._extract_backorder_from_dw(["HK1099673BBLC"], ["Hoka"])
        query, _ = read_sql.call_args[0]
        assert "view_ordenes_compra_detalle" in query
        assert "view_recepcion_orden_compra_resumen" in query
        assert "LEFT JOIN received" in query
        # Lookback filter must remain — guards against unbounded scans
        assert "fecha_creacion" in query and "CURRENT_DATE" in query
        # HAVING on aggregated sum — guards against WHERE-vs-HAVING regression
        assert "HAVING" in query.upper() and "SUM(" in query
        # params: banner_names first, then parent_skus
        assert read_sql.call_args[1]["params"] == ("Hoka", "HK1099673BBLC")
        pd.testing.assert_frame_equal(out, raw)

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_dw_connection", side_effect=RuntimeError("net")):
            assert eb._extract_backorder_from_dw(["HK"], ["Hoka"]) is None


class TestExtractReplenishmentFromDw:
    def test_empty_parents_returns_none(self):
        assert eb._extract_replenishment_from_dw([], ["Hoka"]) is None

    def test_empty_banners_returns_none(self):
        assert eb._extract_replenishment_from_dw(["HK1"], []) is None

    def test_returns_dataframe_on_success(self):
        raw = pd.DataFrame({
            "cod_padre": ["HK1162012CYG"],
            "sku": ["HK1162012CYG090"],
            "centro": ["7501"],
            "units_in_transit": [12],
            "units_received_window": [45],
            "avg_transit_days": [3.5],
            "n_transfers": [8],
        })
        with patch.object(eb, "get_dw_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw) as read_sql:
            out = eb._extract_replenishment_from_dw(["HK1162012CYG"], ["Hoka"])
        query, _ = read_sql.call_args[0]
        assert "traspaso_detalle" in query
        assert "traspaso_cabecera" in query
        # Banner filter must go via venta_organizacion name (not raw ID),
        # so all brands can share the helper
        assert "organizacion_ventas_nombre" in query
        # Lookback window must remain to guard scans
        assert "fecha_creacion" in query and "CURRENT_DATE" in query
        # HAVING must guard each bucket separately so a net-zero (in-transit + reversal)
        # pair isn't silently dropped when in-transit is still non-zero.
        assert "HAVING" in query.upper()
        assert query.count("fecha_recepcion IS NULL") >= 2  # select + having
        # Output contract includes cod_padre for parent-SKU joins downstream
        assert "sku_padre_sap" in query
        assert read_sql.call_args[1]["params"] == ("HK1162012CYG", "Hoka")
        pd.testing.assert_frame_equal(out, raw)

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_dw_connection", side_effect=RuntimeError("net")):
            assert eb._extract_replenishment_from_dw(["HK1"], ["Hoka"]) is None
