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
        with patch.object(eb, "get_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw) as read_sql:
            out = eb._extract_costs_from_dw(["HKABC", "HKDEF"])
        assert read_sql.called
        query, _ = read_sql.call_args[0]
        assert "datawarehouse.costo" in query
        assert "datawarehouse.producto" in query
        assert list(out.columns) == ["sku", "cost"]
        assert list(out["cost"]) == [42000, 58000]

    def test_thin_child_coverage_is_logged(self, capsys):
        raw = pd.DataFrame({
            "sku": ["SPARSE"],
            "cost": [40000],
            "n_children_with_cost": [1],
            "n_children_total": [10],
        })
        with patch.object(eb, "get_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw):
            eb._extract_costs_from_dw(["SPARSE"])
        assert "<30% child coverage" in capsys.readouterr().out

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_connection", side_effect=RuntimeError("net")):
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
        with patch.object(eb, "get_connection", return_value=MagicMock()), \
             patch("src.data.extract_brand.pd.read_sql", return_value=raw) as read_sql:
            out = eb._extract_official_prices_from_dw(["HKABC", "HKDEF"])
        assert read_sql.called
        query, _ = read_sql.call_args[0]
        assert "datawarehouse.producto_precio_padre" in query
        assert "MAX(precio_normal)" in query
        assert "fecha_inicio_validez <= CURRENT_DATE" in query
        assert "fecha_fin_validez" in query  # guard: validity-window filter must stay
        pd.testing.assert_frame_equal(out, raw)

    def test_returns_none_on_db_error(self):
        with patch.object(eb, "get_connection", side_effect=RuntimeError("net")):
            assert eb._extract_official_prices_from_dw(["HKABC"]) is None
