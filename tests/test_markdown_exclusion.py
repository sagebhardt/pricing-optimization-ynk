"""Tests that the lista_precio markdown signal (Phase 5) flows through into
price-feature calculation and elasticity estimation so retail-price signals
don't get contaminated by liquidación / outlet observations.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from src.features.build_features_brand import build_weekly_sales
from src.features import price_elasticity_brand as elasticity


def _base_txn():
    """Minimal transactions DataFrame with the columns the builders expect.
    All rows are full-price retail by default — tests override specific rows
    to set up the scenario they're exercising.
    """
    return pd.DataFrame({
        "sku": ["HK1"] * 4,
        "centro": ["7501"] * 4,
        "fecha": pd.to_datetime(["2026-04-07"] * 4),
        "cantidad": [1, 1, 1, 1],
        "precio_lista": [100000] * 4,
        "precio_final": [100000] * 4,
        "descuento": [0] * 4,
        "folio": ["F1", "F2", "F3", "F4"],
    })


class TestBuildWeeklySalesMarkdownExclusion:
    def test_no_list_category_column_falls_back_cleanly(self):
        """Backward-compatibility: old transactions.parquet without list_category still works."""
        txn = _base_txn()
        weekly = build_weekly_sales(txn)
        assert len(weekly) == 1
        # All 4 txns included in retail price calc (4 × 100000 / 4 = 100000)
        assert weekly.iloc[0]["avg_precio_final"] == 100000

    def test_markdown_txns_excluded_from_price_features(self):
        """A liquidación txn at a lower price should not drag avg_precio_final down."""
        txn = _base_txn()
        txn.loc[2, "precio_final"] = 40000  # clearance price on row 2
        txn["list_category"] = ["retail", "retail", "liquidacion", "retail"]
        weekly = build_weekly_sales(txn)
        # Only the 3 retail txns at 100000 feed price calc → avg stays at 100000
        assert weekly.iloc[0]["avg_precio_final"] == 100000
        # Volume still counts ALL 4 txns
        assert weekly.iloc[0]["units_sold"] == 4

    def test_outlet_txns_also_excluded(self):
        txn = _base_txn()
        txn["list_category"] = ["retail", "outlet", "outlet", "retail"]
        weekly = build_weekly_sales(txn)
        assert weekly.iloc[0]["avg_precio_final"] == 100000

    def test_eventos_and_online_not_excluded(self):
        """Eventos/online aren't markdown regimes — their prices should still count."""
        txn = _base_txn()
        txn["list_category"] = ["retail", "eventos", "online", "retail"]
        txn["precio_final"] = [100000, 90000, 95000, 100000]
        weekly = build_weekly_sales(txn)
        # All 4 txns included → avg = (100000+90000+95000+100000)/4 = 96250
        assert weekly.iloc[0]["avg_precio_final"] == 96250

    def test_markdown_plus_extreme_discount_both_excluded(self):
        """Markdown exclusion compounds with existing filters, doesn't replace them."""
        txn = _base_txn()
        # Row 1 = extreme discount (>50% off), Row 2 = markdown
        txn.loc[1, "precio_final"] = 30000
        txn.loc[1, "descuento"] = 70000
        txn.loc[2, "precio_final"] = 40000
        txn["list_category"] = ["retail", "retail", "liquidacion", "retail"]
        weekly = build_weekly_sales(txn)
        # Only rows 0 and 3 survive (both at 100000)
        assert weekly.iloc[0]["avg_precio_final"] == 100000


def _elasticity_txn(sku="HK1", weeks=4):
    """Minimal txn frame spanning `weeks` distinct weeks for elasticity fixture."""
    rows = []
    base = pd.Timestamp("2026-01-05")  # Monday
    for w in range(weeks):
        fecha = base + pd.Timedelta(days=w * 7)
        rows.append({
            "sku": sku, "centro": "7501", "fecha": fecha, "cantidad": 1,
            "precio_lista": 100000, "precio_final": 100000, "descuento": 0,
            "folio": f"F{w}",
        })
    return pd.DataFrame(rows)


class TestElasticityMarkdownExclusion:
    def _run(self, txn, products=None):
        """Run prepare_elasticity_data with in-memory txn/products (no disk)."""
        if products is None:
            products = pd.DataFrame({
                "material": [txn["sku"].iloc[0]],
                "codigo_padre": [txn["sku"].iloc[0]],
                "primera_jerarquia": ["Footwear"],
                "segunda_jerarquia": ["Running"],
            })
        with patch("src.features.price_elasticity_brand.pd.read_parquet") as rp:
            rp.side_effect = [txn, products]
            return elasticity.prepare_elasticity_data("HOKA")

    def test_markdown_only_week_dropped_from_price_observations(self):
        """A week whose only sales are liquidación should not produce a price observation."""
        txn = _elasticity_txn(weeks=3)
        # Week 0 = retail 100000, Week 1 = retail 100000, Week 2 = liquidación-only at 60000
        txn.loc[2, "precio_final"] = 60000
        txn["list_category"] = ["retail", "retail", "liquidacion"]
        weekly = self._run(txn)
        # Only 2 rows survive (retail weeks) — the liquidación week has no clean price
        assert len(weekly) == 2
        # Both surviving weeks are at retail price
        assert (weekly["avg_price"] == 100000).all()

    def test_no_list_category_column_preserves_legacy_behavior(self):
        """Without list_category, all sales feed price observations (pre-Phase-5 behavior)."""
        txn = _elasticity_txn(weeks=3)
        txn.loc[2, "precio_final"] = 60000
        # Deliberately no list_category column
        weekly = self._run(txn)
        # All 3 weeks have retail prices
        assert len(weekly) == 3
