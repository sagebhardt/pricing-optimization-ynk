"""Tests for the lista_precio classifier in config/price_lists.py."""

from config.price_lists import classify_price_list, is_markdown


class TestClassifyPriceList:
    def test_none_returns_unknown(self):
        assert classify_price_list(None) == "unknown"

    def test_empty_string_returns_unknown(self):
        assert classify_price_list("") == "unknown"
        assert classify_price_list("   ") == "unknown"

    def test_liquidacion_variants(self):
        assert classify_price_list("Liquidación Bold") == "liquidacion"
        assert classify_price_list("Liq. Zona Norte") == "liquidacion"
        assert classify_price_list("Liq. Zona Sur") == "liquidacion"
        assert classify_price_list("Oakley Liquidación") == "liquidacion"
        assert classify_price_list("K1 Liquidadora") == "liquidacion"
        assert classify_price_list("Lista Liquidadora") == "liquidacion"

    def test_outlet_variants(self):
        assert classify_price_list("Outlet la fabrica") == "outlet"
        assert classify_price_list("Bamers Outlets") == "outlet"
        assert classify_price_list("Crocs Outlets") == "outlet"

    def test_eventos(self):
        assert classify_price_list("Bamers Eventos") == "eventos"
        assert classify_price_list("Lista Bels Eventos") == "eventos"
        assert classify_price_list("K1 Eventos") == "eventos"

    def test_online_virtual_marketplace_wholesale(self):
        assert classify_price_list("Bamers Virtual") == "online"
        assert classify_price_list("Belsport Virtual") == "online"
        assert classify_price_list("Bamers Marketplace") == "online"
        assert classify_price_list("QSRX-WholeSale") == "online"

    def test_retail_is_default(self):
        assert classify_price_list("Bamers Tienda") == "retail"
        assert classify_price_list("Oakley Tienda") == "retail"
        assert classify_price_list("Hoka tiendas") == "retail"
        assert classify_price_list("Lista general") == "retail"
        assert classify_price_list("Tiendas Calle") == "retail"

    def test_liquidacion_outranks_retail(self):
        # "Liquidación Bold" contains neither "outlet" nor "evento" so liq wins
        assert classify_price_list("Liquidación Bold") == "liquidacion"


class TestIsMarkdown:
    def test_liquidacion_is_markdown(self):
        assert is_markdown("Liquidación Bold") is True

    def test_outlet_is_markdown(self):
        assert is_markdown("Bamers Outlets") is True

    def test_eventos_not_markdown(self):
        assert is_markdown("Bamers Eventos") is False

    def test_retail_not_markdown(self):
        assert is_markdown("Hoka tiendas") is False

    def test_none_not_markdown(self):
        assert is_markdown(None) is False
