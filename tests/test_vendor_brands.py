"""Tests for config/vendor_brands.py — SKU prefix to vendor brand mapping."""

import pytest
from config.vendor_brands import (
    get_vendor_brand,
    is_ecomm_store,
    VENDOR_BRAND_PREFIXES,
    MULTI_VENDOR_BRANDS,
    _SORTED_PREFIXES,
)


class TestGetVendorBrand:
    """Vendor brand resolution from SKU prefixes."""

    def test_nike(self):
        assert get_vendor_brand("NI1234567890") == "Nike"

    def test_nike_peru(self):
        """NP prefix should also map to Nike."""
        assert get_vendor_brand("NP9876543210") == "Nike"

    def test_jordan_separate(self):
        """JR should be Jordan, NOT Nike."""
        assert get_vendor_brand("JR5555555555") == "Jordan"

    def test_adidas(self):
        assert get_vendor_brand("AD1111111111") == "Adidas"

    def test_puma(self):
        assert get_vendor_brand("PM2222222222") == "Puma"

    def test_new_era(self):
        """NE is New Era, not Nike Equipment."""
        assert get_vendor_brand("NE3333333333") == "New Era"

    def test_carhartt_3char_prefix(self):
        """CAH is 3-char prefix — must match before 2-char CA* prefix."""
        assert get_vendor_brand("CAH32467890232") == "Carhartt"

    def test_3char_prefix_priority(self):
        """3-char prefixes must be checked before 2-char."""
        # CAB should match "Bamers", not "CA" (if CA existed)
        assert get_vendor_brand("CAB12345") == "Bamers"

    def test_unknown_prefix(self):
        assert get_vendor_brand("ZZ1234567890") == "Other"

    def test_empty_sku(self):
        assert get_vendor_brand("") == "Other"

    def test_none_sku(self):
        assert get_vendor_brand(None) == "Other"

    def test_case_insensitive(self):
        assert get_vendor_brand("ni1234567890") == "Nike"
        assert get_vendor_brand("Ni1234567890") == "Nike"

    def test_all_brands_have_known_mapping(self):
        """Every prefix in the config should resolve to a non-Other brand."""
        for prefix, brand in VENDOR_BRAND_PREFIXES.items():
            assert get_vendor_brand(prefix + "0000") == brand

    def test_sorted_prefixes_longest_first(self):
        """Prefixes must be sorted longest first for correct matching."""
        for i in range(len(_SORTED_PREFIXES) - 1):
            assert len(_SORTED_PREFIXES[i]) >= len(_SORTED_PREFIXES[i + 1])

    def test_skechers(self):
        assert get_vendor_brand("SK1234") == "Skechers"

    def test_under_armour(self):
        assert get_vendor_brand("UA5678") == "Under Armour"

    def test_all_bold_codes(self):
        """BOLD brand codes: NI, PM, AD, JR, NB, VN, NE, NP, CV, CAH."""
        expected = {
            "NI": "Nike", "PM": "Puma", "AD": "Adidas", "JR": "Jordan",
            "NB": "New Balance", "VN": "Vans", "NE": "New Era",
            "NP": "Nike", "CV": "Converse",
        }
        for prefix, brand in expected.items():
            assert get_vendor_brand(prefix + "999") == brand, f"{prefix} should map to {brand}"


class TestBrandOverrides:
    """Brand-specific prefix overrides."""

    def test_belsport_lotto(self):
        assert get_vendor_brand("LT1234", "BELSPORT") == "Lotto"

    def test_belsport_alphabet(self):
        assert get_vendor_brand("AL5678", "BELSPORT") == "Alphabet"

    def test_default_lacoste(self):
        assert get_vendor_brand("LT1234") == "Lacoste"

    def test_default_alpinestars(self):
        assert get_vendor_brand("AL5678") == "Alpinestars"

    def test_bold_uses_default(self):
        assert get_vendor_brand("LT1234", "BOLD") == "Lacoste"
        assert get_vendor_brand("AL5678", "BOLD") == "Alpinestars"

    def test_belsport_non_overridden_uses_default(self):
        assert get_vendor_brand("NI1234", "BELSPORT") == "Nike"


class TestIsEcommStore:
    """Ecommerce store detection from store codes."""

    def test_ecomm_hoka(self):
        assert is_ecomm_store("AB75") is True

    def test_ecomm_bold(self):
        assert is_ecomm_store("AB20") is True

    def test_ecomm_generic(self):
        assert is_ecomm_store("AB10") is True

    def test_brick_and_mortar(self):
        assert is_ecomm_store("2002") is False
        assert is_ecomm_store("7501") is False
        assert is_ecomm_store("B002") is False
        assert is_ecomm_store("D002") is False
        assert is_ecomm_store("1000") is False

    def test_case_insensitive(self):
        assert is_ecomm_store("ab75") is True
        assert is_ecomm_store("Ab20") is True

    def test_empty_store(self):
        assert is_ecomm_store("") is False


class TestMultiVendorBrands:
    """Multi-vendor brand configuration."""

    def test_bold_is_multi_vendor(self):
        assert "bold" in MULTI_VENDOR_BRANDS

    def test_belsport_is_multi_vendor(self):
        assert "belsport" in MULTI_VENDOR_BRANDS

    def test_bamers_is_multi_vendor(self):
        assert "bamers" in MULTI_VENDOR_BRANDS

    def test_hoka_is_not_multi_vendor(self):
        assert "hoka" not in MULTI_VENDOR_BRANDS

    def test_oakley_is_not_multi_vendor(self):
        assert "oakley" not in MULTI_VENDOR_BRANDS
