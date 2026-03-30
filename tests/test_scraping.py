"""Tests for competitor scraping framework."""

import pytest
from src.scraping.matcher import normalize_name, fuzzy_match_score, match_product, classify_match


class TestNormalizeName:
    def test_strips_accents(self):
        assert normalize_name("Zapatillón") == "zapatillon"

    def test_lowercase(self):
        assert normalize_name("BONDI 9") == "bondi 9"

    def test_strips_size_suffix(self):
        assert normalize_name("BONDI 9 N° 42") == "bondi 9"
        assert normalize_name("BONDI 9 T/42") == "bondi 9"

    def test_strips_gender(self):
        assert "hombre" not in normalize_name("Speedgoat 6 Hombre")

    def test_collapses_whitespace(self):
        assert normalize_name("  BONDI   9  ") == "bondi 9"

    def test_empty(self):
        assert normalize_name("") == ""
        assert normalize_name(None) == ""


class TestFuzzyMatchScore:
    def test_exact_match(self):
        assert fuzzy_match_score("Bondi 9", "Bondi 9") > 0.99

    def test_high_similarity(self):
        score = fuzzy_match_score("SPEEDGOAT 6 MID GTX", "Hoka Speedgoat 6 Mid GTX")
        assert score > 0.80

    def test_token_sort_helps(self):
        # Reordered words should still match
        score = fuzzy_match_score("GTX Mid Speedgoat 6", "Speedgoat 6 Mid GTX")
        assert score > 0.85

    def test_different_products(self):
        score = fuzzy_match_score("Bondi 9", "Clifton 9")
        assert score < 0.85

    def test_empty_strings(self):
        assert fuzzy_match_score("", "Bondi 9") == 0.0
        assert fuzzy_match_score("Bondi 9", "") == 0.0


class TestMatchProduct:
    def test_ean_match(self):
        method, score = match_product("Bondi 9", "HOKA", "Different Name", ean_matched=True)
        assert method == "exact_ean"
        assert score == 1.0

    def test_high_name_match(self):
        method, score = match_product("BONDI 9", "HOKA", "Hoka Bondi 9")
        assert method in ("high_name", "medium_name")
        assert score >= 0.85

    def test_no_match_different_brand(self):
        method, score = match_product("BONDI 9", "HOKA", "Nike Bondi 9", "Nike")
        assert method == "no_match"

    def test_no_match_different_product(self):
        method, score = match_product("BONDI 9", "HOKA", "Adidas Ultraboost 23")
        assert method == "no_match"

    def test_brand_substring_match(self):
        method, score = match_product("Bondi 9", "HOKA", "HOKA ONE ONE Bondi 9", "HOKA ONE ONE")
        assert method != "no_match"


class TestClassifyMatch:
    def test_ean(self):
        assert classify_match(1.0, True) == "exact_ean"

    def test_high(self):
        assert classify_match(0.92, False) == "high_name"

    def test_medium(self):
        assert classify_match(0.87, False) == "medium_name"

    def test_low(self):
        assert classify_match(0.77, False) == "low_name"

    def test_no_match(self):
        assert classify_match(0.50, False) == "no_match"
