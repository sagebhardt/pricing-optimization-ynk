"""Product matching logic for competitor scraping.

Matches competitor products to our catalog using EAN11 barcode
or fuzzy product name matching.
"""

import re
import unicodedata
from difflib import SequenceMatcher


def normalize_name(name: str) -> str:
    """Normalize a product name for fuzzy matching.

    Strips accents, lowercases, removes size suffixes and punctuation.
    """
    if not name:
        return ""
    # Strip accents
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower().strip()
    # Remove size suffixes like "N° 42" or "T/42" or "talla 42"
    name = re.sub(r"\s+n[°º]\s*[\d,\.]+\s*$", "", name)  # Require ° after N to avoid "cliftoN 9"
    name = re.sub(r"\s*t/[a-z0-9]+\s*$", "", name)
    name = re.sub(r"\s*talla\s*[\d\.]+\s*$", "", name)
    # Remove common noise words
    name = re.sub(r"\b(hombre|mujer|unisex|adulto|infantil|nino|nina)\b", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _strip_brand_prefix(name: str, brand: str) -> str:
    """Strip brand name from beginning of product name if present."""
    if not brand:
        return name
    b = normalize_name(brand)
    if name.startswith(b + " "):
        return name[len(b):].strip()
    # Also handle "hoka one one" prefix
    for variant in [b, b + " one one"]:
        if name.startswith(variant + " "):
            return name[len(variant):].strip()
    return name


def fuzzy_match_score(name_a: str, name_b: str, brand: str = None) -> float:
    """Compute similarity score between two product names.

    Uses SequenceMatcher ratio on normalized names,
    plus a token-sort comparison for reordered words.
    Strips brand prefixes before comparing.
    """
    a = normalize_name(name_a)
    b = normalize_name(name_b)

    if not a or not b:
        return 0.0

    # Strip brand prefix from both (competitor names often include "Hoka Bondi 9")
    if brand:
        a = _strip_brand_prefix(a, brand)
        b = _strip_brand_prefix(b, brand)

    # Direct sequence match
    direct = SequenceMatcher(None, a, b).ratio()

    # Token-sort: sort words alphabetically, then compare
    a_sorted = " ".join(sorted(a.split()))
    b_sorted = " ".join(sorted(b.split()))
    token_sort = SequenceMatcher(None, a_sorted, b_sorted).ratio()

    # Token-subset: if all tokens of the shorter name appear in the longer,
    # it's a strong match (handles "BONDI 9" matching "Bondi 9 Mujer Blue/White")
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    shorter, longer = (a_tokens, b_tokens) if len(a_tokens) <= len(b_tokens) else (b_tokens, a_tokens)
    if shorter and shorter.issubset(longer):
        subset_score = len(shorter) / len(longer)  # Penalize very long names slightly
        subset_score = max(subset_score, 0.85)  # But always >= 0.85 if subset matches
    else:
        subset_score = 0.0

    return max(direct, token_sort, subset_score)


def classify_match(score: float, used_ean: bool) -> str:
    """Classify match confidence level."""
    if used_ean:
        return "exact_ean"
    if score >= 0.90:
        return "high_name"
    if score >= 0.85:
        return "medium_name"
    if score >= 0.75:
        return "low_name"
    return "no_match"


def match_product(our_name: str, our_brand: str, competitor_name: str,
                  competitor_brand: str = None, ean_matched: bool = False,
                  min_score: float = 0.85) -> tuple[str, float]:
    """
    Determine if a competitor product matches ours.

    Returns (match_method, match_score).
    match_method is one of: exact_ean, high_name, medium_name, low_name, no_match
    """
    if ean_matched:
        return "exact_ean", 1.0

    # Brand must match if provided
    if competitor_brand and our_brand:
        our_b = normalize_name(our_brand)
        comp_b = normalize_name(competitor_brand)
        if our_b and comp_b and our_b not in comp_b and comp_b not in our_b:
            return "no_match", 0.0

    # Try with our brand, then with competitor brand for stripping
    score = fuzzy_match_score(our_name, competitor_name, brand=our_brand)
    if competitor_brand and competitor_brand != our_brand:
        score2 = fuzzy_match_score(our_name, competitor_name, brand=competitor_brand)
        score = max(score, score2)
    method = classify_match(score, False)

    if score < min_score:
        return "no_match", score

    return method, score
