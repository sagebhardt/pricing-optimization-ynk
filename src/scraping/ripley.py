"""Ripley.cl scraper — Phase 2 (BOLD priority)."""

from src.scraping.base import CompetitorScraper


class RipleyScraper(CompetitorScraper):
    """ripley.cl — API-based scraper (Phase 2)."""

    name = "ripley"
    base_url = "https://simple.ripley.cl"

    def search_product(self, product_name, brand, ean11=None):
        # TODO: Implement for BOLD phase
        # Ripley has API at simple.ripley.cl/api/v2/
        return []
