"""Paris.cl scraper — Phase 2 (BOLD priority)."""

from src.scraping.base import CompetitorScraper


class ParisScraper(CompetitorScraper):
    """paris.cl — Cencosud platform (Phase 2)."""

    name = "paris"
    base_url = "https://www.paris.cl"

    def search_product(self, product_name, brand, ean11=None):
        # TODO: Implement for BOLD phase
        return []
