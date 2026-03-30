"""Paris.cl scraper.

STATUS: Returns 200 but page is client-side rendered — no product data in HTML (Mar 2026).
Will need Playwright or internal API discovery. Stubbed for now.
"""

from src.scraping.base import CompetitorScraper


class ParisScraper(CompetitorScraper):
    """paris.cl — client-rendered, no product data in HTML. Needs Playwright."""

    name = "paris"
    base_url = "https://www.paris.cl"

    def search_product(self, product_name, brand, ean11=None):
        return []  # Client-rendered — no product data in HTML
