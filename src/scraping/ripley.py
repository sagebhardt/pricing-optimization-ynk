"""Ripley.cl scraper.

STATUS: Returns 403 on both API and HTML search (CloudFlare protected, Mar 2026).
Will need Playwright/headless browser to bypass. Stubbed for now.
"""

from src.scraping.base import CompetitorScraper


class RipleyScraper(CompetitorScraper):
    """ripley.cl — blocked by CloudFlare (403). Needs Playwright."""

    name = "ripley"
    base_url = "https://simple.ripley.cl"

    def search_product(self, product_name, brand, ean11=None):
        return []  # Blocked — CloudFlare 403
