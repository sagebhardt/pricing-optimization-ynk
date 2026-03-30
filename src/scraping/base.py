"""Base class for competitor scrapers with rate limiting and retries."""

import time
import random
from abc import ABC, abstractmethod
from urllib.robotparser import RobotFileParser

import httpx
import pandas as pd

from config.competitors import USER_AGENTS, RATE_LIMITS


class CompetitorScraper(ABC):
    """Abstract base for site-specific competitor scrapers."""

    name: str = "unknown"
    base_url: str = ""

    def __init__(self):
        self.rate_limit = RATE_LIMITS.get(self.name, 2.0)
        self.max_retries = 3
        self._last_request = 0.0
        self._robots_checked = False
        self._robots_allowed = True

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-CL,es;q=0.9,en;q=0.5",
        }

    def _rate_limit_wait(self):
        elapsed = time.time() - self._last_request
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed + random.uniform(0.1, 0.5))
        self._last_request = time.time()

    # Subclasses can set this to True to skip robots.txt check
    # (e.g., for public APIs like WooCommerce Store API or MercadoLibre API)
    skip_robots: bool = False

    def _check_robots(self, url: str = None):
        if self._robots_checked or not self.base_url or self.skip_robots:
            return
        self._robots_checked = True
        check_url = url or f"{self.base_url}/search"
        try:
            rp = RobotFileParser()
            rp.set_url(f"{self.base_url}/robots.txt")
            rp.read()
            self._robots_allowed = rp.can_fetch("*", check_url)
        except Exception:
            self._robots_allowed = True  # Allow on error

    def fetch(self, url: str, params: dict = None) -> httpx.Response | None:
        """HTTP GET with rate limiting, retries, and UA rotation."""
        self._check_robots()
        if not self._robots_allowed:
            return None

        for attempt in range(self.max_retries):
            self._rate_limit_wait()
            try:
                with httpx.Client(follow_redirects=True, timeout=15) as client:
                    resp = client.get(url, params=params, headers=self._get_headers())
                    if resp.status_code == 200:
                        return resp
                    if resp.status_code == 429:
                        wait = int(resp.headers.get("Retry-After", 10))
                        print(f"    Rate limited by {self.name}, waiting {wait}s")
                        time.sleep(wait)
                        continue
                    if resp.status_code in (403, 503):
                        print(f"    {self.name} returned {resp.status_code} — skipping")
                        return None
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                else:
                    print(f"    {self.name} connection failed: {e}")
                    return None
        return None

    @abstractmethod
    def search_product(self, product_name: str, brand: str, ean11: str = None) -> list[dict]:
        """
        Search for a product on this competitor site.

        Returns list of matches, each dict with:
            competitor_url, comp_price, comp_list_price, comp_discount,
            comp_in_stock, matched_name
        """
        ...

    def scrape(self, catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Scrape prices for all products in the catalog.

        catalog must have: codigo_padre, product_name, vendor_brand
        catalog may have: ean11
        """
        results = []
        has_ean = "ean11" in catalog.columns
        total = len(catalog)

        for i, (_, row) in enumerate(catalog.iterrows()):
            product_name = row["product_name"]
            brand = row.get("vendor_brand", "")
            ean = str(row["ean11"]) if has_ean and pd.notna(row.get("ean11")) else None
            parent = row["codigo_padre"]

            try:
                matches = self.search_product(product_name, brand, ean)
                for m in matches:
                    m["codigo_padre"] = parent
                    m["ean11"] = ean
                    m["competitor"] = self.name
                    results.append(m)
            except Exception as e:
                print(f"    [{self.name}] Error scraping {parent}: {e}")

            if (i + 1) % 20 == 0:
                print(f"    [{self.name}] {i + 1}/{total} products scraped, {len(results)} matches")

        return pd.DataFrame(results)
