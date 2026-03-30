"""Scrapers for brand official sites and specialty retailers.

- hoka_cl: cl.hoka.com (WooCommerce Store API v1)
- sparta: sparta.cl (Magento 2 GraphQL)
- marathon: marathon.cl (SFCC HTML + JSON-LD)
"""

import re
import json

from src.scraping.base import CompetitorScraper
from src.scraping.matcher import match_product


class HokaClScraper(CompetitorScraper):
    """cl.hoka.com — WooCommerce Store API v1 (public, no auth)."""

    name = "hoka_cl"
    base_url = "https://cl.hoka.com"
    skip_robots = True  # Public Store API

    def search_product(self, product_name, brand, ean11=None):
        # Clean search query from internal product names
        query = product_name
        # Strip brand prefix
        for prefix in ("HOKA", "HOKA ONE ONE"):
            query = query.replace(prefix, "").strip()
        # Strip gender prefix (M/W at start)
        if len(query) > 2 and query[:2] in ("M ", "W "):
            query = query[2:]
        # Strip internal color codes (3-4 uppercase letters at end, e.g., "BFBG", "BWHT")
        import re
        query = re.sub(r'\s+[A-Z]{3,5}$', '', query)
        # Strip "/ color description" suffix
        query = re.sub(r'\s*/\s*.*$', '', query)
        query = query.strip().lower()

        if len(query) < 3:
            return []

        resp = self.fetch(
            f"{self.base_url}/wp-json/wc/store/v1/products",
            params={"search": query, "per_page": 5},
        )
        if not resp:
            return []

        try:
            products = resp.json()
        except Exception:
            return []

        # WooCommerce search can be finicky — retry with shorter query if no results
        if not products and " " in query:
            short_query = " ".join(query.split()[:1])  # Just model name
            resp2 = self.fetch(
                f"{self.base_url}/wp-json/wc/store/v1/products",
                params={"search": short_query, "per_page": 5},
            )
            if resp2:
                try:
                    products = resp2.json()
                except Exception:
                    pass

        results = []
        for p in products:
            name = p.get("name", "")
            prices = p.get("prices", {})
            price_str = prices.get("price", "0")
            list_price_str = prices.get("regular_price", price_str)

            try:
                price = int(price_str)
                list_price = int(list_price_str)
            except (ValueError, TypeError):
                continue

            if price <= 0:
                continue

            method, score = match_product(product_name, brand, name, "HOKA", ean_matched=False)
            if method == "no_match":
                continue

            discount = round(1 - price / list_price, 3) if list_price > 0 and price < list_price else 0.0

            results.append({
                "competitor_url": p.get("permalink", ""),
                "comp_price": price,
                "comp_list_price": list_price,
                "comp_discount": discount,
                "comp_in_stock": p.get("is_in_stock", False),
                "matched_name": name,
                "match_method": method,
                "match_score": round(score, 3),
            })

        return results


class SpartaScraper(CompetitorScraper):
    """sparta.cl — Magento 2 GraphQL API."""

    name = "sparta"
    base_url = "https://sparta.cl"
    skip_robots = True  # Public GraphQL API

    _GRAPHQL_QUERY = """
    {
      products(search: "%s", pageSize: 10) {
        items {
          name
          sku
          url_key
          stock_status
          price_range {
            minimum_price {
              regular_price { value currency }
              final_price { value currency }
              discount { percent_off }
            }
          }
        }
      }
    }
    """

    def search_product(self, product_name, brand, ean11=None):
        query = product_name.replace('"', '\\"')
        gql = self._GRAPHQL_QUERY % query

        self._rate_limit_wait()
        try:
            import httpx
            with httpx.Client(follow_redirects=True, timeout=15) as client:
                resp = client.post(
                    f"{self.base_url}/graphql",
                    json={"query": gql},
                    headers={**self._get_headers(), "Content-Type": "application/json"},
                )
                if resp.status_code != 200:
                    return []
                data = resp.json()
        except Exception:
            return []

        items = data.get("data", {}).get("products", {}).get("items", [])
        results = []

        for item in items:
            name = item.get("name", "")
            price_range = item.get("price_range", {}).get("minimum_price", {})
            final = price_range.get("final_price", {}).get("value", 0)
            regular = price_range.get("regular_price", {}).get("value", 0)
            discount_pct = price_range.get("discount", {}).get("percent_off", 0)

            price = int(final) if final else 0
            list_price = int(regular) if regular else price
            if price <= 0:
                continue

            method, score = match_product(product_name, brand, name, ean_matched=False)
            if method == "no_match":
                continue

            url_key = item.get("url_key", "")
            results.append({
                "competitor_url": f"{self.base_url}/{url_key}.html" if url_key else "",
                "comp_price": price,
                "comp_list_price": list_price,
                "comp_discount": round(discount_pct / 100, 3) if discount_pct else 0.0,
                "comp_in_stock": item.get("stock_status") == "IN_STOCK",
                "matched_name": name,
                "match_method": method,
                "match_score": round(score, 3),
            })

        return results


class MarathonScraper(CompetitorScraper):
    """marathon.cl — SFCC HTML scraping with JSON-LD."""

    name = "marathon"
    base_url = "https://www.marathon.cl"

    def search_product(self, product_name, brand, ean11=None):
        query = product_name.replace(" ", "+")
        resp = self.fetch(f"{self.base_url}/search", params={"q": query, "sz": 12})
        if not resp:
            return []

        html = resp.text
        results = []

        # Extract product tiles: data-pid, title, price, discount, URL
        tiles = re.findall(
            r'<div[^>]*class="[^"]*product-tile-wrap[^"]*"[^>]*data-pid="(\d+)"[^>]*>.*?</div>\s*</div>\s*</div>',
            html, re.DOTALL,
        )

        if not tiles:
            # Fallback: try simpler patterns
            # Look for product links and prices in broader structure
            links = re.findall(r'<a[^>]*href="(/producto/[^"]+)"[^>]*>', html)
            return self._scrape_product_pages(links[:5], product_name, brand)

        return results

    def _scrape_product_pages(self, links, product_name, brand):
        """Scrape individual product pages for JSON-LD data."""
        results = []
        for link in links:
            url = f"{self.base_url}{link}" if link.startswith("/") else link
            resp = self.fetch(url)
            if not resp:
                continue

            # Extract JSON-LD
            ld_match = re.search(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                resp.text, re.DOTALL,
            )
            if not ld_match:
                continue

            try:
                ld = json.loads(ld_match.group(1))
            except json.JSONDecodeError:
                continue

            if ld.get("@type") != "Product":
                continue

            name = ld.get("name", "")
            offers = ld.get("offers", {})
            if isinstance(offers, list):
                offers = offers[0] if offers else {}

            price_str = offers.get("price", "0")
            try:
                price = int(float(price_str))
            except (ValueError, TypeError):
                continue

            if price <= 0:
                continue

            method, score = match_product(product_name, brand, name, ean_matched=False)
            if method == "no_match":
                continue

            in_stock = "InStock" in offers.get("availability", "")
            results.append({
                "competitor_url": url,
                "comp_price": price,
                "comp_list_price": price,  # JSON-LD often only has final price
                "comp_discount": 0.0,
                "comp_in_stock": in_stock,
                "matched_name": name,
                "match_method": method,
                "match_score": round(score, 3),
            })

        return results


class TheLineScraper(CompetitorScraper):
    """theline.cl — VTEX Intelligent Search API (JSON, no auth)."""

    name = "theline"
    base_url = "https://www.theline.cl"
    skip_robots = True

    def search_product(self, product_name, brand, ean11=None):
        query = f"{brand} {product_name}" if brand else product_name
        resp = self.fetch(
            f"{self.base_url}/api/io/_v/api/intelligent-search/product_search/v2",
            params={"query": query, "count": 10, "locale": "es-CL"},
        )
        if not resp:
            return []

        try:
            data = resp.json()
        except Exception:
            return []

        results = []
        for p in data.get("products", []):
            name = p.get("productName", "")
            p_brand = p.get("brand", "")
            items = p.get("items", [])
            if not items:
                continue

            # Get price from first seller
            sellers = items[0].get("sellers", [])
            if not sellers:
                continue
            offer = sellers[0].get("commertialOffer", {})
            price = int(offer.get("Price", 0))
            list_price = int(offer.get("ListPrice", 0)) or price
            stock = offer.get("AvailableQuantity", 0)

            if price <= 0:
                continue

            method, score = match_product(
                product_name, brand, name, p_brand, ean_matched=False,
            )
            if method == "no_match":
                continue

            discount = round(1 - price / list_price, 3) if list_price > price else 0.0
            url = p.get("link", f"{self.base_url}/{p.get('linkText', '')}/p")

            results.append({
                "competitor_url": url,
                "comp_price": price,
                "comp_list_price": list_price,
                "comp_discount": discount,
                "comp_in_stock": stock > 0,
                "matched_name": name,
                "match_method": method,
                "match_score": round(score, 3),
            })

        return results


def get_brand_site_scraper(name: str) -> CompetitorScraper:
    """Factory for brand site scrapers."""
    scrapers = {
        "hoka_cl": HokaClScraper,
        "sparta": SpartaScraper,
        "marathon": MarathonScraper,
        "theline": TheLineScraper,
    }
    cls = scrapers.get(name)
    if cls is None:
        raise ValueError(f"Unknown brand site scraper: {name}")
    return cls()
