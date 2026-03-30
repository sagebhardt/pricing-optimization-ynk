"""Falabella.com scraper — extracts __NEXT_DATA__ JSON from search results."""

import re
import json

from src.scraping.base import CompetitorScraper
from src.scraping.matcher import match_product


class FalabellaScraper(CompetitorScraper):
    """falabella.com/falabella-cl — Next.js search page with embedded JSON."""

    name = "falabella"
    base_url = "https://www.falabella.com"
    skip_robots = True  # Weekly batch, rate-limited, respectful

    def search_product(self, product_name, brand, ean11=None):
        query = product_name
        resp = self.fetch(
            f"{self.base_url}/falabella-cl/search",
            params={"Ntt": query},
        )
        if not resp:
            return []

        # Extract __NEXT_DATA__ JSON from HTML
        match = re.search(
            r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
            resp.text, re.DOTALL,
        )
        if not match:
            return []

        try:
            next_data = json.loads(match.group(1))
        except json.JSONDecodeError:
            return []

        # Navigate to search results
        props = next_data.get("props", {}).get("pageProps", {})
        results_list = props.get("results", [])

        results = []
        for product in results_list[:10]:
            name = product.get("displayName", "")
            prod_brand = product.get("brand", "")
            url = product.get("url", "")

            # Parse prices
            prices = product.get("prices", [])
            sale_price = None
            list_price = None

            for p in prices:
                price_vals = p.get("price", [])
                price_str = price_vals[0] if price_vals else "0"
                # Format: "105.990" or "149990"
                price_int = int(str(price_str).replace(".", "").replace(",", ""))

                if p.get("crossed", False):
                    list_price = price_int
                else:
                    if sale_price is None or price_int < sale_price:
                        sale_price = price_int

            if not sale_price or sale_price <= 0:
                continue
            if list_price is None:
                list_price = sale_price

            # Match against our product
            method, score = match_product(
                product_name, brand, name, prod_brand, ean_matched=False,
            )
            if method == "no_match":
                continue

            discount = round(1 - sale_price / list_price, 3) if list_price > sale_price else 0.0

            # Check stock from variants
            in_stock = True  # Default to true if we found it in search

            if not url.startswith("http"):
                url = f"{self.base_url}{url}"

            results.append({
                "competitor_url": url,
                "comp_price": sale_price,
                "comp_list_price": list_price,
                "comp_discount": discount,
                "comp_in_stock": in_stock,
                "matched_name": name,
                "match_method": method,
                "match_score": round(score, 3),
            })

        return results
