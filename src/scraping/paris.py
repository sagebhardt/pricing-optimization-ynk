"""Paris.cl scraper via Constructor.io search API.

Paris uses Constructor.io as its search engine. The API key is public
(extracted from their client-side JS bundle at cnstrc.com).
"""

from src.scraping.base import CompetitorScraper
from src.scraping.matcher import match_product


# Constructor.io API key for Paris Chile (Cencosud)
CNSTRC_API_KEY = "key_8pjkPsSkEsJHKgxR"


class ParisScraper(CompetitorScraper):
    """paris.cl — Constructor.io search API (JSON, public key)."""

    name = "paris"
    base_url = "https://ac.cnstrc.com"
    skip_robots = True

    def search_product(self, product_name, brand, ean11=None):
        query = f"{brand} {product_name}" if brand else product_name
        resp = self.fetch(
            f"{self.base_url}/search/{query}",
            params={
                "key": CNSTRC_API_KEY,
                "num_results_per_page": 10,
                "page": 1,
            },
        )
        if not resp:
            return []

        try:
            data = resp.json()
        except Exception:
            return []

        results_list = data.get("response", {}).get("results", [])
        results = []

        for r in results_list:
            d = r.get("data", {})
            name = r.get("value", d.get("description", ""))
            price = d.get("displayedPrice")
            discount_pct = d.get("discountPercentage", 0)
            url = d.get("url", "")

            if not price or price <= 0:
                continue

            price = int(price)
            list_price = int(price / (1 - discount_pct / 100)) if discount_pct > 0 else price
            discount = round(discount_pct / 100, 3) if discount_pct else 0.0

            method, score = match_product(product_name, brand, name, ean_matched=False)
            if method == "no_match":
                continue

            if url and not url.startswith("http"):
                url = f"https://paris.cl{url}"

            results.append({
                "competitor_url": url,
                "comp_price": price,
                "comp_list_price": list_price,
                "comp_discount": discount,
                "comp_in_stock": True,  # Constructor.io only returns available products
                "matched_name": name,
                "match_method": method,
                "match_score": round(score, 3),
            })

        return results
