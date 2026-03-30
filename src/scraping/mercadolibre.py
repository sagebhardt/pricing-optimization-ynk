"""MercadoLibre Chile scraper.

STATUS: Search API returns 403 even with valid OAuth2 tokens (Mar 2026).
MercadoLibre has locked down /sites/MLC/search to require app-level permissions
that seller-type credentials don't have. Will need either:
  - A "marketplace" type app registration with search scope
  - Playwright to solve the website's SHA-256 proof-of-work challenge

The code below is functional — if the API permissions are unlocked, it works.

Configuration (env vars):
    ML_APP_ID          — MercadoLibre app ID
    ML_CLIENT_SECRET   — MercadoLibre client secret
    ML_REFRESH_TOKEN   — OAuth2 refresh token (long-lived)

Or for multi-brand (from agentic-mktplc-ynk):
    ML_BRAND_CREDENTIALS — JSON array of {brand_slug, app_id, client_secret, refresh_token}
"""

import os
import json

import httpx

from src.scraping.base import CompetitorScraper
from src.scraping.matcher import match_product


class MercadoLibreScraper(CompetitorScraper):
    """api.mercadolibre.com — OAuth2 token refresh + search API."""

    name = "mercadolibre"
    base_url = "https://api.mercadolibre.com"
    skip_robots = True  # Public API

    def __init__(self):
        super().__init__()
        self._access_token = None
        self._authenticate()

    def _authenticate(self):
        """Get an access token via refresh token flow."""
        # Try direct env vars first
        app_id = os.environ.get("ML_APP_ID", "")
        secret = os.environ.get("ML_CLIENT_SECRET", "")
        refresh = os.environ.get("ML_REFRESH_TOKEN", "")

        # Fall back to multi-brand credentials (use first available)
        if not (app_id and secret and refresh):
            creds_json = os.environ.get("ML_BRAND_CREDENTIALS", "")
            if creds_json:
                try:
                    creds = json.loads(creds_json)
                    if creds:
                        c = creds[0]  # Any credential works for search
                        app_id = c.get("app_id", "")
                        secret = c.get("client_secret", "")
                        refresh = c.get("refresh_token", "")
                except (json.JSONDecodeError, IndexError):
                    pass

        if not (app_id and secret and refresh):
            return

        try:
            resp = httpx.post(
                f"{self.base_url}/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": app_id,
                    "client_secret": secret,
                    "refresh_token": refresh,
                },
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                self._access_token = data.get("access_token")
                # Update refresh token for next time (ML rotates them)
                new_refresh = data.get("refresh_token")
                if new_refresh and new_refresh != refresh:
                    print(f"    [mercadolibre] Refresh token rotated — update ML_REFRESH_TOKEN")
            else:
                print(f"    [mercadolibre] Auth failed: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            print(f"    [mercadolibre] Auth error: {e}")

    def search_product(self, product_name, brand, ean11=None):
        if not self._access_token:
            return []

        # Search by EAN first if available
        if ean11:
            results = self._search_by_ean(ean11, product_name, brand)
            if results:
                return results

        # Text search with brand prefix
        query = f"{brand} {product_name}" if brand else product_name
        resp = self.fetch(
            f"{self.base_url}/sites/MLC/search",
            params={"q": query, "limit": 5},
        )
        if not resp:
            return []

        try:
            data = resp.json()
        except Exception:
            return []

        return self._parse_results(data.get("results", []), product_name, brand)

    def _search_by_ean(self, ean11, product_name, brand):
        resp = self.fetch(
            f"{self.base_url}/sites/MLC/search",
            params={"q": ean11, "limit": 3},
        )
        if not resp:
            return []

        try:
            data = resp.json()
        except Exception:
            return []

        results = data.get("results", [])
        return self._parse_results(results, product_name, brand, ean_matched=True) if results else []

    def fetch(self, url, params=None):
        """Override to inject access token as header instead of query param."""
        self._rate_limit_wait()
        try:
            headers = {**self._get_headers(), "Authorization": f"Bearer {self._access_token}"}
            with httpx.Client(follow_redirects=True, timeout=15) as client:
                resp = client.get(url, params=params, headers=headers)
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (401, 403):
                    print(f"    [mercadolibre] {resp.status_code} — token may be expired")
                    return None
                if resp.status_code == 429:
                    print(f"    [mercadolibre] Rate limited")
                    return None
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            print(f"    [mercadolibre] Connection error: {e}")
        return None

    def _parse_results(self, results, product_name, brand, ean_matched=False):
        matches = []
        for item in results:
            name = item.get("title", "")
            price = item.get("price", 0)
            original_price = item.get("original_price") or price

            if not price or price <= 0:
                continue

            if not ean_matched:
                method, score = match_product(product_name, brand, name, ean_matched=False)
                if method == "no_match":
                    continue
            else:
                method, score = "exact_ean", 1.0

            discount = round(1 - price / original_price, 3) if original_price > price else 0.0

            matches.append({
                "competitor_url": item.get("permalink", ""),
                "comp_price": int(price),
                "comp_list_price": int(original_price),
                "comp_discount": discount,
                "comp_in_stock": item.get("available_quantity", 0) > 0,
                "matched_name": name,
                "match_method": method,
                "match_score": round(score, 3),
            })

        return matches
