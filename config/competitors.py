"""Competitor scraping configuration per brand."""

# Which competitor sites to scrape per brand.
# Each entry is the adapter name used in src/scraping/.
BRAND_COMPETITORS = {
    "HOKA": ["falabella", "mercadolibre", "hoka_cl", "sparta", "marathon"],
    "BOLD": ["falabella", "ripley", "paris", "mercadolibre", "theline", "nike_cl"],
    "BAMERS": ["falabella", "ripley", "paris", "mercadolibre"],
    "OAKLEY": ["falabella", "ripley", "paris", "mercadolibre"],
    "BELSPORT": ["falabella", "ripley", "paris", "mercadolibre"],
}

# Rate limits per adapter (seconds between requests)
RATE_LIMITS = {
    "falabella": 4.0,
    "ripley": 2.0,
    "paris": 2.0,
    "mercadolibre": 1.0,
    "hoka_cl": 1.5,
    "nike_cl": 2.0,
    "theline": 1.5,
    "sparta": 1.5,
    "marathon": 1.5,
}

# User agents to rotate through
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
]
