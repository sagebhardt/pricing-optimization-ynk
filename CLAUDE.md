# CLAUDE.md — YNK Pricing Optimization

## Project Overview
ML-driven markdown and pricing optimization for Yaneken Retail Group. Predicts optimal markdown timing and depth per parent SKU per store per week. Currently live for HOKA, BOLD, BAMERS, and OAKLEY.

## Quick Commands
```bash
# Run full pipeline for a brand
python run_brand.py HOKA
python run_brand.py BOLD
python run_brand.py BAMERS
python run_brand.py OAKLEY

# Run specific steps
python run_brand.py HOKA --steps aggregate train pricing
python run_brand.py BOLD --steps extract features elasticity lifecycle size_curve enhance aggregate train pricing

# Run weekly automation (all brands + deploy)
./run_weekly.sh                    # All brands
./run_weekly.sh BAMERS OAKLEY      # Specific brands

# Start API locally
python3 -m uvicorn api.main:app --port 8080

# Start dashboard dev server
cd dashboard && npm run dev

# Deploy to Cloud Run
gcloud builds submit . --tag us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-api --project ynk-pricing-optimization
gcloud run deploy pricing-api --image us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-api --region us-central1 --project ynk-pricing-optimization --platform managed --memory 2Gi --cpu 2 --allow-unauthenticated --set-env-vars "PYTHONPATH=/app"
```

## Architecture
```
run_brand.py                    # Pipeline orchestrator (per brand)
run_weekly.sh                   # Weekly automation (all brands + deploy)
src/data/extract_brand.py       # PostgreSQL → parquet
src/features/
  build_features_brand.py       # Base features (child SKU level)
  price_elasticity_brand.py     # Log-log demand elasticity
  lifecycle_brand.py            # Launch→Growth→Peak→Steady→Decline→Clearance
  size_curve_brand.py           # Size depletion tracking + alerts
  build_enhanced_brand.py       # Merge all → v2 features
  aggregate_parent.py           # Child → parent SKU aggregation
src/models/
  train_brand.py                # XGBoost training with time-series CV
  weekly_pricing_brand.py       # Weekly action list generator (filters non-retail stores)
  first_markdown.py             # Prescriptive first-markdown timing model
api/main.py                     # FastAPI (serves dashboard + API, brand-filtered alerts)
dashboard/src/App.jsx           # React dashboard with landing page
config/database.py              # DB config (credentials via .env), brand configs, store exclusions
```

## Active Brands
| Brand | Banner | Brand Codes | Active Stores | Foot Traffic | Classifier AUC | Depth R2 |
|-------|--------|-------------|---------------|-------------|----------------|----------|
| HOKA | HOKA | HK | 3 | Yes (2) | 0.949 | — |
| BOLD | BOLD | NI,PM,AD,JR,NB,VN,NE,NP,CV,CAH | All | Yes (30) | — | — |
| BAMERS | BAMERS | BM,SK,CR,CAB | 25 | Yes (30) | 0.949 | 0.485 |
| OAKLEY | OAKLEY | OK | 8 | Yes (7) | 0.945 | 0.739 |

## Data
- Source: PostgreSQL (credentials in `.env`, loaded via `python-dotenv`)
- Brand data: `data/raw/{brand}/` (parquet)
- Features: `data/processed/{brand}/` (parquet)
- Models: `models/{brand}/` (pickle + JSON metadata)
- Weekly actions: `weekly_actions/{brand}/` (CSV)
- Calendar is shared: `data/raw/calendar.parquet`

## Key Conventions
- Brand names are UPPERCASE in code, lowercase in directory paths
- All prices in CLP, ending in ,990 (Chilean retail convention)
- Discount ladder: 0% → 15% → 20% → 30% → 40%
- Models trained at parent SKU level (not individual sizes)
- Time-series cross-validation (never random splits)
- GCP project: `ynk-pricing-optimization`, Cloud Run service: `pricing-api`
- Non-retail stores (logistics, digital, internal) are excluded from pricing via `EXCLUDE_STORES_PRICING` in config
- Pricing step picks the most recent week with >= 10 rows (avoids incomplete current-week data)

## Adding a New Brand
1. Add entry to `config/database.py` BRANDS dict (banner name, brand codes, stores)
2. Add any non-retail stores to `EXCLUDE_STORES_PRICING` in config
3. Run `python run_brand.py NEWBRAND`
4. Add brand tab to `dashboard/src/App.jsx` BRANDS array and BRAND_STATS
5. Rebuild dashboard: `cd dashboard && npx vite build --outDir ../api/static --emptyOutDir`

## Weekly Automation
- Cron runs every Monday at 6:00 AM Chile time (09:00 UTC)
- Script: `run_weekly.sh` — extracts, aggregates, trains, generates pricing for all brands, then deploys
- Logs: `logs/` directory

## Missing Data (Waiting on Jacques)
- `ynk.stock` — daily inventory snapshots (blocks weeks-of-cover feature)
- `ynk.costos` — unit costs (blocks margin-based optimization)
- `ynk.precios_ofertas` — pricing history (blocks clean markdown event detection)
- See `DATA_BRIEF.md` Section 4 for the full request with all brands' store lists
