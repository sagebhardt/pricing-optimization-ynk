# CLAUDE.md — YNK Pricing Optimization

## Project Overview
ML-driven markdown and pricing optimization for Yáneken Retail Group. Predicts optimal markdown timing and depth per parent SKU per store per week. Currently live for HOKA and BOLD brands.

## Quick Commands
```bash
# Run full pipeline for a brand
python run_brand.py HOKA
python run_brand.py BOLD

# Run specific steps
python run_brand.py HOKA --steps aggregate train pricing
python run_brand.py BOLD --steps extract features elasticity lifecycle size_curve enhance aggregate train pricing

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
  weekly_pricing_brand.py       # Weekly action list generator
  first_markdown.py             # Prescriptive first-markdown timing model
api/main.py                     # FastAPI (serves dashboard + API)
dashboard/src/App.jsx           # React dashboard
```

## Data
- Source: PostgreSQL at 190.54.179.91:5432, database `consultas`, schema `ventas`
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

## Adding a New Brand
1. Add entry to `config/database.py` BRANDS dict (banner name, brand codes, stores)
2. Run `python run_brand.py NEWBRAND`
3. Add brand tab to `dashboard/src/App.jsx` BRANDS array
4. Rebuild dashboard: `cd dashboard && npx vite build --outDir ../api/static --emptyOutDir`

## Missing Data (Waiting on Jacques)
- `ynk.stock` — daily inventory snapshots (blocks weeks-of-cover feature)
- `ynk.costos` — unit costs (blocks margin-based optimization)
- `ynk.precios_ofertas` — pricing history (blocks clean markdown event detection)
