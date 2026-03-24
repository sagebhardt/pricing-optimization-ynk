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
gcloud run deploy pricing-api --image us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-api --region us-central1 --project ynk-pricing-optimization --platform managed --memory 2Gi --cpu 2 --allow-unauthenticated --set-env-vars "PYTHONPATH=/app,GCS_BUCKET=ynk-pricing-decisions,GOOGLE_CLIENT_ID=467343668842-b1imqgobg3l6v6670tnir2nsis5pv56v.apps.googleusercontent.com"
```

## Architecture
```
run_brand.py                    # Pipeline orchestrator (per brand)
run_weekly.sh                   # Weekly automation (extract + features + enhance + aggregate + train + pricing + deploy)
src/data/extract_brand.py       # PostgreSQL → parquet
src/features/
  build_features_brand.py       # Base features (child SKU level, includes stock)
  price_elasticity_brand.py     # Log-log demand elasticity
  lifecycle_brand.py            # Launch→Growth→Peak→Steady→Decline→Clearance
  size_curve_brand.py           # Size depletion tracking + alerts
  build_enhanced_brand.py       # Merge all → v2 features
  aggregate_parent.py           # Child → parent SKU aggregation
src/models/
  train_brand.py                # XGBoost training with time-series CV
  weekly_pricing_brand.py       # Weekly action list generator (confidence tiers, stock-aware urgency)
  first_markdown.py             # Prescriptive first-markdown timing model
api/
  main.py                       # FastAPI (auth middleware, decisions, export, admin, audit, feedback)
  storage.py                    # GCS-backed persistence (decisions, audit, feedback, user config)
config/
  database.py                   # DB config (credentials via .env), brand configs, store exclusions
  auth.py                       # Google SSO roles (admin/brand_manager/viewer), bootstrap admin
dashboard/src/App.jsx           # React dashboard (login, approve/reject, export, admin panel, audit log)
```

## Authentication & Roles
- Google SSO via OAuth2 (Google Identity Services)
- `GOOGLE_CLIENT_ID` env var on Cloud Run — if empty, auth is disabled (dev mode)
- Bootstrap admin (hardcoded, can never be locked out): `sgr@ynk.cl`
- Roles managed via admin panel in dashboard (gear icon) — stored in GCS, no deploys needed
  - **admin**: full access to all brands + user management
  - **brand_manager**: approve/reject/export for assigned brands only
  - **viewer**: read-only (default for any @yaneken.cl or @ynk.cl email)
- Config: `config/auth.py` (bootstrap), GCS `config/users.json` (dynamic)

## Persistence & Storage
- GCS bucket: `gs://ynk-pricing-decisions` (set via `GCS_BUCKET` env var)
- Decisions: `decisions/{brand}/decisions_{week}.json`
- Audit log: `audit/{brand}/{YYYY-MM}.jsonl`
- Exports: `exports/{brand}/cambios_precio_{brand}_{week}.xlsx`
- Feedback: `feedback/{brand}/feedback_{week}.json`
- User config: `config/users.json`
- Local fallback: `decisions/`, `audit/`, `exports/`, `feedback/` directories (dev mode)

## Weekly Workflow
1. **Monday 6am**: `run_weekly.sh` runs → extract + features + enhance + aggregate + train + pricing
2. **Monday**: BU managers open dashboard, review actions with confidence tiers
3. **Manager reviews**: Approve/reject per item (persisted in GCS, survives reloads)
4. **Export**: Click "Exportar" → confirmation dialog → Excel download or clipboard text
5. **Ops**: Receives formatted price change file, implements in POS
6. **Feedback**: Ops reports implementation status via `/feedback` API

## Confidence Tiers
Each recommendation includes a `confidence_tier`:
- **HIGH**: Strong classifier confidence + elasticity data + reliable velocity
- **MEDIUM**: Decent signals, some data gaps
- **LOW**: Weak signals, high uncertainty
- **SPECULATIVE**: Price increases without elasticity (velocity is estimated)

## Active Brands
| Brand | Banner | Brand Codes | Active Stores | Foot Traffic | Classifier AUC | Depth R2 |
|-------|--------|-------------|---------------|-------------|----------------|----------|
| HOKA | HOKA | HK | 3 | Yes (2) | 0.949 | — |
| BOLD | BOLD | NI,PM,AD,JR,NB,VN,NE,NP,CV,CAH | All | Yes (30) | 0.910 | 0.566 |
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
- All prices in CLP, snapped to cognitive price anchors (e.g. 9,990 / 14,990 / 19,990 / 24,990 / 29,990 / 39,990 / 49,990 / 59,990 / 69,990 / 79,990 / 99,990) — see `PRICE_ANCHORS` in `weekly_pricing_brand.py`
- Discount ladder: 0% → 15% → 20% → 30% → 40%
- Models trained at parent SKU level (not individual sizes)
- Time-series cross-validation (never random splits)
- GCP project: `ynk-pricing-optimization`, Cloud Run service: `pricing-api`
- Non-retail stores (logistics, digital, internal) are excluded from pricing via `EXCLUDE_STORES_PRICING` in config
- Pricing step picks the most recent week with >= 10 rows (avoids incomplete current-week data)
- Revenue deltas use anchor-snapped prices (consistent with what managers see)

## Adding a New Brand
1. Add entry to `config/database.py` BRANDS dict (banner name, brand codes, stores)
2. Add any non-retail stores to `EXCLUDE_STORES_PRICING` in config
3. Run `python run_brand.py NEWBRAND`
4. Add brand tab to `dashboard/src/App.jsx` BRANDS array and BRAND_STATS
5. Rebuild dashboard: `cd dashboard && npx vite build --outDir ../api/static --emptyOutDir`

## Weekly Automation
- Cron runs every Monday at 6:00 AM Chile time (09:00 UTC)
- Script: `run_weekly.sh` — extracts, builds features, enhances, aggregates, trains, generates pricing, then deploys
- Steps per brand: `extract features enhance aggregate train pricing`
- Logs: `logs/` directory
- Deploys with `GCS_BUCKET` and `GOOGLE_CLIENT_ID` env vars

## Stock Data
- Stock tables live in `consultas.public.stock_{brand}` (e.g. `stock_bold`, `stock_hoka`, `stock_bamers`, `stock_oakley`)
- Extracted to `data/raw/{brand}/stock.parquet`
- BOLD: 63.7M rows (Jan 2024 – Mar 2026)
- When extracting large stock tables, use monthly batch queries to avoid timeouts
- Stock coverage in pricing output: HOKA 97%, BOLD 96%, BAMERS 94%, OAKLEY 31%

## Known Model Issues (Audit March 2026)
- Top classifier features are discount history (circular: "discounted products stay discounted"). Real predictive power lower than AUC suggests. Future fix: retrain with demand-only features.
- Price increases without elasticity use -25% volume loss estimate (flagged as SPECULATIVE).
- Elasticity estimates conflated with markdown effects — consider excluding markdown periods from estimation.
- No holdout test set — final model trains on all data including validation folds.

## Missing Data (Waiting on Jacques)
- `ynk.costos` — unit costs (blocks margin-based optimization)
- `ynk.precios_ofertas` — pricing history (blocks clean markdown event detection)
- See `DATA_BRIEF.md` Section 4 for the full request with all brands' store lists
