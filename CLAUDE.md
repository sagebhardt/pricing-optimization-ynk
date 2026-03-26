# CLAUDE.md — YNK Pricing Optimization

## Project Overview
ML-driven margin and pricing optimization for Yaneken Retail Group. Predicts optimal discount depth per parent SKU per store per week to maximize gross profit. Currently live for HOKA, BOLD, BAMERS, OAKLEY, and BELSPORT (pending first run).

## Quick Commands
```bash
# Run full pipeline for a brand (locally)
python run_brand.py HOKA
python run_brand.py BOLD
python run_brand.py BELSPORT

# Run specific steps
python run_brand.py HOKA --steps features enhance aggregate train pricing sync

# Run pipeline in the cloud (no laptop needed)
gcloud run jobs execute pricing-pipeline --region us-central1 --project ynk-pricing-optimization
# Single brand:
gcloud run jobs execute pricing-pipeline --region us-central1 --project ynk-pricing-optimization --update-env-vars "PIPELINE_BRANDS=HOKA"

# Start API locally
python3 -m uvicorn api.main:app --port 8080

# Start dashboard dev server
cd dashboard && npm run dev

# Deploy API (slim image, ~2 min build)
gcloud builds submit . --tag us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-api --project ynk-pricing-optimization
gcloud run deploy pricing-api --image us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-api --region us-central1 --project ynk-pricing-optimization --platform managed --memory 512Mi --cpu 1 --min-instances 1 --allow-unauthenticated --set-env-vars "PYTHONPATH=/app,GCS_BUCKET=ynk-pricing-decisions,GOOGLE_CLIENT_ID=467343668842-b1imqgobg3l6v6670tnir2nsis5pv56v.apps.googleusercontent.com"

# Deploy pipeline image (for Cloud Run Jobs)
# Must swap Dockerfile/dockerignore temporarily:
cp .dockerignore .dockerignore.api && cp .dockerignore.pipeline .dockerignore && cp Dockerfile Dockerfile.api && cp Dockerfile.pipeline Dockerfile
gcloud builds submit . --tag us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-pipeline --project ynk-pricing-optimization
cp Dockerfile.api Dockerfile && cp .dockerignore.api .dockerignore && rm Dockerfile.api .dockerignore.api
gcloud run jobs update pricing-pipeline --image us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-pipeline --region us-central1 --project ynk-pricing-optimization
```

## Architecture
```
Pipeline (runs in Cloud Run Job, Monday 6am CLT):
  run_brand.py                    # Pipeline orchestrator (per brand)
  run_pipeline_job.py             # Cloud Run Job entrypoint (all brands)
  run_weekly.sh                   # Legacy local automation (replaced by Job)
  src/data/extract_brand.py       # PostgreSQL → parquet (graceful stock table handling)
  src/features/
    build_features_brand.py       # Base features + official prices + margin targets
    price_elasticity_brand.py     # Log-log demand elasticity
    lifecycle_brand.py            # Launch→Growth→Peak→Steady→Decline→Clearance
    size_curve_brand.py           # Size depletion tracking + alerts
    build_enhanced_brand.py       # Merge all → v2 features
    aggregate_parent.py           # Child → parent SKU aggregation (incl margin targets)
  src/models/
    train_brand.py                # XGBoost with margin-optimized or revenue-based targets
    weekly_pricing_brand.py       # Weekly actions (margin-aware, cost floor, confidence tiers)

API (Cloud Run, slim image ~50MB):
  Dockerfile                      # API image (no ML deps, no data)
  Dockerfile.pipeline             # Pipeline image (full ML + DB deps)
  api/main.py                     # FastAPI (auth, decisions, export, admin, audit, feedback)
  api/storage.py                  # GCS-backed reads (pricing actions, alerts, metadata) + persistence
  config/auth.py                  # Google SSO roles, GCS-backed user config with cache
  config/database.py              # DB config, brand configs, store exclusions
  dashboard/src/App.jsx           # React dashboard (pagination, margin viz, admin panel)
```

## Two Docker Images
- **`pricing-api`** — slim API image (~50MB). No xgboost/shap/sklearn. Reads data from GCS.
- **`pricing-pipeline`** — full pipeline image. Has ML deps + psycopg2. Runs as Cloud Run Job.
- `.dockerignore` is for the API. `.dockerignore.pipeline` is for the pipeline.
- **IMPORTANT**: when building the pipeline image, swap Dockerfiles temporarily (see Quick Commands).

## Cloud Infrastructure
- **Cloud Run Service**: `pricing-api` (512 MiB, 1 CPU, min-instances=1)
- **Cloud Run Job**: `pricing-pipeline` (32 GiB, 8 CPU, 1hr timeout)
- **Cloud Scheduler**: `pricing-pipeline-weekly` (Monday 09:00 UTC / 6am CLT)
- **GCS Bucket**: `gs://ynk-pricing-decisions`
- **DB**: PostgreSQL at `190.54.179.91` (public) / `192.168.18.150` (office)
- **GCP Project**: `ynk-pricing-optimization`
- **Estimated cost**: ~$3-5/month total

## Authentication & Roles
- Google SSO via OAuth2 (Google Identity Services)
- `GOOGLE_CLIENT_ID` env var — if empty, auth disabled (dev mode)
- Bootstrap admin (hardcoded, can never be locked out): `sgr@ynk.cl`
- Roles managed via admin panel in dashboard (gear icon) — stored in GCS
  - **admin**: full access to all brands + user management
  - **brand_manager**: approve/reject/export for assigned brands only
  - **viewer**: read-only (default for any @yaneken.cl or @ynk.cl email)
- Brand-level enforcement: brand managers can only access their assigned brands

## Persistence & Storage (GCS)
- Pipeline outputs: `weekly_actions/{brand}/`, `alerts/{brand}/`, `models/{brand}/`
- Decisions: `decisions/{brand}/decisions_{week}.json`
- Audit log: `audit/{brand}/{YYYY-MM}.jsonl`
- Exports: `exports/{brand}/cambios_precio_{brand}_{week}.xlsx`
- Feedback: `feedback/{brand}/feedback_{week}.json`
- User config: `config/users.json`
- API reads from GCS with 5-minute in-memory cache. Local file fallback for dev.

## Data Flow
```
DB (PostgreSQL) → extract → parquet (local)
                → features (+ official prices + costs) → parquet
                → elasticity / lifecycle / size_curve → enhance → aggregate
                → train (margin-optimized if costs available)
                → pricing (cost floor, confidence tiers, margin columns)
                → sync to GCS
                → API serves from GCS → Dashboard
```

## Training Modes
The model auto-detects which mode to use based on available data:

### Margin-Optimized (when `costs.parquet` exists)
- Classifier: "Should this product be repriced?" (prescriptive)
- Regressor: "What discount maximizes weekly gross profit?" (optimal)
- Targets computed by simulating profit at each discount step
- HOKA: AUC 0.978, +$23.8M margin/week

### Revenue-Based (legacy, when no costs)
- Classifier: "Will this product markdown in 4 weeks?" (descriptive)
- Regressor: "What discount depth will be applied?" (predictive)
- BOLD/BAMERS/OAKLEY use this until costs are provided

## Margin-Aware Pricing
When costs available:
- Never recommends below cost (steps back to shallowest profitable discount)
- Flags thin margins (<20%) in reasons
- Premium pricing above list price allowed when velocity >= 2 u/w (flagged SPECULATIVE)
- Dashboard shows: margin %, margin delta/week, color-coded (green >40%, amber 20-40%, red <20%)
- KPI bar shows revenue AND margin impact side by side

## Official Price Lists
Drop `data/raw/{brand}/official_prices.parquet` (columns: `sku`, `list_price`) to override transaction-derived prices. Prefix matching handles parent-level SKUs automatically (longest match wins). Currently active for HOKA (388 SKUs, 88% coverage).

## Confidence Tiers
- **HIGH**: Strong classifier confidence + elasticity data + reliable velocity
- **MEDIUM**: Decent signals, some data gaps
- **LOW**: Weak signals, high uncertainty
- **SPECULATIVE**: Price increases without elasticity, or premium pricing above list

## Active Brands
| Brand | Banner | Stores | Training Mode | Classifier AUC | Cost Data |
|-------|--------|--------|---------------|----------------|-----------|
| HOKA | HOKA | 3 | Margin-optimized | 0.978 | Yes (376 SKUs) |
| BOLD | BOLD | 35 | Revenue-based | 0.910 | No |
| BAMERS | BAMERS | 25 | Revenue-based | 0.949 | No |
| OAKLEY | OAKLEY | 8 | Revenue-based | 0.945 | No |
| BELSPORT | BELSPORT | 66 | Pending first run | — | No |

## Adding a New Brand
1. Add entry to `config/database.py` BRANDS dict (banner, brand codes, stores)
2. Add stock table to STOCK_TABLES (if exists)
3. Add non-retail stores to EXCLUDE_STORES_PRICING
4. Add brand tab to `dashboard/src/App.jsx` BRANDS, BRAND_STATS, ALL_BRANDS
5. Run pipeline: `gcloud run jobs execute pricing-pipeline --update-env-vars "PIPELINE_BRANDS=NEWBRAND"`
6. Optional: add `costs.parquet` and `official_prices.parquet` to `data/raw/{brand}/`

## Adding Cost Data (Enables Margin Optimization)
1. Get cost list (Excel with SKU + unit cost)
2. Save as `data/raw/{brand}/costs.parquet` (columns: `sku`, `cost`)
3. Re-run pipeline — model auto-switches to margin-optimized training
4. Dashboard will show margin columns automatically

## Dashboard UX
- Pagination: 50 items/page with top + bottom navigation
- Status filter: all / pending / approved / rejected
- Sort: urgency, revenue impact, confidence, store
- Bulk actions: show pending count, confirm on 100+ items
- Freshness banner: shows data week + undecided count
- Export: confirmation dialog with summary before download
- Error toasts: visible feedback on save/export failures
- Admin panel: manage users/roles without deploys (gear icon)
- Audit log: tracks all approve/reject/export actions

## Known Issues
- Regressor R2 lower for margin-optimized targets (0.236 vs 0.363) — harder target, needs more data
- Elasticity estimates conflated with markdown effects — consider excluding markdown periods
- No holdout test set — final model trains on all data including validation folds
- Belsport needs 32 GiB RAM for pipeline (9.2M transactions)
- Belsport has no stock table yet (`stock_belsport` doesn't exist)
- `ynk.precios_ofertas` still missing — blocks clean markdown event detection
