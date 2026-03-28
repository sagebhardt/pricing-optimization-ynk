# CLAUDE.md — YNK Pricing Optimization

## Project Overview
ML-driven margin and pricing optimization for Yaneken Retail Group. Predicts optimal discount depth per parent SKU per store per week to maximize gross profit. All 5 brands are live with margin-optimized models: HOKA, BOLD, BAMERS, OAKLEY, and BELSPORT.

## Quick Commands
```bash
# Run full pipeline for a brand (locally)
python run_brand.py HOKA
python run_brand.py BOLD
python run_brand.py BELSPORT

# Run specific steps
python run_brand.py HOKA --steps elasticity features enhance aggregate train pricing sync

# Run pipeline in the cloud (no laptop needed)
gcloud run jobs execute pricing-pipeline --region us-central1 --project ynk-pricing-optimization
# Single brand:
gcloud run jobs execute pricing-pipeline --region us-central1 --project ynk-pricing-optimization --update-env-vars "PIPELINE_BRANDS=HOKA"

# Start API locally
python3 -m uvicorn api.main:app --port 8080

# Start dashboard dev server
cd dashboard && npm run dev

# Build + deploy (uses scripts/build.sh — safe swap with trap cleanup)
./scripts/build.sh api --deploy        # API image + deploy to Cloud Run
./scripts/build.sh pipeline --deploy   # Pipeline image + update Cloud Run Job
./scripts/build.sh api                 # Build only (no deploy)
./scripts/build.sh pipeline            # Build only (no deploy)
```

## Architecture
```
Pipeline (runs in Cloud Run Job, Monday 6am CLT):
  run_brand.py                    # Pipeline orchestrator (per brand)
  run_pipeline_job.py             # Cloud Run Job entrypoint (subprocess per brand)
  src/data/extract_brand.py       # PostgreSQL → parquet + costs from ti.productos
  src/features/
    price_elasticity_brand.py     # Log-log demand elasticity (runs BEFORE features)
    build_features_brand.py       # Base features + official prices + margin targets
    lifecycle_brand.py            # Launch→Growth→Peak→Steady→Decline→Clearance
    size_curve_brand.py           # Size depletion tracking + alerts
    build_enhanced_brand.py       # Merge all → v2 features
    aggregate_parent.py           # Child → parent SKU aggregation (incl margin targets)
  src/models/
    train_brand.py                # XGBoost with margin-optimized targets
    weekly_pricing_brand.py       # Weekly actions (IVA-adjusted margins, cost floor, tiers)

API (Cloud Run, slim image ~50MB):
  Dockerfile                      # API image (no ML deps, no data)
  Dockerfile.pipeline             # Pipeline image (full ML + DB deps)
  api/main.py                     # FastAPI (auth, decisions, export, admin, audit, feedback)
  api/storage.py                  # GCS-backed reads (pricing actions, alerts, metadata)
  config/auth.py                  # Google SSO roles, GCS-backed user config with cache
  config/database.py              # DB config, brand configs, store exclusions
  dashboard/src/App.jsx           # React dashboard (pagination, margin viz, admin panel)
```

## Two Docker Images
- **`pricing-api`** — slim API image (~50MB). No xgboost/shap/sklearn. Reads data from GCS.
- **`pricing-pipeline`** — full pipeline image. Has ML deps + psycopg2. Runs as Cloud Run Job.
- `.dockerignore` is for the API. `.dockerignore.pipeline` is for the pipeline.
- `.gcloudignore` excludes `/data/`, `/models/` etc. — build context ~1 MiB.
- Use `./scripts/build.sh pipeline` — handles the swap safely with `trap` cleanup.

## Cloud Infrastructure
- **Cloud Run Service**: `pricing-api` (1 GiB, 1 CPU, min-instances=1)
- **Cloud Run Job**: `pricing-pipeline` (32 GiB, 8 CPU, 2hr timeout)
- **Cloud Scheduler**: `pricing-pipeline-weekly` (Monday 09:00 UTC / 6am CLT)
- **GCS Bucket**: `gs://ynk-pricing-decisions`
- **DB**: PostgreSQL at `190.54.179.91` (public) / `192.168.18.150` (office)
- **GCP Project**: `ynk-pricing-optimization`
- **Estimated cost**: ~$9/month total (API ~$5 with min-instances=0, pipeline ~$3, GCS negligible)

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
- Supplemental data: `data/raw/{brand}/costs.parquet`, `official_prices.parquet`
- Decisions: `decisions/{brand}/decisions_{week}.json`
- Audit log: `audit/{brand}/{YYYY-MM}.jsonl`
- Exports: `exports/{brand}/cambios_precio_{brand}_{week}.xlsx`
- Feedback: `feedback/{brand}/feedback_{week}.json`
- User config: `config/users.json`
- API reads from GCS with 5-minute in-memory cache. Local file fallback for dev.

## Data Flow
```
DB (PostgreSQL) → extract → parquet (local)
               → costs from ti.productos (auto USD→CLP conversion)
               → costs/official_prices from GCS (HOKA override)
               → elasticity (BEFORE features — needed for margin targets)
               → features (+ official prices + margin targets using elasticity)
               → lifecycle / size_curve → enhance → aggregate
               → train (margin-optimized for all brands)
               → pricing (IVA-adjusted cost floor, confidence tiers, margin columns)
               → sync to GCS
               → API serves from GCS → Dashboard
```

## Step Order (Important)
```
extract → elasticity → features → lifecycle → size_curve → enhance → aggregate → train → pricing → sync
```
Elasticity MUST run before features because `add_margin_targets` reads elasticity data from disk to estimate velocity at different discount levels. On fresh containers, running features first produces low-variance targets (everything clusters at 30-35% discount).

## Training Mode
All brands use margin-optimized training:
- Classifier: "Should this product be repriced?" (prescriptive)
- Regressor: "What discount maximizes weekly gross profit?" (optimal)
- Targets computed by simulating profit at each of 9 discount steps (0-40%, 5pp increments)
- Margin calculations strip IVA (19%) — `price_neto = price / 1.19` before subtracting cost
- **Holdout evaluation**: last 4 weeks reserved for true out-of-time test (reported in `training_metadata.json`)
- **Brand-specific tuning**: BELSPORT uses deeper trees + aggressive subsampling (see `BRAND_*_OVERRIDES` in `train_brand.py`)
- Early stopping (AUC for classifier, RMSE for regressor) used during CV and holdout eval; final production models train for the full `n_estimators`

## Cost Data Sources
Costs are loaded in order of precedence:
1. **GCS** (`gs://ynk-pricing-decisions/data/raw/{brand}/costs.parquet`) — manually uploaded, highest quality. Used for HOKA.
2. **ti.productos** (DB table) — auto-extracted during pipeline. Costs < 500 are treated as USD and multiplied by 1,000 (calibrated against known HOKA costs, median rate 1,013 CLP/USD). Used for BOLD, BAMERS, OAKLEY, BELSPORT.

## Margin-Aware Pricing
- All margin calculations strip IVA (19%): `margin = price/1.19 - cost`
- Never recommends below cost (steps back to shallowest profitable discount)
- **Minimum margin floor (15%)**: steps back to shallower discount if recommended margin < 15%
- Flags thin margins (<20%) in reasons (both decrease and increase paths)
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
| Brand | Banner | Stores | Training Mode | Regressor R2 | Cost Source |
|-------|--------|--------|---------------|-------------|-------------|
| HOKA | HOKA | 3 | Margin-optimized | 0.781 | GCS (376 SKUs) |
| BOLD | BOLD | 35 | Margin-optimized | 0.648 | ti.productos (5,434 SKUs) |
| BAMERS | BAMERS | 25 | Margin-optimized | 0.938 | ti.productos (2,084 SKUs) |
| OAKLEY | OAKLEY | 8 | Margin-optimized | 0.800 | ti.productos (2,211 SKUs) |
| BELSPORT | BELSPORT | 66 | Margin-optimized | 0.442 | ti.productos (6,422 SKUs) |

## Adding a New Brand
1. Add entry to `config/database.py` BRANDS dict (banner, brand codes, stores)
2. Add stock table to STOCK_TABLES (if exists)
3. Add non-retail stores to EXCLUDE_STORES_PRICING
4. Add brand tab to `dashboard/src/App.jsx` BRANDS, BRAND_STATS, ALL_BRANDS
5. Add brand to `run_pipeline_job.py` BRANDS default list
6. Run pipeline: `gcloud run jobs execute pricing-pipeline --update-env-vars "PIPELINE_BRANDS=NEWBRAND"`
7. Costs auto-extracted from `ti.productos`. For manual override: upload `costs.parquet` to GCS.

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

## Pipeline Performance
Each brand runs as a subprocess (memory fully reclaimed between brands). Stock extraction limited to last 16 weeks. Key optimizations:
- Elasticity: numpy arrays + `np.linalg.lstsq` instead of per-SKU `pd.get_dummies` + sklearn
- Lifecycle: per-group processing with `np.select` (constant memory, no `iterrows`)
- Size curve: vectorized for both stock-based and sales-proxy paths
- Build context: `.gcloudignore` with root-anchored paths — 1 MiB vs 1 GiB
- Library versions pinned in `requirements.txt` for reproducible training

| Brand | Pipeline Time |
|-------|--------------|
| HOKA | ~2 min |
| BOLD | ~35 min |
| BAMERS | ~12 min |
| OAKLEY | ~4 min |
| BELSPORT | ~42 min |

## Known Issues
- Elasticity estimates conflated with markdown effects — consider excluding markdown periods
- Belsport has no stock table yet (`stock_belsport` doesn't exist) — uses sales proxy for size curve
- `ynk.precios_ofertas` still missing — blocks clean markdown event detection
- ti.productos costs have mixed currencies (USD/CLP) — using 1000x heuristic for conversion
- BELSPORT regressor R2=0.442 is weak — 66 heterogeneous stores with no stock data create noise
- Size curve alerts filtered to latest week only (BELSPORT was generating 3.4M rows across all weeks)
