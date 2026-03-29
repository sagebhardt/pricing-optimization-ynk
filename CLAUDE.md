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

# Run tests
python3 -m pytest tests/ -v

# Build + deploy (uses scripts/build.sh — safe swap with trap cleanup)
cd dashboard && npm run build && cd ..            # Build dashboard
rm -rf api/static && cp -r dashboard/dist api/static  # Copy to API static dir
./scripts/build.sh api --deploy        # API image + deploy to Cloud Run
./scripts/build.sh pipeline --deploy   # Pipeline image + update Cloud Run Job
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
    train_brand.py                # XGBoost classifier + LightGBM regressor, margin-optimized targets
    weekly_pricing_brand.py       # Weekly actions (IVA-adjusted margins, cost floor, tiers)

API (Cloud Run, slim image ~50MB):
  Dockerfile                      # API image (no ML deps, no data)
  Dockerfile.pipeline             # Pipeline image (full ML + DB deps)
  api/main.py                     # FastAPI (auth, decisions, export, admin, audit, analytics, planner)
  api/storage.py                  # GCS-backed reads (pricing actions, alerts, metadata, SHAP, elasticity)
  api/pricing_math.py             # Pure-math pricing functions (anchor snapping, velocity estimation)
  config/auth.py                  # Google SSO roles, GCS-backed user config with cache
  config/database.py              # DB config, brand configs, store exclusions
  config/vendor_brands.py         # SKU prefix → vendor brand mapping (Nike, Adidas, etc.)
  dashboard/src/App.jsx           # React dashboard (pagination, margin viz, admin panel)
  dashboard/src/AnalyticsDrawer.jsx  # Analytics panel (model, elasticity, lifecycle, impact)
  dashboard/src/StoreSidebar.jsx  # Store/vendor sidebar navigation
  dashboard/src/ManualPriceModal.jsx # Manual price override with impact estimation
  dashboard/src/ChainViewModal.jsx   # Chain-wide view (SKU across all stores)
  dashboard/src/PlannerQueue.jsx  # Planner approval queue (two-step workflow)
```

## Two Docker Images
- **`pricing-api`** — slim API image (~50MB). No xgboost/shap/sklearn. Reads data from GCS.
- **`pricing-pipeline`** — full pipeline image. Has ML deps + psycopg2. Runs as Cloud Run Job.
- `.dockerignore` is for the API. `.dockerignore.pipeline` is for the pipeline.
- `.gcloudignore` excludes `/data/`, `/models/` etc. — build context ~1 MiB.
- Use `./scripts/build.sh pipeline` — handles the swap safely with `trap` cleanup.

## Cloud Infrastructure
- **Cloud Run Service**: `pricing-api` (1 GiB, 1 CPU, min-instances=0)
- **Cloud Run Job**: `pricing-pipeline` (16 GiB, 8 CPU, 2hr timeout)
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
  - **admin**: full access to all brands + user management + planner approval
  - **brand_manager**: approve/reject/manual price for assigned brands only
  - **planner**: reviews BM decisions, approves for export. Brand-scoped.
  - **viewer**: read-only (default for any @yaneken.cl or @ynk.cl email)
- Brand-level enforcement: brand managers and planners can only access assigned brands
- **Two-step workflow**: BM decides → Planner approves → Export available
  - `REQUIRE_PLANNER_APPROVAL=true` env var enforces strict mode (only `planner_approved` exports)
  - Default (soft rollout): legacy `approved`/`manual` export directly, new `bm_*` statuses need planner

## Persistence & Storage (GCS)
- Pipeline outputs: `weekly_actions/{brand}/`, `alerts/{brand}/`, `models/{brand}/` (incl. SHAP CSVs, elasticity parquets), `outcomes/{brand}/`
- Supplemental data: `data/raw/{brand}/costs.parquet`, `official_prices.parquet`
- Decisions: `decisions/{brand}/decisions_{week}.json`
- Audit log: `audit/{brand}/{YYYY-MM}.jsonl`
- Exports: `exports/{brand}/cambios_precio_{brand}_{week}.xlsx`
- Feedback: `feedback/{brand}/feedback_{week}.json`
- User config: `config/users.json`
- API reads from GCS with 5-minute in-memory cache (elasticity: 1hr). Local file fallback for dev.
- If API returns empty analytics, check GCS file exists (`gsutil ls gs://ynk-pricing-decisions/models/{brand}/`)
- Force cache clear: `gcloud run services update pricing-api --region us-central1 --update-env-vars "CACHE_BUST=$(date +%s)"`

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
extract → elasticity → features → lifecycle → size_curve → enhance → aggregate → train → pricing → outcome → sync
```
Elasticity MUST run before features because `add_margin_targets` reads elasticity data from disk to estimate velocity at different discount levels. On fresh containers, running features first produces low-variance targets (everything clusters at 30-35% discount).

## Training Mode
All brands use margin-optimized training:
- Classifier (XGBoost): "Should this product be repriced?" (prescriptive)
- Regressor (LightGBM): "What discount maximizes weekly gross profit?" (optimal)
- Targets computed by simulating profit at each of 9 discount steps (0-40%, 5pp increments)
- Margin calculations strip IVA (19%) — `price_neto = price / 1.19` before subtracting cost
- **Holdout evaluation**: last 4 weeks reserved for true out-of-time test (reported in `training_metadata.json`)
- **Brand-specific tuning**: BELSPORT uses deeper trees + aggressive subsampling (see `BRAND_*_OVERRIDES` in `train_brand.py`)
- Early stopping (AUC for classifier, RMSE for regressor) used during CV and holdout eval; final production models train for the full `n_estimators`
- **LightGBM regressor notes**: `eval_metric`/`early_stopping_rounds` go to `.fit()` callbacks (not constructor). Use `lgb.early_stopping()` + `lgb.log_evaluation(-1)`. Add `verbose=-1` to suppress warnings. SHAP `TreeExplainer` works natively.

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
| BELSPORT | BELSPORT | 66 | Margin-optimized | 0.538 (holdout 0.704) | ti.productos (6,422 SKUs) |

## Adding a New Brand
1. Add entry to `config/database.py` BRANDS dict (banner, brand codes, stores)
2. Add stock table to STOCK_TABLES (if exists)
3. Add non-retail stores to EXCLUDE_STORES_PRICING
4. Add brand tab to `dashboard/src/App.jsx` BRANDS, BRAND_STATS, ALL_BRANDS
5. Add brand to `run_pipeline_job.py` BRANDS default list
6. Run pipeline: `gcloud run jobs execute pricing-pipeline --update-env-vars "PIPELINE_BRANDS=NEWBRAND"`
7. Costs auto-extracted from `ti.productos`. For manual override: upload `costs.parquet` to GCS.

## Dashboard UX
- **View modes**: Lista (flat list), Tiendas (store sidebar), Marcas (vendor brand sidebar)
- **Analytics panel**: "Analisis" button opens model health, elasticity, lifecycle, impact sections
- **Manual price**: $ button per action → modal with debounced impact estimation, anchor snapping
- **Chain-wide view**: "Ver en todas las tiendas" link → approve SKU across all/ecomm/B&M stores
- **Planner queue**: "Cola Planner" tab for two-step approval workflow
- Pagination: 50 items/page with top + bottom navigation
- Status filter: all / pending / approved / rejected / manual
- Sort: urgency, revenue impact, confidence, store
- Bulk actions: show pending count, confirm on 100+ items
- Freshness banner: shows data week + undecided count
- Export: confirmation dialog with summary before download (uses manual price when set)
- Error toasts: visible feedback on save/export failures
- Admin panel: manage users/roles (admin, brand_manager, planner, viewer) + brand assignments
- Audit log: tracks all approve/reject/manual/chain/planner/export actions

## Vendor Brand Mapping
Multi-brand banners (BOLD, BAMERS, BELSPORT) carry products from multiple vendors. SKU prefix → vendor:
- `config/vendor_brands.py`: prefix mapping (NI/NP→Nike, AD→Adidas, PM→Puma, JR→Jordan, etc.)
- Longest-prefix-first matching (3-char CAH→Carhartt before 2-char)
- `vendor_brand` column added to pricing actions CSV by pipeline (API backfills for old CSVs)
- Brand-specific overrides: `_BRAND_OVERRIDES` in vendor_brands.py (BELSPORT: LT→Lotto, AL→Alphabet)
- `get_vendor_brand(sku, brand=None)` accepts optional brand for override lookup
- Store channels: AB* prefix = ecomm/logistics, all others = B&M
- Dashboard sidebar: adaptive — shows "Marcas" (vendor) or "Categorías" (subcategory) based on data

## API Endpoints
Key endpoints (all auth-protected except /health):
- `GET /analytics/{brand}` — model health, elasticity, lifecycle, impact, prediction vs actual data
- `GET /analytics/outcomes/{brand}` — per-decision prediction vs actual drill-down
- `POST /estimate-impact` — recalculate velocity/revenue/margin for manual price
- `POST /decisions` — save decision (approve/reject/manual, optional chain_scope)
- `POST /decisions/plan` — planner approves/rejects BM decisions
- `GET /decisions/planner-queue` — items awaiting planner review
- `GET /export/price-changes` — export approved items (respects manual prices + planner status)

## Testing
```bash
python3 -m pytest tests/ -v           # 137 tests, <1s
python3 -m pytest tests/ --cov=api    # with coverage
```
Test coverage: pricing_math (97%), vendor_brands (100%), API endpoints, pipeline lift table, role permissions.

## Deploy Workflow
```bash
# Dashboard must be built before API deploy
cd dashboard && npm run build && cd ..
rm -rf api/static && cp -r dashboard/dist api/static
./scripts/build.sh api --deploy
./scripts/build.sh pipeline --deploy
```
`api/static/` is gitignored — it's a build artifact copied from `dashboard/dist/` before each API deploy.

## Pipeline Performance
Each brand runs as a subprocess (memory fully reclaimed between brands). Stock extraction limited to last 16 weeks. Key optimizations:
- **Margin targets**: vectorized with NumPy broadcasting (was iterrows — 50-100x speedup)
- **Training**: `n_jobs=-1` for parallel XGBoost/LightGBM, 2-fold CV (holdout is the real test)
- **LightGBM regressor**: switched from XGBoost after benchmarking (R² +0.01-0.08 across brands, especially BAMERS +0.078)
- **Lift table**: data-driven from actual transactions (falls back to defaults when insufficient data)
- **Velocity formula**: true price change % (not disc_change approximation)
- Elasticity: numpy arrays + `np.linalg.lstsq` instead of per-SKU `pd.get_dummies` + sklearn
- Lifecycle: per-group `np.select` + sparse group skip (density < 20% or < 8 weeks)
- Size curve: reduced groupbys (4→2 per path) + latest-week-only alerts
- Build context: `.gcloudignore` with root-anchored paths — 1 MiB vs 1 GiB

| Brand | Pipeline Time |
|-------|--------------|
| HOKA | ~2 min |
| BOLD | ~20 min |
| BAMERS | ~8 min |
| OAKLEY | ~3 min |
| BELSPORT | ~58 min |

## Model Comparison & Experimentation
- `scripts/compare_models.py` — benchmarks XGBoost vs LightGBM vs CatBoost vs Random Forest on holdout
  - Usage: `python scripts/compare_models.py HOKA` or `--all`
  - Result: LightGBM wins regressor 3/4 brands; RF loses on all brands even with noisy data
- `scripts/cluster_experiment.py` — store clustering experiment (auto-downloads features from GCS)
  - Usage: `python scripts/cluster_experiment.py BELSPORT --k 2,3,4,5`
  - Tests: cluster-as-feature vs separate-models vs single-model baseline
  - BOLD tested: no improvement (stores too homogeneous). BELSPORT (66 stores) is the real candidate.
- `docs/manual_ynk_pricing.pdf` — 21-page user manual (Spanish), generated by `docs/generate_manual.py`

## Known Issues
- Elasticity estimates conflated with markdown effects — consider excluding markdown periods
- Belsport has no stock table yet (`stock_belsport` doesn't exist) — uses sales proxy for size curve
- `ynk.precios_ofertas` still missing — blocks clean markdown event detection
- ti.productos costs have mixed currencies (USD/CLP) — using 1000x heuristic for conversion
- BELSPORT regressor R2 (CV=0.538) improved significantly but holdout (0.704) suggests more room
- Size curve alerts filtered to latest week only (BELSPORT was generating 3.4M rows across all weeks)
- Vendor brand prefixes verified from production data; OAKLEY "Other" = optical services (SERV/SFSS), expected
- Lifecycle thresholds validated: velocity monotonically decreases peak→steady→decline across all brands
- `api/static/` is gitignored — must run `cp -r dashboard/dist api/static` before API deploy
