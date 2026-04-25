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
  src/data/extract_brand.py       # PostgreSQL → parquet + costs from sap_s4.costo
  src/data/extract_brand.py       # PostgreSQL → parquet + costs from sap_s4.costo
  src/scraping/                     # Competitor pricing scrapers (per-site adapters, parallel)
    scrape_brand.py                 # Orchestrator: parallel adapters, historical snapshots
    falabella.py                    # Falabella.com (__NEXT_DATA__ JSON extraction)
    brand_sites.py                  # hoka_cl (WooCommerce API), sparta (GraphQL), marathon (SFCC)
    mercadolibre.py                 # MercadoLibre (OAuth2 — search API blocked as of Mar 2026)
    matcher.py                      # EAN11 exact + fuzzy name matching (brand-prefix stripping)
    base.py                         # Abstract scraper with rate limiting, retries, UA rotation
  src/features/
    price_elasticity_brand.py     # Log-log demand elasticity (runs BEFORE features)
    build_features_brand.py       # Base features + official prices + margin targets
    lifecycle_brand.py            # Launch→Growth→Peak→Steady→Decline→Clearance
    size_curve_brand.py           # Size depletion tracking + alerts
    cross_store_alerts_brand.py   # Cross-store price consistency alerts (channel-aware)
    competitor_features.py        # ML features from competitor data (price index, gap, pressure, trends)
    build_enhanced_brand.py       # Merge all → v2 features
    aggregate_parent.py           # Child → parent SKU aggregation (incl margin targets)
  src/strategy/
    competitive_intel.py          # Competitive intelligence engine (movements, opportunities, threats, brief)
  src/models/
    train_brand.py                # XGBoost classifier + LightGBM regressor, margin-optimized targets
    weekly_pricing_brand.py       # Weekly actions (IVA-adjusted margins, cost floor, tiers)

API (Cloud Run, slim image ~50MB):
  Dockerfile                      # API image (no ML deps, no data)
  Dockerfile.pipeline             # Pipeline image (full ML + DB deps)
  run_competitor_scrape.py        # Standalone mid-week scraping + intelligence job
  api/main.py                     # FastAPI (auth, decisions, export, admin, audit, analytics, planner, strategy)
  api/storage.py                  # GCS-backed reads (pricing actions, alerts, metadata, SHAP, elasticity)
  api/pricing_math.py             # Pure-math pricing functions (anchor snapping, velocity estimation)
  api/simulator.py                # Promotion simulator engine (what-if scenarios, optimal discount search)
  config/auth.py                  # Google SSO roles, GCS-backed user config with cache
  config/database.py              # DB config, brand configs, store exclusions
  config/vendor_brands.py         # SKU prefix → vendor brand mapping (Nike, Adidas, etc.)
  config/competitors.py           # Per-brand competitor site config + rate limits
  dashboard/src/App.jsx           # React dashboard (pagination, margin viz, admin panel)
  dashboard/src/AnalyticsDrawer.jsx  # Analytics panel (model, elasticity, lifecycle, impact)
  dashboard/src/StoreSidebar.jsx  # Store/vendor sidebar navigation
  dashboard/src/ManualPriceModal.jsx # Manual price override with impact estimation
  dashboard/src/ChainViewModal.jsx   # Chain-wide view (SKU across all stores)
  dashboard/src/SimulatorModal.jsx # Promotion simulator (what-if scenarios, optimal discount finder)
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
- **Cloud Run Job**: `competitor-scrape` (4 GiB, 2 CPU, 30min timeout) — mid-week scraping + intelligence
- **Cloud Scheduler**: `pricing-pipeline-weekly` (Monday 09:00 UTC / 6am CLT)
- **Cloud Scheduler**: `competitor-scrape-midweek` (Wednesday 09:00 CLT) — competitor scraping + intelligence brief
- **GCS Bucket**: `gs://ynk-pricing-decisions`
- **DB**: PostgreSQL at `190.54.179.91` (public) / `192.168.18.150` (office). Database name: `dwh`. Primary schema: `sap_s4`. Migrated 2026-04-25 from legacy `consultas` / `datawarehouse`.
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
- Pipeline outputs: `weekly_actions/{brand}/`, `alerts/{brand}/`, `models/{brand}/`, `outcomes/{brand}/`, `competitors/{brand}/`, `competitors/{brand}/history/`, `competitors/{brand}/intelligence/`
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
               → scrape_competitors (Falabella, hoka_cl, sparta, marathon)
               → costs from sap_s4.costo
               → costs/official_prices from GCS (HOKA override)
               → elasticity (BEFORE features — needed for margin targets)
               → features (+ official prices + margin targets + weather data)
               → lifecycle / size_curve → enhance (+ competitor features + category interactions) → aggregate
               → cross_store alerts (price consistency across stores)
               → train (margin-optimized for all brands)
               → pricing (IVA-adjusted cost floor, confidence tiers, competitor-aware urgency)
               → sync to GCS
               → API serves from GCS → Dashboard
```

## Step Order (Important)
```
extract → scrape_competitors → elasticity → features → lifecycle → size_curve → enhance → aggregate → cross_store → train → pricing → competitive_intel → outcome → sync
```
Elasticity MUST run before features because `add_margin_targets` reads elasticity data from disk to estimate velocity at different discount levels. On fresh containers, running features first produces low-variance targets (everything clusters at 30-35% discount).
Competitive intel MUST run after pricing because it reads pricing actions + elasticity to generate opportunities/threats with dollar impact estimates.

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
2. **sap_s4.costo** (DB table in `dwh`) — auto-extracted during pipeline. Used for BOLD, BAMERS, OAKLEY, BELSPORT. Replaces the legacy `ti.productos` source (which had mixed CLP/USD currencies and required a 1000× heuristic).

## Database schemas (`dwh`)
PostgreSQL `dwh` database, 13 schemas. Primary source is `sap_s4`. Other populated schemas worth knowing:
- **`sap_s4`** — SAP master data: producto, costo, stock, factura_*, traspaso_*, orden_compra_*, presupuesto, lista_precio, producto_temporada/marca/color/tipo_material, cliente, etc.
- **`auxiliar`** — YNK custom data: `precio_normal` (reference prices per parent SKU), `flujo_tiendas` (hourly foot traffic), `calendario` (with season tags), `mix_tiendas` (planned assortment), `rebates` (supplier rebate contributions per SKU/banner/event), `producto_atributos_custom_join` (ranking_venta, ciclo_vida, coleccion, evento).
- **`marketplace`** — direct API data from external marketplaces: `fala_orders` + `fala_order_items` + `fala_stock_daily` (Falabella), `meli_*` (MercadoLibre — bypasses the 403 we hit on the search API), `ventas_nubimetrics` (competitive intel platform).
- **`multivende`** — Multivende e-commerce platform: master products, prices, variations, marketplace connections.
- **`sap_commerce`** — corporate ecommerce orders: usuario, pedido, pedido_detalle, consignacion, producto.

Empty (provisioned but no data yet): `google`, `pos`, `public`, `reports`, `salesforce`, `sap_ewm`, `wholesale`, `woocommerce`.

Untapped tables that would meaningfully improve the model (not yet integrated):
- `auxiliar.precio_normal` — reference price per parent SKU. Solves the "elasticity conflated with markdown" issue by labeling each transaction as normal vs. discounted.
- `auxiliar.rebates` — supplier rebate contributions during markdown events. True margin = cost − rebate; current floor calculations are conservative.
- `auxiliar.flujo_tiendas` — hourly foot traffic (we use weekly_entries, much coarser).
- `auxiliar.producto_atributos_custom_join` — YNK merchandising tags (ranking, lifecycle, collection).
- `marketplace.fala_*` / `meli_*` — direct marketplace data; could replace the brittle competitor scraping for Falabella + ML.

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
| BOLD | BOLD | 35 | Margin-optimized | 0.648 | sap_s4.costo (5,434 SKUs) |
| BAMERS | BAMERS | 25 | Margin-optimized | 0.938 | sap_s4.costo (2,084 SKUs) |
| OAKLEY | OAKLEY | 8 | Margin-optimized | 0.800 | sap_s4.costo (2,211 SKUs) |
| BELSPORT | BELSPORT | 66 | Margin-optimized | 0.538 (holdout 0.704) | sap_s4.costo (6,422 SKUs) |

## Adding a New Brand
1. Add entry to `config/database.py` BRANDS dict (banner, brand codes, stores)
2. Add stock table to STOCK_TABLES (if exists)
3. Add non-retail stores to EXCLUDE_STORES_PRICING
4. Add brand tab to `dashboard/src/App.jsx` BRANDS, BRAND_STATS, ALL_BRANDS
5. Add brand to `run_pipeline_job.py` BRANDS default list
6. Run pipeline: `gcloud run jobs execute pricing-pipeline --update-env-vars "PIPELINE_BRANDS=NEWBRAND"`
7. Costs auto-extracted from `sap_s4.costo`. For manual override: upload `costs.parquet` to GCS.

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
- `POST /simulate/promotion` — simulate promotional scenario (discount %, duration, filters) → projected impact
- `POST /simulate/optimal-discount` — find the margin-maximizing discount for filtered products
- `GET /alerts/cross-store` — cross-store pricing consistency alerts (parent-grouped, nested stores)
- `GET /strategy/brief/{brand}` — full competitive intelligence brief (position, movements, opportunities, threats)
- `GET /strategy/opportunities/{brand}` — actionable opportunities ranked by estimated margin impact
- `GET /strategy/threats/{brand}` — competitive threats ranked by severity
- `GET /strategy/movements/{brand}` — recent competitor price movements

## Testing
```bash
python3 -m pytest tests/ -v           # 211 tests, <2s
python3 -m pytest tests/ --cov=api    # with coverage
python3 -m pytest tests/ --cov=api    # with coverage
```
Test coverage: pricing_math (97%), vendor_brands (100%), API endpoints, pipeline lift table, role permissions, cross-store alerts, scraping matcher, competitive intelligence (version matching, movements, positions, opportunities, threats, trends).

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

## Competitor Pricing Scraping
- `src/scraping/` package with per-site adapters, product matcher, rate limiting
- Adapters: Falabella (`__NEXT_DATA__`), hoka_cl (WooCommerce API), sparta.cl (GraphQL), marathon.cl (SFCC HTML)
- MercadoLibre search API returns 403 even with valid OAuth2 tokens (Mar 2026) — needs marketplace-type app or Playwright
- Ripley, Paris adapters stubbed for BOLD Phase 2
- `config/competitors.py`: per-brand site list + rate limits
- Product matching: EAN11 exact match → fuzzy name match (`difflib.SequenceMatcher`, threshold 0.85)
- **Gotcha: robots.txt** — most Chilean sites block scraping; public API adapters need `skip_robots = True`
- **Gotcha: internal product names** — have gender prefixes (M/W) and color codes (BFBG) that need stripping for external search
- **Gotcha: WooCommerce search** — case-sensitive, some model+number combos fail; use lowercase + fallback to shorter query
- Competitor features auto-discovered by ML model: `comp_price_index`, `comp_undercut`, `comp_discount_pressure`, etc.
- Competitor features auto-discovered by ML model: `comp_price_index`, `comp_undercut`, `comp_discount_pressure`, etc.
- **Business rule**: competitor cheaper + healthy velocity = informational only (no urgency boost). Only adds urgency when velocity is weak.

## Weather Data Integration
- `src/features/weather_brand.py` — fetches historical weather from Open-Meteo API (free, no key)
- `config/weather.py` — region → coordinates mapping for all Chilean regions with stores
- Features: `avg_temp`, `max_temp`, `min_temp`, `total_rain`, `rain_days`, `temp_deviation`, `is_rainy_week`
- Merged on (centro → region, week) grain via stores.parquet
- Cached locally as `data/raw/weather_{region}.parquet` to avoid re-fetching
- Integrated in `build_features_brand.py` after seasonality, before foot traffic

## Category Interaction Features
- `src/features/category_interactions.py` — interaction terms between product category and other dimensions
- Features: `cat_x_lifecycle`, `cat_x_season`, `cat_x_velocity`, `cat_x_age` (all integer-encoded categoricals)
- Lets single model learn "Footwear in decline behaves differently than Apparel in decline"
- Auto-discovered by ML model (not in EXCLUDE_COLS); SHAP shows which are predictive
- Preferred over separate per-category models (Apparel/Equipment have too few rows for reliable models)

## Competitive Intelligence Engine
- `src/strategy/competitive_intel.py` — generates weekly intelligence brief per brand
- **Price movements**: detects drops, raises, promo starts/ends, new listings, delistings between weeks
- **Position map**: classifies each SKU as leader/parity/follower based on our price vs competitor min
- **Opportunities**: margin capture (competitor raised price), price leader (inelastic + cheapest), competitor OOS
- **Threats**: undercut on elastic SKU with weak velocity (critical), coordinated drops (market shift)
- **Dollar impact**: estimated weekly margin gain/risk using elasticity + velocity + IVA-adjusted margin math
- **Trend features for ML**: `comp_price_delta_1w/4w`, `comp_discount_trend_4w`, `comp_promo_active/weeks`
- Output: `data/processed/{brand}/competitive_brief.json` + `competitor_trend_features.parquet`
- GCS: `competitors/{brand}/intelligence/competitive_brief.json`
- API: `GET /strategy/brief/{brand}`, `/strategy/opportunities/{brand}`, `/strategy/threats/{brand}`, `/strategy/movements/{brand}`
- Historical data: `competitors/{brand}/history/competitor_prices_{YYYY-WW}.parquet` — one snapshot per week
- **Mid-week scraping**: `run_competitor_scrape.py` — standalone job (scrape + intelligence + GCS sync)
- **Parallel scraping**: adapters run concurrently via ThreadPoolExecutor (4 workers max)
- **Version-aware matching**: "Clifton 9" vs "Clifton 8" penalized below match threshold (prevents false positives on model transitions)

## Cross-Store Pricing Consistency Alerts
- `src/features/cross_store_alerts_brand.py` — detects inconsistencies across stores for same parent SKU
- Channel-aware: `is_ecomm_store()` from `config/vendor_brands.py` (AB* prefix = ecomm)
- Alert types: `price_inconsistency_bm` (>10% B&M spread), `discount_spread` (>10pp), `markdown_split`, `stock_imbalance`, `ecomm_gap` (>15%)
- Velocity-weighted sync price recommendation per parent SKU
- Latest week only (avoids BELSPORT bloat)
- API: `GET /alerts/cross-store?brand=X` — parent-grouped with nested stores array
- Dashboard: alert cards with price spread, per-store prices, reason badges

## Click & Collect Handling
- "Retiro en Tienda" = online order picked up at physical store. BOLD: 67% of online demand is C&C.
- **Velocity**: all channels (retail + C&C) — a markdown affects both (ecomm ≈ store price 54% of time)
- **Price features**: retail-only transactions — prevents ecomm discount codes (15% first purchase, promos) from contaminating store price signal. 30% of mixed SKU-store-weeks had >5% price contamination before fix.
- **Pure C&C stores excluded**: BOLD 2019 (99%), BAMERS B609 (100%), BEC2 (98%) — in `EXCLUDE_STORES_PRICING`
- **Ecomm as channel overlay**: each action row shows online price + gap %. True ecomm velocity = delivery + ALL C&C across stores.
- **Dashboard**: C&C % badge on velocity, store sidebar tags, chain view groups ecomm first then B&M
- C&C columns (`click_collect_units`, `instore_units`, `instore_velocity_4w`, `click_collect_ratio`) excluded from model training (in `EXCLUDE_COLS` in both `train_brand.py` and `weekly_pricing_brand.py`)

## Known Issues
- Elasticity estimates conflated with markdown effects — `auxiliar.precio_normal` is now available in `dwh`; integrate into `price_elasticity_brand.py` to exclude markdown weeks from the regression
- Belsport has no stock table yet (`stock_belsport` doesn't exist) — uses sales proxy for size curve
- BELSPORT regressor R2 (CV=0.538) improved significantly but holdout (0.704) suggests more room
- Size curve alerts filtered to latest week only (BELSPORT was generating 3.4M rows across all weeks)
- Vendor brand prefixes verified from production data; OAKLEY "Other" = optical services (SERV/SFSS), expected
- Lifecycle thresholds validated: velocity monotonically decreases peak→steady→decline across all brands
- `api/static/` is gitignored — must run `cp -r dashboard/dist api/static` before API deploy
- MercadoLibre search API locked down (403) — seller-type OAuth2 tokens lack search scope
- Ripley blocked by CloudFlare (403) — needs ZenRows proxy or Playwright
- Marathon adapter: uses JSON-LD from product pages (primary tile regex removed); discount data depends on `highPrice` field
- Falabella stock signal: now returns None (unknown) when variant data unavailable — `comp_in_stock_count` no longer inflated
