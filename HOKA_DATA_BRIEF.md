# HOKA Data Brief — Pricing & Markdown Optimization POC
**Date:** March 22, 2026
**Source:** `consultas` database @ 190.54.179.91:5432

---

## 1. What We Have (Ready to Use)

### 1.1 Transaction Data — `ventas.ventas_por_vendedor`
| Metric | Value |
|--------|-------|
| HOKA rows | 42,884 |
| Date range | Aug 2023 → Mar 2026 (2.5 years) |
| Distinct SKUs | 3,020 |
| Distinct stores | 4 active |
| Total units sold | 37,872 |
| Total revenue | $3.45B CLP (~$3.5M USD) |
| Total discounts | $235M CLP |

**Fields available per transaction:** date, store, SKU, quantity, list price, discount amount, final price, customer ID, seller, document type, channel.

**Quality notes:**
- Top 2 "SKUs" by volume are service items (shipping charges `000000001000000031` = 9,120 units, adjustments `AJUSTE` = 1,312 units). These must be filtered out.
- 2,532 return transactions (negative quantities) — usable for return rate features.
- Some cross-brand contamination: 34 non-HOKA SKUs (Oakley, Saucony, etc.) sold through HOKA stores. Filter by `grupo_articulos = 'HK'`.

### 1.2 Product Master — `ventas.sku_tableau`
- 2,987 HOKA SKUs with: description, category hierarchy, color, size, parent SKU code, EAN.
- Hierarchy: Footwear (2,749 SKUs), Equipment (101), Apparel (139), Eyewear (22).
- Sub-categories: Sneakers (1,615), Running (787), Street (150), Outdoor (57), Trail Running (55).
- **Parent SKU** (`codigo_padre`) groups size variants — critical for size curve analysis and aggregated sell-through.
- **Season/temporada field is almost entirely NULL** (only 7 SKUs have it). Cannot rely on this for seasonality — must derive from first-receipt date or first-sale date.

### 1.3 Store Master — `ventas.sucursales_tableau`
6 HOKA locations in SAP:

| Store | Type | Region | Status |
|-------|------|--------|--------|
| 7501-Hoka Costanera | Monomarca | Santiago (RM) | Active — flagship, 48% of revenue since Oct 2024 |
| 7502-Hoka Marina | Monomarca | Viña del Mar | Active since Oct 2025 — new store |
| 7599-Hoka Eventos | Monomarca | Santiago (RM) | Pop-up/events only (Aug 2025 – Jan 2026) |
| AB75-Centro Logístico Hoka | Monomarca | Santiago (RM) | E-commerce/warehouse — 44% of all-time units |
| 7505-Hoka Showroom | Monomarca | Santiago (RM) | No sales in data |
| 7500-Hoka Virtual | Monomarca | Santiago (RM) | Marked "No usar" |

**For the POC:** Focus on 7501 (Costanera), 7502 (Marina), and AB75 (e-commerce/logistics). Exclude Eventos and inactive stores.

### 1.4 Foot Traffic — `ventas.flujo_tiendas`
- Hoka Costanera: Nov 2024 → Mar 2026 (5,635 hourly records, 172K total entries)
- Hoka Marina: Nov 2025 → Mar 2026 (1,307 hourly records, 22K entries)
- Hourly granularity with entries, exits, average dwell time.
- **Useful for:** conversion rate calculation (units sold / foot traffic), demand signal.

### 1.5 Markdown Contribution 2024 — `ventas.contribucion_mkdown_2024_sku_x_banner`
- 990 HOKA SKUs with markdown contribution value for 2024.
- Total contribution: $265M CLP. Range: -$61K to +$2.7M per SKU.
- **Useful for:** validation baseline — compare model recommendations against historical outcomes.

### 1.6 Calendar — `ventas.calendario`
- Full calendar table with week/month/year mappings, day-of-week, and **last-year / next-year date mapping** (for YoY comparisons).

---

## 2. What We Can Derive (From Existing Data)

| Feature | How to Derive | Confidence |
|---------|---------------|------------|
| **Markdown events** | Detect when `precio_lista` drops or `descuento` appears for a SKU-store. We see clear patterns (e.g., Bondi 8 going from $149,990 → $104,993 → $97,993). | High |
| **Discount depth at sale** | `descuento / (precio_lista × cantidad)` per transaction | High |
| **Sell-through velocity** | Units/week per SKU-store, rolling 7/14/28 day | High |
| **Product age** | `fecha_pos - first_sale_date` per SKU | High |
| **Size curve completeness** | Count available sizes per parent SKU over time (requires inventory) | Needs stock |
| **Price elasticity (rough)** | Compare velocity at different price points for same SKU | Medium — limited by no controlled experiments |
| **Seasonality index** | YoY patterns using 2.5 years of data + calendar table | Medium — HOKA brand is only 2.5 years old in this data |
| **Return rate** | Returns / gross sales per SKU | High |
| **Conversion rate** | Units sold / foot traffic per store-day (Costanera & Marina only) | Medium — only 2 stores have traffic data |

---

## 3. What's Missing (Must Request from Jacques / SAP Team)

### 3.1 CRITICAL — Blocks Model Training

#### A. Daily Inventory/Stock Snapshots
**Tables exist but are empty:** `ynk.inventario`, `ynk.stock`
**What we need:** Daily stock positions per SKU per store (on-hand + in-transit).
**Why critical:** Cannot calculate weeks-of-cover, sell-through rate vs. inventory, or determine if a SKU wasn't selling because it was out of stock vs. low demand. **This is the single biggest data gap.**
**Scope:** HOKA SKUs, all active stores (7501, 7502, AB75), ideally from Jan 2024 onward. Daily granularity preferred; weekly acceptable for POC.

#### B. Cost Data
**Tables exist but are empty:** `ynk.costo`, `ynk.costos`
**What we need:** Unit cost (landed cost / COGS) per SKU, ideally with date to track cost changes.
**Why critical:** The model optimizes **margin**, not revenue. Without cost, we can't calculate margin per unit, and the target variable becomes revenue-based only — significantly weaker.
**Scope:** HOKA SKUs, current and historical cost. Even a single snapshot of current costs would unblock POC work.

#### C. Pricing/Offer History
**Table exists but is empty:** `ynk.precios_ofertas`
**What we need:** History of official price changes and promotional offers per SKU-store with effective dates.
**Why critical:** We can partially derive markdown events from transaction data (when `precio_lista` changes), but this misses: (a) price changes that happened when no sale occurred, (b) planned offers vs. ad-hoc discounts, (c) the **intent** behind the markdown (seasonal clearance vs. promo vs. competitive response).
**Fallback:** We can work around this by inferring from transaction data for the POC, but the official pricing history would make the model significantly more accurate.

### 3.2 IMPORTANT — Improves Model Quality

#### D. Purchase Orders & Receipts
**Tables exist but are empty:** `ynk.ordenes_compra`, `ynk.recepciones_ordenes_compra`
**What we need:** When each SKU was first received at each store. Purchase order quantities and dates.
**Why useful:** Allows calculating true product age (from receipt, not first sale), incoming supply pipeline, and planned replenishment.

#### E. Budget/Plan Data
**Table exists but is empty:** `ventas.presupuesto`
**What we need:** Sales budget/plan per store-category-period.
**Why useful:** Sell-through vs. plan is a strong signal for markdown timing — a SKU at 40% of plan in week 6 needs different treatment than one at 90%.

#### F. Season/Temporada Assignment
**Current state:** `temporada` field in `sku_tableau` is NULL for 99.8% of HOKA SKUs.
**What we need:** Season assignment per SKU (SS25, FW25, etc.) or at least the intended selling period.
**Why useful:** The model needs to know "end of season" to calculate markdown urgency. Without it, we must estimate from first-sale date + category heuristics.

### 3.3 NICE TO HAVE — Phase 2+

| Data | Source | Use |
|------|--------|-----|
| Competitor prices (HOKA.com, Dafiti, etc.) | Web scraping | Competitive positioning features |
| Weather data (Santiago, Viña del Mar) | Weather API | Demand adjustment for outdoor/running shoes |
| Marketing calendar / campaign dates | Commercial team | Explain demand spikes, avoid conflating promo lift with organic demand |
| HOKA brand MAP/discount constraints | Brand agreement | Hard business rules for recommendation filters |

---

## 4. Data Request for Jacques

**Priority 1 (blocks POC start):**
1. **Populate `ynk.stock` or `ynk.inventario`** with daily stock snapshots for HOKA stores (7501, 7502, AB75). Ideally Jan 2024 → present. Minimum fields: `fecha`, `tienda_id`, `sku_id`, `stock`, `transit`.
2. **Populate `ynk.costos`** with HOKA unit costs. Minimum fields: `sku_id`, `costo`, `fecha`. Even a single current-cost snapshot is enough to start.
3. **Populate `ynk.precios_ofertas`** with HOKA pricing history. Minimum fields: `tienda_id`, `sku_id`, `precio_normal`, `precio_oferta`, `fecha_desde`, `fecha_hasta`.

**Priority 2 (improves POC quality):**
4. Fill `temporada` field in product master for HOKA SKUs, or provide a mapping file.
5. Populate `ynk.ordenes_compra` and `ynk.recepciones_ordenes_compra` for HOKA.

---

## 5. What We Can Start Building Now (Without Missing Data)

Even before the data gaps are filled, we can begin with:

1. **Feature engineering pipeline** using transaction data — sell-through velocity, discount patterns, product age, return rates, weekly/monthly seasonality.
2. **Exploratory analysis** — identify which HOKA SKUs have the highest markdown exposure, which stores show different discount patterns, correlate foot traffic with conversion.
3. **Price lifecycle reconstruction** — derive markdown events from transaction data as a proxy until `precios_ofertas` is populated.
4. **Model scaffolding** — build the XGBoost training pipeline with placeholder features for inventory-dependent signals.

**Bottom line:** Transaction data is rich and clean enough to start feature engineering immediately. But **inventory snapshots and cost data are hard blockers** for training a margin-optimizing model. Request those from Jacques as priority #1.
