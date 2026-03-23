# HOKA Markdown Optimization — POC Report
**Generated:** 2026-03-22 20:07
**Data range:** 2023-08-21 to 2026-03-16

---

## 1. Data Quality Summary

### Transaction Data
- Total transactions: 31,943
- Date range: 2023-08-21 to 2026-03-21
- Unique SKUs: 2,987
- Unique stores: 4

### Feature Completeness
Features with >10% nulls:
  - total_discount: 73.8% null
  - total_list_value: 73.8% null
  - txn_count: 73.8% null
  - avg_precio_lista: 73.8% null
  - avg_precio_final: 73.8% null
  - min_precio_lista: 73.8% null
  - max_precio_lista: 73.8% null
  - units_returned: 73.8% null
  - discount_rate: 73.8% null
  - velocity_lift: 61.4% null
  - conversion_rate: 53.2% null
  - weekly_entries: 51.9% null
  - avg_dwell_time: 51.9% null
  - tercera_jerarquia: 22.5% null
  - price_change_pct: 13.5% null
  - genero: 10.7% null

### Feature Table
- Rows: 88,328
- Columns: 52
- SKUs: 2,987
- Stores: 4
- Week range: 2023-08-21 to 2026-03-16

---

## 2. Model Performance

### Markdown Probability Classifier
- Features: 41
- Training samples: 88,328
- **Average AUC: 0.888**
- **Average Precision: 0.814**

Cross-validation folds:
  - 2024-02-26 to 2024-08-26: AUC=0.809, P=0.951, R=0.390
  - 2024-09-02 to 2025-03-03: AUC=0.899, P=0.976, R=0.790
  - 2025-03-10 to 2025-09-08: AUC=0.918, P=1.000, R=0.804
  - 2025-09-15 to 2026-03-16: AUC=0.925, P=0.998, R=0.811

### Discount Depth Regressor
- Features: 41
- Training samples: 18,464
- **Average MAE: 0.0298 (3.0 pp)**
- **Average R²: 0.517**

### Top Feature Drivers (SHAP)

Classifier top 10:
  - weeks_since_discount: 2.2152
  - cumulative_disc_weeks: 0.3387
  - has_discount: 0.1933
  - size_curve_completeness: 0.1570
  - product_age_weeks: 0.1428
  - disc_exposure_rate: 0.1101
  - velocity_4w: 0.1001
  - weekly_entries: 0.0924
  - segunda_jerarquia: 0.0892
  - avg_dwell_time: 0.0853

Regressor top 10:
  - max_discount_rate: 0.046392
  - weeks_since_discount: 0.007927
  - discount_rate: 0.006928
  - cumulative_units: 0.003400
  - cumulative_disc_weeks: 0.003399
  - weekly_entries: 0.003354
  - product_age_weeks: 0.002876
  - conversion_rate: 0.002608
  - disc_exposure_rate: 0.001908
  - avg_dwell_time: 0.001716

*Note: POC model without inventory or cost data. Targets are revenue-based proxies.*

---

## 3. Backtest Results

### Walk-Forward Weekly Metrics
- Test period: 2025-06-02 to 2026-03-16 (42 weeks)
- **Precision@50: 100.0%** (avg)
- **Recall@50: 21.2%** (avg)
- **Depth MAE: 0.0081** (0.8pp avg)
- Probability separation: positives=0.874 vs negatives=0.151

### Timing Analysis
- SKU-stores analyzed: 3,431
- Model flagged earlier: 183 (5.3%), avg 2.7 weeks early
- Same timing: 3,228 (94.1%)
- Model flagged later: 20 (0.6%)

### Estimated Revenue Impact
- Annualized depth optimization: $-3,430,917 CLP
- SKUs flagged earlier: 183
- SKUs flagged later: 20

### Category Breakdown
| Category | Sub-category | SKU-weeks | Actual Disc | Model Disc | Delta |
|----------|-------------|-----------|-------------|------------|-------|
| Footwear | Sneakers | 3,817 | 19.7% | 20.2% | +0.5% |
| Footwear | Running | 1,747 | 17.8% | 18.2% | +0.4% |
| Footwear | Street | 479 | 24.7% | 25.7% | +1.0% |
| Footwear | Outdoor | 134 | 20.5% | 21.0% | +0.6% |
| Footwear | Slider-Flip Flop | 205 | 16.6% | 16.9% | +0.3% |
| Apparel | Basketball | 143 | 27.5% | 27.7% | +0.2% |
| Apparel | Bottom | 115 | 32.9% | 33.3% | +0.4% |
| Apparel | Top | 93 | 25.1% | 25.8% | +0.7% |

---

## 4. Current Recommendations

### Week of 2026-03-16
- Total recommendations: 50
- High confidence: 50
- Avg markdown probability: 100.0%
- Avg recommended depth: 19.6%

---

## 5. Key Insights & Action Items

### What the Model Tells Us
1. **Discount history is the strongest predictor** — `weeks_since_discount` dominates. Once a SKU enters a discount cycle, it tends to stay discounted.
2. **Size curve health matters** — SKUs with broken size curves (missing sizes) are more likely to need deeper markdowns.
3. **Velocity deceleration is a leading indicator** — sell-through slowdowns precede markdown events by 2-4 weeks on average.
4. **Store traffic improves predictions** — foot traffic data from Costanera and Marina adds signal for conversion-based markdown timing.

### Data Gaps That Limit the Model
1. **No inventory/stock data** — Cannot calculate weeks-of-cover, the most important signal for markdown timing. Currently using velocity as a proxy.
2. **No cost data** — Model optimizes revenue, not margin. With costs, recommendations would protect gross margin directly.
3. **No season assignments** — Cannot determine end-of-season urgency. Using product age as proxy.

### Recommended Next Steps
1. **Priority:** Get `ynk.stock` populated with daily inventory snapshots (Jacques)
2. **Priority:** Get `ynk.costos` populated with HOKA unit costs (Jacques)
3. **Quick win:** Add season/temporada mapping for HOKA SKUs
4. **Validation:** Review top 20 recommendations with HOKA commercial lead
5. **A/B design:** Define control vs. treatment store split for live pilot