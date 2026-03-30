import { useState, useEffect } from 'react'
import { BarChart2, TrendingDown, Activity, DollarSign, Target, ChevronDown, ChevronUp } from 'lucide-react'

function clpCompact(n) {
  if (n === null || n === undefined || n === '' || isNaN(n)) return '\u2014'
  const num = Number(n)
  const sign = num >= 0 ? '+' : ''
  if (Math.abs(num) >= 1_000_000) return `${sign}${(num / 1_000_000).toFixed(1)}M`
  if (Math.abs(num) >= 1_000) return `${sign}${Math.round(num / 1_000)}K`
  return `${sign}${Math.round(num)}`
}

function pct(n, d) { return d > 0 ? Math.round(n / d * 100) : 0 }

function Bar({ value, max, color = 'var(--blue-500)', label, sublabel }) {
  const w = max > 0 ? Math.min(Math.abs(value) / max * 100, 100) : 0
  return (
    <div className="an-bar-row">
      <div className="an-bar-label">{label}</div>
      <div className="an-bar-track">
        <div className="an-bar-fill" style={{ width: `${w}%`, background: color }} />
      </div>
      <div className="an-bar-value">{sublabel}</div>
    </div>
  )
}

function SectionModelo({ data }) {
  if (!data) return null
  const { classifier_auc, regressor_r2, regressor_mae_pp, n_samples, n_features,
          holdout_auc, holdout_r2, holdout_mae_pp, holdout_n_samples,
          training_mode, classifier_shap, regressor_shap } = data

  const maxShap = classifier_shap?.[0]?.mean_abs_shap || 1

  return (
    <div className="an-section">
      <h3 className="an-section-title"><BarChart2 size={16} /> Modelo</h3>
      <div className="an-metrics-sublabel">Cross-Validation</div>
      <div className="an-metrics-grid">
        <div className="an-metric">
          <div className="an-metric-value">{classifier_auc?.toFixed(3) || '\u2014'}</div>
          <div className="an-metric-label">Classifier AUC</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">{regressor_r2?.toFixed(3) || '\u2014'}</div>
          <div className="an-metric-label">Regressor R\u00b2</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">{regressor_mae_pp ? `${regressor_mae_pp}pp` : '\u2014'}</div>
          <div className="an-metric-label">MAE</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">{n_samples?.toLocaleString() || '\u2014'}</div>
          <div className="an-metric-label">Muestras</div>
        </div>
      </div>
      {(holdout_auc || holdout_r2) && (
        <>
          <div className="an-metrics-sublabel">Holdout (\u00faltimas 4 semanas)</div>
          <div className="an-metrics-grid">
            <div className="an-metric">
              <div className="an-metric-value">{holdout_auc?.toFixed(3) || '\u2014'}</div>
              <div className="an-metric-label">Classifier AUC</div>
            </div>
            <div className="an-metric">
              <div className="an-metric-value">{holdout_r2?.toFixed(3) || '\u2014'}</div>
              <div className="an-metric-label">Regressor R\u00b2</div>
            </div>
            <div className="an-metric">
              <div className="an-metric-value">{holdout_mae_pp ? `${holdout_mae_pp}pp` : '\u2014'}</div>
              <div className="an-metric-label">MAE</div>
            </div>
            <div className="an-metric">
              <div className="an-metric-value">{holdout_n_samples?.toLocaleString() || '\u2014'}</div>
              <div className="an-metric-label">Muestras</div>
            </div>
          </div>
        </>
      )}
      {classifier_shap?.length > 0 && (
        <div className="an-shap">
          <div className="an-shap-title">Top features (classifier)</div>
          {classifier_shap.map((f, i) => (
            <Bar key={i} value={f.mean_abs_shap} max={maxShap}
                 color="var(--blue-500)" label={f.feature} sublabel={f.mean_abs_shap.toFixed(3)} />
          ))}
        </div>
      )}
    </div>
  )
}

function SectionElasticidad({ data }) {
  if (!data || !data.total) return null
  const { total, median, elastic_count, inelastic_count, by_confidence, by_subcategory, by_vendor_brand } = data
  const showVendor = by_vendor_brand?.length > 1
  const tableData = showVendor ? by_vendor_brand : by_subcategory

  return (
    <div className="an-section">
      <h3 className="an-section-title"><TrendingDown size={16} /> Elasticidad</h3>
      <div className="an-metrics-grid">
        <div className="an-metric">
          <div className="an-metric-value">{median?.toFixed(2) || '\u2014'}</div>
          <div className="an-metric-label">Mediana</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">{total}</div>
          <div className="an-metric-label">SKUs</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value" style={{color: 'var(--green-600)'}}>{elastic_count}</div>
          <div className="an-metric-label">Elasticos (&lt;-1)</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value" style={{color: 'var(--red-600)'}}>{inelastic_count}</div>
          <div className="an-metric-label">Inelasticos (&gt;-0.5)</div>
        </div>
      </div>
      {by_confidence && Object.keys(by_confidence).length > 0 && (
        <div className="an-confidence">
          {Object.entries(by_confidence).map(([k, v]) => (
            <span key={k} className={`an-conf-badge an-conf-${k}`}>{k}: {v}</span>
          ))}
        </div>
      )}
      {tableData?.length > 0 && (
        <div className="an-elast-table">
          <div className="an-elast-header">
            <span>{showVendor ? 'Marca' : 'Subcategoria'}</span><span>Elasticidad</span><span>SKUs</span>
          </div>
          {tableData.map((row, i) => (
            <div key={i} className="an-elast-row">
              <span className="an-elast-subcat">{showVendor ? row.vendor_brand : row.subcategory}</span>
              <span className={`an-elast-val ${row.median_elasticity < -1 ? 'an-elastic' : row.median_elasticity > -0.5 ? 'an-inelastic' : ''}`}>
                {row.median_elasticity?.toFixed(2) ?? '\u2014'}
              </span>
              <span className="an-elast-count">{row.sku_count}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function SectionCiclo({ data }) {
  if (!data) return null
  const { total_actions, urgency_dist, action_type_dist, confidence_dist } = data
  const urgencyColors = { INCREASE: 'var(--green-500)', HIGH: 'var(--red-500)', MEDIUM: 'var(--amber-500)', LOW: 'var(--slate-400)' }
  const urgencyOrder = ['INCREASE', 'HIGH', 'MEDIUM', 'LOW']

  return (
    <div className="an-section">
      <h3 className="an-section-title"><Activity size={16} /> Ciclo de vida</h3>
      <div className="an-stacked-bar">
        {urgencyOrder.map(u => {
          const count = urgency_dist[u] || 0
          const w = pct(count, total_actions)
          if (w === 0) return null
          return (
            <div key={u} className="an-stacked-segment" style={{ width: `${w}%`, background: urgencyColors[u] }}
                 title={`${u}: ${count} (${w}%)`}>
              {w > 8 && <span>{count}</span>}
            </div>
          )
        })}
      </div>
      <div className="an-legend">
        {urgencyOrder.map(u => {
          const count = urgency_dist[u] || 0
          if (!count) return null
          return (
            <span key={u} className="an-legend-item">
              <span className="an-legend-dot" style={{ background: urgencyColors[u] }} />
              {u === 'INCREASE' ? 'Subir' : u}: {count}
            </span>
          )
        })}
      </div>
      <div className="an-type-split">
        <span>Rebajas: {action_type_dist.decrease || 0}</span>
        <span>Subidas: {action_type_dist.increase || 0}</span>
      </div>
      {confidence_dist && (
        <div className="an-confidence" style={{marginTop: '8px'}}>
          {['HIGH','MEDIUM','LOW','SPECULATIVE'].map(t => {
            const c = confidence_dist[t]
            return c ? <span key={t} className={`an-conf-badge an-conf-${t.toLowerCase()}`}>{t}: {c}</span> : null
          })}
        </div>
      )}
    </div>
  )
}

function SectionImpacto({ data }) {
  if (!data) return null
  const { by_store, by_subcategory, by_vendor_brand, thin_margin_count } = data
  const storeData = (by_store || []).slice(0, 5)
  const rightData = (by_vendor_brand?.length > 1 ? by_vendor_brand : by_subcategory || []).slice(0, 5)
  const maxStore = Math.max(...storeData.map(s => Math.abs(s.rev_delta)), 1)
  const maxRight = Math.max(...rightData.map(s => Math.abs(s.rev_delta)), 1)

  return (
    <div className="an-section">
      <h3 className="an-section-title"><DollarSign size={16} /> Impacto</h3>
      {thin_margin_count > 0 && (
        <div className="an-warning">
          {thin_margin_count} acciones con margen &lt;20%
        </div>
      )}
      <div className="an-impact-cols">
        <div className="an-impact-col">
          <div className="an-impact-title">Por tienda</div>
          {storeData.map((s, i) => (
            <Bar key={i} value={Math.abs(s.rev_delta)} max={maxStore}
                 color={s.rev_delta >= 0 ? 'var(--green-500)' : 'var(--red-400)'}
                 label={s.store_name} sublabel={clpCompact(s.rev_delta)} />
          ))}
        </div>
        <div className="an-impact-col">
          <div className="an-impact-title">Por {by_vendor_brand?.length > 1 ? 'marca' : 'categoria'}</div>
          {rightData.map((s, i) => (
            <Bar key={i} value={Math.abs(s.rev_delta)} max={maxRight}
                 color={s.rev_delta >= 0 ? 'var(--green-500)' : 'var(--red-400)'}
                 label={s.vendor_brand || s.subcategory} sublabel={clpCompact(s.rev_delta)} />
          ))}
        </div>
      </div>
    </div>
  )
}

function SectionPrediccionReal({ data }) {
  if (!data || !data.available) return null
  const { decisions_evaluated, median_velocity_error_pct, pct_direction_correct,
          lift_capture_rate, worst_predictions } = data

  const captureColor = lift_capture_rate >= 70 ? 'var(--green-600)'
    : lift_capture_rate >= 50 ? 'var(--amber-600)' : 'var(--red-600)'
  const captureWidth = Math.min(Math.max(lift_capture_rate || 0, 0), 100)

  return (
    <div className="an-section">
      <h3 className="an-section-title"><Target size={16} /> Prediccion vs Real</h3>
      <div className="an-metrics-grid">
        <div className="an-metric">
          <div className="an-metric-value" style={{ color: captureColor }}>
            {lift_capture_rate != null ? `${lift_capture_rate}%` : '\u2014'}
          </div>
          <div className="an-metric-label">Lift capturado</div>
          <div className="an-capture-bar">
            <div className="an-capture-fill" style={{ width: `${captureWidth}%`, background: captureColor }} />
          </div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">
            {pct_direction_correct != null ? `${pct_direction_correct}%` : '\u2014'}
          </div>
          <div className="an-metric-label">Direccion correcta</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">
            {median_velocity_error_pct != null ? `${median_velocity_error_pct > 0 ? '+' : ''}${median_velocity_error_pct}%` : '\u2014'}
          </div>
          <div className="an-metric-label">Error mediana vel.</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">{decisions_evaluated || 0}</div>
          <div className="an-metric-label">Decisiones evaluadas</div>
        </div>
      </div>
      {worst_predictions?.length > 0 && (
        <div className="an-elast-table" style={{ marginTop: '12px' }}>
          <div className="an-elast-header">
            <span>SKU</span><span>Pred.</span><span>Real</span><span>Conf.</span>
          </div>
          {worst_predictions.map((row, i) => (
            <div key={i} className="an-elast-row">
              <span className="an-elast-subcat" title={`${row.parent_sku} @ ${row.store}`}>
                {row.parent_sku?.slice(0, 10)}
              </span>
              <span className="an-elast-count">{row.predicted_velocity?.toFixed(1) ?? '\u2014'}</span>
              <span className="an-elast-count">{row.actual_velocity?.toFixed(1) ?? '\u2014'}</span>
              <span className={`an-conf-badge an-conf-${(row.confidence_tier || 'low').toLowerCase()}`}>
                {row.confidence_tier}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function SectionCompetencia({ data }) {
  if (!data || !data.coverage?.total_parents) return null
  const { coverage, items, scraped_at } = data
  const byComp = coverage.by_competitor || {}
  const maxCount = Math.max(...Object.values(byComp), 1)

  return (
    <div className="an-section">
      <h3 className="an-section-title"><DollarSign size={16} /> Competencia</h3>
      <div className="an-metrics-grid">
        <div className="an-metric">
          <div className="an-metric-value">{coverage.total_parents}</div>
          <div className="an-metric-label">SKUs rastreados</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">{Object.keys(byComp).length}</div>
          <div className="an-metric-label">Competidores</div>
        </div>
      </div>
      {Object.keys(byComp).length > 0 && (
        <div className="an-shap">
          <div className="an-shap-title">Cobertura por competidor</div>
          {Object.entries(byComp).sort((a, b) => b[1] - a[1]).map(([name, count]) => (
            <Bar key={name} value={count} max={maxCount}
                 color="var(--amber-500)" label={name} sublabel={`${count} SKUs`} />
          ))}
        </div>
      )}
      {items?.length > 0 && (
        <div className="an-elast-table" style={{ marginTop: '8px' }}>
          <div className="an-shap-title">Mayores brechas de precio</div>
          <div className="an-elast-header">
            <span>SKU</span><span>Nuestro</span><span>Min Comp.</span><span>Gap</span>
          </div>
          {items.slice(0, 8).map((item, i) => {
            const ourMin = item.competitors?.reduce((m, c) => Math.min(m, c.price), Infinity)
            return (
              <div key={i} className="an-elast-row">
                <span className="an-elast-sku">{item.parent_sku?.slice(-8)}</span>
                <span>{'\u2014'}</span>
                <span>${ourMin?.toLocaleString()}</span>
                <span>{item.competitors?.length} sites</span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

export default function AnalyticsDrawer({ brand, authFetch }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!brand) return
    const controller = new AbortController()
    setLoading(true)
    setError(null)
    authFetch(`/analytics/${brand}`, { signal: controller.signal })
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { if (e.name !== 'AbortError') { setError(e.message); setLoading(false) } })
    return () => controller.abort()
  }, [brand, authFetch])

  if (loading) return <div className="an-drawer"><div className="an-loading">Cargando analytics...</div></div>
  if (error) return <div className="an-drawer"><div className="an-error">Error: {error}</div></div>
  if (!data) return null

  return (
    <div className="an-drawer">
      <div className="an-grid">
        <SectionModelo data={data.modelo} />
        <SectionElasticidad data={data.elasticidad} />
        <SectionCiclo data={data.ciclo_de_vida} />
        <SectionImpacto data={data.impacto} />
        <SectionCompetencia data={data.competencia} />
        <SectionPrediccionReal data={data.prediccion_vs_real} />
      </div>
    </div>
  )
}
