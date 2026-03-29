import { useState, useEffect } from 'react'
import { BarChart2, TrendingDown, Activity, DollarSign, ChevronDown, ChevronUp } from 'lucide-react'

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
          holdout_auc, holdout_r2, training_mode, classifier_shap, regressor_shap } = data

  const maxShap = classifier_shap?.[0]?.mean_abs_shap || 1

  return (
    <div className="an-section">
      <h3 className="an-section-title"><BarChart2 size={16} /> Modelo</h3>
      <div className="an-metrics-grid">
        <div className="an-metric">
          <div className="an-metric-value">{classifier_auc?.toFixed(3) || '\u2014'}</div>
          <div className="an-metric-label">Classifier AUC</div>
        </div>
        <div className="an-metric">
          <div className="an-metric-value">{regressor_r2?.toFixed(3) || '\u2014'}</div>
          <div className="an-metric-label">Regressor R2</div>
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
      {holdout_auc && (
        <div className="an-holdout">
          Holdout: AUC {holdout_auc.toFixed(3)} | R2 {holdout_r2?.toFixed(3) || '\u2014'}
        </div>
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
  const { total, median, elastic_count, inelastic_count, by_confidence, by_subcategory } = data

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
      {by_subcategory?.length > 0 && (
        <div className="an-elast-table">
          <div className="an-elast-header">
            <span>Subcategoria</span><span>Elasticidad</span><span>SKUs</span>
          </div>
          {by_subcategory.map((row, i) => (
            <div key={i} className="an-elast-row">
              <span className="an-elast-subcat">{row.subcategory}</span>
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

export default function AnalyticsDrawer({ brand, authFetch }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!brand) return
    setLoading(true)
    setError(null)
    authFetch(`/analytics/${brand}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
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
      </div>
    </div>
  )
}
