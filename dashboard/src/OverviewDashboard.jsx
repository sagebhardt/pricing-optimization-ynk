import { useState, useEffect } from 'react'
import { TrendingUp, TrendingDown, AlertTriangle, Check, BarChart2 } from 'lucide-react'

function clpCompact(n) {
  if (n === null || n === undefined || isNaN(n)) return '\u2014'
  const num = Number(n)
  const sign = num >= 0 ? '+' : ''
  if (Math.abs(num) >= 1_000_000) return `${sign}${(num / 1_000_000).toFixed(1)}M`
  if (Math.abs(num) >= 1_000) return `${sign}${Math.round(num / 1_000)}K`
  return `${sign}${Math.round(num)}`
}

function pct(n, d) { return d > 0 ? Math.round(n / d * 100) : 0 }

function BrandCard({ brand, onSelect }) {
  const progress = pct(brand.decided, brand.total_actions)
  const approvedPct = pct(brand.approved, brand.total_actions)

  return (
    <div className="ov-card" onClick={() => onSelect(brand.brand)}>
      <div className="ov-card-header">
        <span className="ov-card-brand">{brand.brand.toUpperCase()}</span>
        <span className="ov-card-week">Sem. {brand.week}</span>
      </div>

      <div className="ov-card-metrics">
        <div className="ov-metric">
          <div className="ov-metric-value">{brand.total_actions}</div>
          <div className="ov-metric-label">Acciones</div>
        </div>
        <div className="ov-metric">
          <div className="ov-metric-value" style={{color: brand.pending > 0 ? 'var(--amber-600)' : 'var(--green-600)'}}>
            {brand.pending}
          </div>
          <div className="ov-metric-label">Pendientes</div>
        </div>
        <div className="ov-metric">
          <div className="ov-metric-value" style={{color: brand.rev_delta >= 0 ? 'var(--green-600)' : 'var(--red-600)'}}>
            {clpCompact(brand.rev_delta)}
          </div>
          <div className="ov-metric-label">Rev delta/sem</div>
        </div>
        <div className="ov-metric">
          <div className="ov-metric-value" style={{color: brand.margin_delta >= 0 ? 'var(--green-600)' : 'var(--red-600)'}}>
            {clpCompact(brand.margin_delta)}
          </div>
          <div className="ov-metric-label">Margen delta</div>
        </div>
      </div>

      <div className="ov-card-bar-section">
        <div className="ov-card-bar-labels">
          <span>Progreso: {progress}%</span>
          <span>{brand.decided}/{brand.total_actions}</span>
        </div>
        <div className="ov-card-bar-track">
          <div className="ov-card-bar-approved" style={{ width: `${approvedPct}%` }} />
          <div className="ov-card-bar-decided" style={{ width: `${progress - approvedPct}%` }} />
        </div>
      </div>

      <div className="ov-card-footer">
        <span className="ov-card-split">
          <TrendingDown size={11} /> {brand.decreases} rebajas
        </span>
        <span className="ov-card-split">
          <TrendingUp size={11} /> {brand.increases} subidas
        </span>
        {brand.thin_margin_count > 0 && (
          <span className="ov-card-split ov-card-split--warn">
            <AlertTriangle size={11} /> {brand.thin_margin_count} margen thin
          </span>
        )}
      </div>

      <div className="ov-card-model">
        <span>AUC {brand.classifier_auc?.toFixed(3) || '\u2014'}</span>
        <span>R2 {brand.regressor_r2?.toFixed(3) || '\u2014'}</span>
        {brand.holdout_r2 && <span>Holdout {brand.holdout_r2.toFixed(3)}</span>}
      </div>
    </div>
  )
}

export default function OverviewDashboard({ authFetch, onSelectBrand, user }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    authFetch('/analytics/overview')
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [authFetch])

  if (loading) {
    return (
      <div className="ov-container">
        <div className="ov-loading">Cargando overview...</div>
      </div>
    )
  }

  if (!data || !data.brands?.length) {
    return (
      <div className="ov-container">
        <div className="ov-empty">No hay datos disponibles</div>
      </div>
    )
  }

  const brands = data.brands
  const totalActions = brands.reduce((s, b) => s + b.total_actions, 0)
  const totalPending = brands.reduce((s, b) => s + b.pending, 0)
  const totalRev = brands.reduce((s, b) => s + b.rev_delta, 0)
  const totalMargin = brands.reduce((s, b) => s + b.margin_delta, 0)

  return (
    <div className="ov-container">
      <div className="ov-header">
        <div>
          <div className="ov-logo">YNK<span className="logo-dot">.</span>pricing</div>
          <h1 className="ov-title">Overview — Todas las marcas</h1>
        </div>
        {user && <span className="ov-user">{user.name || user.email}</span>}
      </div>

      <div className="ov-totals">
        <div className="ov-total">
          <div className="ov-total-value">{totalActions}</div>
          <div className="ov-total-label">Acciones totales</div>
        </div>
        <div className="ov-total">
          <div className="ov-total-value" style={{color: totalPending > 0 ? 'var(--amber-600)' : 'var(--green-600)'}}>
            {totalPending}
          </div>
          <div className="ov-total-label">Pendientes</div>
        </div>
        <div className="ov-total">
          <div className="ov-total-value" style={{color: totalRev >= 0 ? 'var(--green-600)' : 'var(--red-600)'}}>
            {clpCompact(totalRev)}
          </div>
          <div className="ov-total-label">Rev delta total/sem</div>
        </div>
        <div className="ov-total">
          <div className="ov-total-value" style={{color: totalMargin >= 0 ? 'var(--green-600)' : 'var(--red-600)'}}>
            {clpCompact(totalMargin)}
          </div>
          <div className="ov-total-label">Margen delta total</div>
        </div>
      </div>

      <div className="ov-grid">
        {brands.map(b => (
          <BrandCard key={b.brand} brand={b} onSelect={onSelectBrand} />
        ))}
      </div>
    </div>
  )
}
