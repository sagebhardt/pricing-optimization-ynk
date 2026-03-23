import { useState, useEffect, useMemo, useCallback } from 'react'
import { Search, Download, TrendingUp, TrendingDown, ChevronDown, ChevronUp, Check, X, AlertTriangle, ArrowUpRight, ArrowDownRight, Filter, RotateCcw } from 'lucide-react'
import './App.css'

const BRANDS = [
  { id: 'hoka', label: 'HOKA', endpoint: '/pricing-actions?brand=hoka' },
  { id: 'bold', label: 'BOLD', endpoint: '/pricing-actions?brand=bold' },
]

function clp(n) {
  if (n === null || n === undefined || n === '' || isNaN(n)) return '—'
  return '$' + Math.round(Number(n)).toLocaleString('es-CL')
}

function clpCompact(n) {
  if (n === null || n === undefined || n === '' || isNaN(n)) return '—'
  const v = Number(n)
  if (Math.abs(v) >= 1_000_000) return '$' + (v / 1_000_000).toFixed(1) + 'M'
  if (Math.abs(v) >= 1_000) return '$' + (v / 1_000).toFixed(0) + 'K'
  return '$' + v.toLocaleString('es-CL')
}

function ActionRow({ action, status, onDecide }) {
  const [open, setOpen] = useState(false)
  const isIncrease = action.action_type === 'increase'
  const urgencyClass = isIncrease ? 'increase' : (action.urgency || '').toLowerCase()
  const delta = Number(action.rev_delta) || 0

  return (
    <div className={`row row--${urgencyClass} ${status ? `row--${status}` : ''}`}>
      <div className="row-main" onClick={() => setOpen(!open)}>
        <div className="row-badge-col">
          {isIncrease ? (
            <span className="badge badge--increase"><ArrowUpRight size={11} /> SUBIR</span>
          ) : (
            <span className={`badge badge--${urgencyClass}`}>{action.urgency}</span>
          )}
        </div>

        <div className="row-product">
          <div className="product-name">{action.product || action.parent_sku}</div>
          <div className="product-meta">
            <span className="sku-code">{action.parent_sku}</span>
            {action.subcategory && <span> · {action.subcategory}</span>}
            <span> · {action.store_name || action.store}</span>
          </div>
        </div>

        <div className="row-pricing">
          <span className="price-from">{clp(action.current_price)}</span>
          <span className={`price-arrow ${isIncrease ? 'price-arrow--up' : ''}`}>
            {isIncrease ? <ArrowUpRight size={14} /> : <ArrowDownRight size={14} />}
          </span>
          <span className={`price-to ${isIncrease ? 'price-to--up' : ''}`}>{clp(action.recommended_price)}</span>
          <span className={`disc-tag ${isIncrease ? 'disc-tag--up' : ''}`}>{action.recommended_discount}</span>
        </div>

        <div className="row-velocity">
          <span>{action.current_velocity}</span>
          <span className="vel-arrow">→</span>
          <span>{action.expected_velocity}</span>
          <span className="vel-unit">u/sem</span>
        </div>

        <div className={`row-delta ${delta >= 0 ? 'row-delta--pos' : 'row-delta--neg'}`}>
          {delta >= 0 ? '+' : ''}{clpCompact(delta)}
        </div>

        <div className="row-actions" onClick={e => e.stopPropagation()}>
          {!status ? (
            <>
              <button className="btn-approve" onClick={() => onDecide('approved')} title="Aprobar">
                <Check size={14} />
              </button>
              <button className="btn-reject" onClick={() => onDecide('rejected')} title="Rechazar">
                <X size={14} />
              </button>
            </>
          ) : (
            <button className={`btn-decided btn-decided--${status}`} onClick={() => onDecide(null)} title="Deshacer">
              {status === 'approved' ? <Check size={12} /> : <X size={12} />}
              <span>{status === 'approved' ? 'OK' : '—'}</span>
            </button>
          )}
        </div>

        <div className="row-chevron">{open ? <ChevronUp size={14} /> : <ChevronDown size={14} />}</div>
      </div>

      {open && (
        <div className="row-detail">
          <div className="detail-cells">
            <div className="dcell"><span className="dcell-label">Precio lista</span><span className="dcell-val mono">{clp(action.current_list_price)}</span></div>
            <div className="dcell"><span className="dcell-label">Tallas activas</span><span className="dcell-val">{action.sizes_selling} / {action.sizes_total}</span></div>
            <div className="dcell"><span className="dcell-label">Edad</span><span className="dcell-val">{action.product_age_weeks} semanas</span></div>
            <div className="dcell"><span className="dcell-label">Confianza</span><span className="dcell-val">{(Number(action.model_confidence) * 100).toFixed(0)}%</span></div>
            <div className="dcell"><span className="dcell-label">Rev semanal actual</span><span className="dcell-val mono">{clp(action.current_weekly_rev)}</span></div>
            <div className="dcell"><span className="dcell-label">Rev semanal esperado</span><span className="dcell-val mono">{clp(action.expected_weekly_rev)}</span></div>
          </div>
          <div className="detail-reason"><AlertTriangle size={13} /> {action.reasons}</div>
        </div>
      )}
    </div>
  )
}

function exportCSV(items, filename) {
  if (!items.length) return
  const headers = Object.keys(items[0])
  const csv = [
    headers.join(','),
    ...items.map(row => headers.map(h => {
      const v = row[h]
      return typeof v === 'string' && v.includes(',') ? `"${v}"` : v
    }).join(','))
  ].join('\n')
  const blob = new Blob([csv], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

function App() {
  const [brand, setBrand] = useState(BRANDS[0])
  const [actions, setActions] = useState([])
  const [alerts, setAlerts] = useState([])
  const [info, setInfo] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [search, setSearch] = useState('')
  const [filterStore, setFilterStore] = useState('all')
  const [filterUrgency, setFilterUrgency] = useState('all')
  const [filterCategory, setFilterCategory] = useState('all')
  const [decisions, setDecisions] = useState({})

  const loadBrand = useCallback((b) => {
    setLoading(true)
    setError(null)
    setActions([])
    setDecisions({})
    setSearch('')
    setFilterStore('all')
    setFilterUrgency('all')
    setFilterCategory('all')
    setBrand(b)

    Promise.all([
      fetch(b.endpoint).then(r => r.json()),
      fetch('/alerts?min_attrition=0.3').then(r => r.json()),
      fetch(`/model/info?brand=${b.id}`).then(r => r.json()),
    ]).then(([ad, al, mi]) => {
      setActions(ad.items || [])
      setAlerts(al.items || [])
      setInfo(mi)
      setLoading(false)
    }).catch(() => {
      setError('No se pudo conectar con la API.')
      setLoading(false)
    })
  }, [])

  useEffect(() => { loadBrand(brand) }, [])

  const stores = useMemo(() =>
    [...new Set(actions.map(a => a.store_name || a.store).filter(Boolean))].sort(),
    [actions]
  )
  const categories = useMemo(() =>
    [...new Set(actions.map(a => a.subcategory).filter(Boolean))].sort(),
    [actions]
  )

  const filtered = useMemo(() => {
    const q = search.toLowerCase()
    return actions.filter(a => {
      if (filterStore !== 'all' && (a.store_name || a.store) !== filterStore) return false
      if (filterUrgency !== 'all') {
        if (filterUrgency === 'INCREASE' && a.action_type !== 'increase') return false
        if (filterUrgency !== 'INCREASE' && (a.urgency !== filterUrgency || a.action_type === 'increase')) return false
      }
      if (filterCategory !== 'all' && a.subcategory !== filterCategory) return false
      if (q && !(
        (a.parent_sku || '').toLowerCase().includes(q) ||
        (a.product || '').toLowerCase().includes(q) ||
        (a.sku || '').toLowerCase().includes(q)
      )) return false
      return true
    })
  }, [actions, filterStore, filterUrgency, filterCategory, search])

  const increases = useMemo(() => filtered.filter(a => a.action_type === 'increase'), [filtered])
  const decreases = useMemo(() => filtered.filter(a => a.action_type !== 'increase'), [filtered])

  const setDecision = useCallback((key, status) => {
    setDecisions(prev => {
      const next = { ...prev }
      if (status === null) delete next[key]
      else next[key] = status
      return next
    })
  }, [])

  const bulkDecide = useCallback((items, status) => {
    setDecisions(prev => {
      const next = { ...prev }
      items.forEach(a => {
        const k = `${a.parent_sku}-${a.store}`
        if (!next[k]) next[k] = status
      })
      return next
    })
  }, [])

  const reviewedCount = Object.keys(decisions).length
  const approvedItems = actions.filter(a => decisions[`${a.parent_sku}-${a.store}`] === 'approved')
  const rejectedCount = Object.values(decisions).filter(v => v === 'rejected').length
  const approvedImpact = approvedItems.reduce((s, a) => s + (Number(a.rev_delta) || 0), 0)

  const handleExport = () => {
    exportCSV(approvedItems, `acciones_aprobadas_${brand.id}_${new Date().toISOString().slice(0, 10)}.csv`)
  }

  if (loading) return <div className="loading-screen"><div className="spinner" /><span>Cargando {brand.label}...</span></div>
  if (error) return <div className="error-screen"><AlertTriangle size={24} /><p>{error}</p><p className="error-hint">python3 -m uvicorn api.main:app --port 8080</p></div>

  return (
    <div className="app">
      {/* Header */}
      <header className="header">
        <div className="header-brand">
          <h1 className="logo">YNK<span className="logo-dot">.</span>pricing</h1>
          <nav className="brand-tabs">
            {BRANDS.map(b => (
              <button
                key={b.id}
                className={`brand-tab ${b.id === brand.id ? 'brand-tab--active' : ''}`}
                onClick={() => { if (b.id !== brand.id) loadBrand(b) }}
              >
                {b.label}
              </button>
            ))}
          </nav>
        </div>
        <div className="header-meta">
          {info && <span className="meta-tag">v{info.version} AUC {info.classifier?.avg_auc?.toFixed(3)}</span>}
          <span className="meta-tag">{actions.length} acciones</span>
        </div>
      </header>

      {/* Stats */}
      <div className="stats-row">
        <div className="kpi">
          <div className="kpi-value">{actions.filter(a => a.action_type === 'increase').length}</div>
          <div className="kpi-label">Subir precio</div>
          <div className="kpi-bar kpi-bar--increase" />
        </div>
        <div className="kpi">
          <div className="kpi-value kpi-value--high">{actions.filter(a => a.urgency === 'HIGH').length}</div>
          <div className="kpi-label">Alta urgencia</div>
          <div className="kpi-bar kpi-bar--high" />
        </div>
        <div className="kpi">
          <div className="kpi-value kpi-value--med">{actions.filter(a => a.urgency === 'MEDIUM').length}</div>
          <div className="kpi-label">Media</div>
          <div className="kpi-bar kpi-bar--med" />
        </div>
        <div className="kpi">
          <div className="kpi-value kpi-value--low">{actions.filter(a => a.urgency === 'LOW').length}</div>
          <div className="kpi-label">Baja</div>
          <div className="kpi-bar kpi-bar--low" />
        </div>
        <div className="kpi kpi--progress">
          <div className="kpi-value">{reviewedCount}<span className="kpi-of">/{actions.length}</span></div>
          <div className="kpi-label">Revisadas</div>
          <div className="progress-track"><div className="progress-fill" style={{ width: `${actions.length ? (reviewedCount / actions.length) * 100 : 0}%` }} /></div>
        </div>
        <div className="kpi kpi--impact">
          <div className={`kpi-value ${approvedImpact >= 0 ? 'kpi-value--pos' : 'kpi-value--neg'}`}>{approvedImpact >= 0 ? '+' : ''}{clpCompact(approvedImpact)}</div>
          <div className="kpi-label">Impacto aprobado</div>
          <div className="kpi-bar kpi-bar--impact" />
        </div>
      </div>

      {/* Toolbar */}
      <div className="toolbar">
        <div className="search-box">
          <Search size={15} />
          <input
            type="text"
            placeholder="Buscar SKU o producto..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>

        <div className="filter-group">
          <Filter size={14} />
          <select value={filterStore} onChange={e => setFilterStore(e.target.value)}>
            <option value="all">Todas las tiendas</option>
            {stores.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
          <select value={filterUrgency} onChange={e => setFilterUrgency(e.target.value)}>
            <option value="all">Toda urgencia</option>
            <option value="INCREASE">Subir precio</option>
            <option value="HIGH">Alta</option>
            <option value="MEDIUM">Media</option>
            <option value="LOW">Baja</option>
          </select>
          {categories.length > 1 && (
            <select value={filterCategory} onChange={e => setFilterCategory(e.target.value)}>
              <option value="all">Toda categoria</option>
              {categories.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          )}
        </div>

        <div className="toolbar-actions">
          <span className="result-count">{filtered.length} resultados</span>
          <button className="tbtn tbtn--approve" onClick={() => bulkDecide(filtered, 'approved')}>
            <Check size={13} /> Aprobar filtradas
          </button>
          <button className="tbtn tbtn--reject" onClick={() => bulkDecide(filtered, 'rejected')}>
            <X size={13} /> Rechazar filtradas
          </button>
          {approvedItems.length > 0 && (
            <button className="tbtn tbtn--export" onClick={handleExport}>
              <Download size={13} /> Exportar ({approvedItems.length})
            </button>
          )}
        </div>
      </div>

      {/* Price Increases Section */}
      {increases.length > 0 && (
        <section className="section section--increases">
          <div className="section-header">
            <TrendingUp size={16} />
            <h2>Subir precio — Recuperar margen ({increases.length})</h2>
          </div>
          <div className="list">
            {increases.map(a => {
              const k = `${a.parent_sku}-${a.store}`
              return (
                <ActionRow
                  key={k}
                  action={a}
                  status={decisions[k] || null}
                  onDecide={st => setDecision(k, st)}
                />
              )
            })}
          </div>
        </section>
      )}

      {/* Markdowns Section */}
      {decreases.length > 0 && (
        <section className="section section--markdowns">
          <div className="section-header">
            <TrendingDown size={16} />
            <h2>Markdown ({decreases.length})</h2>
          </div>
          <div className="list">
            {decreases.map(a => {
              const k = `${a.parent_sku}-${a.store}`
              return (
                <ActionRow
                  key={k}
                  action={a}
                  status={decisions[k] || null}
                  onDecide={st => setDecision(k, st)}
                />
              )
            })}
          </div>
        </section>
      )}

      {filtered.length === 0 && <div className="empty-state">No hay acciones con estos filtros</div>}

      {/* Alerts */}
      {alerts.length > 0 && (
        <section className="section section--alerts">
          <div className="section-header">
            <AlertTriangle size={16} />
            <h2>Alertas de curva de tallas ({alerts.length})</h2>
          </div>
          <div className="alert-grid">
            {alerts.slice(0, 15).map((a, i) => (
              <div key={i} className="alert-card">
                <div className="alert-sku">{a.parent_sku}</div>
                <div className="alert-store">{a.store}</div>
                <div className="alert-sizes">{a.active_sizes}/{a.total_sizes} tallas</div>
                <div className="alert-attrition">{(a.attrition_rate * 100).toFixed(0)}%</div>
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  )
}

export default App
