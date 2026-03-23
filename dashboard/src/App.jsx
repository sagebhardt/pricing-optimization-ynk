import { useState, useEffect, useMemo, useCallback } from 'react'
import { Search, Download, TrendingUp, TrendingDown, ChevronDown, ChevronUp, Check, X, AlertTriangle, ArrowUpRight, ArrowDownRight, Filter } from 'lucide-react'
import './App.css'

const BRANDS = [
  { id: 'hoka',   label: 'HOKA',   endpoint: '/pricing-actions?brand=hoka' },
  { id: 'bold',   label: 'BOLD',   endpoint: '/pricing-actions?brand=bold' },
  { id: 'bamers', label: 'BAMERS', endpoint: '/pricing-actions?brand=bamers' },
  { id: 'oakley', label: 'OAKLEY', endpoint: '/pricing-actions?brand=oakley' },
]

const BRAND_STATS = [
  { id: 'hoka',   label: 'HOKA',   actions: 100,   skus: '3K',  stores: 4  },
  { id: 'bold',   label: 'BOLD',   actions: 1777,  skus: '57K', stores: 35 },
  { id: 'bamers', label: 'BAMERS', actions: 651,   skus: '9K',  stores: 25 },
  { id: 'oakley', label: 'OAKLEY', actions: 292,   skus: '5K',  stores: 8  },
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

// ── Landing Page ────────────────────────────────────────────────────────────

function LandingPage({ onEnter }) {
  return (
    <div className="landing">
      <div className="landing-inner">

        <div className="landing-hero">
          <div className="landing-logo">YNK<span className="logo-dot">.</span>pricing</div>
          <h1 className="landing-headline">Optimizacion de precios basada en ML</h1>
          <p className="landing-sub">
            El sistema predice el momento y la profundidad optima de markdown por
            producto, por tienda, por semana. Detecta cuando conviene subir precios
            para recuperar margen. Activo para HOKA, BOLD, BAMERS y OAKLEY.
          </p>
          <button className="landing-cta" onClick={onEnter}>
            Entrar al dashboard
          </button>
        </div>

        <div className="landing-stats-grid">
          {BRAND_STATS.map(b => (
            <div key={b.id} className="lstat-card">
              <div className="lstat-brand">{b.label}</div>
              <div className="lstat-row">
                <div className="lstat-item">
                  <span className="lstat-val">{b.actions.toLocaleString('es-CL')}</span>
                  <span className="lstat-label">acciones esta semana</span>
                </div>
                <div className="lstat-item">
                  <span className="lstat-val">{b.skus}</span>
                  <span className="lstat-label">SKUs</span>
                </div>
                <div className="lstat-item">
                  <span className="lstat-val">{b.stores}</span>
                  <span className="lstat-label">tiendas</span>
                </div>
              </div>
            </div>
          ))}
        </div>

        <div className="landing-grid">

          <div className="lcard">
            <div className="lcard-label">Que hace</div>
            <h2 className="lcard-title">Decisiones de precio automatizadas</h2>
            <p className="lcard-body">
              Cada semana, el sistema analiza cada SKU padre en cada tienda y genera
              una recomendacion de accion: aplicar markdown, subir precio o no hacer nada.
              Las recomendaciones incluyen el descuento exacto a aplicar, el impacto en
              revenue esperado y el nivel de urgencia.
            </p>
            <div className="lcard-pill-row">
              <span className="lpill lpill--red">Alta urgencia</span>
              <span className="lpill lpill--amber">Media</span>
              <span className="lpill lpill--slate">Baja</span>
              <span className="lpill lpill--green">Subir precio</span>
            </div>
          </div>

          <div className="lcard">
            <div className="lcard-label">Datos utilizados</div>
            <h2 className="lcard-title">1.8M+ transacciones procesadas</h2>
            <p className="lcard-body">
              El modelo entrena sobre el historial completo de ventas por tienda y talla.
              Solo para BAMERS hay mas de 1.8 millones de filas de transacciones. Sobre esa
              base se construyen las siguientes features:
            </p>
            <ul className="lcard-list">
              <li>Ciclo de vida del producto — Lanzamiento, Crecimiento, Peak, Estable, Declive, Liquidacion</li>
              <li>Elasticidad precio-demanda por SKU y tienda (log-log regression)</li>
              <li>Velocidad de ventas y su tendencia reciente</li>
              <li>Curva de tallas — que porcentaje de tallas sigue activo</li>
              <li>Trafico de tienda y estacionalidad de semana del ano</li>
            </ul>
          </div>

          <div className="lcard">
            <div className="lcard-label">Como optimiza</div>
            <h2 className="lcard-title">Dos modelos encadenados</h2>
            <p className="lcard-body">
              Primero un clasificador decide si corresponde hacer markdown. Si la
              respuesta es si, un regresor calcula la profundidad optima dentro de
              la escalera de descuentos permitida.
            </p>
            <div className="ldiscount-ladder">
              <span className="ldl-step ldl-step--0">0%</span>
              <span className="ldl-arrow">&rarr;</span>
              <span className="ldl-step">15%</span>
              <span className="ldl-arrow">&rarr;</span>
              <span className="ldl-step">20%</span>
              <span className="ldl-arrow">&rarr;</span>
              <span className="ldl-step">30%</span>
              <span className="ldl-arrow">&rarr;</span>
              <span className="ldl-step ldl-step--max">40%</span>
            </div>
            <p className="lcard-body" style={{ marginTop: '12px' }}>
              Cuando un producto esta vendiendo bien con descuento profundo, el sistema
              puede recomendar subir el precio para recuperar margen sin sacrificar volumen.
            </p>
          </div>

          <div className="lcard">
            <div className="lcard-label">Metricas del modelo</div>
            <h2 className="lcard-title">Como leer los numeros</h2>
            <div className="lmetric-list">
              <div className="lmetric">
                <div className="lmetric-val">0.949</div>
                <div className="lmetric-name">AUC — Clasificador</div>
                <div className="lmetric-desc">
                  Acierta el 94.9% de las veces al decidir si aplicar o no un markdown.
                  Un modelo aleatorio tendria 0.5.
                </div>
              </div>
              <div className="lmetric">
                <div className="lmetric-val">7.9pp</div>
                <div className="lmetric-name">MAE — Error de profundidad</div>
                <div className="lmetric-desc">
                  En promedio se equivoca en menos de 8 puntos porcentuales al predecir
                  el descuento optimo. Si el modelo dice 30%, el real optimo esta entre 22% y 38%.
                </div>
              </div>
              <div className="lmetric">
                <div className="lmetric-val">0.739</div>
                <div className="lmetric-name">R2 — Regresor (OAKLEY)</div>
                <div className="lmetric-desc">
                  El modelo explica el 73.9% de la variacion en la profundidad de descuento.
                  Benchmarks de la industria para este tipo de problema estan entre 0.4 y 0.8.
                </div>
              </div>
            </div>
          </div>

          <div className="lcard">
            <div className="lcard-label">Sistema de recomendaciones</div>
            <h2 className="lcard-title">Logica de urgencia por ciclo de vida</h2>
            <p className="lcard-body">
              La urgencia de cada accion considera en conjunto el ciclo de vida del
              producto, la salud de la curva de tallas, la tendencia de velocidad
              y la elasticidad estimada.
            </p>
            <div className="llc-stages">
              <div className="llc-stage"><span className="llc-dot llc-dot--green" />Lanzamiento</div>
              <div className="llc-stage"><span className="llc-dot llc-dot--green" />Crecimiento</div>
              <div className="llc-stage"><span className="llc-dot llc-dot--amber" />Peak</div>
              <div className="llc-stage"><span className="llc-dot llc-dot--amber" />Estable</div>
              <div className="llc-stage"><span className="llc-dot llc-dot--red" />Declive</div>
              <div className="llc-stage"><span className="llc-dot llc-dot--red" />Liquidacion</div>
            </div>
            <p className="lcard-body" style={{ marginTop: '12px' }}>
              Un producto en fase de liquidacion con mas del 40% de tallas agotadas
              y tendencia de ventas negativa genera una alerta de alta urgencia automaticamente.
              Un producto en crecimiento con ventas aceleradas puede recibir recomendacion
              de subir precio.
            </p>
          </div>

          <div className="lcard lcard--next">
            <div className="lcard-label">Proximos pasos</div>
            <h2 className="lcard-title">Datos pendientes con Jacques / equipo SAP</h2>
            <p className="lcard-body">
              Hay tres fuentes de datos que mejorarian materialmente la precision del sistema.
              Cada una esta bloqueada esperando extraccion desde SAP.
            </p>
            <div className="lnext-list">
              <div className="lnext-item">
                <div className="lnext-name">Stock diario (ynk.stock)</div>
                <div className="lnext-desc">
                  Habilita la feature semanas de cobertura (weeks-of-cover). Permite
                  calcular en cuantas semanas se agotara el inventario al ritmo actual
                  de ventas — el driver mas directo para decidir si hacer markdown hoy
                  o esperar.
                </div>
              </div>
              <div className="lnext-item">
                <div className="lnext-name">Costos unitarios (ynk.costos)</div>
                <div className="lnext-desc">
                  Habilita optimizacion por margen en lugar de revenue. Actualmente
                  el modelo maximiza revenue; con costos se puede evitar recomendar
                  descuentos que destruyen margen bruto aunque suban volumen.
                </div>
              </div>
              <div className="lnext-item">
                <div className="lnext-name">Historial de precios (ynk.precios_ofertas)</div>
                <div className="lnext-desc">
                  Permite detectar eventos de markdown limpios en el historial de
                  entrenamiento. Actualmente los eventos se infieren de cambios en
                  precio de venta, lo que introduce ruido en el label del clasificador.
                </div>
              </div>
            </div>
          </div>

        </div>

        <div className="landing-footer">
          <span>YNK Pricing Optimization</span>
          <span className="lfoot-sep">&middot;</span>
          <span>Modelo v2 — XGBoost con validacion cruzada por serie de tiempo</span>
          <span className="lfoot-sep">&middot;</span>
          <span>GCP us-central1</span>
        </div>

      </div>
    </div>
  )
}

// ── Action Row ───────────────────────────────────────────────────────────────

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
            {action.subcategory && <span> &middot; {action.subcategory}</span>}
            <span> &middot; {action.store_name || action.store}</span>
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
          <span className="vel-arrow">&rarr;</span>
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

// ── CSV export ───────────────────────────────────────────────────────────────

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

// ── App ──────────────────────────────────────────────────────────────────────

function App() {
  const [view, setView] = useState('landing')
  const [brand, setBrand] = useState(BRANDS[0])
  const [actions, setActions] = useState([])
  const [alerts, setAlerts] = useState([])
  const [info, setInfo] = useState(null)
  const [loading, setLoading] = useState(false)
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
      fetch(`/alerts?brand=${b.id}&min_attrition=0.3`).then(r => r.json()),
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

  const handleEnter = useCallback(() => {
    setView('dashboard')
    loadBrand(BRANDS[0])
  }, [loadBrand])

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
  const approvedImpact = approvedItems.reduce((s, a) => s + (Number(a.rev_delta) || 0), 0)

  const handleExport = () => {
    exportCSV(approvedItems, `acciones_aprobadas_${brand.id}_${new Date().toISOString().slice(0, 10)}.csv`)
  }

  if (view === 'landing') return <LandingPage onEnter={handleEnter} />

  if (loading) return <div className="loading-screen"><div className="spinner" /><span>Cargando {brand.label}...</span></div>
  if (error) return <div className="error-screen"><AlertTriangle size={24} /><p>{error}</p><p className="error-hint">python3 -m uvicorn api.main:app --port 8080</p></div>

  return (
    <div className="app">
      <header className="header">
        <div className="header-brand">
          <button className="logo logo--btn" onClick={() => setView('landing')} title="Volver al inicio">
            YNK<span className="logo-dot">.</span>pricing
          </button>
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

      <div className="toolbar">
        <div className="search-box">
          <Search size={15} />
          <input type="text" placeholder="Buscar SKU o producto..." value={search} onChange={e => setSearch(e.target.value)} />
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
          <button className="tbtn tbtn--approve" onClick={() => bulkDecide(filtered, 'approved')}><Check size={13} /> Aprobar filtradas</button>
          <button className="tbtn tbtn--reject" onClick={() => bulkDecide(filtered, 'rejected')}><X size={13} /> Rechazar filtradas</button>
          {approvedItems.length > 0 && (
            <button className="tbtn tbtn--export" onClick={handleExport}><Download size={13} /> Exportar ({approvedItems.length})</button>
          )}
        </div>
      </div>

      {increases.length > 0 && (
        <section className="section section--increases">
          <div className="section-header"><TrendingUp size={16} /><h2>Subir precio — Recuperar margen ({increases.length})</h2></div>
          <div className="list">
            {increases.map(a => {
              const k = `${a.parent_sku}-${a.store}`
              return <ActionRow key={k} action={a} status={decisions[k] || null} onDecide={st => setDecision(k, st)} />
            })}
          </div>
        </section>
      )}

      {decreases.length > 0 && (
        <section className="section section--markdowns">
          <div className="section-header"><TrendingDown size={16} /><h2>Markdown ({decreases.length})</h2></div>
          <div className="list">
            {decreases.map(a => {
              const k = `${a.parent_sku}-${a.store}`
              return <ActionRow key={k} action={a} status={decisions[k] || null} onDecide={st => setDecision(k, st)} />
            })}
          </div>
        </section>
      )}

      {filtered.length === 0 && <div className="empty-state">No hay acciones con estos filtros</div>}

      {alerts.length > 0 && (
        <section className="section section--alerts">
          <div className="section-header"><AlertTriangle size={16} /><h2>Alertas de curva de tallas ({alerts.length})</h2></div>
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
