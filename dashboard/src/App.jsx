import { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import { Search, Download, TrendingUp, TrendingDown, ChevronDown, ChevronUp, Check, X, AlertTriangle, ArrowUpRight, ArrowDownRight, Filter, ClipboardCopy, Clock, LogOut, Settings, UserPlus, Trash2, BarChart2, Store, Tag, DollarSign, Target, Globe } from 'lucide-react'
import AnalyticsDrawer from './AnalyticsDrawer'
import StoreSidebar from './StoreSidebar'
import ManualPriceModal from './ManualPriceModal'
import ChainViewModal from './ChainViewModal'
import PlannerQueue from './PlannerQueue'
import SimulatorModal from './SimulatorModal'
import OverviewDashboard from './OverviewDashboard'
import ChannelListaView from './ChannelListaView'
import './App.css'

const BRANDS = [
  { id: 'hoka',   label: 'HOKA',   endpoint: '/pricing-actions?brand=hoka' },
  { id: 'bold',   label: 'BOLD',   endpoint: '/pricing-actions?brand=bold' },
  { id: 'bamers', label: 'BAMERS', endpoint: '/pricing-actions?brand=bamers' },
  { id: 'oakley', label: 'OAKLEY', endpoint: '/pricing-actions?brand=oakley' },
  { id: 'belsport', label: 'BELSPORT', endpoint: '/pricing-actions?brand=belsport' },
]

// Brands whose pipeline emits per-channel actions (mirrors CHANNEL_GRAIN_BRANDS
// in config/database.py). A Canal/Por-tienda sub-toggle is shown for these only.
const CHANNEL_GRAIN_BRANDS = new Set(['bold', 'bamers', 'belsport'])

const BRAND_STATS = [
  { id: 'hoka',   label: 'HOKA',   actions: 100,   skus: '3K',  stores: 4  },
  { id: 'bold',   label: 'BOLD',   actions: 1777,  skus: '57K', stores: 35 },
  { id: 'bamers', label: 'BAMERS', actions: 651,   skus: '9K',  stores: 25 },
  { id: 'oakley', label: 'OAKLEY', actions: 292,   skus: '5K',  stores: 8  },
  { id: 'belsport', label: 'BELSPORT', actions: 0, skus: '47K', stores: 66 },
]

function clp(n) {
  if (n === null || n === undefined || n === '' || isNaN(n)) return '\u2014'
  return '$' + Math.round(Number(n)).toLocaleString('es-CL')
}

function clpCompact(n) {
  if (n === null || n === undefined || n === '' || isNaN(n)) return '\u2014'
  const v = Number(n)
  if (Math.abs(v) >= 1_000_000) return '$' + (v / 1_000_000).toFixed(1) + 'M'
  if (Math.abs(v) >= 1_000) return '$' + (v / 1_000).toFixed(0) + 'K'
  return '$' + v.toLocaleString('es-CL')
}

// ── Login Screen ──────────────────────────────────────────────────────────────

function LoginScreen({ clientId, onLogin }) {
  const btnRef = useRef(null)

  useEffect(() => {
    if (!window.google?.accounts?.id || !btnRef.current) return
    google.accounts.id.initialize({
      client_id: clientId,
      callback: (response) => onLogin(response.credential),
    })
    google.accounts.id.renderButton(btnRef.current, {
      theme: 'outline', size: 'large', text: 'signin_with',
      shape: 'rectangular', width: 300,
    })
  }, [clientId, onLogin])

  return (
    <div className="login-screen">
      <div className="login-card">
        <div className="login-logo">YNK<span className="logo-dot">.</span>pricing</div>
        <p className="login-sub">Inicia sesion con tu cuenta de Google</p>
        <div ref={btnRef} className="login-btn-wrap" />
      </div>
    </div>
  )
}

// ── Landing Page ──────────────────────────────────────────────────────────────

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
            <h2 className="lcard-title">Datos pendientes con el equipo de Planning</h2>
            <p className="lcard-body">
              Hay tres fuentes de datos que mejorarian materialmente la precision del sistema.
              Cada una esta bloqueada esperando extraccion desde el equipo de Planning.
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

// ── Action Row ────────────────────────────────────────────────────────────────

function ActionRow({ action, status, onDecide, onManual, onChainView, canApprove, feedback }) {
  const [open, setOpen] = useState(false)
  const isIncrease = action.action_type === 'increase'
  const tier = action.confidence_tier || ''
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
          <div className="product-name">
            {action.product || action.parent_sku}
            {tier && <span className={`tier-badge tier-badge--${tier.toLowerCase()}`}>{tier}</span>}
            {feedback && <span className={`fb-badge fb-badge--${feedback.implemented ? 'yes' : 'no'}`}>{feedback.implemented ? 'Impl.' : 'No impl.'}</span>}
          </div>
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
          {action.click_collect_ratio > 0.1 && <span className="cc-badge">{Math.round(action.click_collect_ratio * 100)}% C&C</span>}
        </div>

        <div className={`row-delta ${delta >= 0 ? 'row-delta--pos' : 'row-delta--neg'}`}>
          {delta >= 0 ? '+' : ''}{clpCompact(delta)}
          {action.margin_pct != null && <span className={`margin-badge ${action.margin_pct < 20 ? 'margin--danger' : action.margin_pct < 40 ? 'margin--warn' : ''}`}>{action.margin_pct}%m</span>}
        </div>

        {canApprove && (
          <div className="row-actions" onClick={e => e.stopPropagation()}>
            {!status ? (
              <>
                <button className="btn-approve" onClick={() => onDecide('approved')} title="Aprobar">
                  <Check size={14} />
                </button>
                <button className="btn-reject" onClick={() => onDecide('rejected')} title="Rechazar">
                  <X size={14} />
                </button>
                <button className="btn-manual" onClick={() => onManual(action)} title="Precio manual">
                  <DollarSign size={14} />
                </button>
              </>
            ) : status === 'manual' ? (
              <button className="btn-decided btn-decided--manual" onClick={() => onDecide(null)} title="Deshacer">
                <DollarSign size={12} />
                <span>Manual</span>
              </button>
            ) : (
              <button className={`btn-decided btn-decided--${status}`} onClick={() => onDecide(null)} title="Deshacer">
                {status === 'approved' ? <Check size={12} /> : <X size={12} />}
                <span>{status === 'approved' ? 'OK' : '\u2014'}</span>
              </button>
            )}
          </div>
        )}

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
            {action.unit_cost && <div className="dcell"><span className="dcell-label">Costo unitario</span><span className="dcell-val mono">{clp(action.unit_cost)}</span></div>}
            {action.margin_pct != null && <div className="dcell"><span className="dcell-label">Margen</span><span className={`dcell-val mono ${action.margin_pct < 20 ? 'margin--danger' : action.margin_pct < 40 ? 'margin--warn' : 'margin--ok'}`}>{action.margin_pct}%</span></div>}
            {action.margin_delta != null && <div className="dcell"><span className="dcell-label">Delta margen/sem</span><span className={`dcell-val mono ${action.margin_delta >= 0 ? 'margin--ok' : 'margin--danger'}`}>{action.margin_delta >= 0 ? '+' : ''}{clpCompact(action.margin_delta)}</span></div>}
          </div>
          {action.ecomm_price != null && action.ecomm_price !== "" && (
            <div className={`detail-ecomm ${Math.abs(Number(action.ecomm_price_gap_pct) || 0) > 5 ? 'detail-ecomm--gap' : ''}`}>
              <span className="ecomm-label">Online:</span>
              <span className="ecomm-price mono">{clp(action.ecomm_price)}</span>
              {action.ecomm_price_gap_pct !== "" && action.ecomm_price_gap_pct != null && Math.abs(Number(action.ecomm_price_gap_pct)) > 2 && (
                <span className={`ecomm-gap ${Number(action.ecomm_price_gap_pct) > 0 ? 'ecomm-gap--higher' : 'ecomm-gap--lower'}`}>
                  {Number(action.ecomm_price_gap_pct) > 0 ? 'Tienda' : 'Online'} {Math.abs(Number(action.ecomm_price_gap_pct)).toFixed(0)}% {Number(action.ecomm_price_gap_pct) > 0 ? 'más caro' : 'más barato'}
                </span>
              )}
              {action.ecomm_velocity !== "" && action.ecomm_velocity != null && (
                <span className="ecomm-vel">{Number(action.ecomm_velocity).toFixed(1)} u/sem online total</span>
              )}
            </div>
          )}
          <div className="detail-reason"><AlertTriangle size={13} /> {action.reasons}</div>
          {onChainView && (
            <button className="detail-chain-btn" onClick={() => onChainView(action.parent_sku)}>
              Ver en todas las tiendas
            </button>
          )}
        </div>
      )}
    </div>
  )
}

// ── Admin Panel ───────────────────────────────────────────────────────────────

const ALL_BRANDS = ['hoka', 'bold', 'bamers', 'oakley', 'belsport']

function AdminPanel({ authFetch, onClose }) {
  const [cfg, setCfg] = useState(null)
  const [email, setEmail] = useState('')
  const [role, setRole] = useState('viewer')
  const [brands, setBrands] = useState([])
  const [name, setName] = useState('')
  const [saving, setSaving] = useState(false)

  const load = useCallback(() => {
    authFetch('/admin/users').then(r => r.json()).then(setCfg).catch(() => {})
  }, [authFetch])

  useEffect(() => { load() }, [load])

  const handleAdd = () => {
    if (!email) return
    setSaving(true)
    authFetch('/admin/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, role, brands: ['brand_manager', 'planner'].includes(role) ? brands : null, name }),
    }).then(() => {
      setEmail(''); setName(''); setRole('viewer'); setBrands([])
      load()
    }).finally(() => setSaving(false))
  }

  const handleDelete = (userEmail) => {
    if (!confirm(`Eliminar ${userEmail}?`)) return
    authFetch(`/admin/users?email=${encodeURIComponent(userEmail)}`, { method: 'DELETE' })
      .then(() => load())
  }

  const toggleBrand = (b) => {
    setBrands(prev => prev.includes(b) ? prev.filter(x => x !== b) : [...prev, b])
  }

  if (!cfg) return null

  const users = Object.entries(cfg.users || {}).sort((a, b) => a[0].localeCompare(b[0]))

  return (
    <div className="admin-overlay" onClick={onClose}>
      <div className="admin-panel" onClick={e => e.stopPropagation()}>
        <div className="admin-header">
          <h2>Administrar usuarios</h2>
          <button className="admin-close" onClick={onClose}><X size={18} /></button>
        </div>

        <div className="admin-body">
          <table className="admin-table">
            <thead>
              <tr><th>Email</th><th>Nombre</th><th>Rol</th><th>Marcas</th><th></th></tr>
            </thead>
            <tbody>
              {users.map(([uemail, u]) => (
                <tr key={uemail}>
                  <td className="admin-email">{uemail}</td>
                  <td>{u.name || '\u2014'}</td>
                  <td><span className={`role-tag role-tag--${u.role}`}>{u.role}</span></td>
                  <td>{u.brands ? u.brands.join(', ') : 'todas'}</td>
                  <td>
                    <button className="admin-del" onClick={() => handleDelete(uemail)} title="Eliminar">
                      <Trash2 size={13} />
                    </button>
                  </td>
                </tr>
              ))}
              {users.length === 0 && (
                <tr><td colSpan={5} className="admin-empty">No hay usuarios configurados. Los de @{(cfg.allowed_domains || []).join(', @')} entran como viewer.</td></tr>
              )}
            </tbody>
          </table>

          <div className="admin-add">
            <h3><UserPlus size={14} /> Agregar usuario</h3>
            <div className="admin-form">
              <input placeholder="email@yaneken.cl" value={email} onChange={e => setEmail(e.target.value)} />
              <input placeholder="Nombre (opcional)" value={name} onChange={e => setName(e.target.value)} />
              <select value={role} onChange={e => setRole(e.target.value)}>
                <option value="admin">Admin</option>
                <option value="brand_manager">Brand Manager</option>
                <option value="planner">Planner</option>
                <option value="viewer">Viewer</option>
              </select>
              {['brand_manager', 'planner'].includes(role) && (
                <div className="admin-brands">
                  {ALL_BRANDS.map(b => (
                    <label key={b} className="admin-brand-check">
                      <input type="checkbox" checked={brands.includes(b)} onChange={() => toggleBrand(b)} />
                      {b.toUpperCase()}
                    </label>
                  ))}
                </div>
              )}
              <button className="admin-save" onClick={handleAdd} disabled={saving || !email}>
                {saving ? 'Guardando...' : 'Agregar'}
              </button>
            </div>
          </div>

          <div className="admin-domains">
            <span className="admin-domains-label">Dominios permitidos (viewer por defecto):</span>
            <span className="admin-domains-val">{(cfg.allowed_domains || []).map(d => `@${d}`).join(', ')}</span>
          </div>
        </div>
      </div>
    </div>
  )
}

// ── App ───────────────────────────────────────────────────────────────────────

function App() {
  // Auth state
  const [authConfig, setAuthConfig] = useState(null)
  const [user, setUser] = useState(null)
  const [authToken, setAuthToken] = useState(() => sessionStorage.getItem('ynk_token') || '')

  // Dashboard state
  const [view, setView] = useState('landing')
  const [brand, setBrand] = useState(BRANDS[0])
  const [actions, setActions] = useState([])
  const [alerts, setAlerts] = useState([])
  const [crossStoreAlerts, setCrossStoreAlerts] = useState([])
  const [compAnalytics, setCompAnalytics] = useState(null)
  const [aiOpen, setAiOpen] = useState(false)
  const [aiQuestion, setAiQuestion] = useState('')
  const [aiMessages, setAiMessages] = useState([])
  const [aiLoading, setAiLoading] = useState(false)
  const [performanceData, setPerformanceData] = useState(null)
  const [outcomeDetails, setOutcomeDetails] = useState([])
  const [auditLog, setAuditLog] = useState([])
  const [feedback, setFeedback] = useState({})
  const [info, setInfo] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [search, setSearch] = useState('')
  const [filterStore, setFilterStore] = useState('all')
  const [filterUrgency, setFilterUrgency] = useState('all')
  const [filterCategory, setFilterCategory] = useState('all')
  const [decisions, setDecisions] = useState({})
  const [week, setWeek] = useState('')
  const [copyMsg, setCopyMsg] = useState('')
  const [showAdmin, setShowAdmin] = useState(false)
  const [showExportConfirm, setShowExportConfirm] = useState(false)
  const [showAnalytics, setShowAnalytics] = useState(false)
  const [showSimulator, setShowSimulator] = useState(false)
  const [viewMode, setViewMode] = useState('list')  // 'list' | 'tiendas' | 'marcas'
  const [filterVendor, setFilterVendor] = useState(null)
  const [manualAction, setManualAction] = useState(null)  // action object for ManualPriceModal
  const [chainSku, setChainSku] = useState(null)          // parent_sku for ChainViewModal
  const [toast, setToast] = useState(null)
  const [filterStatus, setFilterStatus] = useState('all')  // all | pending | approved | rejected
  const [sortBy, setSortBy] = useState('urgency')           // urgency | revenue | confidence | store
  const [page, setPage] = useState(1)
  const PAGE_SIZE = 50

  const MULTI_VENDOR_BRANDS = ['bold', 'bamers', 'belsport']
  const isMultiVendorBrand = brand && MULTI_VENDOR_BRANDS.includes(brand.id)

  // Permissions
  const canApprove = user?.permissions?.includes('approve')
  const canPlan = user?.permissions?.includes('plan')
  const canExport = user?.permissions?.includes('export')
  const canAudit = user?.permissions?.includes('audit')
  const [showPlannerQueue, setShowPlannerQueue] = useState(false)
  // 'store' (per-parent x store, legacy) or 'channel' (per-parent x bm/ecomm).
  // Default is 'channel' because the whole point of the restructure is to
  // reduce action volume for BMs — the legacy per-store view is now the
  // escape hatch, not the primary. Brands not in CHANNEL_GRAIN_BRANDS are
  // always forced to 'store' (see loadBrand).
  const [grain, setGrain] = useState('channel')

  // Brands visible to this user
  const visibleBrands = useMemo(() => {
    if (!user?.brands) return BRANDS
    return BRANDS.filter(b => user.brands.includes(b.id))
  }, [user])

  // ── Auth helpers ──

  const authFetch = useCallback((url, options = {}) => {
    const token = authToken || sessionStorage.getItem('ynk_token')
    if (token) {
      options.headers = { ...options.headers, 'Authorization': `Bearer ${token}` }
    }
    return fetch(url, options).then(r => {
      if (r.status === 401) {
        sessionStorage.removeItem('ynk_token')
        setAuthToken('')
        setUser(null)
      }
      return r
    })
  }, [authToken])

  const fetchUser = useCallback((token) => {
    fetch('/auth/me', { headers: { 'Authorization': `Bearer ${token}` } })
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(u => setUser(u))
      .catch(() => {
        sessionStorage.removeItem('ynk_token')
        setAuthToken('')
      })
  }, [])

  // Fetch auth config on mount
  useEffect(() => {
    fetch('/auth/config')
      .then(r => r.json())
      .then(cfg => {
        setAuthConfig(cfg)
        if (!cfg.required) {
          setUser({ email: 'dev@local', name: 'Developer', picture: '', role: 'admin',
                    permissions: ['approve', 'audit', 'export', 'manage', 'plan', 'read'], brands: null })
        } else {
          const saved = sessionStorage.getItem('ynk_token')
          if (saved) fetchUser(saved)
        }
      })
      .catch(() => setAuthConfig({ required: false, client_id: '' }))
  }, [fetchUser])

  const handleLogin = useCallback((credential) => {
    sessionStorage.setItem('ynk_token', credential)
    setAuthToken(credential)
    fetchUser(credential)
  }, [fetchUser])

  const handleSignOut = useCallback(() => {
    sessionStorage.removeItem('ynk_token')
    setAuthToken('')
    setUser(null)
    if (window.google?.accounts?.id) google.accounts.id.disableAutoSelect()
  }, [])

  const toastTimerRef = useRef(null)
  const showToast = useCallback((msg, type = 'info') => {
    clearTimeout(toastTimerRef.current)
    setToast({ msg, type })
    toastTimerRef.current = setTimeout(() => setToast(null), 3500)
  }, [])

  // ── Data loading ──

  const loadBrand = useCallback((b, grainOverride) => {
    setLoading(true)
    setError(null)
    setActions([])
    setDecisions({})
    setAuditLog([])
    setSearch('')
    setFilterStore('all')
    setFilterUrgency('all')
    setFilterCategory('all')
    setFilterStatus('all')
    setShowAnalytics(false)
    setViewMode('list')
    setFilterVendor(null)
    setSortBy('urgency')
    setPage(1)
    setBrand(b)

    // A brand that doesn't publish channel-grain data can never be in channel mode.
    const effectiveGrain = CHANNEL_GRAIN_BRANDS.has(b.id)
      ? (grainOverride ?? grain)
      : 'store'
    if (effectiveGrain !== grain) setGrain(effectiveGrain)

    const actionsUrl = effectiveGrain === 'channel'
      ? `/pricing-actions?brand=${b.id}&grain=channel`
      : b.endpoint
    const decisionsUrl = effectiveGrain === 'channel'
      ? `/decisions?brand=${b.id}&grain=channel`
      : `/decisions?brand=${b.id}`

    Promise.all([
      authFetch(actionsUrl).then(r => r.json()),
      authFetch(`/alerts?brand=${b.id}&min_attrition=0.3`).then(r => r.json()),
      authFetch(`/model/info?brand=${b.id}`).then(r => r.json()),
      authFetch(decisionsUrl).then(r => r.json()),
      canAudit ? authFetch(`/audit?brand=${b.id}&limit=50`).then(r => r.json()).catch(() => ({ items: [] })) : Promise.resolve({ items: [] }),
      authFetch(`/feedback?brand=${b.id}`).then(r => r.json()).catch(() => ({ items: {} })),
      authFetch(`/alerts/cross-store?brand=${b.id}`).then(r => r.json()).catch(() => ({ items: [] })),
    ]).then(([ad, al, mi, dec, aud, fb, cs]) => {
      setActions(ad.items || [])
      setAlerts(al.items || [])
      setCrossStoreAlerts(cs.items || [])
      // Fetch performance + competitor analytics
      authFetch(`/analytics/competitors/${b.id}`).then(r => r.json()).then(ca => {
        setCompAnalytics(ca?.available ? ca : null)
      }).catch(() => {})
      authFetch(`/analytics/${b.id}`).then(r => r.json()).then(an => {
        setPerformanceData(an?.prediccion_vs_real || null)
      }).catch(() => {})
      authFetch(`/analytics/outcomes/${b.id}`).then(r => r.json()).then(od => {
        setOutcomeDetails(od?.items || [])
      }).catch(() => {})
      setInfo(mi)
      setWeek(ad.week || '')
      setAuditLog(aud.items || [])
      setFeedback(fb.items || {})
      const decMap = {}
      Object.entries(dec.decisions || {}).forEach(([k, v]) => {
        decMap[k] = v.status
      })
      setDecisions(decMap)
      setLoading(false)
    }).catch(() => {
      setError('No se pudo conectar con la API.')
      setLoading(false)
    })
  }, [authFetch, canAudit, grain])

  const handleEnter = useCallback(() => {
    setView('dashboard')
    loadBrand(visibleBrands[0] || BRANDS[0])
  }, [loadBrand, visibleBrands])

  // Sub-toggle between channel/store grain. Only allowed for brands that
  // publish channel-grain CSVs (CHANNEL_GRAIN_BRANDS); for others we ignore.
  // Switching to channel forces viewMode='list' because the sidebar views
  // (Tiendas/Marcas) and analytics tabs are per-store-grain constructs.
  const toggleGrain = useCallback((newGrain) => {
    if (!CHANNEL_GRAIN_BRANDS.has(brand?.id) && newGrain === 'channel') return
    if (newGrain === grain) return
    if (newGrain === 'channel') setViewMode('list')
    setGrain(newGrain)
    loadBrand(brand, newGrain)
  }, [brand, grain, loadBrand])

  // ── Filters ──

  const stores = useMemo(() =>
    [...new Set(actions.map(a => a.store_name || a.store).filter(Boolean))].sort(),
    [actions]
  )
  const categories = useMemo(() =>
    [...new Set(actions.map(a => a.subcategory).filter(Boolean))].sort(),
    [actions]
  )

  // Store roster for sidebar navigation
  const storeRoster = useMemo(() => {
    const map = {}
    actions.forEach(a => {
      const name = a.store_name || a.store
      if (!name) return
      if (!map[name]) map[name] = { key: a.store, name, total: 0, pending: 0, decided: 0, highCount: 0, medCount: 0, revDelta: 0, ccSum: 0, ccCount: 0 }
      const s = map[name]
      s.total++
      const dec = decisions[`${a.parent_sku}-${a.store}`]
      if (dec) { s.decided++ } else { s.pending++ }
      if (a.urgency === 'HIGH') s.highCount++
      if (a.urgency === 'MEDIUM') s.medCount++
      s.revDelta += Number(a.rev_delta) || 0
      if (a.click_collect_ratio > 0) { s.ccSum += a.click_collect_ratio; s.ccCount++ }
    })
    return Object.values(map).sort((a, b) => b.pending - a.pending || b.total - a.total)
  }, [actions, decisions])

  // Vendor brand roster for sidebar navigation (multi-brand only)
  // Decide whether to group sidebar by vendor brand or subcategory:
  // use vendor if multi-vendor brand with >1 distinct vendor, otherwise use subcategory
  const useVendorGrouping = useMemo(() => {
    if (!isMultiVendorBrand) return false
    const vendors = new Set(actions.map(a => a.vendor_brand || 'Other'))
    return vendors.size > 1
  }, [actions, isMultiVendorBrand])

  const groupRoster = useMemo(() => {
    const map = {}
    actions.forEach(a => {
      const key = useVendorGrouping ? (a.vendor_brand || 'Other') : (a.subcategory || 'Other')
      if (!map[key]) map[key] = { key, name: key, total: 0, pending: 0, decided: 0, highCount: 0, medCount: 0, revDelta: 0 }
      const s = map[key]
      s.total++
      const dec = decisions[`${a.parent_sku}-${a.store}`]
      if (dec) { s.decided++ } else { s.pending++ }
      if (a.urgency === 'HIGH') s.highCount++
      if (a.urgency === 'MEDIUM') s.medCount++
      s.revDelta += Number(a.rev_delta) || 0
    })
    return Object.values(map).sort((a, b) => b.pending - a.pending || b.total - a.total)
  }, [actions, decisions, useVendorGrouping])

  const filtered = useMemo(() => {
    const q = search.toLowerCase()
    let result = actions.filter(a => {
      if (filterStore !== 'all' && (a.store_name || a.store) !== filterStore) return false
      if (filterVendor) {
        const val = useVendorGrouping ? (a.vendor_brand || 'Other') : (a.subcategory || 'Other')
        if (val !== filterVendor) return false
      }
      if (filterUrgency !== 'all') {
        if (filterUrgency === 'INCREASE' && a.action_type !== 'increase') return false
        if (filterUrgency !== 'INCREASE' && (a.urgency !== filterUrgency || a.action_type === 'increase')) return false
      }
      if (filterCategory !== 'all' && a.subcategory !== filterCategory) return false
      if (filterStatus !== 'all') {
        const status = decisions[`${a.parent_sku}-${a.store}`] || 'pending'
        if (filterStatus !== status) return false
      }
      if (q && !(
        (a.parent_sku || '').toLowerCase().includes(q) ||
        (a.product || '').toLowerCase().includes(q) ||
        (a.sku || '').toLowerCase().includes(q)
      )) return false
      return true
    })

    // Sort
    const urgencyOrder = { 'INCREASE': -1, 'HIGH': 0, 'MEDIUM': 1, 'LOW': 2 }
    if (sortBy === 'urgency') {
      result.sort((a, b) => (urgencyOrder[a.urgency] ?? 3) - (urgencyOrder[b.urgency] ?? 3) || (Number(b.rev_delta) || 0) - (Number(a.rev_delta) || 0))
    } else if (sortBy === 'revenue') {
      result.sort((a, b) => Math.abs(Number(b.rev_delta) || 0) - Math.abs(Number(a.rev_delta) || 0))
    } else if (sortBy === 'confidence') {
      result.sort((a, b) => (Number(b.model_confidence) || 0) - (Number(a.model_confidence) || 0))
    } else if (sortBy === 'store') {
      result.sort((a, b) => (a.store_name || a.store || '').localeCompare(b.store_name || b.store || ''))
    }
    return result
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [actions, filterStore, filterVendor, filterUrgency, filterCategory, filterStatus, search, sortBy, useVendorGrouping,
      // Only re-filter on decisions when status filter is active (avoids re-render on every approve/reject)
      ...(filterStatus !== 'all' ? [decisions] : [])])

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE))
  useEffect(() => { if (page > totalPages) setPage(totalPages) }, [page, totalPages])
  const paged = useMemo(() => filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE), [filtered, page])
  const increases = useMemo(() => paged.filter(a => a.action_type === 'increase'), [paged])
  const decreases = useMemo(() => paged.filter(a => a.action_type !== 'increase'), [paged])

  // ── Decisions ──

  const setDecision = useCallback((key, status) => {
    setDecisions(prev => {
      const next = { ...prev }
      if (status === null) delete next[key]
      else next[key] = status
      return next
    })
    if (week) {
      authFetch('/decisions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: brand.id, week, key, status, grain }),
      }).then(r => { if (!r.ok) showToast('Error guardando decision', 'error') })
        .catch(() => showToast('Error de conexion', 'error'))
    }
  }, [brand, week, authFetch, grain])

  // Manual price confirm: store "manual" status locally, send manual_price + impact to API.
  // At channel grain the key is {parent_sku}-{bm|ecomm} (set by the caller).
  const handleManualConfirm = useCallback((action, snappedPrice, impact) => {
    const key = grain === 'channel'
      ? `${action.parent_sku}-${action.channel}`
      : `${action.parent_sku}-${action.store}`
    setDecisions(prev => ({ ...prev, [key]: 'manual' }))
    setManualAction(null)
    if (week) {
      authFetch('/decisions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: brand.id, week, key,
          status: 'manual',
          manual_price: snappedPrice,
          estimated_impact: impact,
          grain,
        }),
      }).then(r => { if (!r.ok) showToast('Error guardando precio manual', 'error') })
        .catch(() => showToast('Error de conexion', 'error'))
    }
  }, [brand, week, authFetch, grain])

  // Chain-wide approve: apply to all/ecomm/bm stores for a parent SKU
  const handleChainApply = useCallback((chainKey, scope) => {
    const parentSku = chainKey.split('-chain-')[0]
    // Optimistically set all matching store keys as approved
    setDecisions(prev => {
      const next = { ...prev }
      actions.forEach(a => {
        if (a.parent_sku !== parentSku) return
        const isEc = String(a.store).toUpperCase().startsWith('AB')
        if (scope === 'ecomm' && !isEc) return
        if (scope === 'bm' && isEc) return
        const k = `${a.parent_sku}-${a.store}`
        if (!next[k]) next[k] = 'approved'
      })
      return next
    })
    // Send to API with chain_scope
    if (week) {
      authFetch('/decisions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: brand.id, week, key: chainKey, status: 'approved', chain_scope: scope }),
      }).then(r => { if (!r.ok) showToast('Error aplicando cadena', 'error') })
        .catch(() => showToast('Error de conexion', 'error'))
    }
    setChainSku(null)
  }, [actions, brand, week, authFetch])

  const bulkDecide = useCallback((items, status) => {
    const keyFor = grain === 'channel'
      ? (a => `${a.parent_sku}-${a.channel}`)
      : (a => `${a.parent_sku}-${a.store}`)
    const keys = items.map(keyFor)
    setDecisions(prev => {
      const next = { ...prev }
      keys.forEach(k => { if (!next[k]) next[k] = status })
      return next
    })
    if (week && keys.length > 0) {
      authFetch('/decisions/bulk', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: brand.id, week, keys, status, grain }),
      }).then(r => { if (!r.ok) showToast('Error guardando decisiones', 'error') })
        .catch(() => showToast('Error de conexion', 'error'))
    }
  }, [brand, week, authFetch, grain])

  // Sidebar: approve all pending for a store/vendor (uses full actions, not filtered)
  const handleSidebarApprove = useCallback((groupName, groupField) => {
    const pending = actions.filter(a => {
      let name
      if (groupField === 'store') name = a.store_name || a.store
      else name = useVendorGrouping ? (a.vendor_brand || 'Other') : (a.subcategory || 'Other')
      return name === groupName && !decisions[`${a.parent_sku}-${a.store}`]
    })
    if (pending.length > 100 && !confirm(`Aprobar ${pending.length} acciones pendientes?`)) return
    bulkDecide(pending, 'approved')
  }, [actions, decisions, bulkDecide, useVendorGrouping])

  // Sidebar: select a store/vendor
  const handleSidebarSelect = useCallback((name) => {
    if (viewMode === 'tiendas') {
      setFilterStore(name)
    } else if (viewMode === 'marcas') {
      setFilterVendor(name)
    }
    setPage(1)
  }, [viewMode])

  // Switch view mode
  const switchViewMode = useCallback((mode) => {
    setViewMode(mode)
    setFilterStore('all')
    setFilterVendor(null)
    setFilterStatus('all')
    setFilterUrgency('all')
    setFilterCategory('all')
    setSearch('')
    setPage(1)
  }, [])

  const reviewedCount = Object.keys(decisions).length
  // Action → decision key. At channel grain the row has no `store`, only `channel`.
  const actionKey = useCallback((a) => (
    grain === 'channel' ? `${a.parent_sku}-${a.channel}` : `${a.parent_sku}-${a.store}`
  ), [grain])
  const approvedItems = actions.filter(a => decisions[actionKey(a)] === 'approved')
  const approvedImpact = approvedItems.reduce((s, a) => s + (Number(a.rev_delta) || 0), 0)

  // ── Export ──

  const rejectedCount = actions.filter(a => decisions[`${a.parent_sku}-${a.store}`] === 'rejected').length
  const undecidedCount = actions.length - reviewedCount

  const doExportExcel = () => {
    setShowExportConfirm(false)
    authFetch(`/export/price-changes?brand=${brand.id}`)
      .then(r => {
        if (!r.ok) throw new Error('Export failed')
        return r.blob()
      })
      .then(blob => {
        const a = document.createElement('a')
        a.href = URL.createObjectURL(blob)
        a.download = `cambios_precio_${brand.id}_${week}.xlsx`
        a.click()
        URL.revokeObjectURL(a.href)
        showToast(`Excel exportado: ${approvedItems.length} acciones`, 'ok')
      })
      .catch(() => showToast('Error generando export', 'error'))
  }

  const doExportText = () => {
    setShowExportConfirm(false)
    authFetch(`/export/price-changes?brand=${brand.id}&format=text`)
      .then(r => {
        if (!r.ok) return r.text().then(t => { throw new Error(t) })
        return r.text()
      })
      .then(text => {
        navigator.clipboard.writeText(text)
        showToast('Texto copiado al portapapeles', 'ok')
      })
      .catch(e => showToast(e.message || 'Error', 'error'))
  }

  // ── Auth gate ──

  if (!authConfig) return <div className="loading-screen"><div className="spinner" /><span>Cargando...</span></div>
  if (authConfig.required && !user) return <LoginScreen clientId={authConfig.client_id} onLogin={handleLogin} />

  // ── Render ──

  if (view === 'landing') return (
    <OverviewDashboard
      authFetch={authFetch}
      user={user}
      onSelectBrand={(brandId) => {
        const b = BRANDS.find(x => x.id === brandId)
        if (b) { setView('dashboard'); loadBrand(b) }
      }}
    />
  )

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
            {visibleBrands.map(b => (
              <button
                key={b.id}
                className={`brand-tab ${b.id === brand.id ? 'brand-tab--active' : ''}`}
                onClick={() => { if (b.id !== brand.id) loadBrand(b) }}
              >
                {b.label}
              </button>
            ))}
          </nav>
          {week && (
            <span className="header-freshness" title="Semana de datos">
              Sem. {week}
              {undecidedCount > 0 && (
                <span className="header-freshness__pending"> · {undecidedCount} sin revisar</span>
              )}
            </span>
          )}
        </div>
        <div className="header-meta">
          <div className="header-utils">
            <button
              className={`util-btn ${showAnalytics ? 'util-btn--active' : ''}`}
              onClick={() => setShowAnalytics(!showAnalytics)}
              title="Salud del modelo (classifier, elasticidad, ciclo de vida)"
            >
              <BarChart2 size={13} /> Salud del modelo
            </button>
            <button className="util-btn" onClick={() => setShowSimulator(true)} title="Simulador de promociones">
              <TrendingUp size={13} /> Simulador
            </button>
            {canPlan && (
              <button
                className={`util-btn ${showPlannerQueue ? 'util-btn--active' : ''}`}
                onClick={() => setShowPlannerQueue(!showPlannerQueue)}
                title="Cola de aprobacion para planner"
              >
                Cola Planner
              </button>
            )}
          </div>
          {info && <span className="meta-tag">v{info.version} AUC {info.classifier?.avg_auc?.toFixed(3)}</span>}
          {user && (
            <div className="header-user">
              {user.picture && <img src={user.picture} className="user-avatar" alt="" referrerPolicy="no-referrer" />}
              <span className="user-name">{user.name}</span>
              <span className="role-tag">{user.role}</span>
              {user.permissions?.includes('manage') && (
                <button className="btn-signout" onClick={() => setShowAdmin(true)} title="Administrar usuarios"><Settings size={14} /></button>
              )}
              {authConfig.required && (
                <button className="btn-signout" onClick={handleSignOut} title="Cerrar sesion"><LogOut size={14} /></button>
              )}
            </div>
          )}
        </div>
      </header>

      {week && showPlannerQueue && (
        <PlannerQueue brand={brand?.id} grain={grain} authFetch={authFetch} showToast={showToast} />
      )}

      {week && !showPlannerQueue && (<>

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
          <div className="kpi-label">Rev aprobado</div>
          <div className="kpi-bar kpi-bar--impact" />
        </div>
        {(() => {
          const marginItems = approvedItems.filter(a => a.margin_delta != null)
          if (marginItems.length === 0) return null
          const totalMargin = marginItems.reduce((s, a) => s + (Number(a.margin_delta) || 0), 0)
          return (
            <div className="kpi kpi--margin">
              <div className={`kpi-value ${totalMargin >= 0 ? 'kpi-value--pos' : 'kpi-value--neg'}`}>{totalMargin >= 0 ? '+' : ''}{clpCompact(totalMargin)}</div>
              <div className="kpi-label">Margen aprobado</div>
              <div className="kpi-bar kpi-bar--margin" />
            </div>
          )
        })()}
      </div>

      {/*
        3-section IA: Acciones (primary list — the daily work) / Rendimiento
        (model health + outcomes) / Competencia (market position). The previous
        Lista/Tiendas/Marcas/Producto all fed the Acciones stream with different
        groupings — they move to a quieter sub-row below.
      */}
      {(() => {
        const isAcciones = ['list', 'tiendas', 'marcas', 'producto'].includes(viewMode)
        const isRend = viewMode === 'performance'
        const isComp = viewMode === 'competencia'
        return (
          <>
            <div className="section-tabs">
              <button
                className={`section-tab ${isAcciones ? 'section-tab--active' : ''}`}
                onClick={() => switchViewMode('list')}
              >
                Acciones
                {actions.length > 0 && <span className="section-tab__badge">{actions.length}</span>}
              </button>
              <button
                className={`section-tab ${isRend ? 'section-tab--active' : ''}`}
                onClick={() => switchViewMode('performance')}
              >
                Rendimiento
              </button>
              <button
                className={`section-tab ${isComp ? 'section-tab--active' : ''}`}
                onClick={() => switchViewMode('competencia')}
              >
                Competencia
              </button>
            </div>

            {/* Sub-grouping row — only shows inside Acciones, only at store grain */}
            {isAcciones && grain !== 'channel' && (
              <div className="sub-grouping">
                <span className="sub-grouping__label">Agrupar:</span>
                <div className="sub-grouping__tabs">
                  <button className={`sub-tab ${viewMode === 'list' ? 'sub-tab--active' : ''}`} onClick={() => switchViewMode('list')}>
                    Lista
                  </button>
                  <button className={`sub-tab ${viewMode === 'tiendas' ? 'sub-tab--active' : ''}`} onClick={() => switchViewMode('tiendas')}>
                    <Store size={12} /> Por tienda
                  </button>
                  <button className={`sub-tab ${viewMode === 'marcas' ? 'sub-tab--active' : ''}`} onClick={() => switchViewMode('marcas')}>
                    <Tag size={12} /> Por {useVendorGrouping ? 'marca' : 'categoria'}
                  </button>
                  <button className={`sub-tab ${viewMode === 'producto' ? 'sub-tab--active' : ''}`} onClick={() => switchViewMode('producto')}>
                    <BarChart2 size={12} /> Por producto
                  </button>
                </div>
              </div>
            )}
          </>
        )
      })()}

      <div className={`dashboard-body ${(viewMode === 'tiendas' || viewMode === 'marcas') ? 'dashboard-body--sidebar' : ''}`}>
      {viewMode === 'tiendas' && (
        <StoreSidebar roster={storeRoster} activeItem={filterStore !== 'all' ? filterStore : null}
                      onSelect={name => handleSidebarSelect(name)} onApprove={name => handleSidebarApprove(name, 'store')}
                      canApprove={canApprove} title="Tiendas" />
      )}
      {viewMode === 'marcas' && (
        <StoreSidebar roster={groupRoster} activeItem={filterVendor}
                      onSelect={name => handleSidebarSelect(name)} onApprove={name => handleSidebarApprove(name, 'vendor')}
                      canApprove={canApprove} title={useVendorGrouping ? 'Marcas' : 'Categorias'} />
      )}
      <div className="main-content">

      {viewMode === 'producto' ? (() => {
        // Group actions by parent_sku, split into B&M and Ecom channels
        const isEcomm = (store) => String(store).toUpperCase().startsWith('AB')
        const q = search.toLowerCase()
        const grouped = {}
        // Apply all filters at action level before grouping
        const filteredActions = actions.filter(a => {
          if (q && !(
            (a.parent_sku || '').toLowerCase().includes(q) ||
            (a.product || '').toLowerCase().includes(q) ||
            (a.sku || '').toLowerCase().includes(q)
          )) return false
          if (filterUrgency !== 'all') {
            if (filterUrgency === 'INCREASE' && a.action_type !== 'increase') return false
            if (filterUrgency !== 'INCREASE' && (a.urgency !== filterUrgency || a.action_type === 'increase')) return false
          }
          if (filterCategory !== 'all' && a.subcategory !== filterCategory) return false
          if (filterStatus !== 'all') {
            const status = decisions[`${a.parent_sku}-${a.store}`] || 'pending'
            if (filterStatus !== status) return false
          }
          return true
        })
        filteredActions.forEach(a => {
          const sku = a.parent_sku
          if (!grouped[sku]) grouped[sku] = { sku, product: a.product, category: a.category, subcategory: a.subcategory, vendor_brand: a.vendor_brand, age: 0, bm: [], ecom: [] }
          grouped[sku].age = Math.max(grouped[sku].age, Number(a.product_age_weeks) || 0)
          const channel = isEcomm(a.store) ? 'ecom' : 'bm'
          grouped[sku][channel].push(a)
        })
        let skuList = Object.values(grouped)
          .filter(g => g.bm.length > 0 || g.ecom.length > 0)

        // Apply product-level filters (age, vendor)
        if (filterVendor) {
          skuList = skuList.filter(g => (g.vendor_brand || 'Other') === filterVendor)
        }
        const prodFilterAge = filterStore  // reuse filterStore state for age filter in producto mode
        if (prodFilterAge !== 'all') {
          const [lo, hi] = prodFilterAge.split('-').map(Number)
          skuList = skuList.filter(g => g.age >= lo && (hi ? g.age <= hi : true))
        }

        skuList.sort((a, b) => {
          if (sortBy === 'revenue') {
            const aRev = [...a.bm, ...a.ecom].reduce((s, x) => s + (Number(x.rev_delta) || 0), 0)
            const bRev = [...b.bm, ...b.ecom].reduce((s, x) => s + (Number(x.rev_delta) || 0), 0)
            return Math.abs(bRev) - Math.abs(aRev)
          }
          if (sortBy === 'age') return (b.age || 0) - (a.age || 0)
          // Default: vendor brand → subcategory → rev delta
          const aBrand = (a.vendor_brand || 'ZZZ').toLowerCase()
          const bBrand = (b.vendor_brand || 'ZZZ').toLowerCase()
          if (aBrand !== bBrand) return aBrand.localeCompare(bBrand)
          const catPriority = (cat) => {
            const c = (cat || '').toLowerCase()
            if (c.includes('sneaker') || c.includes('running') || c.includes('zapatilla') || c.includes('footwear')) return 0
            if (c.includes('outdoor') || c.includes('trail')) return 1
            return 2
          }
          const aCat = catPriority(a.subcategory || a.category)
          const bCat = catPriority(b.subcategory || b.category)
          if (aCat !== bCat) return aCat - bCat
          const aRev = [...a.bm, ...a.ecom].reduce((s, x) => s + (Number(x.rev_delta) || 0), 0)
          const bRev = [...b.bm, ...b.ecom].reduce((s, x) => s + (Number(x.rev_delta) || 0), 0)
          return bRev - aRev
        })

        // Product-level vendor brands for filter dropdown
        const prodVendors = [...new Set(Object.values(grouped).map(g => g.vendor_brand || 'Other'))].sort()

        // Pagination
        const PROD_PAGE = 50
        const prodTotal = Math.max(1, Math.ceil(skuList.length / PROD_PAGE))
        const prodPage = Math.min(page, prodTotal)
        const pagedSkus = skuList.slice((prodPage - 1) * PROD_PAGE, prodPage * PROD_PAGE)

        const median = arr => { const s = arr.slice().sort((a,b) => a-b); return s[Math.floor(s.length/2)] || 0 }
        const clp = n => n ? '$' + Math.round(n).toLocaleString('es-CL') : '—'

        return (
          <div className="prod-page">
            <div className="toolbar">
              <div className="search-box">
                <Search size={15} />
                <input type="text" placeholder="Buscar SKU o producto..." value={search} onChange={e => { setSearch(e.target.value); setPage(1) }} />
              </div>
              <div className="filter-group">
                <Filter size={14} />
                <select value={filterStatus} onChange={e => { setFilterStatus(e.target.value); setPage(1) }}>
                  <option value="all">Todo estado</option>
                  <option value="pending">Sin revisar</option>
                  <option value="approved">Aprobadas</option>
                  <option value="rejected">Rechazadas</option>
                  <option value="manual">Precio manual</option>
                </select>
                <select value={filterUrgency} onChange={e => { setFilterUrgency(e.target.value); setPage(1) }}>
                  <option value="all">Toda urgencia</option>
                  <option value="INCREASE">Subir precio</option>
                  <option value="HIGH">Alta</option>
                  <option value="MEDIUM">Media</option>
                  <option value="LOW">Baja</option>
                </select>
                {prodVendors.length > 1 && (
                  <select value={filterVendor || ''} onChange={e => { setFilterVendor(e.target.value || null); setPage(1) }}>
                    <option value="">Toda marca</option>
                    {prodVendors.map(v => <option key={v} value={v}>{v}</option>)}
                  </select>
                )}
                {categories.length > 1 && (
                  <select value={filterCategory} onChange={e => { setFilterCategory(e.target.value); setPage(1) }}>
                    <option value="all">Toda categoria</option>
                    {categories.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                )}
                <select value={prodFilterAge} onChange={e => { setFilterStore(e.target.value); setPage(1) }}>
                  <option value="all">Toda antiguedad</option>
                  <option value="0-4">0-4 semanas (nuevo)</option>
                  <option value="5-12">5-12 semanas</option>
                  <option value="13-26">13-26 semanas</option>
                  <option value="27-52">27-52 semanas</option>
                  <option value="53-Infinity">53+ semanas</option>
                </select>
                <select value={sortBy} onChange={e => { setSortBy(e.target.value); setPage(1) }}>
                  <option value="urgency">Ordenar: Marca</option>
                  <option value="revenue">Ordenar: Impacto</option>
                  <option value="age">Ordenar: Antiguedad</option>
                </select>
              </div>
              <div className="toolbar-actions">
                <span className="result-count">{skuList.length} productos</span>
              </div>
            </div>

            {prodTotal > 1 && (
              <div className="pagination">
                <button disabled={prodPage <= 1} onClick={() => setPage(p => p - 1)}>&laquo; Anterior</button>
                <span className="page-info">Pagina {prodPage} de {prodTotal} ({skuList.length} productos)</span>
                <button disabled={prodPage >= prodTotal} onClick={() => setPage(p => p + 1)}>Siguiente &raquo;</button>
              </div>
            )}

            <div className="prod-list">
              {pagedSkus.map(g => {
                const bmPrices = g.bm.map(a => a.current_price).filter(Boolean)
                const bmRec = g.bm.map(a => a.recommended_price).filter(Boolean)
                const bmVel = g.bm.reduce((s, a) => s + (Number(a.current_velocity) || 0), 0)
                const bmRev = g.bm.reduce((s, a) => s + (Number(a.rev_delta) || 0), 0)
                const bmPending = g.bm.filter(a => !decisions[`${a.parent_sku}-${a.store}`]).length

                const ecPrices = g.ecom.map(a => a.current_price).filter(Boolean)
                const ecRec = g.ecom.map(a => a.recommended_price).filter(Boolean)
                const ecVel = g.ecom.reduce((s, a) => s + (Number(a.current_velocity) || 0), 0)
                const ecRev = g.ecom.reduce((s, a) => s + (Number(a.rev_delta) || 0), 0)
                const ecPending = g.ecom.filter(a => !decisions[`${a.parent_sku}-${a.store}`]).length

                return (
                  <div key={g.sku} className="prod-card">
                    <div className="prod-header">
                      <span className="prod-name">{g.product}</span>
                      <span className="sku-code">{g.sku}</span>
                      {g.vendor_brand && <span className="prod-vendor">{g.vendor_brand}</span>}
                      {g.age > 0 && <span className="prod-age"><Clock size={10} /> {g.age}sem</span>}
                    </div>
                    <div className="prod-channels">
                      {g.bm.length > 0 && (
                        <div className="prod-channel">
                          <div className="prod-ch-label"><Store size={12} /> B&M ({g.bm.length} tiendas)</div>
                          <div className="prod-ch-row">
                            <span className="prod-ch-price">{clp(median(bmPrices))} → {clp(median(bmRec))}</span>
                            <span className="prod-ch-vel">{bmVel.toFixed(1)} u/sem</span>
                            <span className={`prod-ch-rev ${bmRev >= 0 ? 'prod-ch-rev--pos' : 'prod-ch-rev--neg'}`}>{bmRev >= 0 ? '+' : ''}{clpCompact(bmRev)}</span>
                            {canApprove && bmPending > 0 && (
                              <button className="prod-ch-btn" onClick={() => {
                                g.bm.forEach(a => {
                                  const key = `${a.parent_sku}-${a.store}`
                                  if (!decisions[key]) handleDecision(a.parent_sku, a.store, 'approved')
                                })
                              }}>Aprobar B&M ({bmPending})</button>
                            )}
                            {bmPending === 0 && <span className="prod-ch-done"><Check size={12} /></span>}
                          </div>
                        </div>
                      )}
                      {g.ecom.length > 0 && (
                        <div className="prod-channel prod-channel--ecom">
                          <div className="prod-ch-label"><Globe size={12} /> Ecom</div>
                          <div className="prod-ch-row">
                            <span className="prod-ch-price">{clp(median(ecPrices))} → {clp(median(ecRec))}</span>
                            <span className="prod-ch-vel">{ecVel.toFixed(1)} u/sem</span>
                            <span className={`prod-ch-rev ${ecRev >= 0 ? 'prod-ch-rev--pos' : 'prod-ch-rev--neg'}`}>{ecRev >= 0 ? '+' : ''}{clpCompact(ecRev)}</span>
                            {canApprove && ecPending > 0 && (
                              <button className="prod-ch-btn prod-ch-btn--ecom" onClick={() => {
                                g.ecom.forEach(a => {
                                  const key = `${a.parent_sku}-${a.store}`
                                  if (!decisions[key]) handleDecision(a.parent_sku, a.store, 'approved')
                                })
                              }}>Aprobar Ecom ({ecPending})</button>
                            )}
                            {ecPending === 0 && <span className="prod-ch-done"><Check size={12} /></span>}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                )
              })}
            </div>

            {skuList.length === 0 && <div className="empty-state">No hay productos con estos filtros</div>}

            {prodTotal > 1 && (
              <div className="pagination">
                <button disabled={prodPage <= 1} onClick={() => { setPage(p => p - 1); window.scrollTo(0, 0) }}>&laquo; Anterior</button>
                <span className="page-info">Pagina {prodPage} de {prodTotal}</span>
                <button disabled={prodPage >= prodTotal} onClick={() => { setPage(p => p + 1); window.scrollTo(0, 0) }}>Siguiente &raquo;</button>
              </div>
            )}
          </div>
        )
      })() : viewMode === 'competencia' ? (
        <div className="comp-page">
          <div className="section-header"><DollarSign size={16} /><h2>Competencia: Posicionamiento de precios</h2></div>
          {compAnalytics ? (
            <>
              {/* Search box for SKU drill-down */}
              <div className="comp-search">
                <Search size={15} />
                <input type="text" placeholder="Buscar SKU o producto..." value={search} onChange={e => setSearch(e.target.value)} />
              </div>

              {/* SKU detail view when searching */}
              {search.length >= 3 && (() => {
                const q = search.toLowerCase()
                const matches = compAnalytics.overpriced.concat(compAnalytics.underpriced || [])
                  .filter(p => p.parent_sku?.toLowerCase().includes(q))
                const summaryItems = (compAnalytics._all_items || summary?.items || [])
                  .filter(it => it.parent_sku?.toLowerCase().includes(q))
                // Cross-ref with full competitor data from the summary endpoint
                const skuActions = actions.filter(a => a.parent_sku?.toLowerCase().includes(q))
                if (matches.length > 0 || skuActions.length > 0) return (
                  <div className="comp-section">
                    <h3>Detalle: {matches.length || skuActions.length} SKUs encontrados</h3>
                    {matches.slice(0, 10).map((p, i) => (
                      <div key={i} className="comp-sku-card">
                        <div className="comp-sku-header">
                          <span className="sku-code">{p.parent_sku}</span>
                          <span className={`comp-position comp-position--${p.position}`}>{p.position === 'expensive' ? 'Más caro' : p.position === 'cheaper' ? 'Más barato' : 'Paridad'}</span>
                        </div>
                        <div className="comp-sku-prices">
                          <div className="comp-sku-price"><span className="comp-sku-plabel">Nuestro</span><span className="mono">${p.our_price?.toLocaleString()}</span></div>
                          <div className="comp-sku-price"><span className="comp-sku-plabel">Min competidor</span><span className="mono">${p.comp_min?.toLocaleString()}</span></div>
                          <div className="comp-sku-price"><span className="comp-sku-plabel">Promedio</span><span className="mono">${p.comp_avg?.toLocaleString()}</span></div>
                          <div className="comp-sku-price"><span className="comp-sku-plabel">Brecha</span><span className={p.gap_pct > 5 ? 'comp-gap--bad' : p.gap_pct < -5 ? 'comp-gap--good' : ''}>{p.gap_pct > 0 ? '+' : ''}{p.gap_pct}%</span></div>
                        </div>
                        {p.competitors?.length > 0 && (
                          <div className="comp-sku-competitors">
                            {p.competitors.sort((a,b) => a.price - b.price).map((c, j) => (
                              <div key={j} className="comp-sku-comp">
                                <span className="comp-sku-cname">{c.name}</span>
                                <span className="mono">${c.price?.toLocaleString()}</span>
                                <span className={c.in_stock ? 'comp-stock--yes' : 'comp-stock--no'}>{c.in_stock ? 'En stock' : 'Sin stock'}</span>
                                {c.url && <a href={c.url} target="_blank" rel="noopener noreferrer" className="comp-sku-link">Ver</a>}
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )
                return null
              })()}
              <div className="comp-kpis">
                <div className="comp-kpi">
                  <div className="comp-kpi-value">{compAnalytics.position_summary.avg_price_index.toFixed(2)}x</div>
                  <div className="comp-kpi-label">Índice de precio</div>
                  <div className="comp-kpi-desc">{compAnalytics.position_summary.avg_price_index > 1.02 ? 'Más caro que el mercado' : compAnalytics.position_summary.avg_price_index < 0.98 ? 'Más barato que el mercado' : 'Alineado con el mercado'}</div>
                </div>
                <div className="comp-kpi">
                  <div className="comp-kpi-value comp-kpi--green">{compAnalytics.position_summary.cheaper_pct}%</div>
                  <div className="comp-kpi-label">Más baratos</div>
                  <div className="comp-kpi-desc">{compAnalytics.position_summary.cheaper} productos</div>
                </div>
                <div className="comp-kpi">
                  <div className="comp-kpi-value">{compAnalytics.position_summary.parity_pct}%</div>
                  <div className="comp-kpi-label">Paridad</div>
                  <div className="comp-kpi-desc">{compAnalytics.position_summary.parity} productos</div>
                </div>
                <div className="comp-kpi">
                  <div className="comp-kpi-value comp-kpi--red">{compAnalytics.position_summary.expensive_pct}%</div>
                  <div className="comp-kpi-label">Más caros</div>
                  <div className="comp-kpi-desc">{compAnalytics.position_summary.expensive} productos</div>
                </div>
              </div>

              {compAnalytics.by_competitor?.length > 0 && (
                <div className="comp-section">
                  <h3>Por competidor</h3>
                  <div className="comp-table">
                    <div className="comp-table-header">
                      <span>Competidor</span><span>Productos</span><span>Más barato</span><span>Brecha promedio</span>
                    </div>
                    {compAnalytics.by_competitor.map(c => (
                      <div key={c.name} className="comp-table-row">
                        <span style={{fontWeight: 600}}>{c.name}</span>
                        <span>{c.products}</span>
                        <span>{c.cheapest_pct}%</span>
                        <span className={c.avg_gap_pct > 5 ? 'comp-gap--bad' : c.avg_gap_pct < -5 ? 'comp-gap--good' : ''}>{c.avg_gap_pct > 0 ? '+' : ''}{c.avg_gap_pct}%</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {compAnalytics.overpriced?.length > 0 && (
                <div className="comp-section">
                  <h3>Oportunidades: donde somos más caros</h3>
                  <div className="comp-table">
                    <div className="comp-table-header comp-table-header--6">
                      <span>SKU</span><span>Nuestro</span><span>Min comp.</span><span>Brecha</span><span>Competidor</span><span>N</span>
                    </div>
                    {compAnalytics.overpriced.filter(p => p.gap_pct > 0).map((p, i) => (
                      <div key={i} className="comp-table-row comp-table-row--6">
                        <span className="sku-code">{p.parent_sku?.slice(0, 14)}</span>
                        <span className="mono">${p.our_price?.toLocaleString()}</span>
                        <span className="mono">${p.comp_min?.toLocaleString()}</span>
                        <span className="comp-gap--bad">+{p.gap_pct}%</span>
                        <span>{p.cheapest_site}</span>
                        <span>{p.n_competitors}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {compAnalytics.underpriced?.length > 0 && (
                <div className="comp-section">
                  <h3>Pricing power: donde somos más baratos</h3>
                  <div className="comp-table">
                    <div className="comp-table-header comp-table-header--6">
                      <span>SKU</span><span>Nuestro</span><span>Min comp.</span><span>Brecha</span><span>Competidor</span><span>N</span>
                    </div>
                    {compAnalytics.underpriced.filter(p => p.gap_pct < 0).map((p, i) => (
                      <div key={i} className="comp-table-row comp-table-row--6">
                        <span className="sku-code">{p.parent_sku?.slice(0, 14)}</span>
                        <span className="mono">${p.our_price?.toLocaleString()}</span>
                        <span className="mono">${p.comp_min?.toLocaleString()}</span>
                        <span className="comp-gap--good">{p.gap_pct}%</span>
                        <span>{p.cheapest_site}</span>
                        <span>{p.n_competitors}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          ) : (
            <div className="empty-state">Sin datos de competencia. Ejecuta el pipeline para obtener precios de competidores.</div>
          )}
        </div>
      ) : viewMode === 'performance' ? (
        <div className="perf-page">
          <div className="section-header"><Target size={16} /><h2>Rendimiento: {performanceData?.available ? `${performanceData.decisions_evaluated} decisiones evaluadas` : 'Sin datos aún'}</h2></div>
          {performanceData?.available ? (
            <>
              <div className="perf-kpis">
                <div className="perf-kpi">
                  <div className={`perf-kpi-value ${(performanceData.lift_capture_rate || 0) >= 70 ? 'perf-kpi--green' : (performanceData.lift_capture_rate || 0) >= 50 ? 'perf-kpi--amber' : 'perf-kpi--red'}`}>
                    {performanceData.lift_capture_rate != null ? `${performanceData.lift_capture_rate}%` : '\u2014'}
                  </div>
                  <div className="perf-kpi-label">Lift capturado</div>
                  <div className="perf-kpi-desc">% del impacto predicho que se materializó</div>
                </div>
                <div className="perf-kpi">
                  <div className="perf-kpi-value">{performanceData.pct_direction_correct != null ? `${performanceData.pct_direction_correct}%` : '\u2014'}</div>
                  <div className="perf-kpi-label">Dirección correcta</div>
                  <div className="perf-kpi-desc">% de decisiones donde el modelo acertó la dirección</div>
                </div>
                <div className="perf-kpi">
                  <div className="perf-kpi-value">{performanceData.median_velocity_error_pct != null ? `${performanceData.median_velocity_error_pct > 0 ? '+' : ''}${performanceData.median_velocity_error_pct}%` : '\u2014'}</div>
                  <div className="perf-kpi-label">Error mediana velocidad</div>
                  <div className="perf-kpi-desc">Error típico en predicción de velocidad</div>
                </div>
                <div className="perf-kpi">
                  <div className="perf-kpi-value">{performanceData.weeks_evaluated || 0}</div>
                  <div className="perf-kpi-label">Semanas evaluadas</div>
                  <div className="perf-kpi-desc">Semanas con decisiones implementadas</div>
                </div>
              </div>

              {performanceData.by_confidence && Object.keys(performanceData.by_confidence).length > 0 && (
                <div className="perf-section">
                  <h3>Por nivel de confianza</h3>
                  <div className="perf-table">
                    <div className="perf-table-header">
                      <span>Tier</span><span>Decisiones</span><span>Dirección correcta</span><span>Error velocidad</span>
                    </div>
                    {Object.entries(performanceData.by_confidence).sort((a,b) => b[1].count - a[1].count).map(([tier, d]) => (
                      <div key={tier} className="perf-table-row">
                        <span className={`badge badge--${tier.toLowerCase()}`}>{tier}</span>
                        <span>{d.count}</span>
                        <span>{d.pct_direction_correct != null ? `${d.pct_direction_correct}%` : '\u2014'}</span>
                        <span>{d.median_velocity_error_pct != null ? `${d.median_velocity_error_pct}%` : '\u2014'}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {performanceData.by_action_type && Object.keys(performanceData.by_action_type).length > 0 && (
                <div className="perf-section">
                  <h3>Por tipo de acción</h3>
                  <div className="perf-table">
                    <div className="perf-table-header">
                      <span>Tipo</span><span>Decisiones</span><span>Dirección correcta</span><span>Error velocidad</span>
                    </div>
                    {Object.entries(performanceData.by_action_type).map(([type, d]) => (
                      <div key={type} className="perf-table-row">
                        <span style={{fontWeight: 600}}>{type === 'decrease' ? 'Markdown' : type === 'increase' ? 'Aumento' : type}</span>
                        <span>{d.count}</span>
                        <span>{d.pct_direction_correct != null ? `${d.pct_direction_correct}%` : '\u2014'}</span>
                        <span>{d.median_velocity_error_pct != null ? `${d.median_velocity_error_pct}%` : '\u2014'}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {outcomeDetails.length > 0 && (
                <div className="perf-section">
                  <h3>Detalle por decisión ({outcomeDetails.length})</h3>
                  <div className="perf-table perf-table--detail">
                    <div className="perf-table-header">
                      <span>SKU</span><span>Tienda</span><span>Vel. pred.</span><span>Vel. real</span><span>Error</span><span>Conf.</span>
                    </div>
                    {outcomeDetails.slice(0, 50).map((row, i) => (
                      <div key={i} className="perf-table-row">
                        <span className="sku-code">{row.parent_sku?.slice(0, 12)}</span>
                        <span>{row.store}</span>
                        <span>{row.predicted_velocity?.toFixed(1) ?? '\u2014'}</span>
                        <span>{row.actual_velocity?.toFixed(1) ?? '\u2014'}</span>
                        <span className={Math.abs(row.velocity_error_pct || 0) > 50 ? 'perf-err--bad' : ''}>{row.velocity_error_pct != null ? `${row.velocity_error_pct > 0 ? '+' : ''}${Math.round(row.velocity_error_pct)}%` : '\u2014'}</span>
                        <span className={`an-conf-badge an-conf-${(row.confidence_tier || 'low').toLowerCase()}`}>{row.confidence_tier}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          ) : (
            <div className="empty-state">
              Las decisiones necesitan 1-2 semanas de datos para evaluar el rendimiento.<br/>
              Aprueba recomendaciones y el sistema comparará predicciones vs resultados reales.
            </div>
          )}
        </div>
      ) : (
      <>

      {CHANNEL_GRAIN_BRANDS.has(brand?.id) && (
        <div className="grain-bar">
          <span className="grain-bar__label">Grano</span>
          <div className="grain-bar__tabs">
            <button
              className={`grain-tab ${grain === 'channel' ? 'grain-tab--active' : ''}`}
              onClick={() => toggleGrain('channel')}
              title="Una accion por parent SKU x canal (B&M / Ecomm)"
            >
              Canal
            </button>
            <button
              className={`grain-tab ${grain === 'store' ? 'grain-tab--active' : ''}`}
              onClick={() => toggleGrain('store')}
              title="Una accion por parent SKU x tienda"
            >
              Por tienda
            </button>
          </div>
          {grain === 'channel' && (
            <span className="grain-bar__hint">
              Vista nueva. Las excepciones por tienda aparecen abajo en <strong>inconsistencias</strong>.
            </span>
          )}
        </div>
      )}

      {grain === 'channel' ? (
        <ChannelListaView
          actions={actions}
          decisions={decisions}
          week={week}
          brandId={brand?.id}
          canApprove={canApprove}
          canExport={canExport}
          onDecide={setDecision}
          onBulkDecide={bulkDecide}
          onExport={() => setShowExportConfirm(true)}
          approvedCount={approvedItems.length}
          onSwitchToStoreGrain={() => toggleGrain('store')}
        />
      ) : (<>

      <div className="toolbar">
        <div className="search-box">
          <Search size={15} />
          <input type="text" placeholder="Buscar SKU o producto..." value={search} onChange={e => { setSearch(e.target.value); setPage(1) }} />
        </div>
        <div className="filter-group">
          <Filter size={14} />
          <select value={filterStatus} onChange={e => { setFilterStatus(e.target.value); setPage(1) }}>
            <option value="all">Todo estado</option>
            <option value="pending">Sin revisar</option>
            <option value="approved">Aprobadas</option>
            <option value="rejected">Rechazadas</option>
            <option value="manual">Precio manual</option>
          </select>
          <select value={filterStore} onChange={e => { setFilterStore(e.target.value); setPage(1) }}>
            <option value="all">Todas las tiendas</option>
            {stores.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
          <select value={filterUrgency} onChange={e => { setFilterUrgency(e.target.value); setPage(1) }}>
            <option value="all">Toda urgencia</option>
            <option value="INCREASE">Subir precio</option>
            <option value="HIGH">Alta</option>
            <option value="MEDIUM">Media</option>
            <option value="LOW">Baja</option>
          </select>
          {categories.length > 1 && !(viewMode === 'marcas' && !useVendorGrouping) && (
            <select value={filterCategory} onChange={e => { setFilterCategory(e.target.value); setPage(1) }}>
              <option value="all">Toda categoria</option>
              {categories.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          )}
          <select value={sortBy} onChange={e => { setSortBy(e.target.value); setPage(1) }}>
            <option value="urgency">Ordenar: Urgencia</option>
            <option value="revenue">Ordenar: Impacto</option>
            <option value="confidence">Ordenar: Confianza</option>
            <option value="store">Ordenar: Tienda</option>
          </select>
        </div>
        <div className="toolbar-actions">
          <span className="result-count">{filtered.length} resultados</span>
          {canApprove && (() => {
            const pendingInFilter = filtered.filter(a => !decisions[`${a.parent_sku}-${a.store}`]).length
            return pendingInFilter > 0 && (
              <>
                <button className="tbtn tbtn--approve" onClick={() => {
                  if (pendingInFilter > 100 && !confirm(`Aprobar ${pendingInFilter} acciones pendientes?`)) return
                  bulkDecide(filtered.filter(a => !decisions[`${a.parent_sku}-${a.store}`]), 'approved')
                }}><Check size={13} /> Aprobar ({pendingInFilter})</button>
                <button className="tbtn tbtn--reject" onClick={() => {
                  if (pendingInFilter > 100 && !confirm(`Rechazar ${pendingInFilter} acciones pendientes?`)) return
                  bulkDecide(filtered.filter(a => !decisions[`${a.parent_sku}-${a.store}`]), 'rejected')
                }}><X size={13} /> Rechazar ({pendingInFilter})</button>
              </>
            )
          })()}
          {canExport && approvedItems.length > 0 && (
            <button className="tbtn tbtn--export" onClick={() => setShowExportConfirm(true)}><Download size={13} /> Exportar ({approvedItems.length})</button>
          )}
        </div>
      </div>

      {showAnalytics && (
        <AnalyticsDrawer brand={brand?.id} authFetch={authFetch} />
      )}

      {totalPages > 1 && (
        <div className="pagination">
          <button disabled={page <= 1} onClick={() => setPage(p => p - 1)}>&laquo; Anterior</button>
          <span className="page-info">Pagina {page} de {totalPages} ({filtered.length} acciones)</span>
          <button disabled={page >= totalPages} onClick={() => setPage(p => p + 1)}>Siguiente &raquo;</button>
        </div>
      )}

      {increases.length > 0 && (
        <section className="section section--increases">
          <div className="section-header"><TrendingUp size={16} /><h2>Subir precio — Recuperar margen ({increases.length})</h2></div>
          <div className="list">
            {increases.map(a => {
              const k = `${a.parent_sku}-${a.store}`
              return <ActionRow key={k} action={a} status={decisions[k] || null} onDecide={st => setDecision(k, st)} onManual={a => setManualAction(a)} onChainView={sku => setChainSku(sku)} canApprove={canApprove} feedback={feedback[k]} />
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
              return <ActionRow key={k} action={a} status={decisions[k] || null} onDecide={st => setDecision(k, st)} onManual={a => setManualAction(a)} onChainView={sku => setChainSku(sku)} canApprove={canApprove} feedback={feedback[k]} />
            })}
          </div>
        </section>
      )}

      {filtered.length === 0 && <div className="empty-state">No hay acciones con estos filtros</div>}

      {totalPages > 1 && (
        <div className="pagination">
          <button disabled={page <= 1} onClick={() => { setPage(p => p - 1); window.scrollTo(0, 0) }}>&laquo; Anterior</button>
          <span className="page-info">Pagina {page} de {totalPages}</span>
          <button disabled={page >= totalPages} onClick={() => { setPage(p => p + 1); window.scrollTo(0, 0) }}>Siguiente &raquo;</button>
        </div>
      )}

      </>)}{/* end grain === 'channel' ? ChannelListaView : <per-store lista> */}

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

      {crossStoreAlerts.length > 0 && (
        <section className="section section--cross-store">
          <div className="section-header"><AlertTriangle size={16} /><h2>Inconsistencias de precio entre tiendas ({crossStoreAlerts.length})</h2></div>
          <div className="alert-grid">
            {crossStoreAlerts.slice(0, 20).map((a, i) => (
              <div key={i} className="alert-card alert-card--cross-store">
                <div className="alert-sku">{a.parent_sku}</div>
                <div className="alert-spread">{(a.price_spread * 100).toFixed(0)}% spread</div>
                <div className="alert-stores">
                  {a.stores.map((s, j) => (
                    <span key={j} className={`cs-store ${s.channel}`}>
                      {s.store}: ${s.price?.toLocaleString()}
                      {s.discount_rate > 0.05 ? ` (-${(s.discount_rate * 100).toFixed(0)}%)` : ''}
                    </span>
                  ))}
                </div>
                {a.sync_price && <div className="alert-sync">Sync: ${a.sync_price.toLocaleString()}</div>}
                <div className="alert-reasons-list">
                  {a.alert_reasons.split(';').filter(Boolean).map((r, j) => (
                    <span key={j} className="reason-badge">{r.replace(/_/g, ' ')}</span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </section>
      )}

      {canAudit && auditLog.length > 0 && (
        <section className="section section--audit">
          <div className="section-header"><Clock size={16} /><h2>Historial de cambios</h2></div>
          <div className="audit-list">
            {auditLog.slice(0, 30).map((e, i) => (
              <div key={i} className="audit-entry">
                <span className="audit-time">{new Date(e.timestamp).toLocaleString('es-CL', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</span>
                <span className="audit-user">{e.user_name || e.user_email}</span>
                <span className={`audit-action audit-action--${e.action}`}>{e.action}</span>
                <span className="audit-detail">{e.key || (e.count ? `${e.count} items` : '')}</span>
              </div>
            ))}
          </div>
        </section>
      )}

      </>
      )}

      </div>{/* main-content */}
      </div>{/* dashboard-body */}
      </>)}{/* end week && !showPlannerQueue */}

      {chainSku && (
        <ChainViewModal
          parentSku={chainSku}
          actions={actions}
          decisions={decisions}
          brand={brand?.id}
          week={week}
          authFetch={authFetch}
          onApplyChain={handleChainApply}
          onClose={() => setChainSku(null)}
        />
      )}

      {manualAction && (
        <ManualPriceModal
          action={manualAction}
          brand={brand?.id}
          authFetch={authFetch}
          onConfirm={(price, impact) => handleManualConfirm(manualAction, price, impact)}
          onClose={() => setManualAction(null)}
        />
      )}

      {showSimulator && (
        <SimulatorModal
          brand={brand?.id}
          authFetch={authFetch}
          onClose={() => setShowSimulator(false)}
        />
      )}

      {showAdmin && <AdminPanel authFetch={authFetch} onClose={() => setShowAdmin(false)} />}

      {showExportConfirm && (
        <div className="admin-overlay" onClick={() => setShowExportConfirm(false)}>
          <div className="export-dialog" onClick={e => e.stopPropagation()}>
            <h3>Exportar cambios de precio</h3>
            <p className="export-brand">{brand.label} — Semana {week}</p>
            <div className="export-summary">
              <div className="export-stat export-stat--ok"><Check size={14} /> {approvedItems.length} aprobadas</div>
              <div className="export-stat export-stat--no"><X size={14} /> {rejectedCount} rechazadas</div>
              {undecidedCount > 0 && <div className="export-stat export-stat--warn"><AlertTriangle size={14} /> {undecidedCount} sin revisar (no se exportan)</div>}
            </div>
            <div className="export-impact">Impacto estimado: {approvedImpact >= 0 ? '+' : ''}{clpCompact(approvedImpact)}/semana</div>
            <div className="export-buttons">
              <button className="tbtn tbtn--export" onClick={doExportExcel}><Download size={13} /> Descargar Excel</button>
              <button className="tbtn tbtn--copy" onClick={doExportText}><ClipboardCopy size={13} /> Copiar texto</button>
              <button className="tbtn" onClick={() => setShowExportConfirm(false)}>Cancelar</button>
            </div>
          </div>
        </div>
      )}

      {toast && <div className={`toast toast--${toast.type}`}>{toast.msg}</div>}

      {/* AI Assistant */}
      <button className="ai-fab" onClick={() => setAiOpen(!aiOpen)} title="Asistente IA">
        {aiOpen ? <X size={20} /> : <BarChart2 size={20} />}
      </button>
      {aiOpen && (
        <div className="ai-panel">
          <div className="ai-header">Asistente de Pricing — {brand?.label}</div>
          <div className="ai-messages">
            {aiMessages.length === 0 && (
              <div className="ai-hint">Pregunta sobre pricing, competencia, rendimiento del modelo...</div>
            )}
            {aiMessages.map((m, i) => (
              <div key={i} className={`ai-msg ai-msg--${m.role}`}>
                <div className="ai-msg-text">{m.text}</div>
              </div>
            ))}
            {aiLoading && <div className="ai-msg ai-msg--assistant"><div className="ai-msg-text">Pensando...</div></div>}
          </div>
          <form className="ai-input" onSubmit={e => {
            e.preventDefault()
            if (!aiQuestion.trim() || aiLoading) return
            const q = aiQuestion.trim()
            setAiMessages(prev => [...prev, { role: 'user', text: q }])
            setAiQuestion('')
            setAiLoading(true)
            authFetch('/ai/ask', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ question: q, brand: brand?.id || 'hoka' }),
            })
              .then(r => r.json())
              .then(d => {
                setAiMessages(prev => [...prev, { role: 'assistant', text: d.answer || d.error || 'Sin respuesta' }])
                setAiLoading(false)
              })
              .catch(() => {
                setAiMessages(prev => [...prev, { role: 'assistant', text: 'Error de conexión' }])
                setAiLoading(false)
              })
          }}>
            <input type="text" value={aiQuestion} onChange={e => setAiQuestion(e.target.value)}
                   placeholder="¿Por qué este SKU está en alta urgencia?" />
            <button type="submit" disabled={aiLoading}>→</button>
          </form>
        </div>
      )}
    </div>
  )
}

export default App
