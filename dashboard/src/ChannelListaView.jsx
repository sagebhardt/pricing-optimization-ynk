import { useState, useMemo } from 'react'
import { Check, X, Search, Filter, TrendingUp, TrendingDown, AlertTriangle, Download, Store, DollarSign } from 'lucide-react'

const PAGE_SIZE = 50

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

function parsePct(s) {
  if (!s) return 0
  const m = String(s).match(/([0-9.]+)/)
  return m ? parseFloat(m[1]) : 0
}

function ChannelRow({ action, status, onDecide, onManual, canApprove }) {
  const key = `${action.parent_sku}-${action.channel}`
  const isApproved = ['approved', 'bm_approved', 'planner_approved', 'manual', 'bm_manual'].includes(status)
  const isRejected = ['rejected', 'bm_rejected', 'planner_rejected'].includes(status)
  const isDecided = !!status
  const revDelta = Number(action.rev_delta) || 0
  const mandatory = action.mandatory_review === true || action.mandatory_review === 'True'
  const varianceVal = Math.min(1, Number(action.per_store_variance_pct) || 0)
  const mp = action.margin_pct == null ? null : Number(action.margin_pct)
  const marginClass = mp == null ? 'ch-margin--na'
    : mp < 20 ? 'ch-margin--thin'
    : mp >= 40 ? 'ch-margin--strong'
    : 'ch-margin--ok'
  const urg = String(action.urgency || 'LOW').toUpperCase()
  const urgCls = action.action_type === 'increase' ? 'ch-urg--increase'
    : urg === 'HIGH' ? 'ch-urg--high'
    : urg === 'MEDIUM' ? 'ch-urg--medium'
    : 'ch-urg--low'
  const rowCls = [
    'ch-row',
    isApproved ? 'ch-row--approved' : '',
    isRejected ? 'ch-row--rejected' : '',
    status === 'manual' || status === 'bm_manual' ? 'ch-row--manual' : '',
  ].filter(Boolean).join(' ')

  return (
    <div className={rowCls}
         data-action={action.action_type}
         data-mandatory={mandatory ? 'true' : 'false'}>
      <div>
        <div className="ch-sku">{action.parent_sku}</div>
        <div className="ch-product">{action.product}</div>
        <div className="ch-meta">
          <span className="ch-meta__tiendas"><Store size={10} /> {action.n_stores} {action.n_stores === 1 ? 'tienda' : 'tiendas'}</span>
          {mandatory && (
            <span className="ch-meta__warn" title={`${Math.round(varianceVal * 100)}% de las tiendas difieren del canal`}>
              <AlertTriangle size={10} /> revisar por tienda
            </span>
          )}
          {Number(action.rebate_amount) > 0 && (
            <span className="ch-meta__rebate" title={`Aporte proveedor: ${clp(action.rebate_amount)}/unidad — el costo efectivo es menor durante el evento`}>
              rebate {clpCompact(action.rebate_amount)}
            </span>
          )}
          {action.aggregation_method === 'velocity_weighted_fallback' && (
            <span className="ch-meta__fallback" title="Sin datos de stock — promedio ponderado por velocidad">fallback</span>
          )}
        </div>
        {varianceVal > 0.05 && (
          <div className="ch-variance" aria-label={`Varianza ${Math.round(varianceVal * 100)}%`}>
            <div className="ch-variance__fill" style={{ width: `${Math.round(varianceVal * 100)}%` }} />
          </div>
        )}
      </div>

      <div>
        <span className={`ch-badge ch-badge--${action.channel}`}>
          {action.channel === 'ecomm' ? 'Ecomm' : 'B&M'}
        </span>
      </div>

      <div>
        <span className={`ch-urg ${urgCls}`}>
          {urg === 'INCREASE' || action.action_type === 'increase' ? 'Subir' : urg}
        </span>
      </div>

      <div className="ch-price">
        <span className="ch-price__from">{clp(action.current_price)}</span>
        <span className="ch-price__to">{clp(action.recommended_price)}</span>
        <span className="ch-price__disc">{action.recommended_discount}</span>
      </div>

      <div className="ch-velocity">
        <div className="ch-velocity__now">{Number(action.current_velocity).toFixed(1)} u/sem</div>
        <div className="ch-velocity__exp">→ {Number(action.expected_velocity).toFixed(1)} u/sem</div>
      </div>

      <div className={`ch-delta ${revDelta >= 0 ? 'ch-delta--pos' : 'ch-delta--neg'}`}>
        {revDelta >= 0 ? '+' : ''}{clpCompact(revDelta)}
      </div>

      <div className={`ch-margin ${marginClass}`}>
        {mp == null ? '—' : `${mp.toFixed(0)}%`}
      </div>

      <div className="ch-actions">
        {canApprove && !isDecided && (
          <>
            <button className="ch-btn ch-btn--approve" onClick={() => onDecide(key, 'approved')} title="Aprobar recomendacion">
              <Check size={14} />
            </button>
            <button className="ch-btn ch-btn--manual" onClick={() => onManual(action)} title="Precio manual">
              <DollarSign size={13} />
            </button>
            <button className="ch-btn ch-btn--reject" onClick={() => onDecide(key, 'rejected')} title="Rechazar">
              <X size={14} />
            </button>
          </>
        )}
        {isDecided && (
          <button className="ch-btn ch-btn--decided" onClick={() => onDecide(key, null)} title="Deshacer">
            {isApproved ? 'Aprobada' : isRejected ? 'Rechazada' : status} ×
          </button>
        )}
      </div>
    </div>
  )
}

function ColumnHeader() {
  return (
    <div className="ch-head">
      <div>SKU / Producto</div>
      <div>Canal</div>
      <div>Urgencia</div>
      <div className="ch-head__r">Precio</div>
      <div className="ch-head__r">Velocidad</div>
      <div className="ch-head__r">Rev / sem</div>
      <div className="ch-head__r">Margen</div>
      <div className="ch-head__r">Acción</div>
    </div>
  )
}

export default function ChannelListaView({
  actions,
  decisions,
  week,
  brandId,
  canApprove,
  canExport,
  onDecide,
  onManual,
  onBulkDecide,
  onExport,
  approvedCount,
  authFetch,
  onSwitchToStoreGrain,
}) {
  // Empty state: channel CSVs don't exist yet for this brand (pipeline
  // hasn't run since the channel_aggregate step was deployed). Show an
  // actionable message rather than a bland "no hay acciones" string.
  if ((actions || []).length === 0) {
    return (
      <div className="ch-empty">
        <div className="ch-empty__icon">
          <AlertTriangle size={28} />
        </div>
        <h3 className="ch-empty__title">Canal aún no disponible para esta marca</h3>
        <p className="ch-empty__body">
          El último pipeline generó las acciones en modo por tienda. El próximo
          run (lunes 6 AM CLT) escribirá también la versión por canal. Mientras
          tanto puedes seguir trabajando con la vista por tienda.
        </p>
        {onSwitchToStoreGrain && (
          <button className="ch-empty__cta" onClick={onSwitchToStoreGrain}>
            Ver acciones por tienda
          </button>
        )}
      </div>
    )
  }

  const [search, setSearch] = useState('')
  const [filterChannel, setFilterChannel] = useState('all')
  const [filterStatus, setFilterStatus] = useState('all')
  const [filterUrgency, setFilterUrgency] = useState('all')
  const [sortBy, setSortBy] = useState('urgency')
  const [page, setPage] = useState(1)

  const filtered = useMemo(() => {
    const q = search.toLowerCase()
    let result = (actions || []).filter(a => {
      if (filterChannel !== 'all' && a.channel !== filterChannel) return false
      if (filterUrgency !== 'all') {
        if (filterUrgency === 'INCREASE' && a.action_type !== 'increase') return false
        if (filterUrgency !== 'INCREASE' && (a.urgency !== filterUrgency || a.action_type === 'increase')) return false
      }
      if (filterStatus !== 'all') {
        const status = decisions[`${a.parent_sku}-${a.channel}`] || 'pending'
        const bucket = ['approved', 'bm_approved', 'planner_approved', 'manual', 'bm_manual'].includes(status) ? 'approved'
          : ['rejected', 'bm_rejected', 'planner_rejected'].includes(status) ? 'rejected'
          : 'pending'
        if (filterStatus !== bucket) return false
      }
      if (q && !(
        (a.parent_sku || '').toLowerCase().includes(q) ||
        (a.product || '').toLowerCase().includes(q)
      )) return false
      return true
    })
    const urgencyOrder = { INCREASE: -1, HIGH: 0, MEDIUM: 1, LOW: 2 }
    if (sortBy === 'urgency') {
      result.sort((a, b) => (urgencyOrder[a.urgency] ?? 3) - (urgencyOrder[b.urgency] ?? 3)
        || (Number(b.rev_delta) || 0) - (Number(a.rev_delta) || 0))
    } else if (sortBy === 'revenue') {
      result.sort((a, b) => Math.abs(Number(b.rev_delta) || 0) - Math.abs(Number(a.rev_delta) || 0))
    } else if (sortBy === 'variance') {
      result.sort((a, b) => (Number(b.per_store_variance_pct) || 0) - (Number(a.per_store_variance_pct) || 0))
    } else if (sortBy === 'discount') {
      result.sort((a, b) => parsePct(b.recommended_discount) - parsePct(a.recommended_discount))
    }
    return result
  }, [actions, decisions, search, filterChannel, filterStatus, filterUrgency, sortBy])

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE))
  const paged = useMemo(() => filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE), [filtered, page])

  const increases = useMemo(() => paged.filter(a => a.action_type === 'increase'), [paged])
  const decreases = useMemo(() => paged.filter(a => a.action_type !== 'increase'), [paged])

  const pendingInFilter = filtered.filter(a => !decisions[`${a.parent_sku}-${a.channel}`]).length

  const nBm = actions.filter(a => a.channel === 'bm').length
  const nEcomm = actions.filter(a => a.channel === 'ecomm').length
  const nMandatory = actions.filter(a => a.mandatory_review === true || a.mandatory_review === 'True').length

  return (
    <>
      <div className="toolbar">
        <div className="search-box">
          <Search size={15} />
          <input type="text" placeholder="Buscar SKU o producto..." value={search}
                 onChange={e => { setSearch(e.target.value); setPage(1) }} />
        </div>
        <div className="filter-group">
          <Filter size={14} />
          <select value={filterStatus} onChange={e => { setFilterStatus(e.target.value); setPage(1) }}>
            <option value="all">Todo estado</option>
            <option value="pending">Sin revisar</option>
            <option value="approved">Aprobadas</option>
            <option value="rejected">Rechazadas</option>
          </select>
          <select value={filterChannel} onChange={e => { setFilterChannel(e.target.value); setPage(1) }}>
            <option value="all">Todos canales ({nBm + nEcomm})</option>
            <option value="bm">B&amp;M ({nBm})</option>
            <option value="ecomm">Ecomm ({nEcomm})</option>
          </select>
          <select value={filterUrgency} onChange={e => { setFilterUrgency(e.target.value); setPage(1) }}>
            <option value="all">Toda urgencia</option>
            <option value="INCREASE">Subir precio</option>
            <option value="HIGH">Alta</option>
            <option value="MEDIUM">Media</option>
            <option value="LOW">Baja</option>
          </select>
          <select value={sortBy} onChange={e => { setSortBy(e.target.value); setPage(1) }}>
            <option value="urgency">Ordenar: Urgencia</option>
            <option value="revenue">Ordenar: Impacto</option>
            <option value="variance">Ordenar: Varianza</option>
            <option value="discount">Ordenar: Descuento</option>
          </select>
        </div>
        <div className="toolbar-actions">
          <span className="result-count">{filtered.length} resultados</span>
          {nMandatory > 0 && (
            <span className="ch-meta__warn" style={{ fontSize: 11 }}>
              <AlertTriangle size={12} /> {nMandatory} requieren revisar por tienda
            </span>
          )}
          {canApprove && pendingInFilter > 0 && (
            <>
              <button className="tbtn tbtn--approve" onClick={() => {
                if (pendingInFilter > 100 && !confirm(`Aprobar ${pendingInFilter} acciones pendientes?`)) return
                onBulkDecide(filtered.filter(a => !decisions[`${a.parent_sku}-${a.channel}`]), 'approved')
              }}><Check size={13} /> Aprobar ({pendingInFilter})</button>
              <button className="tbtn tbtn--reject" onClick={() => {
                if (pendingInFilter > 100 && !confirm(`Rechazar ${pendingInFilter} acciones pendientes?`)) return
                onBulkDecide(filtered.filter(a => !decisions[`${a.parent_sku}-${a.channel}`]), 'rejected')
              }}><X size={13} /> Rechazar ({pendingInFilter})</button>
            </>
          )}
          {canExport && approvedCount > 0 && (
            <button className="tbtn tbtn--export" onClick={onExport}>
              <Download size={13} /> Exportar ({approvedCount})
            </button>
          )}
        </div>
      </div>

      <ColumnHeader />

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
          <div>
            {increases.map(a => (
              <ChannelRow
                key={`${a.parent_sku}-${a.channel}`}
                action={a}
                status={decisions[`${a.parent_sku}-${a.channel}`] || null}
                onDecide={onDecide}
                onManual={onManual}
                canApprove={canApprove}
              />
            ))}
          </div>
        </section>
      )}

      {decreases.length > 0 && (
        <section className="section section--markdowns">
          <div className="section-header"><TrendingDown size={16} /><h2>Markdown ({decreases.length})</h2></div>
          <div>
            {decreases.map(a => (
              <ChannelRow
                key={`${a.parent_sku}-${a.channel}`}
                action={a}
                status={decisions[`${a.parent_sku}-${a.channel}`] || null}
                onDecide={onDecide}
                onManual={onManual}
                canApprove={canApprove}
              />
            ))}
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
    </>
  )
}
