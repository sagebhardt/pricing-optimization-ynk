import { useState, useEffect, useMemo } from 'react'
import { Check, X, AlertTriangle, DollarSign } from 'lucide-react'

function clp(n) {
  if (n === null || n === undefined || n === '' || isNaN(n)) return '\u2014'
  return '$' + Math.round(Number(n)).toLocaleString('es-CL')
}

function clpCompact(n) {
  if (n === null || n === undefined || isNaN(n)) return '\u2014'
  const num = Number(n)
  const sign = num >= 0 ? '+' : ''
  if (Math.abs(num) >= 1_000_000) return `${sign}${(num / 1_000_000).toFixed(1)}M`
  if (Math.abs(num) >= 1_000) return `${sign}${Math.round(num / 1_000)}K`
  return `${sign}${Math.round(num)}`
}

function StatusBadge({ status, manualPrice }) {
  if (status === 'bm_manual' || status === 'manual') {
    return <span className="pq-badge pq-badge--manual"><DollarSign size={10} /> Manual {manualPrice ? clp(manualPrice) : ''}</span>
  }
  if (status === 'bm_approved' || status === 'approved') {
    return <span className="pq-badge pq-badge--approved"><Check size={10} /> Aprobado</span>
  }
  if (status === 'bm_rejected' || status === 'rejected') {
    return <span className="pq-badge pq-badge--rejected"><X size={10} /> Rechazado</span>
  }
  return <span className="pq-badge">{status}</span>
}

export default function PlannerQueue({ brand, authFetch, showToast }) {
  const [items, setItems] = useState([])
  const [week, setWeek] = useState(null)
  const [loading, setLoading] = useState(false)
  const [selected, setSelected] = useState(new Set())

  useEffect(() => {
    if (!brand) return
    const controller = new AbortController()
    setLoading(true)
    setSelected(new Set())  // reset selection on brand change
    authFetch(`/decisions/planner-queue?brand=${brand}`, { signal: controller.signal })
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(data => {
        setItems(data.items || [])
        setWeek(data.week)
        setLoading(false)
      })
      .catch(e => { if (e.name !== 'AbortError') { setLoading(false); showToast?.(e.message || 'Error cargando cola', 'error') } })
    return () => controller.abort()
  }, [brand, authFetch])

  const toggleSelect = (key) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const selectAll = () => {
    if (selected.size === items.length) setSelected(new Set())
    else setSelected(new Set(items.map(i => i.decision_key)))
  }

  const handlePlannerDecision = (status) => {
    const keys = [...selected]
    if (keys.length === 0) return
    authFetch('/decisions/plan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ brand, week, keys, status }),
    })
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(data => {
        if (data.ok) {
          setItems(prev => prev.filter(i => !selected.has(i.decision_key)))
          setSelected(new Set())
          showToast?.(`${data.changed} acciones ${status === 'planner_approved' ? 'aprobadas' : 'rechazadas'}`, 'info')
        } else {
          showToast?.('Error del servidor', 'error')
        }
      })
      .catch(e => showToast?.(e.message || 'Error guardando', 'error'))
  }

  // Group by store for overview
  const storeGroups = useMemo(() => {
    const map = {}
    items.forEach(i => {
      const s = i.store_name || i.store
      if (!map[s]) map[s] = { name: s, count: 0, revDelta: 0 }
      map[s].count++
      map[s].revDelta += Number(i.rev_delta) || 0
    })
    return Object.values(map).sort((a, b) => b.count - a.count)
  }, [items])

  if (loading) return <div className="pq-loading">Cargando cola de aprobacion...</div>

  return (
    <div className="pq-container">
      <div className="pq-header">
        <h2>Cola de aprobacion — Planner</h2>
        <span className="pq-meta">{brand?.toUpperCase()} | Semana {week} | {items.length} pendientes</span>
      </div>

      {items.length === 0 ? (
        <div className="pq-empty">
          <Check size={24} />
          <p>No hay acciones pendientes de aprobacion</p>
        </div>
      ) : (
        <>
          <div className="pq-summary">
            {storeGroups.slice(0, 8).map(g => (
              <span key={g.name} className="pq-store-chip">
                {g.name}: <strong>{g.count}</strong> ({clpCompact(g.revDelta)})
              </span>
            ))}
            {storeGroups.length > 8 && <span className="pq-store-chip">+{storeGroups.length - 8} tiendas mas</span>}
          </div>

          <div className="pq-toolbar">
            <label className="pq-select-all">
              <input type="checkbox" checked={selected.size === items.length} onChange={selectAll} />
              Seleccionar todo ({items.length})
            </label>
            {selected.size > 0 && (
              <div className="pq-bulk-actions">
                <button className="pq-btn pq-btn--approve" onClick={() => handlePlannerDecision('planner_approved')}>
                  <Check size={13} /> Aprobar ({selected.size})
                </button>
                <button className="pq-btn pq-btn--reject" onClick={() => handlePlannerDecision('planner_rejected')}>
                  <X size={13} /> Rechazar ({selected.size})
                </button>
              </div>
            )}
          </div>

          <div className="pq-table-wrap">
            <table className="pq-table">
              <thead>
                <tr>
                  <th></th>
                  <th>SKU</th>
                  <th>Producto</th>
                  <th>Tienda</th>
                  <th>Decision BM</th>
                  <th>Precio actual</th>
                  <th>Precio rec.</th>
                  <th>Rev delta</th>
                  <th>Margen</th>
                  <th>BM</th>
                </tr>
              </thead>
              <tbody>
                {items.map(item => (
                  <tr key={item.decision_key} className={selected.has(item.decision_key) ? 'pq-row--selected' : ''}>
                    <td>
                      <input type="checkbox" checked={selected.has(item.decision_key)}
                             onChange={() => toggleSelect(item.decision_key)} />
                    </td>
                    <td className="pq-mono">{item.parent_sku}</td>
                    <td className="pq-product">{String(item.product || '').slice(0, 30)}</td>
                    <td>{item.store_name || item.store}</td>
                    <td><StatusBadge status={item.bm_status} manualPrice={item.manual_price} /></td>
                    <td className="pq-mono">{clp(item.current_price)}</td>
                    <td className="pq-mono">{item.manual_price ? clp(item.manual_price) : clp(item.recommended_price)}</td>
                    <td className="pq-mono" style={{color: Number(item.rev_delta) >= 0 ? 'var(--green-600)' : 'var(--red-600)'}}>
                      {clpCompact(item.rev_delta)}
                    </td>
                    <td>{item.margin_pct != null ? `${item.margin_pct}%` : '\u2014'}</td>
                    <td className="pq-user">{String(item.bm_user || '').split('@')[0]}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
