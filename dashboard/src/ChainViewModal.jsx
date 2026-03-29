import { useMemo } from 'react'
import { X, Check, Store, Globe, ShoppingBag, Link2 } from 'lucide-react'

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

function isEcommStore(code) {
  return String(code).toUpperCase().startsWith('AB')
}

export default function ChainViewModal({ parentSku, actions, decisions, brand, week, authFetch, onApplyChain, onClose }) {
  // All actions for this parent SKU
  const storeRows = useMemo(() => {
    return actions
      .filter(a => a.parent_sku === parentSku)
      .map(a => {
        const key = `${a.parent_sku}-${a.store}`
        const dec = decisions[key]
        return {
          ...a,
          key,
          status: dec || null,
          channel: isEcommStore(a.store) ? 'ecomm' : 'bm',
        }
      })
      .sort((a, b) => (a.store_name || a.store).localeCompare(b.store_name || b.store))
  }, [parentSku, actions, decisions])

  const productName = storeRows[0]?.product || parentSku
  const category = storeRows[0]?.subcategory || ''

  // Consensus: most common recommended discount
  const discountCounts = {}
  storeRows.forEach(r => {
    const d = r.recommended_discount || '?'
    discountCounts[d] = (discountCounts[d] || 0) + 1
  })
  const consensus = Object.entries(discountCounts).sort((a, b) => b[1] - a[1])[0]

  // Aggregates
  const totalVelocity = storeRows.reduce((s, r) => s + (Number(r.current_velocity) || 0), 0)
  const totalRevDelta = storeRows.reduce((s, r) => s + (Number(r.rev_delta) || 0), 0)
  const pending = storeRows.filter(r => !r.status).length
  const ecommCount = storeRows.filter(r => r.channel === 'ecomm').length
  const bmCount = storeRows.filter(r => r.channel === 'bm').length
  const ecommPending = storeRows.filter(r => r.channel === 'ecomm' && !r.status).length
  const bmPending = storeRows.filter(r => r.channel === 'bm' && !r.status).length

  const handleApply = (scope) => {
    const chainKey = `${parentSku}-chain-${scope}`
    onApplyChain(chainKey, scope)
  }

  return (
    <div className="cv-overlay" onClick={onClose}>
      <div className="cv-modal" onClick={e => e.stopPropagation()}>
        <div className="cv-header">
          <div>
            <h3><Link2 size={16} /> Vista cadena</h3>
            <div className="cv-product">
              <span className="cv-sku">{parentSku}</span>
              <span className="cv-name">{productName}</span>
              {category && <span className="cv-cat">{category}</span>}
            </div>
          </div>
          <button className="cv-close" onClick={onClose}><X size={18} /></button>
        </div>

        <div className="cv-summary">
          <div className="cv-stat">
            <div className="cv-stat-value">{storeRows.length}</div>
            <div className="cv-stat-label">Tiendas</div>
          </div>
          <div className="cv-stat">
            <div className="cv-stat-value">{totalVelocity.toFixed(1)}</div>
            <div className="cv-stat-label">Vel total u/sem</div>
          </div>
          <div className="cv-stat">
            <div className="cv-stat-value" style={{color: totalRevDelta >= 0 ? 'var(--green-600)' : 'var(--red-600)'}}>
              {clpCompact(totalRevDelta)}
            </div>
            <div className="cv-stat-label">Rev delta/sem</div>
          </div>
          {consensus && (
            <div className="cv-stat">
              <div className="cv-stat-value">{consensus[0]}</div>
              <div className="cv-stat-label">Consenso ({consensus[1]}/{storeRows.length})</div>
            </div>
          )}
          <div className="cv-stat">
            <div className="cv-stat-value">{pending}</div>
            <div className="cv-stat-label">Pendientes</div>
          </div>
        </div>

        <div className="cv-table-wrap">
          <table className="cv-table">
            <thead>
              <tr>
                <th>Tienda</th>
                <th>Canal</th>
                <th>Precio actual</th>
                <th>Rec.</th>
                <th>Desc.</th>
                <th>Vel.</th>
                <th>Rev delta</th>
                <th>Margen</th>
                <th>Estado</th>
              </tr>
            </thead>
            <tbody>
              {storeRows.map(r => (
                <tr key={r.key} className={r.status ? `cv-row--${r.status}` : ''}>
                  <td className="cv-store">{r.store_name || r.store}</td>
                  <td><span className={`cv-channel cv-channel--${r.channel}`}>{r.channel === 'ecomm' ? 'EC' : 'B&M'}</span></td>
                  <td className="cv-mono">{clp(r.current_price)}</td>
                  <td className="cv-mono">{clp(r.recommended_price)}</td>
                  <td>{r.recommended_discount}</td>
                  <td>{r.current_velocity}</td>
                  <td className="cv-mono" style={{color: Number(r.rev_delta) >= 0 ? 'var(--green-600)' : 'var(--red-600)'}}>
                    {clpCompact(r.rev_delta)}
                  </td>
                  <td>{r.margin_pct != null ? `${r.margin_pct}%` : '\u2014'}</td>
                  <td>
                    {r.status === 'approved' && <span className="cv-status cv-status--approved"><Check size={10} /> OK</span>}
                    {r.status === 'rejected' && <span className="cv-status cv-status--rejected"><X size={10} /></span>}
                    {r.status === 'manual' && <span className="cv-status cv-status--manual">Manual</span>}
                    {!r.status && <span className="cv-status cv-status--pending">Pendiente</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {pending > 0 && (
          <div className="cv-actions">
            <span className="cv-actions-label">Aprobar pendientes:</span>
            <button className="cv-scope-btn" onClick={() => handleApply('all')}>
              <Globe size={13} /> Todas ({pending})
            </button>
            {ecommPending > 0 && (
              <button className="cv-scope-btn cv-scope-btn--ecomm" onClick={() => handleApply('ecomm')}>
                <ShoppingBag size={13} /> Solo ecomm ({ecommPending})
              </button>
            )}
            {bmPending > 0 && (
              <button className="cv-scope-btn cv-scope-btn--bm" onClick={() => handleApply('bm')}>
                <Store size={13} /> Solo B&M ({bmPending})
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
