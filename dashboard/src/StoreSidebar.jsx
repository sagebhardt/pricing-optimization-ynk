import { useRef, useEffect } from 'react'
import { Check, ChevronRight } from 'lucide-react'

function clpCompact(n) {
  if (n === null || n === undefined || isNaN(n)) return '\u2014'
  const num = Number(n)
  const sign = num >= 0 ? '+' : ''
  if (Math.abs(num) >= 1_000_000) return `${sign}${(num / 1_000_000).toFixed(1)}M`
  if (Math.abs(num) >= 1_000) return `${sign}${Math.round(num / 1_000)}K`
  return `${sign}${Math.round(num)}`
}

function StoreRow({ item, active, onSelect, onApprove, canApprove }) {
  const ref = useRef(null)
  const pct = item.total > 0 ? Math.round((item.decided / item.total) * 100) : 0
  const allDone = item.pending === 0

  useEffect(() => {
    if (active && ref.current) ref.current.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
  }, [active])

  return (
    <div ref={ref}
         className={`sb-row ${active ? 'sb-row--active' : ''} ${allDone ? 'sb-row--done' : ''}`}
         onClick={() => onSelect(item.name)}>
      <div className="sb-row-main">
        <span className="sb-row-name">
          {item.name}
          {item.ccCount > 0 && item.ccSum / item.ccCount > 0.2 && (
            <span className="sb-cc-tag">{Math.round(item.ccSum / item.ccCount * 100)}% C&C</span>
          )}
        </span>
        {item.pending > 0 ? (
          <span className={`sb-badge ${item.highCount > 0 ? 'sb-badge--high' : item.medCount > 0 ? 'sb-badge--med' : 'sb-badge--low'}`}>
            {item.pending}
          </span>
        ) : (
          <span className="sb-badge sb-badge--done"><Check size={10} /></span>
        )}
      </div>
      <div className="sb-row-meta">
        <div className="sb-bar-track">
          <div className="sb-bar-fill" style={{ width: `${pct}%` }} />
        </div>
        <span className="sb-rev">{clpCompact(item.revDelta)}</span>
      </div>
      {canApprove && item.pending > 0 && (
        <button className="sb-approve-btn" onClick={e => { e.stopPropagation(); onApprove(item.name) }}
                title={`Aprobar ${item.pending} pendientes`}>
          Aprobar ({item.pending})
        </button>
      )}
    </div>
  )
}

export default function StoreSidebar({ roster, activeItem, onSelect, onApprove, canApprove, title = 'Tiendas' }) {
  const totalPending = roster.reduce((s, r) => s + r.pending, 0)
  const totalItems = roster.reduce((s, r) => s + r.total, 0)

  return (
    <div className="sb-sidebar">
      <div className="sb-header">
        <span className="sb-title">{title}</span>
        <span className="sb-summary">{totalPending} pendientes / {totalItems}</span>
      </div>
      <div className="sb-list">
        {roster.map(item => (
          <StoreRow key={item.key || item.name} item={item} active={activeItem === item.name}
                    onSelect={onSelect} onApprove={onApprove} canApprove={canApprove} />
        ))}
      </div>
    </div>
  )
}
