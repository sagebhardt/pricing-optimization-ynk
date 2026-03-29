import { useState, useEffect, useRef, useCallback } from 'react'
import { X, AlertTriangle, Check, DollarSign } from 'lucide-react'

function clp(n) {
  if (n === null || n === undefined || n === '' || isNaN(n)) return '\u2014'
  return '$' + Math.round(Number(n)).toLocaleString('es-CL')
}

export default function ManualPriceModal({ action, brand, authFetch, onConfirm, onClose }) {
  const [inputPrice, setInputPrice] = useState('')
  const [impact, setImpact] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const inputRef = useRef(null)
  const debounceRef = useRef(null)

  // Focus input on open
  useEffect(() => {
    inputRef.current?.focus()
    if (action?.recommended_price) {
      setInputPrice(String(action.recommended_price))
    }
    return () => clearTimeout(debounceRef.current)
  }, [action])

  // Debounced impact estimation
  const fetchImpact = useCallback((price) => {
    if (!price || isNaN(price) || Number(price) <= 0) {
      setImpact(null)
      return
    }
    setLoading(true)
    setError(null)
    authFetch('/estimate-impact', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: brand,
        parent_sku: action.parent_sku,
        store: action.store,
        manual_price: Number(price),
      }),
    })
      .then(r => {
        if (!r.ok) throw new Error('Error estimando impacto')
        return r.json()
      })
      .then(data => { setImpact(data); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }, [brand, action, authFetch])

  const handlePriceChange = (e) => {
    const val = e.target.value.replace(/[^\d]/g, '')
    setInputPrice(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => fetchImpact(val), 400)
  }

  const handleConfirm = () => {
    if (!impact || !impact.snapped_price) return
    onConfirm(impact.snapped_price, impact)
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && impact && !loading) handleConfirm()
    if (e.key === 'Escape') onClose()
  }

  const marginColor = (pct) => {
    if (pct === null || pct === undefined) return 'var(--slate-500)'
    if (pct >= 40) return 'var(--green-600)'
    if (pct >= 20) return 'var(--amber-600)'
    return 'var(--red-600)'
  }

  return (
    <div className="mp-overlay" onClick={onClose}>
      <div className="mp-modal" onClick={e => e.stopPropagation()} onKeyDown={handleKeyDown}>
        <div className="mp-header">
          <h3>Precio manual</h3>
          <button className="mp-close" onClick={onClose}><X size={16} /></button>
        </div>

        <div className="mp-product">
          <span className="mp-sku">{action.parent_sku}</span>
          <span className="mp-name">{action.product}</span>
          <span className="mp-store">{action.store_name || action.store}</span>
        </div>

        <div className="mp-current">
          <div className="mp-current-item">
            <span className="mp-current-label">Lista</span>
            <span className="mp-current-value">{clp(action.current_list_price)}</span>
          </div>
          <div className="mp-current-item">
            <span className="mp-current-label">Actual</span>
            <span className="mp-current-value">{clp(action.current_price)}</span>
          </div>
          <div className="mp-current-item">
            <span className="mp-current-label">Modelo</span>
            <span className="mp-current-value">{clp(action.recommended_price)}</span>
          </div>
        </div>

        <div className="mp-input-section">
          <label className="mp-input-label"><DollarSign size={14} /> Nuevo precio</label>
          <input
            ref={inputRef}
            type="text"
            className="mp-input"
            placeholder="Ej: 29990"
            value={inputPrice}
            onChange={handlePriceChange}
          />
          {impact?.snapped_price && Number(inputPrice) !== impact.snapped_price && (
            <div className="mp-snap-hint">Se ajusta a {clp(impact.snapped_price)}</div>
          )}
        </div>

        {loading && <div className="mp-loading">Calculando impacto...</div>}
        {error && <div className="mp-error">{error}</div>}

        {impact && !loading && (
          <div className="mp-impact">
            <div className="mp-impact-row">
              <span>Precio final</span>
              <span className="mp-impact-value">{clp(impact.snapped_price)}</span>
            </div>
            <div className="mp-impact-row">
              <span>Velocidad esperada</span>
              <span className="mp-impact-value">{impact.velocity} u/sem</span>
            </div>
            <div className="mp-impact-row">
              <span>Revenue semanal</span>
              <span className="mp-impact-value">{clp(impact.weekly_revenue)}</span>
            </div>
            {impact.margin_pct !== null && (
              <div className="mp-impact-row">
                <span>Margen</span>
                <span className="mp-impact-value" style={{ color: marginColor(impact.margin_pct) }}>
                  {impact.margin_pct}%
                </span>
              </div>
            )}
            {impact.margin_delta !== null && (
              <div className="mp-impact-row">
                <span>Delta margen/sem</span>
                <span className="mp-impact-value">{clp(impact.margin_delta)}</span>
              </div>
            )}
            {impact.warning && (
              <div className={`mp-warning mp-warning--${impact.warning}`}>
                <AlertTriangle size={13} />
                {impact.warning === 'below_cost' && 'Precio por debajo del costo'}
                {impact.warning === 'below_margin_floor' && 'Margen bajo piso minimo (15%)'}
                {impact.warning === 'thin_margin' && 'Margen delgado (<20%)'}
                {impact.warning === 'no_cost_data' && 'Sin datos de costo — margen no disponible'}
              </div>
            )}
          </div>
        )}

        <div className="mp-actions">
          <button className="mp-btn mp-btn--cancel" onClick={onClose}>Cancelar</button>
          <button className="mp-btn mp-btn--confirm" disabled={!impact || loading || impact?.warning === 'below_cost'}
                  onClick={handleConfirm}>
            <Check size={14} /> Confirmar {impact?.snapped_price ? clp(impact.snapped_price) : ''}
          </button>
        </div>
      </div>
    </div>
  )
}
