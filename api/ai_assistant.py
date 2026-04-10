"""AI-powered pricing assistant using Gemini.

Provides natural language Q&A over pricing data, model metrics,
competitor insights, and recommendations.
"""

import os
import json


GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-2.5-flash"

SYSTEM_PROMPT = """Eres el asistente de pricing de Yaneken Retail Group. Ayudas a brand managers
a entender las recomendaciones de pricing, el rendimiento del modelo, y datos de competencia.

Responde siempre en español chileno. Sé conciso y directo. Usa datos específicos cuando los tengas.
Si no tienes datos suficientes para responder, dilo claramente.

Contexto del sistema:
- 5 marcas: HOKA (3 tiendas), BOLD (35 tiendas, Nike/Adidas/Puma), BAMERS (25 tiendas, Skechers),
  OAKLEY (8 tiendas), BELSPORT (66 tiendas)
- El modelo predice descuento óptimo por SKU padre × tienda × semana para maximizar margen bruto
- Modelos: XGBoost (clasificador) + LightGBM (regresor), con datos de elasticidad, lifecycle, clima, competencia
- Click & collect: precios se calculan solo con ventas retail (cupones y C&C excluidos)
- Competidores monitoreados: Falabella, Paris, theline.cl, nike.cl, hoka.cl, sparta.cl, marathon.cl
"""


def build_context(brand: str) -> str:
    """Build data context for the LLM from current pricing data."""
    from api import storage

    parts = []

    # Model metrics
    meta = storage.load_model_info(brand)
    if meta:
        reg = meta.get("regressor", {})
        rh = reg.get("holdout", {})
        parts.append(f"Modelo {brand}: R²(CV)={reg.get('avg_r2', '?'):.3f}, "
                     f"Holdout R²={rh.get('r2', '?')}, MAE={reg.get('avg_mae', 0)*100:.1f}pp, "
                     f"Features={meta.get('classifier', {}).get('n_features', '?')}")

    # Pricing actions summary
    actions = storage.load_pricing_actions(brand)
    items = actions.get("items", [])
    if items:
        high = sum(1 for a in items if a.get("urgency") == "HIGH")
        increases = sum(1 for a in items if a.get("action_type") == "increase")
        total_rev = sum(a.get("rev_delta", 0) for a in items)
        parts.append(f"Acciones semana {actions.get('week', '?')}: {len(items)} total, "
                     f"{high} alta urgencia, {increases} subidas, rev delta ${total_rev:+,}/sem")

        # Top 5 actions
        top = sorted(items, key=lambda x: -(x.get("rev_delta", 0)))[:5]
        parts.append("Top 5 oportunidades:")
        for a in top:
            parts.append(f"  {a.get('parent_sku', '?')} @ {a.get('store_name', '?')}: "
                        f"${a.get('current_price', 0):,} → ${a.get('recommended_price', 0):,} "
                        f"({a.get('urgency', '?')}) rev ${a.get('rev_delta', 0):+,}/sem "
                        f"razón: {a.get('reasons', '')[:80]}")

    # Competitor data
    comp = storage.load_competitor_summary(brand)
    if comp.get("items"):
        parts.append(f"Competencia: {comp['coverage']['total_parents']} productos monitoreados "
                     f"en {comp['coverage'].get('by_competitor', {})}")

    # SHAP features
    shap = storage.load_shap_features(brand, "regressor")
    if shap:
        parts.append("Top features SHAP: " + ", ".join(f"{s['feature']}={s['mean_abs_shap']:.4f}" for s in shap[:8]))

    return "\n".join(parts)


def ask(question: str, brand: str) -> str:
    """Ask a question about pricing data using Gemini."""
    if not GEMINI_API_KEY:
        return "API key de Gemini no configurada. Configura GEMINI_API_KEY."

    from google import genai

    context = build_context(brand)

    prompt = f"""{SYSTEM_PROMPT}

DATOS ACTUALES ({brand.upper()}):
{context}

PREGUNTA DEL USUARIO:
{question}

Responde de forma concisa y accionable. Si la pregunta se refiere a un SKU específico, busca en los datos."""

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        return response.text
    except Exception as e:
        return f"Error al consultar Gemini: {e}"
