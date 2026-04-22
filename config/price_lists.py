"""Classifier for datawarehouse.lista_precio.descripcion → market bucket.

SAP-backed `datawarehouse.lista_precio` has 55 named price lists across brands
(e.g., "Hoka tiendas", "Liquidación Bold", "Outlet la fabrica", "Bamers Virtual",
"Bamers Eventos"). Each invoice in `factura_cabecera` references one list, so
the list name is the cleanest available signal for whether a sale happened under
a regular-retail vs markdown/clearance regime. Used to fix the long-standing
"elasticity conflated with markdown effects" known issue.

Categories:
- retail: regular store or general list (default)
- liquidacion: clearance/liquidation lists
- outlet: outlet store lists
- eventos: event/special-sale lists
- online: marketplace, virtual, wholesale
- unknown: null or blank descripcion
"""

import re

# Match "liq " or "liq." as a standalone prefix (not inside other words).
# The "liquid*" variants are caught by the substring check above this regex;
# this regex only exists for the rare bare "Liq." form.
_LIQ_PATTERN = re.compile(r"\bliq[\s.]", re.IGNORECASE)


def classify_price_list(desc):
    """Return the market bucket for a lista_precio.descripcion value."""
    if not desc or not str(desc).strip():
        return "unknown"
    s = str(desc).lower()
    if "liquid" in s or _LIQ_PATTERN.search(s):
        return "liquidacion"
    if "outlet" in s:
        return "outlet"
    if "evento" in s:
        return "eventos"
    if "virtual" in s or "marketplace" in s or "wholesale" in s:
        return "online"
    return "retail"


def is_markdown(desc):
    """True when the list represents a markdown regime (clearance/outlet)."""
    return classify_price_list(desc) in ("liquidacion", "outlet")
