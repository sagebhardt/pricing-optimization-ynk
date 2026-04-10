"""Weekly pricing report — generates summary for email/notification.

Called after pipeline completes on Monday morning.
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def generate_weekly_report(brand: str = None) -> dict:
    """Generate a weekly summary report for a brand or all brands."""
    from api import storage

    brands = [brand] if brand else ["hoka", "bold", "bamers", "oakley", "belsport"]
    sections = []

    for b in brands:
        actions = storage.load_pricing_actions(b)
        items = actions.get("items", [])
        week = actions.get("week", "?")

        if not items:
            sections.append({"brand": b.upper(), "week": week, "total": 0})
            continue

        high = sum(1 for a in items if a.get("urgency") == "HIGH")
        medium = sum(1 for a in items if a.get("urgency") == "MEDIUM")
        increases = sum(1 for a in items if a.get("action_type") == "increase")
        decreases = sum(1 for a in items if a.get("action_type") == "decrease")
        total_rev = sum(a.get("rev_delta", 0) for a in items)
        total_margin = sum(a.get("margin_delta", 0) for a in items if a.get("margin_delta"))

        # Top opportunities (highest rev delta)
        top_opps = sorted(items, key=lambda x: -(x.get("rev_delta", 0)))[:3]

        # Competitor insights
        comp = storage.load_competitor_summary(b)
        comp_count = comp.get("coverage", {}).get("total_parents", 0)

        # Cross-store alerts
        cross = storage.load_cross_store_alerts(b)
        alert_count = cross["codigo_padre"].nunique() if len(cross) > 0 and "codigo_padre" in cross.columns else 0

        sections.append({
            "brand": b.upper(),
            "week": week,
            "total": len(items),
            "high": high,
            "medium": medium,
            "increases": increases,
            "decreases": decreases,
            "rev_delta": int(total_rev),
            "margin_delta": int(total_margin),
            "top_opportunities": [
                {"sku": a.get("parent_sku", ""), "product": a.get("product", ""), "rev_delta": a.get("rev_delta", 0)}
                for a in top_opps
            ],
            "competitor_products": comp_count,
            "cross_store_alerts": alert_count,
        })

    return {"brands": sections, "week": sections[0]["week"] if sections else "?"}


def format_email_html(report: dict) -> str:
    """Format report as HTML email."""
    week = report["week"]
    html = f"""
    <div style="font-family: -apple-system, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #1e293b;">
        <h2 style="color: #0f172a; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px;">
            YNK Pricing — Semana {week}
        </h2>
    """

    total_actions = sum(b["total"] for b in report["brands"])
    total_rev = sum(b.get("rev_delta", 0) for b in report["brands"])
    html += f"""
        <p style="font-size: 15px; color: #475569;">
            <strong>{total_actions}</strong> acciones de pricing pendientes.
            Impacto estimado: <strong>${total_rev:+,} CLP/semana</strong>.
        </p>
    """

    for b in report["brands"]:
        if b["total"] == 0:
            continue

        color = "#dc2626" if b["high"] > 10 else "#f59e0b" if b["high"] > 0 else "#22c55e"
        html += f"""
        <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 14px 18px; margin: 12px 0;">
            <h3 style="margin: 0 0 8px; color: #0f172a;">{b["brand"]}
                <span style="font-size: 12px; color: {color}; font-weight: 600; margin-left: 8px;">
                    {b["high"]} ALTA · {b["medium"]} MEDIA
                </span>
            </h3>
            <div style="font-size: 13px; color: #64748b; line-height: 1.6;">
                {b["total"]} acciones ({b["increases"]} subidas, {b["decreases"]} bajadas)<br>
                Rev: <strong>${b["rev_delta"]:+,}</strong>/sem · Margen: <strong>${b["margin_delta"]:+,}</strong>/sem
        """
        if b.get("competitor_products"):
            html += f"""<br>{b["competitor_products"]} productos con datos de competencia"""
        if b.get("cross_store_alerts"):
            html += f"""<br>⚠️ {b["cross_store_alerts"]} alertas de consistencia entre tiendas"""

        if b.get("top_opportunities"):
            html += """<br><span style="font-size: 11px; color: #94a3b8;">Top oportunidades:</span>"""
            for opp in b["top_opportunities"]:
                html += f"""<br>· {opp["product"][:35]} — ${opp["rev_delta"]:+,}/sem"""

        html += """
            </div>
        </div>
        """

    html += f"""
        <p style="font-size: 12px; color: #94a3b8; margin-top: 20px;">
            <a href="https://pricing-api-467343668842.us-central1.run.app" style="color: #3b82f6;">Abrir dashboard</a>
        </p>
    </div>
    """
    return html


def format_plain_text(report: dict) -> str:
    """Format report as plain text."""
    lines = [f"YNK Pricing — Semana {report['week']}", ""]
    total_actions = sum(b["total"] for b in report["brands"])
    total_rev = sum(b.get("rev_delta", 0) for b in report["brands"])
    lines.append(f"{total_actions} acciones pendientes. Impacto: ${total_rev:+,} CLP/sem.")
    lines.append("")

    for b in report["brands"]:
        if b["total"] == 0:
            continue
        lines.append(f"[{b['brand']}] {b['total']} acciones ({b['high']} alta, {b['medium']} media)")
        lines.append(f"  Rev: ${b['rev_delta']:+,}/sem · Margen: ${b['margin_delta']:+,}/sem")
        if b.get("top_opportunities"):
            for opp in b["top_opportunities"]:
                lines.append(f"  · {opp['product'][:35]} — ${opp['rev_delta']:+,}/sem")
        lines.append("")

    lines.append("Dashboard: https://pricing-api-467343668842.us-central1.run.app")
    return "\n".join(lines)


if __name__ == "__main__":
    os.environ.setdefault("GCS_BUCKET", "ynk-pricing-decisions")
    report = generate_weekly_report()
    print(format_plain_text(report))
