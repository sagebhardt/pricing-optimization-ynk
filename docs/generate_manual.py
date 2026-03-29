#!/usr/bin/env python3
"""Generate the YNK Pricing Optimization user manual as PDF."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, mm
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, HRFlowable, Image
)
from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate, Frame
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.fonts import addMapping
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from io import BytesIO
from pathlib import Path
import datetime

# ── Colors ──────────────────────────────────────────────────────────────────
NAVY = HexColor("#1a2332")
DARK = HexColor("#2d3748")
MEDIUM = HexColor("#4a5568")
LIGHT_TEXT = HexColor("#718096")
ACCENT = HexColor("#2563eb")
ACCENT_DARK = HexColor("#1d4ed8")
GREEN = HexColor("#059669")
AMBER = HexColor("#d97706")
RED = HexColor("#dc2626")
BG_LIGHT = HexColor("#f8fafc")
BG_BLUE = HexColor("#eff6ff")
BORDER = HexColor("#e2e8f0")
TABLE_HEADER = HexColor("#1e293b")
TABLE_ALT = HexColor("#f1f5f9")

# ── Styles ──────────────────────────────────────────────────────────────────
styles = getSampleStyleSheet()

styles.add(ParagraphStyle(
    'CoverTitle', fontName='Helvetica-Bold', fontSize=32, leading=38,
    textColor=NAVY, alignment=TA_LEFT, spaceAfter=12,
))
styles.add(ParagraphStyle(
    'CoverSubtitle', fontName='Helvetica', fontSize=14, leading=20,
    textColor=MEDIUM, alignment=TA_LEFT, spaceAfter=6,
))
styles.add(ParagraphStyle(
    'CoverMeta', fontName='Helvetica', fontSize=11, leading=16,
    textColor=LIGHT_TEXT, alignment=TA_LEFT,
))
styles.add(ParagraphStyle(
    'H1', fontName='Helvetica-Bold', fontSize=20, leading=26,
    textColor=NAVY, spaceBefore=24, spaceAfter=12,
))
styles.add(ParagraphStyle(
    'H2', fontName='Helvetica-Bold', fontSize=15, leading=20,
    textColor=DARK, spaceBefore=18, spaceAfter=8,
))
styles.add(ParagraphStyle(
    'H3', fontName='Helvetica-Bold', fontSize=12, leading=16,
    textColor=ACCENT_DARK, spaceBefore=12, spaceAfter=6,
))
styles.add(ParagraphStyle(
    'Body', fontName='Helvetica', fontSize=10, leading=15,
    textColor=DARK, alignment=TA_JUSTIFY, spaceAfter=6,
))
styles.add(ParagraphStyle(
    'BodyBold', fontName='Helvetica-Bold', fontSize=10, leading=15,
    textColor=DARK, spaceAfter=6,
))
styles.add(ParagraphStyle(
    'MyBullet', fontName='Helvetica', fontSize=10, leading=15,
    textColor=DARK, leftIndent=18, bulletIndent=6, spaceAfter=3,
    bulletFontName='Helvetica', bulletFontSize=10,
))
styles.add(ParagraphStyle(
    'Note', fontName='Helvetica-Oblique', fontSize=9.5, leading=14,
    textColor=MEDIUM, leftIndent=12, rightIndent=12, spaceAfter=8,
    borderColor=ACCENT, borderWidth=0, borderPadding=4,
))
styles.add(ParagraphStyle(
    'MyCode', fontName='Courier', fontSize=8.5, leading=12,
    textColor=DARK, leftIndent=12, spaceAfter=6,
    backColor=BG_LIGHT, borderColor=BORDER, borderWidth=0.5, borderPadding=6,
))
styles.add(ParagraphStyle(
    'Caption', fontName='Helvetica-Oblique', fontSize=9, leading=13,
    textColor=LIGHT_TEXT, alignment=TA_CENTER, spaceAfter=12,
))
styles.add(ParagraphStyle(
    'BetaBanner', fontName='Helvetica-Bold', fontSize=11, leading=16,
    textColor=ACCENT_DARK, alignment=TA_CENTER, spaceAfter=6,
    backColor=BG_BLUE, borderColor=ACCENT, borderWidth=1, borderPadding=10,
))
styles.add(ParagraphStyle(
    'Footer', fontName='Helvetica', fontSize=8, leading=10,
    textColor=LIGHT_TEXT, alignment=TA_CENTER,
))
styles.add(ParagraphStyle(
    'TOCEntry', fontName='Helvetica', fontSize=11, leading=18,
    textColor=DARK, leftIndent=12, spaceAfter=2,
))
styles.add(ParagraphStyle(
    'TOCEntry2', fontName='Helvetica', fontSize=10, leading=16,
    textColor=MEDIUM, leftIndent=30, spaceAfter=1,
))

# ── Helper functions ────────────────────────────────────────────────────────

def h1(text):
    return Paragraph(text, styles['H1'])

def h2(text):
    return Paragraph(text, styles['H2'])

def h3(text):
    return Paragraph(text, styles['H3'])

def p(text):
    return Paragraph(text, styles['Body'])

def pb(text):
    return Paragraph(text, styles['BodyBold'])

def bullet(text):
    return Paragraph(f"<bullet>&bull;</bullet> {text}", styles['MyBullet'])

def note(text):
    return Paragraph(f"<i>{text}</i>", styles['Note'])

def sp(h=6):
    return Spacer(1, h)

def hr():
    return HRFlowable(width="100%", thickness=0.5, color=BORDER, spaceAfter=8, spaceBefore=8)

_cell_style = ParagraphStyle('CellBody', fontName='Helvetica', fontSize=9, leading=12, textColor=DARK)
_cell_header_style = ParagraphStyle('CellHeader', fontName='Helvetica-Bold', fontSize=9, leading=12, textColor=white)

def make_table(data, col_widths=None, has_header=True):
    """Create a styled table. Wraps all string cells in Paragraphs for proper text wrapping."""
    # Convert plain strings to Paragraphs so text wraps within cells
    wrapped = []
    for i, row in enumerate(data):
        style = _cell_header_style if (i == 0 and has_header) else _cell_style
        wrapped.append([Paragraph(str(cell).replace('\n', '<br/>'), style) if isinstance(cell, str) else cell for cell in row])
    t = Table(wrapped, colWidths=col_widths, repeatRows=1 if has_header else 0)
    style_cmds = [
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('GRID', (0, 0), (-1, -1), 0.5, BORDER),
    ]
    if has_header:
        style_cmds += [
            ('BACKGROUND', (0, 0), (-1, 0), TABLE_HEADER),
        ]
    # Alternate row colors
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_cmds.append(('BACKGROUND', (0, i), (-1, i), TABLE_ALT))
    t.setStyle(TableStyle(style_cmds))
    return t

def callout_box(title, text):
    """Blue callout box."""
    content = Paragraph(f"<b>{title}</b><br/><br/>{text}", ParagraphStyle(
        'CalloutContent', fontName='Helvetica', fontSize=9.5, leading=14, textColor=DARK
    ))
    data = [[content]]
    t = Table(data, colWidths=[6.3*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), BG_BLUE),
        ('BOX', (0, 0), (-1, -1), 1, ACCENT),
        ('LEFTPADDING', (0, 0), (-1, -1), 12),
        ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
    ]))
    return t

def warning_box(text):
    """Amber warning box."""
    inner = Paragraph(f"<b>Importante:</b> {text}", ParagraphStyle(
        'WarningBody', fontName='Helvetica', fontSize=9.5, leading=14, textColor=HexColor("#92400e")
    ))
    data = [[inner]]
    t = Table(data, colWidths=[6.5*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), HexColor("#fffbeb")),
        ('BOX', (0, 0), (-1, -1), 1, AMBER),
        ('LEFTPADDING', (0, 0), (-1, -1), 12),
        ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
    ]))
    return t


# ── Page template with header/footer ───────────────────────────────────────

class ManualDocTemplate(BaseDocTemplate):
    def __init__(self, filename, **kwargs):
        BaseDocTemplate.__init__(self, filename, **kwargs)
        frame = Frame(
            self.leftMargin, self.bottomMargin + 20,
            self.width, self.height - 20,
            id='normal'
        )
        template = PageTemplate(id='normal', frames=frame, onPage=self._on_page)
        self.addPageTemplates([template])
        self._page_count = 0

    def _on_page(self, canvas, doc):
        canvas.saveState()
        # Header line
        canvas.setStrokeColor(BORDER)
        canvas.setLineWidth(0.5)
        y_top = doc.height + doc.topMargin + 10
        canvas.line(doc.leftMargin, y_top, doc.leftMargin + doc.width, y_top)
        # Header text
        canvas.setFont('Helvetica', 7.5)
        canvas.setFillColor(LIGHT_TEXT)
        canvas.drawString(doc.leftMargin, y_top + 4, "YNK Pricing Optimization — Manual de Usuario")
        canvas.drawRightString(doc.leftMargin + doc.width, y_top + 4, "BETA v1.0 — Marzo 2026")
        # Footer
        canvas.setStrokeColor(BORDER)
        canvas.line(doc.leftMargin, doc.bottomMargin + 10, doc.leftMargin + doc.width, doc.bottomMargin + 10)
        canvas.setFont('Helvetica', 8)
        canvas.setFillColor(LIGHT_TEXT)
        canvas.drawCentredString(doc.leftMargin + doc.width / 2, doc.bottomMargin - 2,
                                 f"Página {doc.page}")
        canvas.drawRightString(doc.leftMargin + doc.width, doc.bottomMargin - 2,
                               "Confidencial — Yáneken Retail")
        canvas.restoreState()


# ── Build the document ──────────────────────────────────────────────────────

def build_manual():
    output_path = Path(__file__).parent / "manual_ynk_pricing.pdf"
    doc = ManualDocTemplate(
        str(output_path),
        pagesize=letter,
        leftMargin=0.9*inch,
        rightMargin=0.9*inch,
        topMargin=0.7*inch,
        bottomMargin=0.7*inch,
        title="YNK Pricing Optimization — Manual de Usuario",
        author="Yáneken Retail",
    )

    story = []
    W = 6.5 * inch  # usable width

    # ════════════════════════════════════════════════════════════════════════
    # COVER PAGE
    # ════════════════════════════════════════════════════════════════════════
    story.append(sp(100))
    story.append(Paragraph("YNK Pricing<br/>Optimization", styles['CoverTitle']))
    story.append(sp(8))
    story.append(Paragraph("Manual de Usuario", ParagraphStyle(
        'CoverSub2', fontName='Helvetica', fontSize=18, leading=24, textColor=ACCENT,
    )))
    story.append(sp(20))
    story.append(HRFlowable(width="40%", thickness=2, color=ACCENT, spaceAfter=20))
    story.append(Paragraph("Optimización de precios y márgenes basada en Machine Learning", styles['CoverSubtitle']))
    story.append(Paragraph("para Yáneken Retail", styles['CoverSubtitle']))
    story.append(sp(30))
    story.append(Paragraph(f"Versión BETA 1.0 — Marzo 2026", styles['CoverMeta']))
    story.append(Paragraph("Sebastián Gebhardt (SGR) — CEO, Yáneken Retail", styles['CoverMeta']))
    story.append(sp(6))
    story.append(Paragraph("Confidencial — Uso interno", styles['CoverMeta']))
    story.append(sp(6))
    story.append(Paragraph('<link href="https://pricing-api-467343668842.us-central1.run.app" color="#2563eb"><u>pricing-api-467343668842.us-central1.run.app</u></link>', styles['CoverMeta']))
    story.append(sp(40))
    story.append(Paragraph(
        "BETA — VERSION PRELIMINAR<br/>"
        "Este sistema está en fase beta. Las recomendaciones requieren validación "
        "por el equipo comercial antes de su implementación. Su feedback es esencial "
        "para mejorar la precisión y utilidad de la herramienta.",
        styles['BetaBanner']
    ))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # TABLE OF CONTENTS
    # ════════════════════════════════════════════════════════════════════════
    story.append(h1("Contenido"))
    story.append(sp(8))
    toc_items = [
        ("1.", "Introducción"),
        ("2.", "Conceptos Clave y Métricas"),
        ("", "2.1 Elasticidad-Precio"),
        ("", "2.2 Clasificador (Debería repricearse?)"),
        ("", "2.3 Regresor (A que descuento?)"),
        ("", "2.4 Métricas de Evaluación"),
        ("", "2.5 Ciclo de Vida del Producto"),
        ("", "2.6 Curva de Tallas"),
        ("3.", "Cómo se Genera una Recomendación"),
        ("", "3.1 Niveles de Confianza"),
        ("", "3.2 Clasificación de Urgencia"),
        ("", "3.3 Protecciones de Margen y Costo"),
        ("", "3.4 Anclaje de Precios"),
        ("4.", "Operación del Dashboard"),
        ("", "4.1 Vistas y Navegación"),
        ("", "4.2 Toma de Decisiones"),
        ("", "4.3 Precio Manual"),
        ("", "4.4 Vista Cadena"),
        ("", "4.5 Panel de Analitica"),
        ("", "4.6 Cola del Planner"),
        ("", "4.7 Exportación"),
        ("", "4.8 Administración de Usuarios"),
        ("", "4.9 Predicción vs Realidad (Feedback Loop)"),
        ("5.", "Rendimiento Actual por Marca"),
        ("6.", "Elasticidades: Qué Significan para el Negocio"),
        ("", "6.1-6.6 Resultados por marca e implicaciones estratégicas"),
        ("7.", "Limitaciones y Áreas de Mejora"),
        ("", "7.7 Inventario en Tránsito (Backorder)"),
        ("8.", "El Rol del Criterio Humano"),
        ("9.", "Cómo Darnos Feedback"),
    ]
    for num, title in toc_items:
        if num:
            story.append(Paragraph(f"<b>{num}</b> &nbsp; {title}", styles['TOCEntry']))
        else:
            story.append(Paragraph(title, styles['TOCEntry2']))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # 1. INTRODUCTION
    # ════════════════════════════════════════════════════════════════════════
    story.append(h1("1. Introducción"))
    story.append(p(
        "YNK Pricing Optimization es un sistema de Machine Learning que analiza datos históricos "
        "de ventas, precios, inventario y estacionalidad para recomendar el precio óptimo de cada "
        "producto en cada tienda, cada semana. Su objetivo es <b>maximizar el margen bruto</b> del "
        "Grupo Yáneken, equilibrando la velocidad de venta con la rentabilidad."
    ))
    story.append(p(
        "El sistema cubre actualmente cinco marcas: <b>HOKA</b>, <b>BOLD</b>, <b>BAMERS</b>, "
        "<b>OAKLEY</b> y <b>BELSPORT</b>, abarcando más de 130 tiendas y miles de SKUs activos."
    ))
    story.append(sp(4))
    story.append(callout_box(
        "Qué hace el sistema?",
        "Para cada producto-tienda-semana, el sistema responde dos preguntas:<br/>"
        "<b>1.</b> Debería ajustarse el precio? (Clasificador)<br/>"
        "<b>2.</b> Si la respuesta es si, a que precio? (Regresor)<br/><br/>"
        "Las recomendaciones se presentan en un dashboard web donde el equipo comercial "
        "las revisa, aprueba, rechaza o ajusta manualmente."
    ))
    story.append(sp(8))
    story.append(p(
        "El pipeline de datos se ejecuta automáticamente cada lunes a las 6:00 AM hora Chile, "
        "procesando las ventas de la semana anterior y generando nuevas recomendaciones. "
        "Los resultados están disponibles en el dashboard minutos después."
    ))
    story.append(sp(4))
    story.append(warning_box(
        "Este sistema está en versión beta. Las recomendaciones son una herramienta de apoyo "
        "al criterio comercial, no un reemplazo. Cada decisión de precio debe ser revisada y "
        "aprobada por un humano antes de implementarse."
    ))

    # ════════════════════════════════════════════════════════════════════════
    # 2. KEY CONCEPTS & METRICS
    # ════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(h1("2. Conceptos Clave y Métricas"))

    # 2.1 Elasticity
    story.append(h2("2.1 Elasticidad-Precio"))
    story.append(p(
        "La <b>elasticidad-precio de la demanda</b> mide cuánto cambia la cantidad vendida "
        "cuando cambia el precio. Es el insumo más importante del sistema, ya que permite "
        "estimar el impacto en ventas de un cambio de precio."
    ))
    story.append(sp(4))
    story.append(h3("Cómo se calcula"))
    story.append(p(
        "Se estima mediante una regresión log-log (OLS) sobre datos históricos de ventas:"
    ))
    story.append(sp(4))
    story.append(Paragraph(
        "&nbsp;&nbsp;&nbsp;&nbsp;ln(Cantidad) = &alpha; + <b>&beta;</b> &middot; ln(Precio) + controles + &epsilon;",
        ParagraphStyle('Formula', fontName='Helvetica', fontSize=11, leading=16,
                       textColor=NAVY, alignment=TA_CENTER, spaceAfter=8)
    ))
    story.append(p(
        "El coeficiente <b>&beta;</b> es la elasticidad: indica el cambio porcentual en cantidad "
        "vendida por cada 1% de cambio en precio. Un valor de -1.5 significa que un aumento de "
        "precio del 1% reduce las ventas en 1.5%."
    ))
    story.append(sp(4))
    story.append(p("<b>Variables de control:</b> La regresión incluye efectos de mes (estacionalidad), "
                   "tienda (ubicación) y tendencia temporal, para aislar el efecto puro del precio."))
    story.append(sp(4))
    story.append(h3("Niveles de estimación"))
    story.append(bullet("<b>Por SKU padre:</b> se usa si hay al menos 10 observaciones y variación de precio suficiente (CV > 5%)."))
    story.append(bullet("<b>Por subcategoría:</b> se usa como respaldo cuando no hay datos suficientes a nivel SKU (mínimo 30 observaciones)."))
    story.append(sp(4))
    story.append(h3("Interpretación"))
    story.append(make_table([
        ['Elasticidad', 'Interpretación', 'Implicancia comercial'],
        ['< -1.5', 'Muy elástico', 'Producto sensible al precio. Descuentos generan más volumen y pueden compensar margen.'],
        ['-1.0 a -1.5', 'Elástico', 'Respuesta significativa al precio. Descuentos moderados son efectivos.'],
        ['-0.5 a -1.0', 'Elasticidad unitaria', 'Equilibrio entre precio y volumen. Requiere análisis caso a caso.'],
        ['> -0.5', 'Inelástico', 'Demanda insensible al precio. Descuentos destruyen margen sin compensar en volumen.'],
    ], col_widths=[1.0*inch, 1.2*inch, 4.3*inch]))
    story.append(sp(4))
    story.append(h3("Niveles de confianza de la elasticidad"))
    story.append(make_table([
        ['Confianza', 'R\u00b2', 'Observaciones', 'Variacion de precio'],
        ['Alta', '> 0.30', '> 30', 'CV > 10%'],
        ['Media', '> 0.15', '> 15', 'Cualquiera'],
        ['Baja', '\u2264 0.15', '\u2264 15', 'Cualquiera'],
    ], col_widths=[1.2*inch, 1.2*inch, 1.5*inch, 2.6*inch]))
    story.append(sp(4))
    story.append(note(
        "R\u00b2 indica que proporción de la variabilidad en ventas es explicada por el precio. "
        "Un R\u00b2 de 0.30 significa que el 30% de la variación en ventas se explica por cambios de precio. "
        "El CV (coeficiente de variación) del precio mide cuánta variación de precio ha tenido el producto — "
        "sin variación, no se puede estimar la elasticidad."
    ))

    # 2.2 Classifier
    story.append(h2("2.2 El Clasificador: Debería repricearse?"))
    story.append(p(
        "El clasificador es un modelo <b>XGBoost</b> (gradient boosting) que predice si un producto "
        "debería ser repriceado esta semana. A diferencia de un modelo descriptivo que predice lo que "
        "<i>pasará</i>, este modelo es <b>prescriptivo</b>: predice lo que <i>debería</i> pasar para "
        "maximizar la rentabilidad."
    ))
    story.append(sp(4))
    story.append(h3("Qué aprende?"))
    story.append(p(
        "El clasificador aprende a identificar situaciones donde un cambio de precio mejoraría "
        "el margen bruto semanal. Para generar sus datos de entrenamiento, el sistema simula "
        "el beneficio de 9 niveles de descuento (0% a 40%, en pasos de 5pp) y determina si "
        "alguno de ellos generaría más margen que el precio actual."
    ))
    story.append(sp(4))
    story.append(h3("Variables de entrada (58 features)"))
    story.append(p("El modelo utiliza señales de múltiples dimensiones:"))
    story.append(bullet("<b>Precio actual:</b> descuento vigente, precio vs. lista, precio vs. otras tiendas"))
    story.append(bullet("<b>Velocidad:</b> unidades/semana actual vs. histórica, tendencia, estacionalidad"))
    story.append(bullet("<b>Inventario:</b> stock disponible, semanas de cobertura, curva de tallas"))
    story.append(bullet("<b>Producto:</b> categoría, temporada, edad del producto, ciclo de vida"))
    story.append(bullet("<b>Elasticidad:</b> sensibilidad al precio estimada, nivel de confianza"))
    story.append(bullet("<b>Margen:</b> costo unitario, margen actual, margen objetivo"))

    # 2.3 Regressor
    story.append(h2("2.3 El Regresor: A que descuento?"))
    story.append(p(
        "Para los productos que el clasificador marca como candidatos a repricing, el regresor "
        "(también XGBoost) predice el <b>descuento óptimo que maximiza el margen bruto semanal</b>."
    ))
    story.append(sp(4))
    story.append(h3("Cómo calcula el descuento óptimo?"))
    story.append(p(
        "Durante el entrenamiento, el sistema simula 9 escenarios de descuento (0%, 5%, 10%, ..., 40%) "
        "para cada producto-tienda-semana histórico. Para cada escenario:"
    ))
    story.append(bullet("Estima la velocidad de venta esperada usando la elasticidad-precio"))
    story.append(bullet("Calcula el precio neto (sin IVA del 19%)"))
    story.append(bullet("Calcula margen unitario = precio neto - costo"))
    story.append(bullet("Calcula margen semanal = margen unitario x velocidad esperada"))
    story.append(sp(4))
    story.append(p(
        "El descuento que produce el mayor margen semanal se convierte en el <b>target de entrenamiento</b>. "
        "El regresor aprende a predecir este óptimo directamente a partir de las features del producto."
    ))

    # 2.4 Evaluation metrics
    story.append(h2("2.4 Métricas de Evaluación"))
    story.append(p("Las siguientes métricas miden la calidad de los modelos:"))
    story.append(sp(4))
    story.append(h3("Clasificador"))
    story.append(make_table([
        ['Métrica', 'Qué mide', 'Rango', 'Interpretación'],
        ['AUC-ROC', 'Capacidad de separar repricing\nvs. no-repricing', '0 a 1', 'AUC > 0.90 = excelente.\n0.50 = aleatorio.'],
        ['Average\nPrecision', 'Precision promedio en todos\nlos umbrales de decisión', '0 a 1', 'Menos sensible al\ndesbalance de clases que AUC.'],
        ['Precision', 'De los que marco como repricing,\ncuántos realmente debian serlo', '0 a 1', 'Alta precisión = pocas falsas alarmas.'],
        ['Recall', 'De los que debian repricearse,\ncuántos logro identificar', '0 a 1', 'Alto recall = pocas oportunidades pérdidas.'],
    ], col_widths=[1.0*inch, 2.0*inch, 0.7*inch, 2.8*inch]))
    story.append(sp(8))
    story.append(h3("Regresor"))
    story.append(make_table([
        ['Métrica', 'Qué mide', 'Rango', 'Interpretación'],
        ['R\u00b2', 'Proporción de variabilidad del\ndescuento óptimo que el modelo\nexplica', '0 a 1', 'R\u00b2=0.80 significa que el modelo\nexplica el 80% de la variación.\n> 0.60 es bueno para retail.'],
        ['MAE\n(pp)', 'Error promedio en puntos\nporcentuales de descuento', '0+', 'MAE=5pp: el modelo se equivoca\nen promedio 5pp (ej: recomienda\n25% cuando el óptimo es 30%).'],
        ['RMSE', 'Error cuadrático medio\n(penaliza errores grandes)', '0+', 'Más sensible a errores grandes\nque MAE.'],
    ], col_widths=[0.8*inch, 2.0*inch, 0.7*inch, 3.0*inch]))
    story.append(sp(4))
    story.append(h3("Evaluación holdout (fuera de muestra)"))
    story.append(p(
        "Además de la validación cruzada, el sistema reserva las <b>últimas 4 semanas</b> de datos "
        "como test fuera de tiempo. Esto simula el rendimiento real del modelo prediciendo el futuro, "
        "no datos que ya vio. Las métricas de holdout son la referencia más confiable del rendimiento "
        "esperado en producción."
    ))

    # 2.5 Lifecycle
    story.append(h2("2.5 Ciclo de Vida del Producto"))
    story.append(p(
        "Cada producto se clasifica automáticamente en una etapa de su ciclo de vida basándose "
        "en el patrón de sus ventas. Esto permite que el modelo ajuste sus recomendaciones "
        "según la etapa — no es lo mismo un lanzamiento que un producto en liquidación."
    ))
    story.append(sp(4))
    story.append(make_table([
        ['Etapa', 'Descripción', 'Criterio'],
        ['Lanzamiento', 'Primeras semanas en tienda', 'Primeras 4 semanas de venta'],
        ['Crecimiento', 'Demanda en alza', 'Velocidad 4 semanas > velocidad 8 semanas y por encima de la mediana'],
        ['Peak', 'Máximo de demanda', 'Velocidad 4 semanas \u2265 80% del máximo histórico'],
        ['Estable', 'Demanda constante', 'Velocidad dentro de rango normal (estado por defecto)'],
        ['Declive', 'Demanda cayendo', 'Velocidad < 50% de la mediana o cayendo 4+ semanas'],
        ['Liquidación', 'Fin de vida + descuento', 'En declive y con descuento > 15%'],
    ], col_widths=[1.2*inch, 1.8*inch, 3.5*inch]))

    # 2.6 Size curve
    story.append(h2("2.6 Curva de Tallas"))
    story.append(p(
        "Para calzado, la disponibilidad de tallas es crítica para las ventas. El sistema monitorea "
        "la <b>curva de tallas</b> de cada producto, detectando cuando se rompe (faltan tallas clave) "
        "y usando esto como señal para recomendar markdown."
    ))
    story.append(sp(4))
    story.append(p("<b>Métricas clave:</b>"))
    story.append(bullet("<b>Attrition rate:</b> porcentaje de tallas pérdidas respecto al peak. Attrition > 30% genera alerta."))
    story.append(bullet("<b>Core completeness:</b> disponibilidad de tallas clave (7-10.5). Si cae bajo 50%, se genera alerta."))
    story.append(bullet("<b>Fragmentación:</b> que tan irregulares son las tallas restantes (talla 7, luego salta a 10)."))
    story.append(sp(4))
    story.append(note(
        "Las alertas de curva de tallas son visibles en el dashboard y se incorporan como features en los "
        "modelos. Un producto con curva de tallas rota tiene menos probabilidad de venderse a precio completo."
    ))

    # ════════════════════════════════════════════════════════════════════════
    # 3. HOW RECOMMENDATIONS ARE GENERATED
    # ════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(h1("3. Cómo se Genera una Recomendación"))
    story.append(p(
        "Cada semana, el pipeline procesa los datos y genera una lista de acciones de precio. "
        "El proceso completo, de datos crudos a recomendación, es el siguiente:"
    ))
    story.append(sp(4))
    # Pipeline steps
    steps_data = [
        ['Paso', 'Descripción'],
        ['1. Extracción', 'Descarga de ventas, precios y costos desde la base de datos PostgreSQL.'],
        ['2. Elasticidad', 'Estima la sensibilidad al precio por SKU y por categoría.'],
        ['3. Features', 'Construye 58 variables: velocidad, inventario, estacionalidad, precios oficiales, targets de margen.'],
        ['4. Ciclo de vida', 'Clasifica cada producto en su etapa (lanzamiento a liquidación).'],
        ['5. Curva de tallas', 'Evalua disponibilidad de tallas y genera alertas.'],
        ['6. Enriquecimiento', 'Fusiona todas las señales en un dataset unificado.'],
        ['7. Agregación', 'Consolida de SKU hijo a SKU padre (modelo-color a modelo).'],
        ['8. Entrenamiento', 'Re-entrena clasificador y regresor con los datos más recientes.'],
        ['9. Pricing', 'Genera recomendaciones aplicando reglas de margen, costo y anclaje.'],
        ['10. Sync', 'Sube resultados a la nube para que el dashboard los muestre.'],
    ]
    story.append(make_table(steps_data, col_widths=[1.2*inch, 5.3*inch]))

    # 3.1 Confidence tiers
    story.append(h2("3.1 Niveles de Confianza"))
    story.append(p(
        "Cada recomendación incluye un <b>nivel de confianza</b> que indica cuánta evidencia "
        "respalda la sugerencia. Esto ayuda al equipo comercial a priorizar su revisión."
    ))
    story.append(sp(4))
    story.append(make_table([
        ['Nivel', 'Significado', 'Acción sugerida'],
        ['ALTO', 'Fuerte confianza del clasificador +\nelasticidad disponible + velocidad confiable', 'Puede aprobarse con revisión rápida'],
        ['MEDIO', 'Señales decentes, algunos datos faltantes', 'Revisar antes de aprobar'],
        ['BAJO', 'Señales débiles, alta incertidumbre', 'Evaluar con criterio comercial. Considerar rechazar.'],
        ['ESPECULATIVO', 'Subida de precio sin elasticidad, o\nprecio premium sobre lista', 'Requiere validación manual obligatoria'],
    ], col_widths=[1.3*inch, 2.4*inch, 2.8*inch]))
    story.append(sp(4))
    story.append(h3("Composición del puntaje de confianza"))
    story.append(bullet("Probabilidad del clasificador > 85%: +3 puntos; > 70%: +2; > 50%: +1"))
    story.append(bullet("Datos de elasticidad disponibles: +2 puntos"))
    story.append(bullet("Velocidad de venta >= 2 u/sem: +2 puntos; >= 1 u/sem: +1"))
    story.append(bullet("Edad del producto >= 8 semanas: +1 punto"))
    story.append(bullet("Puntaje >= 6: ALTO | >= 4: MEDIO | < 4: BAJO"))

    # 3.2 Urgency
    story.append(h2("3.2 Clasificación de Urgencia"))
    story.append(p(
        "Además de la confianza, cada markdown recibe una <b>urgencia</b> que indica cuánto "
        "necesita el producto un ajuste de precio. La urgencia se calcula con un puntaje multifactorial:"
    ))
    story.append(sp(4))
    story.append(bullet("<b>Velocidad colapsada</b> (< 50% del baseline): urgencia +3"))
    story.append(bullet("<b>Ciclo de vida en Liquidación:</b> +3 | Declive: +2"))
    story.append(bullet("<b>Curva de tallas crítica</b> (attrition > 50%): +3"))
    story.append(bullet("<b>Sobrestock</b> (> 20 semanas de cobertura): +3"))
    story.append(bullet("<b>Tallas clave agotadas</b> (best sellers < 50% stock): +2"))
    story.append(sp(4))
    story.append(make_table([
        ['Puntaje', 'Urgencia', 'Significado'],
        ['\u2265 5', 'ALTA', 'Acción inmediata recomendada. Producto en riesgo de quedar obsoleto.'],
        ['3-4', 'MEDIA', 'Requiere atención esta semana.'],
        ['< 3', 'BAJA', 'Oportunidad de optimización, sin urgencia.'],
    ], col_widths=[1.0*inch, 1.0*inch, 4.5*inch]))

    # 3.3 Margin and cost protections
    story.append(h2("3.3 Protecciones de Margen y Costo"))
    story.append(p("El sistema nunca recomienda un precio que viole estos pisos:"))
    story.append(sp(4))
    story.append(bullet("<b>Piso de costo:</b> el precio recomendado nunca es inferior al costo unitario neto (sin IVA)."))
    story.append(bullet("<b>Piso de margen (15%):</b> si el descuento recomendado dejaría un margen bruto < 15%, "
                        "el sistema retrocede al descuento más profundo que mantenga al menos 15% de margen."))
    story.append(bullet("<b>Calculo de margen:</b> Margen = (Precio / 1.19 - Costo) / (Precio / 1.19). "
                        "Se divide por 1.19 para remover el IVA antes de calcular el margen."))
    story.append(sp(4))
    story.append(p("Los márgenes se visualizan en el dashboard con colores:"))
    story.append(bullet('<font color="#059669"><b>Verde (> 40%):</b></font> margen saludable'))
    story.append(bullet('<font color="#d97706"><b>Ambar (20-40%):</b></font> margen aceptable'))
    story.append(bullet('<font color="#dc2626"><b>Rojo (< 20%):</b></font> margen bajo, requiere atención'))

    # 3.4 Price anchoring
    story.append(h2("3.4 Anclaje de Precios"))
    story.append(p(
        "Los precios recomendados se ajustan a <b>puntos de anclaje cognitivo</b> — terminaciones "
        "que el consumidor percibe como precios naturales (ej: $29.990 en vez de $31.200). "
        "El sistema nunca recomienda un precio en \"zona muerta\" (ej: $28.000 o $33.500)."
    ))
    story.append(sp(4))
    story.append(make_table([
        ['Rango de precio', 'Pasos', 'Ejemplos'],
        ['< $10.000', '$1.000', '$2.990, $5.990, $9.990'],
        ['$10.000 - $20.000', '$2.000 - $3.000', '$12.990, $14.990, $19.990'],
        ['$20.000 - $50.000', '$5.000', '$24.990, $29.990, $39.990'],
        ['$50.000 - $100.000', '$5.000 - $10.000', '$54.990, $69.990, $89.990'],
        ['$100.000 - $200.000', '$10.000 - $20.000', '$109.990, $149.990, $199.990'],
        ['> $200.000', 'Pasos mayores', '$249.990, $299.990'],
    ], col_widths=[1.5*inch, 1.5*inch, 3.5*inch]))

    # ════════════════════════════════════════════════════════════════════════
    # 4. DASHBOARD OPERATIONS
    # ════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(h1("4. Operación del Dashboard"))
    story.append(p(
        "El dashboard es la interfaz principal para interactuar con las recomendaciones. "
        "Se accede vía navegador web con autenticación Google SSO (su cuenta @yaneken.cl o @ynk.cl)."
    ))
    story.append(sp(4))
    story.append(p(
        '<b>URL:</b> <link href="https://pricing-api-467343668842.us-central1.run.app" '
        'color="#2563eb"><u>https://pricing-api-467343668842.us-central1.run.app</u></link>'
    ))

    # 4.1 Views
    story.append(h2("4.1 Vistas y Navegación"))
    story.append(p("El dashboard ofrece tres modos de vista, accesibles desde los botones superiores:"))
    story.append(sp(4))
    story.append(make_table([
        ['Vista', 'Descripción', 'Uso recomendado'],
        ['Lista', 'Listado plano de todas las acciones,\ncon filtros y buscador', 'Vista principal para revisión\nindividual de recomendaciones'],
        ['Tiendas', 'Sidebar lateral agrupa acciones por\ntienda con métricas resumen', 'Revisión por punto de venta.\nBotón de aprobación masiva por tienda.'],
        ['Marcas /\nCategorías', 'Sidebar agrupa por marca de vendor\n(Nike, Adidas, etc.) o por subcategoría\n(Clogs, Sneakers, etc.)', 'Análisis por marca en multi-brand\n(BOLD, BELSPORT). En mono-brand\n(HOKA) muestra por categoría.'],
    ], col_widths=[1.1*inch, 2.5*inch, 2.9*inch]))
    story.append(sp(4))
    story.append(p("<b>Filtros disponibles:</b>"))
    story.append(bullet("<b>Tienda:</b> todas o una específica"))
    story.append(bullet("<b>Urgencia:</b> Alta, Media, Baja, Subir precio"))
    story.append(bullet("<b>Categoría:</b> subcategoría de producto"))
    story.append(bullet("<b>Estado:</b> Pendiente, Aprobado, Rechazado, Manual"))
    story.append(bullet("<b>Búsqueda:</b> por SKU o nombre de producto"))
    story.append(bullet("<b>Orden:</b> urgencia, impacto en revenue, confianza, tienda"))

    # 4.2 Decisión making
    story.append(h2("4.2 Toma de Decisiones"))
    story.append(p("Para cada recomendación, hay tres opciones:"))
    story.append(sp(4))
    story.append(bullet('<b>Aprobar</b> (check verde): acepta la recomendación del modelo tal cual.'))
    story.append(bullet('<b>Rechazar</b> (X roja): descarta la recomendación. El producto mantiene su precio actual.'))
    story.append(bullet('<b>Precio manual</b> (botón $): abre un modal donde se puede ingresar un precio distinto al recomendado.'))
    story.append(sp(4))
    story.append(p(
        "<b>Acciones masivas:</b> Los botones \"Aprobar (N)\" y \"Rechazar (N)\" en la barra de herramientas "
        "aplican la decisión a todas las acciones pendientes visibles en el filtro actual. Si son más de "
        "100, se pide confirmación."
    ))

    # 4.3 Manual price
    story.append(h2("4.3 Precio Manual"))
    story.append(p(
        "Al hacer clic en el botón $ de una acción, se abre el modal de precio manual. "
        "Este permite:"
    ))
    story.append(bullet("Ingresar un precio custom (el sistema lo ajusta automáticamente al anclaje más cercano)"))
    story.append(bullet("Ver en tiempo real el impacto estimado: velocidad esperada, revenue y margen delta"))
    story.append(bullet("Alerta si el precio está bajo costo o con margen < 20%"))
    story.append(sp(4))
    story.append(note(
        "El precio manual, una vez guardado, se usa en la exportación en lugar del precio recomendado. "
        "Esto permite que el equipo ajuste la recomendación sin perder el tracking del sistema."
    ))

    # 4.4 Chain view
    story.append(h2("4.4 Vista Cadena"))
    story.append(p(
        "El link \"Ver en todas las tiendas\" permite ver un SKU en todas las tiendas donde tiene recomendación. "
        "Desde ahi se puede aprobar masivamente por canal:"
    ))
    story.append(bullet("<b>Todas las tiendas:</b> aprueba en todo el país"))
    story.append(bullet("<b>Sólo ecommerce:</b> aprueba solo en tiendas con prefijo AB*"))
    story.append(bullet("<b>Sólo B&M:</b> aprueba solo en tiendas físicas"))

    # 4.5 Analytics
    story.append(h2("4.5 Panel de Analítica"))
    story.append(p(
        "El botón \"Análisis\" abre un panel lateral con cinco secciones:"
    ))
    story.append(sp(4))
    story.append(bullet("<b>Modelo:</b> métricas de rendimiento del clasificador y regresor (AUC, R\u00b2, features más importantes)."))
    story.append(bullet("<b>Elasticidad:</b> distribución de elasticidades por vendor o subcategoría, SKUs elásticos vs. inelásticos."))
    story.append(bullet("<b>Ciclo de vida:</b> distribución de productos por etapa y urgencia."))
    story.append(bullet("<b>Impacto:</b> resumen del revenue y margen delta por tienda y por marca/categoría."))
    story.append(bullet("<b>Predicción vs Realidad:</b> compara las predicciones del modelo con las ventas reales "
                        "después de implementar un cambio de precio. Muestra tasa de captura de lift, precisión "
                        "direccional, y las peores predicciones para identificar dónde mejorar."))

    # 4.6 Planner queue
    story.append(h2("4.6 Cola del Planner (Flujo de Dos Pasos)"))
    story.append(p(
        "El sistema soporta un flujo de aprobación en dos pasos:"
    ))
    story.append(bullet("<b>Paso 1:</b> El Brand Manager aprueba, rechaza o pone precio manual."))
    story.append(bullet("<b>Paso 2:</b> El Planner revisa las decisiones del BM y da su visto bueno final."))
    story.append(sp(4))
    story.append(p(
        "La pestaña \"Cola Planner\" muestra las decisiones pendientes de aprobación del planner. "
        "Sólo las decisiones aprobadas por el planner se incluyen en la exportación cuando el modo "
        "estricto está activado."
    ))

    # 4.7 Export
    story.append(h2("4.7 Exportación"))
    story.append(p(
        "El botón de exportación genera un archivo Excel (.xlsx) con los cambios de precio aprobados. "
        "Antes de exportar, se muestra un resumen con el número de items, impacto en revenue y "
        "en margen. El archivo usa el precio manual cuando se ha definido uno, o el precio recomendado "
        "si la acción fue aprobada sin modificaciones."
    ))

    # 4.8 Admin
    story.append(h2("4.8 Administración de Usuarios"))
    story.append(p("El panel de administración (icono de engranaje, solo visible para admins) permite:"))
    story.append(bullet("Agregar usuarios con roles: <b>Admin</b>, <b>Brand Manager</b>, <b>Planner</b>, <b>Viewer</b>"))
    story.append(bullet("Asignar marcas específicas a Brand Managers y Planners"))
    story.append(bullet("Eliminar usuarios"))
    story.append(sp(4))
    story.append(p(
        "Cualquier usuario con email @yaneken.cl o @ynk.cl puede ingresar al dashboard como viewer "
        "(solo lectura). Los roles de mayor permiso deben asignarse explícitamente."
    ))

    # 4.9 Feedback loop
    story.append(h2("4.9 Predicción vs Realidad (Feedback Loop)"))
    story.append(p(
        "El panel de analítica incluye una sección de <b>Predicción vs Realidad</b> que compara "
        "lo que el modelo predijo con lo que realmente ocurrió después de implementar un cambio de precio. "
        "Esta es la validación más importante del sistema."
    ))
    story.append(sp(4))
    story.append(h3("Cómo funciona"))
    story.append(p(
        "Cada semana, el pipeline compara las decisiones aprobadas de las últimas 1-4 semanas "
        "con las ventas reales registradas en ese período. Para cada SKU-tienda donde se implementó "
        "un cambio de precio, calcula:"
    ))
    story.append(bullet("<b>Velocidad predicha vs real:</b> cuántas unidades/semana predijo el modelo vs cuántas se vendieron realmente."))
    story.append(bullet("<b>Revenue predicho vs real:</b> ingresos esperados vs ingresos reales."))
    story.append(bullet("<b>Lift vs baseline:</b> cuánto mejoró la venta respecto al estado anterior (antes del cambio de precio)."))
    story.append(sp(4))
    story.append(h3("Métricas clave"))
    story.append(make_table([
        ['Métrica', 'Qué mide', 'Cómo interpretar'],
        ['Tasa de captura\nde lift', 'Qué fracción del lift predicho\nse materializó realmente', 'Verde ≥ 70%: el modelo es confiable.\n'
         'Ámbar 50-70%: sesgo optimista moderado.\nRojo < 50%: modelo sobre-estima el efecto.'],
        ['Precisión\ndireccional', 'En qué % de casos la velocidad\nse movió en la dirección predicha', '> 75%: el modelo acierta la dirección.\n'
         '< 60%: señal de alerta, revisar elasticidades.'],
        ['Error mediano\nde velocidad', 'Diferencia mediana entre velocidad\npredicha y real (en %)', 'Negativo = modelo sobre-estima demanda.\n'
         'Positivo = modelo sub-estima demanda.'],
    ], col_widths=[1.2*inch, 2.2*inch, 3.1*inch]))
    story.append(sp(4))
    story.append(h3("Peores predicciones"))
    story.append(p(
        "El panel muestra las 5 predicciones con mayor error. Esto permite identificar <b>patrones</b>: "
        "si los errores se concentran en una categoría, tienda o nivel de confianza específico, "
        "indica dónde el modelo necesita mejoras o dónde el criterio humano debería tener más peso."
    ))
    story.append(sp(4))
    story.append(callout_box(
        "Por qué es importante",
        "Sin esta validación, no sabemos si las recomendaciones realmente mejoran el negocio. "
        "La tasa de captura de lift es el número más importante del sistema — si consistentemente "
        "es < 50%, las elasticidades están sobre-estimadas y el modelo necesita recalibración. "
        "Si es > 70%, las recomendaciones están generando el valor esperado."
    ))
    story.append(sp(4))
    story.append(note(
        "Los datos de predicción vs realidad se acumulan semana a semana. Las primeras semanas "
        "tendrán pocos datos; la sección se vuelve más confiable después de 4-8 semanas de uso activo."
    ))

    # ════════════════════════════════════════════════════════════════════════
    # 5. CURRENT PERFORMANCE BY BRAND
    # ════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(h1("5. Rendimiento Actual por Marca"))
    story.append(p(
        "La siguiente tabla resume el rendimiento de los modelos para cada marca, "
        "medido en la ultima ejecución del pipeline (Marzo 2026)."
    ))
    story.append(sp(8))
    story.append(make_table([
        ['Marca', 'Tiendas', 'AUC\nClasificador', 'R\u00b2\nRegresor', 'MAE\n(pp)', 'Muestras', 'Fuente\ncostos'],
        ['HOKA', '3', '0.988', '0.820', '4.7', '10,822', 'GCS (manual)'],
        ['BOLD', '35', '0.910', '0.566', '7.2', '~600K', 'ti.productos'],
        ['BAMERS', '25', '0.950', '0.492', '7.8', '~200K', 'ti.productos'],
        ['OAKLEY', '8', '0.941', '0.596', '6.3', '~100K', 'ti.productos'],
        ['BELSPORT', '66', '0.948', '0.538*', '1.6', '2,792,288', 'ti.productos'],
    ], col_widths=[1.0*inch, 0.7*inch, 0.9*inch, 0.8*inch, 0.7*inch, 1.0*inch, 1.4*inch]))
    story.append(sp(4))
    story.append(note(
        "* BELSPORT: R\u00b2 de validación cruzada = 0.538, pero R\u00b2 de holdout (últimas 4 semanas) = 0.704, "
        "lo que sugiere que el modelo generaliza bien a datos futuros. El MAE bajo (1.6pp) refleja que la "
        "mayoría de productos en BELSPORT tienen descuentos concentrados en un rango estrecho."
    ))
    story.append(sp(8))
    story.append(h3("Interpretación"))
    story.append(bullet(
        "<b>HOKA</b> tiene el mejor rendimiento porque opera pocas tiendas con una marca única, "
        "generando datos muy consistentes."
    ))
    story.append(bullet(
        "<b>BOLD</b> y <b>BELSPORT</b> son más complejos: multi-marca, muchas tiendas, "
        "y mayor heterogeneidad en comportamiento de compra. Sus R\u00b2 más bajos son esperables."
    ))
    story.append(bullet(
        "Un R\u00b2 de 0.55 significa que el modelo explica el 55% de la variación en el descuento "
        "óptimo. El 45% restante depende de factores no capturados (clima, competencia local, eventos). "
        "Esto es donde el <b>criterio humano complementa al modelo</b>."
    ))

    # ════════════════════════════════════════════════════════════════════════
    # 6. ELASTICITIES — WHAT THEY MEAN FOR THE BUSINESS
    # ════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(h1("6. Elasticidades: Qué Significan para el Negocio"))
    story.append(p(
        "La elasticidad-precio es la señal más importante que el sistema usa para predecir el impacto "
        "de un cambio de precio. A continuación se presentan las elasticidades obtenidas para cada marca "
        "y sus implicaciones comerciales."
    ))
    story.append(sp(4))
    story.append(h2("6.1 Panorama General"))
    story.append(make_table([
        ['Marca', 'SKUs\nanalizados', 'Mediana', 'Elásticos\n(< -1)', 'Unitarios\n(-1 a -0.5)', 'Inelásticos\n(> -0.5)'],
        ['HOKA', '345', '-0.12', '31 (9%)', '48 (14%)', '266 (77%)'],
        ['BOLD', '6,915', '-0.09', '409 (6%)', '558 (8%)', '5,948 (86%)'],
        ['BAMERS', '1,469', '-0.29', '122 (8%)', '246 (17%)', '1,101 (75%)'],
        ['OAKLEY', '1,233', '-0.08', '85 (7%)', '157 (13%)', '991 (80%)'],
    ], col_widths=[0.9*inch, 0.8*inch, 0.8*inch, 1.0*inch, 1.0*inch, 1.0*inch]))
    story.append(sp(4))
    story.append(callout_box(
        "Hallazgo clave",
        "La gran mayoría de los productos en todas las marcas son <b>inelásticos</b> (> -0.5). "
        "Esto significa que, en promedio, los descuentos no generan suficiente volumen adicional "
        "para compensar la pérdida de margen. <b>El sistema usa esta información para ser conservador "
        "con los markdowns y priorizar la protección de margen.</b>"
    ))
    story.append(sp(8))

    story.append(h2("6.2 HOKA"))
    story.append(p(
        "Con solo 3 tiendas y una marca premium, HOKA muestra elasticidad mediana de <b>-0.12</b> — "
        "casi completamente inelástica. El 77% de los productos no responde significativamente al precio."
    ))
    story.append(sp(4))
    story.append(h3("Implicancia comercial"))
    story.append(bullet("<b>No hacer descuentos innecesarios:</b> la demanda de HOKA depende más de la marca y el producto que del precio."))
    story.append(bullet("<b>Markdowns solo por curva de tallas rota o fin de temporada:</b> cuando ya no hay tallas clave, el descuento ayuda a liquidar."))
    story.append(bullet("<b>Subidas de precio viables:</b> productos con alta velocidad pueden tolerar precios mayores sin perder ventas."))

    story.append(h2("6.3 BOLD"))
    story.append(p(
        "BOLD es el caso más complejo: 35 tiendas, múltiples marcas vendor (Nike, Adidas, Puma, etc.), "
        "y 6,915 SKUs analizados. Elasticidad mediana de <b>-0.09</b>, altamente inelástico."
    ))
    story.append(sp(4))
    story.append(h3("Por marca vendor"))
    story.append(make_table([
        ['Vendor', 'SKUs', 'Elasticidad mediana', 'Interpretación'],
        ['Nike', '3,115', '-0.12', 'Levemente más sensible al precio que el promedio'],
        ['Adidas', '1,310', '-0.08', 'Inelástico — marca consolidada'],
        ['Puma', '1,464', '-0.07', 'Inelástico — descuentos poco efectivos'],
        ['New Balance', '267', '-0.07', 'Inelástico'],
        ['New Era', '350', '-0.02', 'Muy inelástico — accesorios de colección'],
    ], col_widths=[1.1*inch, 0.6*inch, 1.3*inch, 3.5*inch]))
    story.append(sp(4))
    story.append(h3("Categorías más sensibles al precio"))
    story.append(bullet("<b>Calcetines (SOCKS):</b> -0.37 — la categoría más elástica. Descuentos pueden impulsar volumen."))
    story.append(bullet("<b>Mochilas (BACKPACK):</b> -0.36 — responden bien a promociones."))
    story.append(bullet("<b>Running:</b> -0.04 — muy inelástico. Descuentos destruyen margen sin generar volumen."))

    story.append(h2("6.4 BAMERS"))
    story.append(p(
        "BAMERS muestra la elasticidad mediana más alta del grupo: <b>-0.29</b>. Con mayor proporción "
        "de confianza alta (28%), sus estimaciones son las más robustas."
    ))
    story.append(sp(4))
    story.append(h3("Categorías destacadas"))
    story.append(bullet("<b>Botas (Boots):</b> -0.40 — sensibles al precio. Descuentos de temporada son efectivos."))
    story.append(bullet("<b>Mochilas (BACKPACK):</b> -0.52 — la categoría más elástica en todo el portafolio."))
    story.append(bullet("<b>Sneakers:</b> -0.21 — moderadamente inelásticos. Descuentos moderados funcionan para liquidar."))
    story.append(sp(4))
    story.append(callout_box(
        "Oportunidad BAMERS",
        "BAMERS es la marca donde los descuentos tienen más potencial de generar volumen incremental. "
        "El sistema puede ser más agresivo aquí que en otras marcas, especialmente en Botas y Mochilas."
    ))

    story.append(h2("6.5 OAKLEY"))
    story.append(p(
        "Elasticidad mediana de <b>-0.08</b>, similar a BOLD. El 80% de los productos son inelásticos. "
        "Las categorías de apparel (Top, Bottom) dominan el portafolio."
    ))
    story.append(sp(4))
    story.append(bullet("<b>Outdoor:</b> -0.28 — la categoría más sensible. Descuentos justificados en fin de temporada."))
    story.append(bullet("<b>Eyewear (Lifestyle/Performance):</b> inelástico — producto premium donde el precio señaliza calidad."))
    story.append(bullet("<b>Jockey/Gorros:</b> +0.32 — elasticidad positiva sugiere que el precio no es factor (o datos insuficientes)."))

    story.append(sp(8))
    story.append(h2("6.6 Implicaciones Estratégicas"))
    story.append(sp(4))
    story.append(bullet("<b>Proteger margen es la prioridad #1:</b> con elasticidades tan bajas, cada punto de descuento innecesario es margen perdido sin compensación en volumen."))
    story.append(bullet("<b>Descuentos quirúrgicos, no masivos:</b> concentrar markdowns en productos con señales claras (curva de tallas rota, fin de temporada, sobrestock)."))
    story.append(bullet("<b>Categorías accesorios responden mejor:</b> calcetines, mochilas y bolsos son más sensibles al precio en todas las marcas."))
    story.append(bullet("<b>Calzado de running/performance es inelástico:</b> los clientes compran por funcionalidad, no por precio."))
    story.append(bullet("<b>Marcas premium (HOKA, Oakley eyewear):</b> evitar descuentos que erosionen el posicionamiento."))

    # ════════════════════════════════════════════════════════════════════════
    # 7. LIMITATIONS & IMPROVEMENTS
    # ════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(h1("7. Limitaciones Conocidas y Áreas de Mejora"))
    story.append(p(
        "Es fundamental ser transparente sobre lo que el sistema <b>no</b> hace bien y donde "
        "necesita mejoras. Estas son las limitaciones conocidas:"
    ))
    story.append(sp(8))

    story.append(h2("7.1 Datos de Stock"))
    story.append(p(
        "<b>BELSPORT no tiene tabla de stock.</b> El sistema usa un proxy basado en ventas recientes "
        "para estimar disponibilidad de tallas, pero esto es significativamente menos preciso que "
        "datos de inventario reales. Productos sin ventas recientes (pero con stock) aparecen como "
        "\"sin stock\", y el sistema no puede recomendar acciones sobre ellos."
    ))
    story.append(sp(4))
    story.append(callout_box(
        "Mejora propuesta",
        "Integrar la tabla de stock real de BELSPORT (si existe en algun sistema) o implementar "
        "un feed de inventario. Esto mejoraría tanto las recomendaciones como las alertas de curva de tallas."
    ))

    story.append(h2("7.2 Elasticidad y Eventos de Markdown"))
    story.append(p(
        "Las estimaciones de elasticidad actualmente <b>no distinguen entre cambios de precio regulares "
        "y eventos de liquidación</b> (markdowns masivos de fin de temporada). Esto puede inflar la "
        "elasticidad estimada, ya que los markdowns coinciden con periodos de alta venta por otras razones "
        "(rebajas de temporada, Black Friday, etc.)."
    ))
    story.append(sp(4))
    story.append(callout_box(
        "Mejora propuesta",
        "La tabla <b>ynk.precios_ofertas</b> permitiría identificar periodos de oferta formal y excluirlos "
        "de la estimación de elasticidad. Actualmente esta tabla no está disponible en la base de datos."
    ))

    story.append(h2("7.3 Costos de Productos"))
    story.append(p(
        "Los costos de BOLD, BAMERS, OAKLEY y BELSPORT se extraen de <b>ti.productos</b>, que "
        "tiene monedas mixtas (USD y CLP). El sistema aplica una heurística: costos < 500 se tratan "
        "como USD y se multiplican por 1,000 (tasa calibrada). Esto funciona razonablemente pero "
        "no es exacto para todos los productos."
    ))
    story.append(sp(4))
    story.append(callout_box(
        "Mejora propuesta",
        "Obtener una fuente de costos con moneda explícita, o una tasa de cambio actualizada automáticamente. "
        "Para HOKA los costos se suben manualmente (más precisión)."
    ))

    story.append(h2("7.4 Factores Externos No Capturados"))
    story.append(p("El modelo no tiene acceso a información sobre:"))
    story.append(bullet("Acciones de la competencia (precios, promociones)"))
    story.append(bullet("Clima y estacionalidad granular (ej: ola de frio que impulsa ventas de botas)"))
    story.append(bullet("Campanas de marketing en curso"))
    story.append(bullet("Eventos locales (ej: apertura de tienda de la competencia)"))
    story.append(bullet("Cambios en tendencias de moda"))
    story.append(sp(4))
    story.append(p(
        "Estos factores explican la porción de variabilidad que el modelo no captura (el \"45%\" del R\u00b2=0.55). "
        "<b>Aquí es donde el conocimiento del equipo comercial es irremplazable.</b>"
    ))

    story.append(h2("7.5 Productos Nuevos (Cold Start)"))
    story.append(p(
        "Productos con menos de 4 semanas de ventas tienen poca historia para generar señales "
        "confiables. El sistema puede generar recomendaciones de baja confianza para estos productos, "
        "pero se recomienda priorizar el criterio humano."
    ))

    story.append(h2("7.6 Subidas de Precio"))
    story.append(p(
        "Las recomendaciones de subida de precio son inherentemente más inciertas que los markdowns, "
        "ya que hay menos datos históricos de subidas. El sistema marca estas recomendaciones como "
        "<b>ESPECULATIVO</b> cuando no tiene datos de elasticidad. Deben revisarse caso a caso."
    ))

    story.append(h2("7.7 Inventario en Tránsito (Backorder)"))
    story.append(p(
        "El sistema actualmente no tiene visibilidad sobre <b>inventario en tránsito</b> — productos "
        "que ya están comprados y llegarán en las próximas semanas. Esto significa que puede recomendar "
        "markdowns agresivos en un producto con stock bajo, sin saber que un reabastecimiento está en camino."
    ))
    story.append(sp(4))
    story.append(p(
        "Esto es especialmente relevante para productos en etapa de declive aparente pero que en realidad "
        "van a recibir stock nuevo. Sin esta información, el modelo interpreta el stock bajo como señal "
        "de liquidación cuando podría ser solo un quiebre temporal."
    ))
    story.append(sp(4))
    story.append(callout_box(
        "Mejora propuesta",
        "Integrar datos de órdenes de compra pendientes o inventario en tránsito. Esto permitiría "
        "al modelo distinguir entre un producto que se está agotando definitivamente vs. uno que "
        "está por recibir reposición, evitando markdowns innecesarios."
    ))

    # ════════════════════════════════════════════════════════════════════════
    # 8. ROLE OF HUMAN JUDGMENT
    # ════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(h1("8. El Rol del Criterio Humano"))
    story.append(sp(4))
    story.append(callout_box(
        "Principio fundamental",
        "El sistema es un <b>asistente de decisión</b>, no un tomador de decisiones. "
        "Su función es procesar miles de datos, identificar oportunidades y presentarlas "
        "de forma priorizada. La decisión final siempre es del equipo comercial."
    ))
    story.append(sp(8))
    story.append(p("El criterio humano es especialmente crítico en las siguientes situaciones:"))
    story.append(sp(4))

    story.append(h3("Cuando confiar más en el modelo"))
    story.append(bullet("Confianza <b>ALTA</b> + urgencia <b>ALTA</b> + margen saludable: el modelo tiene datos sólidos y múltiples señales alineadas."))
    story.append(bullet("Markdowns de productos en declive con curva de tallas rota: patrón bien documentado en los datos."))
    story.append(bullet("Categorías con alta elasticidad y muchas observaciones: la estimación de impacto es confiable."))
    story.append(sp(8))

    story.append(h3("Cuando aplicar más criterio propio"))
    story.append(bullet("Confianza <b>BAJA</b> o <b>ESPECULATIVO</b>: el modelo no tiene suficientes datos."))
    story.append(bullet("Productos nuevos (< 4 semanas) o lanzamientos estratégicos."))
    story.append(bullet("Eventos comerciales próximos que el modelo no conoce (Black Friday, vuelta a clases)."))
    story.append(bullet("Productos con posicionamiento de marca premium donde el descuento dañaría la percepción."))
    story.append(bullet("Categorías con R\u00b2 bajo: el modelo es menos preciso, complementar con experiencia."))
    story.append(bullet("Subidas de precio marcadas como ESPECULATIVO."))
    story.append(sp(8))

    story.append(h3("Feedback que mejora el sistema"))
    story.append(p(
        "Cada decisión que el equipo toma (aprobar, rechazar, precio manual) es información valiosa. "
        "Con el tiempo, los patrones de decisión del equipo pueden incorporarse para mejorar el modelo. "
        "Algunas formas de feedback especialmente útiles:"
    ))
    story.append(sp(4))
    story.append(bullet("<b>Rechazos sistemáticos:</b> si una categoría siempre se rechaza, el modelo puede estar mal calibrado para ese segmento."))
    story.append(bullet("<b>Precios manuales:</b> cuando el equipo consistentemente ajusta el precio en una dirección, indica un sesgo del modelo."))
    story.append(bullet("<b>Resultados post-implementación:</b> comparar la venta real después del cambio de precio con la proyección del modelo es la validación definitiva."))

    # ════════════════════════════════════════════════════════════════════════
    # 9. FEEDBACK
    # ════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(h1("9. Cómo Darnos Feedback"))
    story.append(sp(4))
    story.append(Paragraph(
        "ESTA ES UNA VERSION BETA<br/>"
        "Su experiencia usando la herramienta es esencial para mejorarla.<br/>"
        "Necesitamos su feedback activo para hacer esto util de verdad.",
        styles['BetaBanner']
    ))
    story.append(sp(12))
    story.append(p("Nos interesa especialmente saber:"))
    story.append(sp(4))
    story.append(bullet("Recomendaciones que les parecieron <b>claramente equivocadas</b> — y por que."))
    story.append(bullet("Categorías o tiendas donde el sistema parece <b>funcionar particularmente bien o mal</b>."))
    story.append(bullet("Información que ustedes usan para tomar decisiones de precio y que <b>el sistema no tiene</b>."))
    story.append(bullet("Funcionalidades que <b>faltan en el dashboard</b> o que harían su trabajo más eficiente."))
    story.append(bullet("Productos donde <b>implementaron la recomendación</b> y pueden comparar con el resultado real."))
    story.append(sp(12))

    story.append(h2("Canales de comunicación"))
    story.append(sp(4))
    story.append(make_table([
        ['Canal', 'Uso'],
        ['Dashboard (rechazo/manual)', 'Feedback implícito: cada decisión queda registrada y se usa para analizar.'],
        ['Email a sgr@ynk.cl', 'Feedback detallado, sugerencias de mejora, reportes de errores.'],
        ['Reuniones semanales', 'Revisión de resultados, calibración de modelos, priorización de mejoras.'],
    ], col_widths=[2.2*inch, 4.3*inch]))

    story.append(sp(20))
    story.append(hr())
    story.append(sp(8))
    story.append(Paragraph(
        "<b>Gracias por ser parte de esta fase beta.</b><br/><br/>"
        "Este sistema fue construido para apoyar su experticia, no para reemplazarla. "
        "Cada semana que lo usan, cada decisión que toman, cada feedback que nos dan, "
        "lo hace mejor. Juntos podemos construir una herramienta que realmente haga la "
        "diferencia en la rentabilidad de Yáneken.",
        ParagraphStyle('ClosingMsg', fontName='Helvetica', fontSize=11, leading=17,
                       textColor=DARK, alignment=TA_CENTER, spaceAfter=12)
    ))

    # Build
    doc.build(story)
    print(f"Manual generated: {output_path}")
    return output_path

if __name__ == "__main__":
    build_manual()
