"""
app.py — Buscador de contratos SECOP y Análisis de Demanda (Streamlit).

Usa el CSV exportado como base de datos local.
Permite buscar contratos por palabras clave en el objeto del contrato,
filtrar por ciudad/tipo/estado, y generar un informe formal de
Análisis de la Demanda con los contratos seleccionados.
"""

import re
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

# ────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ────────────────────────────────────────────────────────────

CSV_PATH = Path(__file__).parent / "output" / "secop_analisis_sector_completo_20260214_061621.csv"

st.set_page_config(
    page_title="Análisis del Sector SECOP — Santander",
    page_icon="�",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ────────────────────────────────────────────────────────────
# ESTILOS CSS PERSONALIZADOS
# ────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* ══════════════════════════════════════════════════════════
   SECOP Análisis del Sector — Professional Theme
   WCAG AA compliant contrast ratios throughout
   ══════════════════════════════════════════════════════════ */

@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}

/* ── Eliminar decoración default de Streamlit ── */
header[data-testid="stHeader"] { background: transparent !important; }
.block-container { padding-top: 1rem !important; }

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   HEADER PRINCIPAL
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.main-header {
    background: linear-gradient(135deg, #0f2b46 0%, #1a3f6b 50%, #1e5299 100%);
    padding: 2.2rem 2.8rem;
    border-radius: 14px;
    margin-bottom: 1.8rem;
    color: #ffffff;
    box-shadow: 0 8px 32px rgba(15,43,70,0.35);
    position: relative;
    overflow: hidden;
}
.main-header::before {
    content: '';
    position: absolute; top: 0; right: 0;
    width: 200px; height: 100%;
    background: radial-gradient(circle at 80% 40%, rgba(255,255,255,0.06) 0%, transparent 60%);
}
.main-header h1 {
    margin: 0; font-size: 1.9rem; font-weight: 800;
    letter-spacing: -0.5px; color: #ffffff;
    text-shadow: 0 1px 3px rgba(0,0,0,0.15);
}
.main-header p {
    margin: 0.5rem 0 0 0; font-size: 0.88rem;
    color: #c8ddf0; font-weight: 400;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   TARJETAS DE MÉTRICAS — Alto contraste
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.metric-card {
    background: #ffffff;
    border: 1px solid #e1e5eb;
    border-radius: 12px;
    padding: 1.3rem 1rem;
    text-align: center;
    box-shadow: 0 2px 10px rgba(0,0,0,0.05);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    border-top: 3px solid #1a3f6b;
}
.metric-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 6px 24px rgba(0,0,0,0.1);
}
.metric-card .metric-icon {
    font-size: 1.5rem; margin-bottom: 0.3rem;
}
.metric-card .metric-value {
    font-size: 1.65rem; font-weight: 800; color: #0f2b46;
    margin: 0.25rem 0; line-height: 1.2;
}
.metric-card .metric-label {
    font-size: 0.72rem; color: #4a5568; text-transform: uppercase;
    letter-spacing: 0.6px; font-weight: 600;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   SIDEBAR — Fondo blanco, máximo contraste
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
section[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #e2e8f0 !important;
}
section[data-testid="stSidebar"] > div {
    padding-top: 0.5rem;
}

/* Sidebar title & panel header */
.sidebar-brand {
    background: #0f2b46;
    margin: -1rem -1rem 1.2rem -1rem;
    padding: 1.4rem 1.2rem;
    border-radius: 0 0 12px 12px;
    text-align: center;
}
.sidebar-brand h3 {
    color: #ffffff; margin: 0; font-size: 1rem;
    font-weight: 700; letter-spacing: 0.3px;
}
.sidebar-brand p {
    color: #a0bdd5; margin: 0.3rem 0 0 0;
    font-size: 0.72rem; font-weight: 400;
}

/* Section labels in sidebar */
.filter-label {
    font-size: 0.78rem; font-weight: 700; color: #1a202c;
    margin: 0.8rem 0 0.3rem 0; padding: 0;
    display: flex; align-items: center; gap: 0.4rem;
}
.filter-label .fl-icon {
    display: inline-flex; align-items: center;
    justify-content: center; width: 22px; height: 22px;
    background: #edf2f7; border-radius: 6px; font-size: 0.7rem;
}

/* Sidebar divider */
.sidebar-divider {
    height: 1px; background: #e2e8f0;
    margin: 1rem 0; border: none;
}

/* DB counter card in sidebar */
.db-counter {
    text-align: center; padding: 1rem;
    background: #0f2b46; border-radius: 10px;
    margin-top: 0.5rem;
}
.db-counter .db-label {
    color: #a0bdd5; font-size: 0.7rem;
    text-transform: uppercase; font-weight: 600;
    letter-spacing: 0.8px;
}
.db-counter .db-value {
    color: #ffffff; font-size: 1.5rem;
    font-weight: 800; margin: 0.2rem 0;
}
.db-counter .db-sub {
    color: #7ea8c9; font-size: 0.68rem; font-weight: 400;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   BARRA DE BÚSQUEDA
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.search-container {
    background: #f7f9fc;
    border: 2px solid #d5dce6;
    border-radius: 12px;
    padding: 1rem 1.3rem;
    margin-bottom: 1.2rem;
    transition: border-color 0.3s, box-shadow 0.3s;
}
.search-container:focus-within {
    border-color: #1a3f6b;
    box-shadow: 0 0 0 3px rgba(26,63,107,0.1);
}
.search-hint {
    font-size: 0.78rem; color: #4a5568;
    margin-top: 0.4rem;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   TABLA DE DATOS
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 2px 16px rgba(0,0,0,0.06);
    border: 1px solid #e2e8f0;
}

/* Tabs */
button[data-baseweb="tab"] {
    font-weight: 600 !important;
    font-size: 0.88rem !important;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   ANÁLISIS HEADER
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.analisis-header {
    background: linear-gradient(135deg, #0f2b46 0%, #1a3f6b 100%);
    color: #ffffff;
    padding: 1.3rem 2rem;
    border-radius: 12px;
    margin: 1rem 0;
    box-shadow: 0 4px 16px rgba(15,43,70,0.25);
}
.analisis-header h2 {
    margin: 0; font-size: 1.25rem; font-weight: 700;
    color: #ffffff;
}
.analisis-header p {
    color: #c8ddf0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   ESTADÍSTICOS
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 0.8rem;
    margin: 1rem 0 1.5rem 0;
}
.stat-card {
    background: #f7f9fc;
    border: 1px solid #e1e5eb;
    border-radius: 10px;
    padding: 1rem 1.2rem;
    border-left: 4px solid #1a3f6b;
}
.stat-card .stat-title {
    font-size: 0.68rem; font-weight: 700;
    color: #4a5568; text-transform: uppercase;
    letter-spacing: 0.5px; margin-bottom: 0.3rem;
}
.stat-card .stat-value {
    font-size: 1.25rem; font-weight: 800;
    color: #0f2b46; margin-bottom: 0.15rem;
}
.stat-card .stat-sub {
    font-size: 0.7rem; color: #718096;
}
.stats-section-title {
    font-size: 0.9rem; font-weight: 700; color: #0f2b46;
    margin: 1.2rem 0 0.6rem 0;
    display: flex; align-items: center; gap: 0.5rem;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   FICHAS DE CONTRATO — Alto contraste
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.contract-card {
    background: #ffffff;
    border: 1px solid #d5dce6;
    border-radius: 10px;
    padding: 0;
    margin: 1.2rem 0;
    overflow: hidden;
    box-shadow: 0 2px 10px rgba(0,0,0,0.06);
    transition: box-shadow 0.2s;
}
.contract-card:hover {
    box-shadow: 0 4px 20px rgba(0,0,0,0.1);
}
.contract-card-header {
    background: #0f2b46;
    color: #ffffff;
    padding: 0.7rem 1.3rem;
    font-weight: 700;
    font-size: 0.82rem;
    letter-spacing: 0.5px;
}
.contract-card table {
    width: 100%; border-collapse: collapse;
}
.contract-card table td {
    padding: 0.55rem 1rem;
    font-size: 0.8rem;
    color: #1a202c;
    border-bottom: 1px solid #edf2f7;
    vertical-align: top;
    line-height: 1.4;
}
.contract-card table td:first-child {
    width: 170px;
    font-weight: 700;
    color: #0f2b46;
    background: #f0f4f8;
    border-right: 2px solid #d5dce6;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}
.contract-card table tr:last-child td {
    border-bottom: none;
}
.contract-card table tr:hover td {
    background: #fafbfd;
}
.contract-card table tr:hover td:first-child {
    background: #e8eef4;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   BOTONES DE DESCARGA
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.stDownloadButton > button {
    background: #0f2b46 !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 0.65rem 1.6rem !important;
    font-weight: 700 !important;
    font-size: 0.82rem !important;
    transition: all 0.25s ease !important;
    box-shadow: 0 2px 8px rgba(15,43,70,0.25) !important;
    letter-spacing: 0.2px !important;
}
.stDownloadButton > button:hover {
    background: #1a3f6b !important;
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 20px rgba(15,43,70,0.35) !important;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   UTILIDADES
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.custom-divider {
    height: 2px;
    background: linear-gradient(90deg, #0f2b46 0%, #1a3f6b 30%, transparent 100%);
    margin: 1.8rem 0;
    border: none;
}

/* Empty state */
.empty-state {
    text-align: center; padding: 3.5rem 2rem;
}
.empty-state .es-icon {
    font-size: 2.8rem; margin-bottom: 0.5rem;
}
.empty-state .es-title {
    font-size: 1.05rem; font-weight: 600; color: #2d3748;
    margin: 0.3rem 0;
}
.empty-state .es-desc {
    font-size: 0.82rem; color: #718096;
}

/* Success count pill */
.result-pill {
    display: inline-block;
    background: #e6f4ea; color: #1a7431;
    padding: 0.3rem 1rem; border-radius: 20px;
    font-size: 0.78rem; font-weight: 600;
    margin-bottom: 0.8rem;
}
</style>
""", unsafe_allow_html=True)

# ────────────────────────────────────────────────────────────
# CARGA DE DATOS
# ────────────────────────────────────────────────────────────


@st.cache_data
def cargar_datos() -> pd.DataFrame:
    """Carga el CSV y prepara columnas para búsqueda."""
    df = pd.read_csv(CSV_PATH, dtype=str)
    df["valor_del_contrato"] = pd.to_numeric(df["valor_del_contrato"], errors="coerce")
    df["valor_pagado"] = pd.to_numeric(df["valor_pagado"], errors="coerce")
    df["fecha_inicio"] = pd.to_datetime(
        df["fecha_de_inicio_del_contrato"], errors="coerce"
    )
    df["fecha_fin"] = pd.to_datetime(
        df["fecha_de_fin_del_contrato"], errors="coerce"
    )
    # Columna de búsqueda normalizada (minúsculas, sin tildes extra)
    df["_busqueda"] = df["objeto_del_contrato"].fillna("").str.lower()
    return df


def buscar(df: pd.DataFrame, texto: str) -> pd.DataFrame:
    """Busca contratos cuyo objeto contenga TODAS las palabras clave.

    Cada palabra se busca de forma independiente (AND lógico).
    Soporta búsqueda parcial — no requiere coincidencia exacta.
    """
    if not texto.strip():
        return df

    palabras = texto.lower().split()
    mascara = pd.Series([True] * len(df), index=df.index)

    for palabra in palabras:
        # Escapar caracteres regex especiales del input
        palabra_safe = re.escape(palabra)
        mascara &= df["_busqueda"].str.contains(palabra_safe, na=False)

    return df[mascara]


# ────────────────────────────────────────────────────────────
# FUNCIONES DE EXPORTACIÓN
# ────────────────────────────────────────────────────────────


def _extraer_url(url_raw: str) -> str:
    """Extrae URL limpia del campo urlproceso."""
    url_raw = str(url_raw)
    if "url" in url_raw and "secop.gov.co" in url_raw:
        import ast
        try:
            url_dict = ast.literal_eval(url_raw)
            return url_dict.get("url", url_raw)
        except Exception:
            return url_raw
    return url_raw if url_raw and url_raw != "nan" else "N/D"


def _calcular_plazo(row) -> str:
    """Calcula el plazo entre fecha inicio y fin."""
    f_ini = row.get("fecha_inicio")
    f_fin = row.get("fecha_fin")
    if pd.notna(f_ini) and pd.notna(f_fin):
        dias = (f_fin - f_ini).days
        if dias >= 30:
            meses = round(dias / 30)
            return f"{meses} {'MES' if meses == 1 else 'MESES'} ({dias} días)"
        return f"{dias} DÍAS"
    return "N/D"


def _generar_informe_texto(contratos: pd.DataFrame) -> str:
    """Genera el informe de Análisis de Demanda en texto plano."""
    lineas = []
    lineas.append("ANÁLISIS DE LA DEMANDA")
    lineas.append("=" * 70)
    lineas.append("")
    lineas.append(
        "Se validan en el portal de contratación SECOP procesos adelantados "
        "en los últimos años por algunas entidades estatales del departamento "
        "y por este municipio para satisfacer las necesidades requeridas."
    )
    lineas.append(
        "De acuerdo a la consulta en el portal único de contratación estatal — "
        "SECOP www.colombiacompra.gov.co, se observa la siguiente información:"
    )
    lineas.append("")

    for idx, (_, row) in enumerate(contratos.iterrows(), 1):
        proceso = str(row.get("proceso_de_compra", "N/D"))
        modalidad = str(row.get("modalidad_de_contratacion", "N/D")).upper()
        contratista = str(row.get("proveedor_adjudicado", "N/D")).upper()
        entidad = str(row.get("nombre_entidad", "N/D")).upper()
        ciudad = str(row.get("ciudad", "")).upper()
        contratante = f"{entidad}, {ciudad}" if ciudad else entidad
        objeto = str(row.get("objeto_del_contrato", "N/D")).upper()
        valor = row.get("valor_del_contrato", 0)
        valor_fmt = f"$ {valor:,.0f}" if pd.notna(valor) else "N/D"
        plazo = _calcular_plazo(row)
        enlace = _extraer_url(str(row.get("urlproceso", "")))

        lineas.append(f"{'─' * 70}")
        lineas.append(f"CONTRATO {idx}")
        lineas.append(f"{'─' * 70}")
        lineas.append(f"{'No PROCESO SECOP':<20} {proceso}")
        lineas.append(f"{'MODALIDAD':<20} {modalidad}")
        lineas.append(f"{'CONTRATISTA':<20} {contratista}")
        lineas.append(f"{'CONTRATANTE':<20} {contratante}")
        lineas.append(f"{'OBJETO':<20} {objeto}")
        lineas.append(f"{'VALOR':<20} {valor_fmt}")
        lineas.append(f"{'PLAZO':<20} {plazo}")
        lineas.append(f"{'ENLACE':<20} {enlace}")
        lineas.append(f"{'OBSERVACIONES':<20} Se evidencia adicional al contrato")
        lineas.append("")

    lineas.append(f"Fecha de generación: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    return "\n".join(lineas)


def _generar_informe_excel(contratos: pd.DataFrame) -> bytes:
    """Genera el informe de Análisis de Demanda en formato Excel."""
    filas = []
    for idx, (_, row) in enumerate(contratos.iterrows(), 1):
        proceso = str(row.get("proceso_de_compra", "N/D"))
        modalidad = str(row.get("modalidad_de_contratacion", "N/D")).upper()
        contratista = str(row.get("proveedor_adjudicado", "N/D")).upper()
        entidad = str(row.get("nombre_entidad", "N/D")).upper()
        ciudad = str(row.get("ciudad", "")).upper()
        contratante = f"{entidad}, {ciudad}" if ciudad else entidad
        objeto = str(row.get("objeto_del_contrato", "N/D")).upper()
        valor = row.get("valor_del_contrato", 0)
        valor_fmt = f"$ {valor:,.0f}" if pd.notna(valor) else "N/D"
        plazo = _calcular_plazo(row)
        enlace = _extraer_url(str(row.get("urlproceso", "")))

        filas.append({
            "No": idx,
            "No PROCESO SECOP": proceso,
            "MODALIDAD": modalidad,
            "CONTRATISTA": contratista,
            "CONTRATANTE": contratante,
            "OBJETO": objeto,
            "VALOR": valor_fmt,
            "PLAZO": plazo,
            "ENLACE": enlace,
            "OBSERVACIONES": "Se evidencia adicional al contrato",
        })

    df_informe = pd.DataFrame(filas)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_informe.to_excel(writer, index=False, sheet_name="Análisis Demanda")
        ws = writer.sheets["Análisis Demanda"]

        # Ajustar anchos
        anchos = {"A": 5, "B": 22, "C": 20, "D": 35, "E": 40, "F": 60,
                  "G": 18, "H": 20, "I": 60, "J": 40}
        for col_letter, width in anchos.items():
            ws.column_dimensions[col_letter].width = width

        # Ajustar alto de filas para el objeto
        for row_num in range(2, len(df_informe) + 2):
            ws.row_dimensions[row_num].height = 60

    return buffer.getvalue()


def _generar_informe_pdf(contratos: pd.DataFrame, palabras_busqueda: str = "") -> bytes:
    """Genera el informe de Análisis de Demanda en formato PDF profesional."""
    from fpdf import FPDF

    FONT = "DejaVu"
    FONT_DIR = "/usr/share/fonts/truetype/dejavu"

    # Márgenes y dimensiones (A4 = 210 x 297 mm)
    MARGIN_L = 15
    MARGIN_R = 15
    PAGE_W = 210
    USABLE_W = PAGE_W - MARGIN_L - MARGIN_R  # 180 mm
    COL_LABEL = 38
    COL_VALUE = USABLE_W - COL_LABEL  # 142 mm

    # Colores
    AZUL_OSCURO = (0, 51, 102)
    AZUL_CLARO = (230, 240, 250)
    GRIS_FONDO = (245, 245, 248)
    BLANCO = (255, 255, 255)
    NEGRO = (0, 0, 0)
    GRIS_TEXTO = (80, 80, 80)
    GRIS_LINEA = (180, 180, 190)

    class InformePDF(FPDF):
        def header(self):
            # Barra superior azul
            self.set_fill_color(*AZUL_OSCURO)
            self.rect(0, 0, PAGE_W, 14, "F")
            self.set_font(FONT, "B", 9)
            self.set_text_color(*BLANCO)
            self.set_y(3)
            self.cell(0, 8, "ANÁLISIS DE LA DEMANDA — SECOP — COLOMBIA COMPRA EFICIENTE", align="C")
            self.ln(14)

        def footer(self):
            self.set_y(-12)
            self.set_draw_color(*GRIS_LINEA)
            self.line(MARGIN_L, self.get_y(), PAGE_W - MARGIN_R, self.get_y())
            self.ln(2)
            self.set_font(FONT, "", 6.5)
            self.set_text_color(*GRIS_TEXTO)
            self.cell(0, 5, f"Página {self.page_no()}/{{nb}}", align="R")

    pdf = InformePDF(orientation="P", unit="mm", format="A4")
    pdf.add_font(FONT, "", f"{FONT_DIR}/DejaVuSans.ttf")
    pdf.add_font(FONT, "B", f"{FONT_DIR}/DejaVuSans-Bold.ttf")
    pdf.add_font(FONT, "I", f"{FONT_DIR}/DejaVuSans.ttf")
    pdf.add_font(FONT, "BI", f"{FONT_DIR}/DejaVuSans-Bold.ttf")
    pdf.set_left_margin(MARGIN_L)
    pdf.set_right_margin(MARGIN_R)
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()

    # ── Título principal ──
    pdf.set_font(FONT, "B", 15)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.cell(0, 10, "ANÁLISIS DE LA DEMANDA", new_x="LMARGIN", new_y="NEXT")
    # Línea decorativa bajo el título
    pdf.set_draw_color(*AZUL_OSCURO)
    pdf.set_line_width(0.6)
    pdf.line(MARGIN_L, pdf.get_y(), MARGIN_L + 60, pdf.get_y())
    pdf.set_line_width(0.2)
    pdf.ln(5)

    # ── Texto introductorio ──
    pdf.set_font(FONT, "", 8.5)
    pdf.set_text_color(*GRIS_TEXTO)
    intro = (
        "Se validan en el portal de contratación SECOP procesos adelantados "
        "en los últimos años por algunas entidades estatales del departamento "
        "y por este municipio para satisfacer las necesidades requeridas.\n\n"
        "De acuerdo a la consulta en el portal único de contratación estatal — "
        "SECOP (www.colombiacompra.gov.co), se observa la siguiente información:"
    )
    pdf.multi_cell(0, 4.5, intro)
    pdf.ln(4)

    if palabras_busqueda:
        pdf.set_fill_color(*AZUL_CLARO)
        pdf.set_font(FONT, "B", 8)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, f"  Palabras clave: {palabras_busqueda}", fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)

    total = len(contratos)
    valor_total = contratos["valor_del_contrato"].sum()
    valor_promedio = contratos["valor_del_contrato"].mean()
    valor_mediana = contratos["valor_del_contrato"].median()
    valor_min_c = contratos["valor_del_contrato"].min()
    valor_max_c = contratos["valor_del_contrato"].max()
    n_entidades = contratos["nombre_entidad"].nunique()
    n_ciudades = contratos["ciudad"].nunique()
    n_modalidades = contratos["modalidad_de_contratacion"].nunique()
    n_proveedores = contratos["proveedor_adjudicado"].nunique()

    top_mod = contratos["modalidad_de_contratacion"].value_counts()
    top_mod_nombre = top_mod.index[0] if len(top_mod) > 0 else "N/D"
    top_mod_pct = (top_mod.iloc[0] / total * 100) if len(top_mod) > 0 else 0
    top_ent = contratos["nombre_entidad"].value_counts()
    top_ent_nombre = top_ent.index[0] if len(top_ent) > 0 else "N/D"
    top_ent_count = top_ent.iloc[0] if len(top_ent) > 0 else 0

    # ── Sección de Estadísticos ──
    pdf.set_font(FONT, "B", 11)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.cell(0, 8, "ESTADÍSTICOS DEL RESULTADO", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*AZUL_OSCURO)
    pdf.set_line_width(0.4)
    pdf.line(MARGIN_L, pdf.get_y(), MARGIN_L + 50, pdf.get_y())
    pdf.set_line_width(0.2)
    pdf.ln(4)

    # Tabla de estadísticos
    stat_rows = [
        ("Total contratos", f"{total:,}"),
        ("Entidades únicas", f"{n_entidades:,}"),
        ("Ciudades", f"{n_ciudades:,}"),
        ("Proveedores únicos", f"{n_proveedores:,}"),
        ("Modalidades", f"{n_modalidades}"),
        ("Valor total", f"$ {valor_total:,.0f}"),
        ("Valor promedio", f"$ {valor_promedio:,.0f}" if pd.notna(valor_promedio) else "N/D"),
        ("Valor mediana", f"$ {valor_mediana:,.0f}" if pd.notna(valor_mediana) else "N/D"),
        ("Valor mínimo", f"$ {valor_min_c:,.0f}" if pd.notna(valor_min_c) else "N/D"),
        ("Valor máximo", f"$ {valor_max_c:,.0f}" if pd.notna(valor_max_c) else "N/D"),
        ("Modalidad predominante", f"{top_mod_nombre} ({top_mod_pct:.1f}%)"),
        ("Entidad con más contratos", f"{top_ent_nombre} ({top_ent_count})"),
    ]

    col_label_w = 55
    col_val_w = USABLE_W - col_label_w
    for i, (label, value) in enumerate(stat_rows):
        y0 = pdf.get_y()
        if i % 2 == 0:
            pdf.set_fill_color(*GRIS_FONDO)
        else:
            pdf.set_fill_color(*BLANCO)

        pdf.set_font(FONT, "B", 7.5)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.set_xy(MARGIN_L, y0)
        pdf.cell(col_label_w, 6, f"  {label}", border=0, fill=True)

        pdf.set_font(FONT, "", 7.5)
        pdf.set_text_color(*NEGRO)
        pdf.set_xy(MARGIN_L + col_label_w, y0)
        pdf.cell(col_val_w, 6, f"  {value}", border=0, fill=True)
        pdf.set_y(y0 + 6)

    pdf.ln(6)
    # Línea separadora
    pdf.set_draw_color(*GRIS_LINEA)
    pdf.line(MARGIN_L + 20, pdf.get_y(), PAGE_W - MARGIN_R - 20, pdf.get_y())
    pdf.ln(6)

    def _altura_multi(txt: str, ancho: float, font_size: float = 7.5) -> float:
        """Calcula la altura que ocupará un multi_cell."""
        pdf.set_font(FONT, "", font_size)
        # Estimar caracteres por línea (DejaVu ~2.2mm por char a 7.5pt)
        cpl = max(1, int(ancho / 2.1))
        lineas = 1
        for linea in txt.split("\n"):
            lineas += max(1, -(-len(linea) // cpl)) if linea else 1  # ceil division
        return lineas * 4.2

    # ── Fichas de cada contrato ──
    for idx, (_, row) in enumerate(contratos.iterrows(), 1):
        proceso = str(row.get("proceso_de_compra", "N/D"))
        modalidad = str(row.get("modalidad_de_contratacion", "N/D")).upper()
        contratista = str(row.get("proveedor_adjudicado", "N/D")).upper()
        entidad = str(row.get("nombre_entidad", "N/D")).upper()
        ciudad_val = str(row.get("ciudad", "")).upper()
        contratante = f"{entidad}, {ciudad_val}" if ciudad_val else entidad
        objeto = str(row.get("objeto_del_contrato", "N/D")).upper()
        valor = row.get("valor_del_contrato", 0)
        valor_fmt = f"$ {valor:,.0f}" if pd.notna(valor) else "N/D"
        plazo = _calcular_plazo(row)
        enlace = _extraer_url(str(row.get("urlproceso", "")))

        ficha = [
            ("No PROCESO SECOP", proceso),
            ("MODALIDAD", modalidad),
            ("CONTRATISTA", contratista),
            ("CONTRATANTE", contratante),
            ("OBJETO", objeto),
            ("VALOR", valor_fmt),
            ("PLAZO", plazo),
            ("ENLACE", enlace),
            ("OBSERVACIONES", "Se evidencia adicional al contrato"),
        ]

        # Estimar altura total de la ficha para decidir salto de página
        alt_ficha = 8  # encabezado
        for _, v in ficha:
            alt_ficha += max(6, _altura_multi(v, COL_VALUE - 4))
        if pdf.get_y() + alt_ficha > 270:
            pdf.add_page()

        # ── Encabezado del contrato ──
        pdf.set_font(FONT, "B", 9)
        pdf.set_fill_color(*AZUL_OSCURO)
        pdf.set_text_color(*BLANCO)
        pdf.cell(0, 8, f"   CONTRATO {idx} DE {total}", fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(*NEGRO)

        # ── Filas de la ficha ──
        for i, (campo, valor_campo) in enumerate(ficha):
            # Calcular altura necesaria
            h_val = max(6, _altura_multi(valor_campo, COL_VALUE - 4))

            # Verificar salto de página
            if pdf.get_y() + h_val > 278:
                pdf.add_page()

            y_start = pdf.get_y()
            x_start = pdf.l_margin

            # Color alterno
            if i % 2 == 0:
                fill_color = GRIS_FONDO
            else:
                fill_color = BLANCO

            # ── Celda ETIQUETA ──
            pdf.set_fill_color(*AZUL_CLARO)
            pdf.set_font(FONT, "B", 7.5)
            pdf.set_text_color(*AZUL_OSCURO)
            pdf.set_xy(x_start, y_start)
            pdf.cell(COL_LABEL, h_val, f"  {campo}", border="LTB", fill=True)

            # ── Celda VALOR (multi_cell para wrapping) ──
            pdf.set_fill_color(*fill_color)
            pdf.set_font(FONT, "", 7.5)
            pdf.set_text_color(*NEGRO)
            pdf.set_xy(x_start + COL_LABEL, y_start)

            # Dibujar el fondo y bordes del valor manualmente
            pdf.rect(x_start + COL_LABEL, y_start, COL_VALUE, h_val, "DF")
            pdf.set_xy(x_start + COL_LABEL + 2, y_start + 1)
            pdf.multi_cell(COL_VALUE - 4, 4.2, valor_campo, border=0, fill=False)

            # Mover cursor a la siguiente fila
            pdf.set_y(y_start + h_val)

        # Separador entre contratos
        pdf.ln(4)
        pdf.set_draw_color(*GRIS_LINEA)
        pdf.line(MARGIN_L + 20, pdf.get_y(), PAGE_W - MARGIN_R - 20, pdf.get_y())
        pdf.ln(4)

    # ── Pie del informe ──
    pdf.ln(3)
    pdf.set_draw_color(*AZUL_OSCURO)
    pdf.set_line_width(0.4)
    pdf.line(MARGIN_L, pdf.get_y(), PAGE_W - MARGIN_R, pdf.get_y())
    pdf.set_line_width(0.2)
    pdf.ln(3)
    pdf.set_font(FONT, "I", 7)
    pdf.set_text_color(*GRIS_TEXTO)
    pdf.cell(
        0, 5,
        f"Informe generado el {datetime.now().strftime('%d/%m/%Y %H:%M')}  |  "
        f"Total contratos: {total}  |  "
        f"Fuente: SECOP II — datos.gov.co",
        new_x="LMARGIN", new_y="NEXT",
    )

    return bytes(pdf.output())


# ────────────────────────────────────────────────────────────
# UI
# ────────────────────────────────────────────────────────────

# ── Header principal ──
st.markdown("""
<div class="main-header">
    <h1>📊 Análisis del Sector — SECOP</h1>
    <p>Departamento de Santander &nbsp;·&nbsp; Todas las modalidades &nbsp;·&nbsp; Contratos celebrados</p>
</div>
""", unsafe_allow_html=True)

df = cargar_datos()

# ── Barra de búsqueda ──
st.markdown('<div class="search-container">', unsafe_allow_html=True)
consulta = st.text_input(
    "🔍 Buscar por objeto del contrato",
    placeholder="Ej: suministro alimentos, vigilancia, consultoría, combustible...",
    help="Escribe palabras clave separadas por espacio. Se buscan contratos cuyo objeto contenga TODAS las palabras (AND lógico).",
    label_visibility="collapsed",
)
if not consulta.strip():
    st.markdown('<p class="search-hint">💡 Escribe palabras clave para filtrar contratos por objeto. Ejemplo: <em>suministro alimentos</em></p>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

# ── Sidebar profesional ──
with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
        <h3>🎯 Panel de Filtros</h3>
        <p>Refina tu búsqueda con los controles</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<p class="filter-label"><span class="fl-icon">📑</span> Modalidad de contratación</p>', unsafe_allow_html=True)
    modalidades = sorted(df["modalidad_de_contratacion"].dropna().unique())
    modalidad_sel = st.multiselect("Modalidad", modalidades, label_visibility="collapsed")

    st.markdown('<p class="filter-label"><span class="fl-icon">🏙️</span> Ciudad</p>', unsafe_allow_html=True)
    ciudades = sorted(df["ciudad"].dropna().unique())
    ciudad_sel = st.multiselect("Ciudad", ciudades, label_visibility="collapsed")

    st.markdown('<p class="filter-label"><span class="fl-icon">📝</span> Tipo de contrato</p>', unsafe_allow_html=True)
    tipos = sorted(df["tipo_de_contrato"].dropna().unique())
    tipo_sel = st.multiselect("Tipo", tipos, label_visibility="collapsed")

    st.markdown('<p class="filter-label"><span class="fl-icon">📌</span> Estado</p>', unsafe_allow_html=True)
    estados = sorted(df["estado_contrato"].dropna().unique())
    estado_sel = st.multiselect("Estado", estados, label_visibility="collapsed")

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    # Fechas
    st.markdown('<p class="filter-label"><span class="fl-icon">📅</span> Rango de fechas</p>', unsafe_allow_html=True)
    fecha_min = df["fecha_inicio"].min()
    fecha_max = df["fecha_fin"].max()
    if pd.isna(fecha_min):
        fecha_min = pd.Timestamp("2015-01-01")
    if pd.isna(fecha_max):
        fecha_max = pd.Timestamp.now()

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        fecha_desde = st.date_input(
            "Desde",
            value=fecha_min.date(),
            min_value=fecha_min.date(),
            max_value=fecha_max.date(),
        )
    with col_f2:
        fecha_hasta = st.date_input(
            "Hasta",
            value=fecha_max.date(),
            min_value=fecha_min.date(),
            max_value=fecha_max.date(),
        )

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    # Valor
    st.markdown('<p class="filter-label"><span class="fl-icon">💰</span> Valor del contrato</p>', unsafe_allow_html=True)
    max_valor_m = int(df["valor_del_contrato"].max() / 1_000_000) + 1
    rango_valor = st.slider(
        "Millones COP",
        min_value=0,
        max_value=max_valor_m,
        value=(0, max_valor_m),
        step=1,
        label_visibility="collapsed",
    )

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="db-counter">
            <div class="db-label">Base de datos</div>
            <div class="db-value">{len(df):,}</div>
            <div class="db-sub">contratos indexados</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ── Aplicar búsqueda y filtros ──
resultado = buscar(df, consulta)

if modalidad_sel:
    resultado = resultado[resultado["modalidad_de_contratacion"].isin(modalidad_sel)]
if ciudad_sel:
    resultado = resultado[resultado["ciudad"].isin(ciudad_sel)]
if tipo_sel:
    resultado = resultado[resultado["tipo_de_contrato"].isin(tipo_sel)]
if estado_sel:
    resultado = resultado[resultado["estado_contrato"].isin(estado_sel)]

resultado = resultado[
    (resultado["valor_del_contrato"] >= rango_valor[0] * 1_000_000)
    & (resultado["valor_del_contrato"] <= rango_valor[1] * 1_000_000)
]

# Filtro de fechas
resultado = resultado[
    (resultado["fecha_inicio"].isna() | (resultado["fecha_inicio"] >= pd.Timestamp(fecha_desde)))
    & (resultado["fecha_fin"].isna() | (resultado["fecha_fin"] <= pd.Timestamp(fecha_hasta)))
]

# ── Métricas con tarjetas ──
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">📄</div>
        <div class="metric-value">{len(resultado):,}</div>
        <div class="metric-label">Contratos encontrados</div>
    </div>""", unsafe_allow_html=True)

with col2:
    valor_total = resultado["valor_del_contrato"].sum()
    if valor_total >= 1_000_000_000:
        valor_display = f"${valor_total/1_000_000_000:,.1f}B"
    elif valor_total >= 1_000_000:
        valor_display = f"${valor_total/1_000_000:,.0f}M"
    else:
        valor_display = f"${valor_total:,.0f}"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">💰</div>
        <div class="metric-value">{valor_display}</div>
        <div class="metric-label">Valor total</div>
    </div>""", unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">🏛️</div>
        <div class="metric-value">{resultado['nombre_entidad'].nunique():,}</div>
        <div class="metric-label">Entidades únicas</div>
    </div>""", unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">🏙️</div>
        <div class="metric-value">{resultado['ciudad'].nunique():,}</div>
        <div class="metric-label">Ciudades</div>
    </div>""", unsafe_allow_html=True)

st.markdown("")

# ── Columnas a mostrar ──
columnas_display = [
    "nombre_entidad",
    "ciudad",
    "modalidad_de_contratacion",
    "objeto_del_contrato",
    "tipo_de_contrato",
    "estado_contrato",
    "valor_del_contrato",
    "valor_pagado",
    "proveedor_adjudicado",
    "fecha_inicio",
    "fecha_fin",
    "urlproceso",
]
columnas_existentes = [c for c in columnas_display if c in resultado.columns]

if resultado.empty:
    st.markdown("""
    <div class="empty-state">
        <div class="es-icon">🔍</div>
        <p class="es-title">No se encontraron contratos</p>
        <p class="es-desc">Ajusta los filtros o modifica las palabras clave de búsqueda</p>
    </div>
    """, unsafe_allow_html=True)
else:
    # Tabs para organizar contenido
    tab_tabla, tab_analisis = st.tabs(["📊 Tabla de Resultados", "📋 Análisis de la Demanda"])

    with tab_tabla:
        st.dataframe(
            resultado[columnas_existentes].reset_index(drop=True),
            use_container_width=True,
            height=550,
            column_config={
                "nombre_entidad": st.column_config.TextColumn("Entidad", width="medium"),
                "ciudad": st.column_config.TextColumn("Ciudad", width="small"),
                "modalidad_de_contratacion": st.column_config.TextColumn("Modalidad", width="medium"),
                "objeto_del_contrato": st.column_config.TextColumn("Objeto del Contrato", width="large"),
                "tipo_de_contrato": st.column_config.TextColumn("Tipo", width="small"),
                "estado_contrato": st.column_config.TextColumn("Estado", width="small"),
                "valor_del_contrato": st.column_config.NumberColumn("Valor Contrato", format="$%,.0f"),
                "valor_pagado": st.column_config.NumberColumn("Valor Pagado", format="$%,.0f"),
                "proveedor_adjudicado": st.column_config.TextColumn("Proveedor", width="medium"),
                "fecha_inicio": st.column_config.DateColumn("Inicio", format="DD/MM/YYYY"),
                "fecha_fin": st.column_config.DateColumn("Fin", format="DD/MM/YYYY"),
                "urlproceso": st.column_config.TextColumn("URL", width="small"),
            },
        )

        st.download_button(
            label="📥 Descargar tabla filtrada (CSV)",
            data=resultado[columnas_existentes].to_csv(index=False).encode("utf-8-sig"),
            file_name="contratos_filtrados.csv",
            mime="text/csv",
        )

    with tab_analisis:
        if not consulta.strip():
            st.markdown("""
            <div class="empty-state">
                <div class="es-icon">💡</div>
                <p class="es-title">Ingresa palabras clave en el buscador</p>
                <p class="es-desc">El Análisis de la Demanda se genera automáticamente con los contratos encontrados</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            contratos_informe = resultado.reset_index(drop=True)
            total_contratos = len(contratos_informe)

            # Header del análisis
            st.markdown(f"""
            <div class="analisis-header">
                <h2>📋 ANÁLISIS DE LA DEMANDA</h2>
                <p style="margin:0.3rem 0 0 0; opacity:0.85; font-size:0.85rem;">
                    {total_contratos} contratos encontrados · Palabras clave: <strong>{consulta}</strong>
                </p>
            </div>
            """, unsafe_allow_html=True)

            col_exp1, col_exp2, col_exp_spacer = st.columns([1, 1, 2])

            with col_exp1:
                csv_bytes = contratos_informe[columnas_existentes].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label="📥 Descargar Informe (CSV)",
                    data=csv_bytes,
                    file_name="analisis_demanda.csv",
                    mime="text/csv",
                )

            with col_exp2:
                pdf_bytes = _generar_informe_pdf(contratos_informe, consulta)
                st.download_button(
                    label="📥 Descargar Informe (PDF)",
                    data=pdf_bytes,
                    file_name="analisis_demanda.pdf",
                    mime="application/pdf",
                )

            st.markdown(
                "Se validan en el portal de contratación SECOP procesos adelantados "
                "en los últimos años por algunas entidades estatales del departamento "
                "y por este municipio para satisfacer las necesidades requeridas. "
                "De acuerdo a la consulta en el portal único de contratación estatal — "
                "SECOP [www.colombiacompra.gov.co](https://www.colombiacompra.gov.co), "
                "se observa la siguiente información:"
            )

            # ── Sección de Estadísticos ──
            valor_total_inf = contratos_informe["valor_del_contrato"].sum()
            valor_promedio = contratos_informe["valor_del_contrato"].mean()
            valor_mediana = contratos_informe["valor_del_contrato"].median()
            valor_min = contratos_informe["valor_del_contrato"].min()
            valor_max = contratos_informe["valor_del_contrato"].max()
            n_entidades = contratos_informe["nombre_entidad"].nunique()
            n_ciudades = contratos_informe["ciudad"].nunique()
            n_modalidades = contratos_informe["modalidad_de_contratacion"].nunique()
            n_proveedores = contratos_informe["proveedor_adjudicado"].nunique()

            # Top modalidad
            top_mod = contratos_informe["modalidad_de_contratacion"].value_counts()
            top_mod_nombre = top_mod.index[0] if len(top_mod) > 0 else "N/D"
            top_mod_pct = (top_mod.iloc[0] / total_contratos * 100) if len(top_mod) > 0 else 0

            # Top entidad
            top_ent = contratos_informe["nombre_entidad"].value_counts()
            top_ent_nombre = top_ent.index[0] if len(top_ent) > 0 else "N/D"
            top_ent_count = top_ent.iloc[0] if len(top_ent) > 0 else 0

            def _fmt_cop(v):
                if pd.isna(v) or v == 0:
                    return "$0"
                if abs(v) >= 1_000_000_000:
                    return f"${v/1_000_000_000:,.2f}B"
                if abs(v) >= 1_000_000:
                    return f"${v/1_000_000:,.1f}M"
                return f"${v:,.0f}"

            st.markdown('<p class="stats-section-title">📊 Estadísticos del Resultado</p>', unsafe_allow_html=True)

            st.markdown(f"""
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-title">Total contratos</div>
                    <div class="stat-value">{total_contratos:,}</div>
                    <div class="stat-sub">{n_entidades} entidades · {n_ciudades} ciudades</div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">Valor total</div>
                    <div class="stat-value">{_fmt_cop(valor_total_inf)}</div>
                    <div class="stat-sub">Suma de todos los contratos</div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">Valor promedio</div>
                    <div class="stat-value">{_fmt_cop(valor_promedio)}</div>
                    <div class="stat-sub">Mediana: {_fmt_cop(valor_mediana)}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">Rango de valores</div>
                    <div class="stat-value">{_fmt_cop(valor_min)} — {_fmt_cop(valor_max)}</div>
                    <div class="stat-sub">Mínimo — Máximo</div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">Proveedores únicos</div>
                    <div class="stat-value">{n_proveedores:,}</div>
                    <div class="stat-sub">{n_modalidades} modalidades</div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">Modalidad predominante</div>
                    <div class="stat-value" style="font-size:0.95rem;">{top_mod_nombre}</div>
                    <div class="stat-sub">{top_mod_pct:.1f}% de los contratos</div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">Entidad con más contratos</div>
                    <div class="stat-value" style="font-size:0.85rem;">{top_ent_nombre}</div>
                    <div class="stat-sub">{top_ent_count} contratos</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)

            # Fichas de contratos con HTML profesional
            for idx, (_, row) in enumerate(contratos_informe.iterrows(), 1):
                proceso = str(row.get("proceso_de_compra", "N/D"))
                modalidad = str(row.get("modalidad_de_contratacion", "N/D")).upper()
                contratista = str(row.get("proveedor_adjudicado", "N/D")).upper()
                entidad = str(row.get("nombre_entidad", "N/D")).upper()
                ciudad = str(row.get("ciudad", "")).upper()
                contratante = f"{entidad}, {ciudad}" if ciudad else entidad
                objeto = str(row.get("objeto_del_contrato", "N/D")).upper()
                valor = row.get("valor_del_contrato", 0)
                valor_fmt = f"$ {valor:,.0f}" if pd.notna(valor) else "N/D"
                plazo = _calcular_plazo(row)
                enlace = _extraer_url(str(row.get("urlproceso", "")))

                st.markdown(f"""
                <div class="contract-card">
                    <div class="contract-card-header">
                        CONTRATO {idx} DE {total_contratos}
                    </div>
                    <table>
                        <tr><td>No PROCESO SECOP</td><td>{proceso}</td></tr>
                        <tr><td>MODALIDAD</td><td>{modalidad}</td></tr>
                        <tr><td>CONTRATISTA</td><td>{contratista}</td></tr>
                        <tr><td>CONTRATANTE</td><td>{contratante}</td></tr>
                        <tr><td>OBJETO</td><td>{objeto}</td></tr>
                        <tr><td>VALOR</td><td>{valor_fmt}</td></tr>
                        <tr><td>PLAZO</td><td>{plazo}</td></tr>
                        <tr><td>ENLACE</td><td style="word-break:break-all;">{enlace}</td></tr>
                        <tr><td>OBSERVACIONES</td><td>Se evidencia adicional al contrato</td></tr>
                    </table>
                </div>
                """, unsafe_allow_html=True)

