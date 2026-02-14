"""
config.py — Constantes de configuración centralizadas para el pipeline SECOP I.

Contiene:
  • URL base y selectores CSS / XPath del portal de consultas.
  • Nombres canónicos de las columnas del DataFrame de salida.
  • Parámetros de reintentos, timeouts y rutas de exportación.
  • Configuración de logging estructurado (JSON-ready).

Se importa desde todos los demás módulos para evitar valores
mágicos y facilitar el mantenimiento cuando el portal cambie selectores.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ────────────────────────────────────────────────────────────
# 1. RUTAS Y DIRECTORIOS
# ────────────────────────────────────────────────────────────

BASE_DIR: Path = Path(__file__).resolve().parent
OUTPUT_DIR: Path = BASE_DIR / "output"
LOG_DIR: Path = BASE_DIR / "logs"

# Crear directorios si no existen
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ────────────────────────────────────────────────────────────
# 2. URL Y ENDPOINTS DE SECOP I
# ────────────────────────────────────────────────────────────

SECOP_BASE_URL: str = "https://www.contratos.gov.co"
SECOP_CONSULTA_URL: str = f"{SECOP_BASE_URL}/consultas/inicioConsulta.do"
SECOP_RESULTADO_URL: str = f"{SECOP_BASE_URL}/consultas/resultadoListadoProcesos.do"
SECOP_DETALLE_URL: str = f"{SECOP_BASE_URL}/consultas/detalleProceso.do"

# ────────────────────────────────────────────────────────────
# 3. SELECTORES DEL FORMULARIO DE BÚSQUEDA
#    (CSS Selectors y XPaths verificados contra el DOM de SECOP I)
# ────────────────────────────────────────────────────────────

# --- Campos de texto ---
SEL_KEYWORD_INPUT: str = "input#palabraClave"          # Objeto del contrato
SEL_NUMERO_PROCESO: str = "input#numProceso"            # Número de proceso
SEL_ENTIDAD: str = "input#nombreEntidad"                # Entidad

# --- Campos de fecha (formato dd/MM/yyyy) ---
SEL_FECHA_INICIO: str = "input#fechaIni"                # Fecha apertura desde
SEL_FECHA_FIN: str = "input#fechaFin"                   # Fecha apertura hasta

# --- Selects (dropdowns) — IDs verificados contra el DOM real de SECOP I ---
SEL_OBJETO: str = "select#objeto"                       # Producto o Servicio (UNSPSC)
SEL_MODALIDAD: str = "select#tipoProceso"               # Modalidad de contratación
SEL_DEPARTAMENTO: str = "select#selDepartamento"        # Departamento de ejecución
SEL_MUNICIPIO: str = "select#selMunicipio"              # Municipio (carga AJAX tras depto)
SEL_ESTADO: str = "select#estado"                       # Estado del proceso (carga dinámica)
SEL_FAMILIA: str = "select#familia"                     # Familia UNSPSC

# --- Botones ---
SEL_BTN_BUSCAR: str = "img#ctl00_ContentPlaceHolder1_imgBuscar"  # Botón buscar (es <img>)
SEL_BTN_LIMPIAR: str = "input#btnLimpiar"               # Botón limpiar formulario

# ────────────────────────────────────────────────────────────
# 4. SELECTORES DE RESULTADOS E IFRAME
# ────────────────────────────────────────────────────────────

# El portal carga los resultados dentro de un <iframe>.
IFRAME_NAME: str = "iframeVentana"
IFRAME_XPATH: str = "//iframe[@name='iframeVentana']"

# Tabla de resultados
SEL_TABLA_RESULTADOS: str = "table.tbl_resulados"       # Nombre real de la clase (sic)
SEL_TABLA_RESULTADOS_FALLBACK: str = "table"            # Fallback genérico

# --- Paginación ---
SEL_PAGINA_SIGUIENTE: str = "a.sig"                     # Link "Siguiente"
SEL_PAGINA_ANTERIOR: str = "a.ant"                      # Link "Anterior"
SEL_PAGINAS: str = "td.paginas a"                       # Links numéricos de página
SEL_TOTAL_REGISTROS: str = "span#totalRegistros"        # Total de registros

# --- Link de detalle de cada proceso ---
SEL_LINK_DETALLE: str = "a[href*='detalleProceso']"

# ────────────────────────────────────────────────────────────
# 5. NOMBRES CANÓNICOS DE COLUMNAS
#    Mapeo posicional (índice → nombre) de la tabla de resultados.
# ────────────────────────────────────────────────────────────

COLUMNAS_RESULTADO: list[str] = [
    "numero_proceso",
    "entidad",
    "objeto_contrato",
    "modalidad",
    "fecha_apertura",
    "fecha_cierre",
    "cuantia",
    "estado",
    "departamento",
    "municipio",
]

# Columnas numéricas que requieren conversión monetaria
COLUMNAS_MONETARIAS: list[str] = [
    "cuantia",
    "valor_estimado",
    "valor_adjudicado",
    "valor_contrato",
]

# Columnas de fecha que requieren parseo
COLUMNAS_FECHA: list[str] = [
    "fecha_apertura",
    "fecha_cierre",
    "fecha_adjudicacion",
    "fecha_contrato",
]

# ────────────────────────────────────────────────────────────
# 6. COLUMNAS DEL DETALLE INDIVIDUAL DE PROCESO
# ────────────────────────────────────────────────────────────

COLUMNAS_DETALLE: list[str] = [
    "numero_proceso",
    "entidad",
    "objeto_contrato",
    "modalidad",
    "fecha_apertura",
    "fecha_cierre",
    "fecha_adjudicacion",
    "valor_estimado",
    "valor_adjudicado",
    "valor_contrato",
    "proveedor",
    "nit_proveedor",
    "departamento",
    "municipio",
    "estado",
    "url_detalle",
]

# ────────────────────────────────────────────────────────────
# 7. TIMEOUTS Y REINTENTOS
# ────────────────────────────────────────────────────────────

DEFAULT_TIMEOUT: int = 30                               # Segundos para WebDriverWait
PAGE_LOAD_WAIT: float = 3.0                             # Espera tras clic de paginación
MAX_RETRIES: int = 3                                    # Reintentos por operación
RETRY_BACKOFF: float = 2.0                              # Factor de backoff exponencial
MAX_PAGES: int = 200                                    # Límite de seguridad de paginación
RECAPTCHA_WAIT: int = 120                               # Espera para resolución manual

# ────────────────────────────────────────────────────────────
# 8. CHROME DRIVER OPTIONS
# ────────────────────────────────────────────────────────────

CHROME_HEADLESS: bool = os.getenv("SECOP_HEADLESS", "0") == "1"

CHROME_ARGUMENTS: list[str] = [
    "--start-maximized",
    "--disable-blink-features=AutomationControlled",
    "--disable-extensions",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--window-size=1920,1080",
    "--lang=es-CO",
]

CHROME_PREFS: dict = {
    "profile.managed_default_content_settings.images": 2,   # Desactivar imágenes
    "intl.accept_languages": "es-CO,es",
}

# ────────────────────────────────────────────────────────────
# 9. EXPORTACIÓN
# ────────────────────────────────────────────────────────────

CSV_SEPARATOR: str = ","
CSV_ENCODING: str = "utf-8-sig"                         # BOM para Excel en español
PARQUET_ENGINE: str = "pyarrow"

# ────────────────────────────────────────────────────────────
# 10. DATACLASS DE PARÁMETROS DE BÚSQUEDA
#     Encapsula todos los filtros posibles del formulario.
# ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SearchParams:
    """Parámetros de búsqueda para el formulario de SECOP I.

    Todos los campos son opcionales.  Si un campo es ``None`` el
    scraper NO interactúa con ese control del formulario.

    Attributes:
        palabra_clave:  Texto libre para el campo *Objeto del contrato*.
        numero_proceso: Número específico de un proceso.
        entidad:        Nombre (parcial) de la entidad compradora.
        fecha_inicio:   Fecha apertura desde (``dd/MM/yyyy``).
        fecha_fin:      Fecha apertura hasta  (``dd/MM/yyyy``).
        objeto:         Valor (value) del dropdown *Producto o Servicio*.
        modalidad:      Valor (value) del dropdown *Modalidad de Contratación*.
        departamento:   Valor (value) del dropdown *Departamento*.
        municipio:      Valor (value) o texto del dropdown *Municipio*.
        estado:         Texto visible del dropdown *Estado* (carga dinámica).
        familia:        Código o texto de la familia UNSPSC.
        max_pages:      Límite de páginas a recorrer (override global).
    """

    palabra_clave: Optional[str] = None
    numero_proceso: Optional[str] = None
    entidad: Optional[str] = None
    fecha_inicio: Optional[str] = None
    fecha_fin: Optional[str] = None
    objeto: Optional[str] = None
    modalidad: Optional[str] = None
    departamento: Optional[str] = None
    municipio: Optional[str] = None
    estado: Optional[str] = None
    familia: Optional[str] = None
    max_pages: int = MAX_PAGES


# ────────────────────────────────────────────────────────────
# 10b. CONFIGURACIONES PREDETERMINADAS DE BÚSQUEDA
# ────────────────────────────────────────────────────────────

# Valores de los <option value="..."> del DOM real de SECOP I
VALOR_DEPTO_SANTANDER: str = "668000"
VALOR_MODALIDAD_MINIMA_CUANTIA: str = "13"
TEXTO_ESTADO_CELEBRADO: str = "Celebrado"

SEARCH_SANTANDER_MINIMA_CELEBRADO = SearchParams(
    departamento=VALOR_DEPTO_SANTANDER,
    modalidad=VALOR_MODALIDAD_MINIMA_CUANTIA,
    estado=TEXTO_ESTADO_CELEBRADO,
)


# ────────────────────────────────────────────────────────────
# 11. CONFIGURACIÓN DE LOGGING ESTRUCTURADO
# ────────────────────────────────────────────────────────────

LOG_FORMAT: str = (
    "%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-25s | %(message)s"
)
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL: int = logging.DEBUG if os.getenv("SECOP_DEBUG", "0") == "1" else logging.INFO
LOG_FILE: Path = LOG_DIR / "secop_pipeline.log"


def setup_logging() -> None:
    """Configura logging con salida a consola **y** a archivo rotativo.

    Se invoca una sola vez desde ``main.py`` en el punto de entrada.
    Usa ``RotatingFileHandler`` para evitar archivos de log gigantes.
    """
    from logging.handlers import RotatingFileHandler

    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)

    # Evitar handlers duplicados en re-imports
    if root_logger.handlers:
        return

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Handler de consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Handler de archivo rotativo (5 MB, 5 backups)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
