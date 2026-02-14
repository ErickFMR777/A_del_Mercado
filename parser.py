"""
parser.py — Extracción y estructuración de datos de las tablas HTML de SECOP I.

Responsabilidades:
  1. Recibir HTML crudo (una o múltiples páginas) desde ``scraper.py``.
  2. Localizar la tabla de resultados con BeautifulSoup.
  3. Extraer encabezados y filas de la tabla.
  4. Asignar nombres de columnas canónicos (``config.COLUMNAS_RESULTADO``).
  5. Extraer URLs de detalle embebidas en los links de cada fila.
  6. Consolidar múltiples páginas en un único DataFrame.

Principios de diseño:
  • Tolerancia a cambios menores en la estructura del DOM.
  • Logging detallado de cada etapa de parsing.
  • Nunca modifica tipos de datos — eso corresponde a ``cleaning.py``.
"""

from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup, Tag

from config import (
    COLUMNAS_RESULTADO,
    SECOP_BASE_URL,
    SEL_TABLA_RESULTADOS,
    SEL_TABLA_RESULTADOS_FALLBACK,
)
from exceptions import SecopEmptyTableError, SecopParsingError

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# 1. LOCALIZAR TABLA EN EL HTML
# ════════════════════════════════════════════════════════════


def _encontrar_tabla(soup: BeautifulSoup) -> Optional[Tag]:
    """Localiza la tabla principal de resultados en el DOM.

    Estrategia de búsqueda (de mayor a menor especificidad):
      1. Clase CSS exacta del portal (``tbl_resulados``, sic).
      2. Tabla con mayor número de filas (heurística).
      3. Primera tabla del documento (último recurso).

    Args:
        soup: Árbol BeautifulSoup del HTML.

    Returns:
        Elemento ``<table>`` encontrado, o ``None``.
    """
    # Intento 1: clase CSS conocida del portal
    # Nota: el portal usa "tbl_resulados" (un solo 't') — typo original.
    clase_css = SEL_TABLA_RESULTADOS.replace("table.", "")
    tabla = soup.find("table", class_=clase_css)
    if tabla:
        logger.debug("Tabla encontrada por clase CSS '%s'.", clase_css)
        return tabla

    # Intento 2: tabla con más filas <tr>
    tablas = soup.find_all("table")
    if tablas:
        tabla_mayor = max(tablas, key=lambda t: len(t.find_all("tr")))
        num_filas = len(tabla_mayor.find_all("tr"))
        if num_filas > 1:
            logger.debug(
                "Tabla seleccionada por heurística (mayor filas: %d de %d tablas).",
                num_filas,
                len(tablas),
            )
            return tabla_mayor

    # Intento 3: primera tabla
    tabla = soup.find("table")
    if tabla:
        logger.debug("Usando primera tabla del documento (fallback).")
        return tabla

    return None


# ════════════════════════════════════════════════════════════
# 2. EXTRAER ENCABEZADOS
# ════════════════════════════════════════════════════════════


def _extraer_encabezados(tabla: Tag) -> list[str]:
    """Extrae los encabezados de la tabla desde ``<th>`` o la primera fila ``<tr>``.

    Normaliza cada encabezado: minúsculas, sin espacios dobles,
    caracteres especiales reemplazados por ``_``.

    Returns:
        Lista de encabezados normalizados (puede estar vacía).
    """
    encabezados: list[str] = []

    # Intentar desde <thead> → <th>
    thead = tabla.find("thead")
    if thead:
        ths = thead.find_all("th")
        if ths:
            encabezados = [th.get_text(strip=True) for th in ths]

    # Fallback: primera fila <tr> con <th>
    if not encabezados:
        primera_fila = tabla.find("tr")
        if primera_fila:
            ths = primera_fila.find_all("th")
            if ths:
                encabezados = [th.get_text(strip=True) for th in ths]

    # Normalizar
    encabezados_norm = []
    for h in encabezados:
        h_clean = re.sub(r"[^a-záéíóúñü0-9]+", "_", h.lower().strip())
        h_clean = re.sub(r"_+", "_", h_clean).strip("_")
        encabezados_norm.append(h_clean)

    logger.debug("Encabezados extraídos (%d): %s", len(encabezados_norm), encabezados_norm)
    return encabezados_norm


# ════════════════════════════════════════════════════════════
# 3. EXTRAER FILAS DE DATOS
# ════════════════════════════════════════════════════════════


def _extraer_filas(tabla: Tag) -> list[list[str]]:
    """Extrae todas las filas de datos (``<td>``) de la tabla.

    Ignora filas que:
      • Contienen solo ``<th>`` (encabezados).
      • Están completamente vacías.
      • Pertenecen al pie de paginación.

    Returns:
        Lista de listas de strings (una lista interna por fila).
    """
    filas: list[list[str]] = []

    # Buscar filas en <tbody>; si no existe, en toda la tabla
    contenedor = tabla.find("tbody") or tabla

    for tr in contenedor.find_all("tr"):
        celdas = tr.find_all("td")
        if not celdas:
            continue

        fila = [td.get_text(strip=True) for td in celdas]

        # Descartar filas dummy (todo vacío o solo espacios)
        if all(c == "" for c in fila):
            continue

        filas.append(fila)

    logger.debug("Filas de datos extraídas: %d", len(filas))
    return filas


# ════════════════════════════════════════════════════════════
# 4. EXTRAER URLs DE DETALLE DE CADA FILA
# ════════════════════════════════════════════════════════════


def _extraer_urls_detalle_html(tabla: Tag) -> list[Optional[str]]:
    """Extrae la URL de detalle de proceso de cada fila de la tabla.

    Busca el primer ``<a>`` con ``href`` que contenga ``detalleProceso``
    dentro de cada ``<tr>``.

    Returns:
        Lista paralela a las filas con la URL de detalle (o ``None``).
    """
    urls: list[Optional[str]] = []
    contenedor = tabla.find("tbody") or tabla

    for tr in contenedor.find_all("tr"):
        celdas = tr.find_all("td")
        if not celdas:
            continue

        url_detalle: Optional[str] = None
        for a in tr.find_all("a", href=True):
            href = a["href"]
            if "detalleProceso" in href or "detalle" in href.lower():
                url_detalle = urljoin(SECOP_BASE_URL, href)
                break

        urls.append(url_detalle)

    return urls


# ════════════════════════════════════════════════════════════
# 5. PARSEAR UNA SOLA PÁGINA HTML → DataFrame
# ════════════════════════════════════════════════════════════


def parsear_pagina(html: str) -> pd.DataFrame:
    """Convierte el HTML de una página de resultados en un DataFrame.

    Flujo:
      1. Parsear HTML con BeautifulSoup.
      2. Localizar la tabla.
      3. Extraer encabezados y filas.
      4. Asignar columnas canónicas o los encabezados del portal.
      5. Añadir columna ``url_detalle`` con los links de cada fila.

    Args:
        html: String HTML de una sola página de resultados.

    Returns:
        DataFrame con los datos crudos (sin limpiar ni tipificar).

    Raises:
        SecopParsingError: Si no se encuentra tabla o las filas están vacías.
    """
    soup = BeautifulSoup(html, "html.parser")
    tabla = _encontrar_tabla(soup)

    if tabla is None:
        raise SecopParsingError(
            "No se encontró tabla de resultados en el HTML.",
            context={"html_length": len(html)},
        )

    encabezados = _extraer_encabezados(tabla)
    filas = _extraer_filas(tabla)

    if not filas:
        raise SecopParsingError(
            "Tabla encontrada pero sin filas de datos.",
            context={"encabezados": encabezados},
        )

    # Determinar columnas
    num_cols = len(filas[0])
    if encabezados and len(encabezados) == num_cols:
        columnas = encabezados
    elif num_cols <= len(COLUMNAS_RESULTADO):
        columnas = COLUMNAS_RESULTADO[:num_cols]
    else:
        columnas = [f"col_{i}" for i in range(num_cols)]
        logger.warning(
            "Número de columnas (%d) no coincide con ningún esquema conocido. "
            "Usando nombres genéricos.",
            num_cols,
        )

    # Igualar longitud de filas (rellenar si faltan columnas)
    filas_normalizadas = []
    for fila in filas:
        if len(fila) < num_cols:
            fila.extend([""] * (num_cols - len(fila)))
        elif len(fila) > num_cols:
            fila = fila[:num_cols]
        filas_normalizadas.append(fila)

    df = pd.DataFrame(filas_normalizadas, columns=columnas)

    # Añadir URLs de detalle
    urls = _extraer_urls_detalle_html(tabla)
    if urls and len(urls) == len(df):
        df["url_detalle"] = urls
    else:
        df["url_detalle"] = None
        logger.debug(
            "URLs de detalle no mapeables (urls=%d, filas=%d).",
            len(urls) if urls else 0,
            len(df),
        )

    logger.info("Página parseada: %d filas × %d columnas.", len(df), len(df.columns))
    return df


# ════════════════════════════════════════════════════════════
# 6. PARSEAR MÚLTIPLES PÁGINAS → DataFrame CONSOLIDADO
# ════════════════════════════════════════════════════════════


def parsear_todas_paginas(paginas_html: list[str]) -> pd.DataFrame:
    """Parsea una lista de HTMLs de páginas y consolida en un solo DataFrame.

    Cada página se parsea de forma independiente.  Las filas se concatenan
    y se elimina el índice para obtener un rango continuo.

    Si alguna página falla el parseo, se registra el error y se omite
    (no detiene el pipeline).

    Args:
        paginas_html: Lista de strings HTML (uno por página).

    Returns:
        DataFrame consolidado con **todas** las filas de **todas** las
        páginas.

    Raises:
        SecopEmptyTableError: Si **ninguna** página produjo datos.
    """
    if not paginas_html:
        raise SecopEmptyTableError(
            "No se recibieron páginas HTML para parsear.",
        )

    dataframes: list[pd.DataFrame] = []
    errores: int = 0

    for idx, html in enumerate(paginas_html, start=1):
        try:
            df_pagina = parsear_pagina(html)
            dataframes.append(df_pagina)
        except SecopParsingError as exc:
            errores += 1
            logger.warning("Página %d: error de parsing — %s", idx, exc)

    if not dataframes:
        raise SecopEmptyTableError(
            f"Ninguna de las {len(paginas_html)} páginas produjo datos "
            f"({errores} errores de parsing).",
        )

    df_consolidado = pd.concat(dataframes, ignore_index=True)

    # Eliminar filas duplicadas exactas (pueden ocurrir en overlaps de paginación)
    antes = len(df_consolidado)
    df_consolidado.drop_duplicates(inplace=True)
    df_consolidado.reset_index(drop=True, inplace=True)
    despues = len(df_consolidado)

    if antes != despues:
        logger.info("Duplicados eliminados: %d → %d filas.", antes, despues)

    logger.info(
        "Parsing completado: %d filas totales de %d páginas (%d errores).",
        len(df_consolidado),
        len(paginas_html),
        errores,
    )

    return df_consolidado
