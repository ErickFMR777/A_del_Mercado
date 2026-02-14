"""
detail_scraper.py — Extracción de detalles individuales de procesos SECOP I.

Responsabilidades:
  1. Navegar a la URL de detalle de un proceso.
  2. Parsear los campos de la ficha de detalle (tabla de key-value).
  3. Extraer campos enriquecidos: proveedor, NIT, valor adjudicado,
     valor estimado, fecha de adjudicación, etc.
  4. Soportar extracción masiva con control de ritmo (rate limiting)
     para evitar bloqueos.
  5. Consolidar todos los detalles en un DataFrame.

Diseño escalable:
  • Cada detalle se extrae de forma independiente y atómica.
  • Si un proceso falla, se registra el error y se continúa con el siguiente.
  • El rate limiter es configurable para respetar el servidor.
  • El módulo se puede invocar en paralelo con múltiples drivers.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup, Tag
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from config import (
    COLUMNAS_DETALLE,
    DEFAULT_TIMEOUT,
    MAX_RETRIES,
    RETRY_BACKOFF,
    SECOP_BASE_URL,
)
from exceptions import SecopParsingError, SecopTimeoutError

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# 1. DATACLASS PARA DETALLE DE PROCESO
# ════════════════════════════════════════════════════════════


@dataclass
class DetalleProceso:
    """Representa los datos detallados de un proceso de contratación.

    Cada campo corresponde a una etiqueta de la ficha de detalle del
    portal SECOP I.  Los campos se llenan progresivamente a medida
    que se encuentran en el HTML.
    """

    numero_proceso: str = ""
    entidad: str = ""
    objeto_contrato: str = ""
    modalidad: str = ""
    fecha_apertura: str = ""
    fecha_cierre: str = ""
    fecha_adjudicacion: str = ""
    valor_estimado: str = ""
    valor_adjudicado: str = ""
    valor_contrato: str = ""
    proveedor: str = ""
    nit_proveedor: str = ""
    departamento: str = ""
    municipio: str = ""
    estado: str = ""
    url_detalle: str = ""

    def to_dict(self) -> dict:
        """Convierte el dataclass a diccionario."""
        return asdict(self)


# ════════════════════════════════════════════════════════════
# 2. MAPEO DE ETIQUETAS DEL PORTAL → CAMPOS DEL DATACLASS
# ════════════════════════════════════════════════════════════

# El portal SECOP I muestra el detalle como pares etiqueta-valor.
# Este mapeo vincula cada etiqueta (normalizada) con el atributo
# correspondiente del dataclass.

_MAPEO_ETIQUETAS: dict[str, str] = {
    # Etiqueta del portal (normalizada en minúsculas) → campo del dataclass
    "número del proceso": "numero_proceso",
    "numero del proceso": "numero_proceso",
    "nro. proceso": "numero_proceso",
    "proceso": "numero_proceso",
    "entidad": "entidad",
    "nombre entidad": "entidad",
    "objeto del contrato": "objeto_contrato",
    "objeto a contratar": "objeto_contrato",
    "descripción": "objeto_contrato",
    "modalidad de contratación": "modalidad",
    "modalidad de contratacion": "modalidad",
    "modalidad": "modalidad",
    "fecha de apertura": "fecha_apertura",
    "fecha apertura": "fecha_apertura",
    "fecha de publicación": "fecha_apertura",
    "fecha de cierre": "fecha_cierre",
    "fecha cierre": "fecha_cierre",
    "fecha de adjudicación": "fecha_adjudicacion",
    "fecha adjudicación": "fecha_adjudicacion",
    "fecha adjudicacion": "fecha_adjudicacion",
    "valor estimado del contrato": "valor_estimado",
    "valor estimado": "valor_estimado",
    "cuantía": "valor_estimado",
    "cuantia": "valor_estimado",
    "presupuesto": "valor_estimado",
    "valor del contrato": "valor_contrato",
    "valor contrato": "valor_contrato",
    "valor adjudicado": "valor_adjudicado",
    "valor de adjudicación": "valor_adjudicado",
    "proveedor adjudicado": "proveedor",
    "contratista": "proveedor",
    "proveedor": "proveedor",
    "razón social": "proveedor",
    "razon social": "proveedor",
    "nit": "nit_proveedor",
    "nit proveedor": "nit_proveedor",
    "identificación del contratista": "nit_proveedor",
    "departamento": "departamento",
    "departamento entidad": "departamento",
    "municipio": "municipio",
    "ciudad": "municipio",
    "ciudad entidad": "municipio",
    "estado": "estado",
    "estado del proceso": "estado",
}


def _normalizar_etiqueta(texto: str) -> str:
    """Normaliza una etiqueta del portal para búsqueda en el mapeo.

    Minúsculas, sin espacios extra, sin caracteres especiales
    excepto letras y espacios.
    """
    s = texto.lower().strip()
    s = re.sub(r"[:\-–—]+$", "", s).strip()  # Quitar : al final
    s = re.sub(r"[^a-záéíóúñü0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# ════════════════════════════════════════════════════════════
# 3. PARSEAR HTML DE DETALLE
# ════════════════════════════════════════════════════════════


def _parsear_detalle_html(html: str, url: str) -> DetalleProceso:
    """Extrae todos los campos de la ficha de detalle de un proceso.

    Busca pares etiqueta-valor en:
      1. Filas ``<tr>`` con exactamente 2-3 ``<td>`` (etiqueta | valor).
      2. Pares ``<dt>`` / ``<dd>`` (lista de definiciones).
      3. ``<span>`` o ``<label>`` seguidos de un ``<span>`` o ``<div>``
         con el valor.

    Args:
        html: HTML de la página de detalle.
        url:  URL original (se almacena en el resultado).

    Returns:
        Instancia de ``DetalleProceso`` con los campos poblados.
    """
    soup = BeautifulSoup(html, "html.parser")
    detalle = DetalleProceso(url_detalle=url)

    campos_encontrados: set[str] = set()

    # --- Estrategia 1: Filas <tr> con pares etiqueta-valor ---
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        etiqueta_raw = tds[0].get_text(strip=True)
        valor_raw = tds[1].get_text(strip=True)

        etiqueta_norm = _normalizar_etiqueta(etiqueta_raw)
        campo = _MAPEO_ETIQUETAS.get(etiqueta_norm)

        if campo and campo not in campos_encontrados:
            setattr(detalle, campo, valor_raw)
            campos_encontrados.add(campo)

    # --- Estrategia 2: Listas de definición <dt>/<dd> ---
    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue

        etiqueta_norm = _normalizar_etiqueta(dt.get_text(strip=True))
        valor_raw = dd.get_text(strip=True)
        campo = _MAPEO_ETIQUETAS.get(etiqueta_norm)

        if campo and campo not in campos_encontrados:
            setattr(detalle, campo, valor_raw)
            campos_encontrados.add(campo)

    # --- Estrategia 3: Labels + spans ---
    for label in soup.find_all(["label", "span"]):
        etiqueta_raw = label.get_text(strip=True)
        etiqueta_norm = _normalizar_etiqueta(etiqueta_raw)
        campo = _MAPEO_ETIQUETAS.get(etiqueta_norm)

        if campo and campo not in campos_encontrados:
            # Buscar el siguiente elemento hermano que contenga el valor
            siguiente = label.find_next_sibling(["span", "div", "p", "td"])
            if siguiente:
                valor_raw = siguiente.get_text(strip=True)
                if valor_raw and valor_raw != etiqueta_raw:
                    setattr(detalle, campo, valor_raw)
                    campos_encontrados.add(campo)

    logger.debug(
        "Detalle parseado para '%s': %d/%d campos encontrados.",
        detalle.numero_proceso or url,
        len(campos_encontrados),
        len(COLUMNAS_DETALLE) - 1,  # -1 por url_detalle
    )

    return detalle


# ════════════════════════════════════════════════════════════
# 4. NAVEGAR Y EXTRAER DETALLE DE UN PROCESO
# ════════════════════════════════════════════════════════════


def extraer_detalle_proceso(
    driver: WebDriver,
    url: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[DetalleProceso]:
    """Navega a la URL de detalle de un proceso y extrae sus datos.

    Incluye manejo de reintentos con backoff exponencial.

    Args:
        driver:  WebDriver activo.
        url:     URL absoluta del detalle del proceso.
        timeout: Segundos máximos de espera de carga.

    Returns:
        ``DetalleProceso`` con los datos extraídos, o ``None`` si falla.
    """
    for intento in range(1, MAX_RETRIES + 1):
        try:
            logger.debug("Navegando a detalle: %s (intento %d)", url, intento)
            driver.get(url)

            # Esperar a que la página cargue (body visible)
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Manejar posibles iframes en la página de detalle
            driver.switch_to.default_content()
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            if iframes:
                try:
                    driver.switch_to.frame(iframes[0])
                except WebDriverException:
                    pass

            html = driver.page_source
            detalle = _parsear_detalle_html(html, url)
            return detalle

        except TimeoutException:
            espera = RETRY_BACKOFF ** intento
            logger.warning(
                "Timeout en detalle '%s' (intento %d/%d, espera %.1f s).",
                url, intento, MAX_RETRIES, espera,
            )
            time.sleep(espera)

        except WebDriverException as exc:
            logger.error("Error WebDriver en detalle '%s': %s", url, exc)
            if intento < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** intento)
            else:
                return None

    logger.error("Agotados reintentos para detalle: %s", url)
    return None


# ════════════════════════════════════════════════════════════
# 5. EXTRACCIÓN MASIVA DE DETALLES
# ════════════════════════════════════════════════════════════


def extraer_detalles_masivo(
    driver: WebDriver,
    urls: list[str],
    delay: float = 1.5,
    max_errores: int = 10,
) -> pd.DataFrame:
    """Extrae detalles de múltiples procesos secuencialmente con rate limiting.

    Args:
        driver:       WebDriver activo.
        urls:         Lista de URLs de detalle a procesar.
        delay:        Segundos de espera entre cada proceso (rate limit).
        max_errores:  Número máximo de errores consecutivos antes de abortar.

    Returns:
        DataFrame con los detalles de todos los procesos extraídos
        exitosamente.
    """
    resultados: list[dict] = []
    errores_consecutivos = 0
    total = len(urls)

    logger.info("Iniciando extracción masiva de detalles: %d procesos.", total)

    for idx, url in enumerate(urls, start=1):
        logger.info("Proceso %d/%d: %s", idx, total, url)

        detalle = extraer_detalle_proceso(driver, url)

        if detalle:
            resultados.append(detalle.to_dict())
            errores_consecutivos = 0
        else:
            errores_consecutivos += 1
            logger.warning(
                "Fallo en proceso %d/%d (errores consecutivos: %d).",
                idx, total, errores_consecutivos,
            )

        # Abortar si hay demasiados errores seguidos
        if errores_consecutivos >= max_errores:
            logger.error(
                "Abortando extracción masiva tras %d errores consecutivos.",
                max_errores,
            )
            break

        # Rate limiting (excepto en el último)
        if idx < total:
            time.sleep(delay)

        # Progreso cada 10 procesos
        if idx % 10 == 0:
            logger.info(
                "Progreso: %d/%d procesados (%d exitosos).",
                idx, total, len(resultados),
            )

    if not resultados:
        logger.warning("No se extrajo ningún detalle de %d URLs.", total)
        return pd.DataFrame(columns=COLUMNAS_DETALLE)

    df = pd.DataFrame(resultados)

    # Reordenar columnas según esquema canónico
    cols_presentes = [c for c in COLUMNAS_DETALLE if c in df.columns]
    cols_extra = [c for c in df.columns if c not in COLUMNAS_DETALLE]
    df = df[cols_presentes + cols_extra]

    logger.info(
        "Extracción masiva completada: %d/%d detalles extraídos.",
        len(df), total,
    )

    return df


# ════════════════════════════════════════════════════════════
# 6. CONSTRUIR BASE HISTÓRICA
# ════════════════════════════════════════════════════════════


def actualizar_base_historica(
    nuevos: pd.DataFrame,
    ruta_historica: str,
    columna_clave: str = "numero_proceso",
) -> pd.DataFrame:
    """Combina nuevos registros con una base histórica existente.

    Lógica:
      1. Si el archivo histórico existe, lo carga.
      2. Concatena los nuevos registros.
      3. Elimina duplicados basándose en ``columna_clave``, conservando
         el registro más reciente.
      4. Guarda el resultado actualizado.

    Args:
        nuevos:          DataFrame con registros nuevos.
        ruta_historica:  Ruta del archivo CSV/Parquet de la base histórica.
        columna_clave:   Columna usada como identificador único.

    Returns:
        DataFrame con la base histórica actualizada.
    """
    from pathlib import Path

    ruta = Path(ruta_historica)

    # Cargar base existente
    if ruta.exists():
        if ruta.suffix == ".parquet":
            historica = pd.read_parquet(ruta)
        else:
            historica = pd.read_csv(ruta, dtype=str)
        logger.info("Base histórica cargada: %d registros.", len(historica))
    else:
        historica = pd.DataFrame()
        logger.info("No existe base histórica, se creará nueva.")

    # Concatenar
    combinado = pd.concat([historica, nuevos], ignore_index=True)

    # Deduplicar (conservar el último = más reciente)
    if columna_clave in combinado.columns:
        antes = len(combinado)
        combinado.drop_duplicates(subset=[columna_clave], keep="last", inplace=True)
        combinado.reset_index(drop=True, inplace=True)
        logger.info(
            "Deduplicación: %d → %d registros (clave: '%s').",
            antes, len(combinado), columna_clave,
        )

    # Guardar
    if ruta.suffix == ".parquet":
        combinado.to_parquet(ruta, index=False, engine="pyarrow")
    else:
        combinado.to_csv(ruta, index=False, encoding="utf-8-sig")

    logger.info("Base histórica actualizada en '%s': %d registros.", ruta, len(combinado))
    return combinado
