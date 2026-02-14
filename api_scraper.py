"""
api_scraper.py — Extracción de datos SECOP II vía API pública de Datos Abiertos.

Alternativa sin navegador al scraper Selenium.  Consulta directamente la
API Socrata de datos.gov.co, que expone los contratos públicos de SECOP II
sin restricción de IP ni CAPTCHA.

Datasets utilizados:
  • Contratos SECOP II: jbjy-vk9h
  • Procesos  SECOP II: p6dx-8zbt

Equivalencias SECOP I → SECOP II:
  • "Celebrado" → estado_contrato IN (Cerrado, terminado, En ejecución,
                   Modificado, Prorrogado)
  • "Contratación Mínima Cuantía" → "Mínima cuantía"
  • Departamento "Santander" (value 668000) → departamento = 'Santander'
"""

from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from config import OUTPUT_DIR, CSV_ENCODING, CSV_SEPARATOR

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────
# CONSTANTES
# ────────────────────────────────────────────────────────────

BASE_URL = "https://www.datos.gov.co/resource"
DATASET_CONTRATOS = "jbjy-vk9h"         # SECOP II — Contratos
DATASET_PROCESOS = "p6dx-8zbt"           # SECOP II — Procesos

# Límite de la API Socrata por request (máx 50 000 por offset).
API_PAGE_SIZE = 50000
API_MAX_RECORDS = 300000  # Suficiente para traer toda la BD de Santander

# Mapeo de values del formulario SECOP I → valores textuales API
MODALIDAD_MAP: dict[str, str] = {
    "1":  "Licitación pública",
    "11": "Selección Abreviada de Menor Cuantía",
    "9":  "Selección abreviada subasta inversa",
    "13": "Mínima cuantía",
    "17": "Selección Abreviada servicios de Salud",
    "10": "Concurso de méritos abierto",
    "12": "Contratación directa",
    "4":  "Contratación régimen especial",
    "2":  "Contratación Directa (con ofertas)",
}

DEPARTAMENTO_MAP: dict[str, str] = {
    "91000":  "Amazonas",
    "5000":   "Antioquia",
    "81000":  "Arauca",
    "8000":   "Atlántico",
    "1100":   "Bogotá D.C.",
    "1300":   "Bolívar",
    "15000":  "Boyacá",
    "17000":  "Caldas",
    "1800":   "Caquetá",
    "85000":  "Casanare",
    "19000":  "Cauca",
    "20000":  "Cesar",
    "27000":  "Chocó",
    "23000":  "Córdoba",
    "25000":  "Cundinamarca",
    "94000":  "Guainía",
    "95000":  "Guaviare",
    "41000":  "Huila",
    "44000":  "La Guajira",
    "47000":  "Magdalena",
    "50000":  "Meta",
    "52000":  "Nariño",
    "54000":  "Norte De Santander",
    "86000":  "Putumayo",
    "63000":  "Quindío",
    "66000":  "Risaralda",
    "88000":  "San Andrés, Providencia y Santa Catalina",
    "668000": "Santander",
    "70000":  "Sucre",
    "73000":  "Tolima",
    "76000":  "Valle del Cauca",
    "97000":  "Vaupés",
    "99000":  "Vichada",
}

# "Celebrado" en SECOP I equivale a contratos formalizados en SECOP II
ESTADO_CELEBRADO_EQUIVALENTES = [
    "Cerrado",
    "terminado",
    "En ejecución",
    "Modificado",
    "Prorrogado",
    "cedido",
]

# Columnas a solicitar a la API
COLUMNAS_API = [
    "nombre_entidad",
    "nit_entidad",
    "departamento",
    "ciudad",
    "modalidad_de_contratacion",
    "estado_contrato",
    "tipo_de_contrato",
    "objeto_del_contrato",
    "valor_del_contrato",
    "valor_pagado",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "documento_proveedor",
    "proveedor_adjudicado",
    "proceso_de_compra",
    "id_contrato",
    "urlproceso",
]


# ────────────────────────────────────────────────────────────
# FUNCIONES PRINCIPALES
# ────────────────────────────────────────────────────────────


def _construir_where(
    departamento: Optional[str] = None,
    modalidad: Optional[str] = None,
    estado: Optional[str] = None,
    palabra_clave: Optional[str] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None,
) -> str:
    """Construye la cláusula $where para la API Socrata.

    Args:
        departamento: Nombre del departamento (texto) o value SECOP I.
        modalidad:    Nombre de la modalidad (texto) o value SECOP I.
        estado:       Texto del estado ('Celebrado' se traduce automáticamente).
        palabra_clave: Texto para buscar en objeto_del_contrato.
        fecha_inicio:  Fecha desde (dd/MM/yyyy) para fecha_de_inicio_del_contrato.
        fecha_fin:     Fecha hasta (dd/MM/yyyy).

    Returns:
        Cadena SoQL para $where.
    """
    condiciones: list[str] = []

    # Departamento: si es un código numérico, traducir
    if departamento:
        depto_texto = DEPARTAMENTO_MAP.get(departamento, departamento)
        condiciones.append(f"departamento='{depto_texto}'")

    # Modalidad: si es un código numérico, traducir; soporta lista
    if modalidad:
        if isinstance(modalidad, (list, tuple)):
            # Traducir cada código/nombre
            mods = [MODALIDAD_MAP.get(m, m) for m in modalidad]
            mods_str = ",".join(f"'{m}'" for m in mods)
            condiciones.append(f"modalidad_de_contratacion in({mods_str})")
        else:
            mod_texto = MODALIDAD_MAP.get(modalidad, modalidad)
            condiciones.append(f"modalidad_de_contratacion='{mod_texto}'")

    # Estado: "Celebrado" se traduce a múltiples estados equivalentes
    if estado:
        if estado.lower() == "celebrado":
            estados_str = ",".join(f"'{e}'" for e in ESTADO_CELEBRADO_EQUIVALENTES)
            condiciones.append(f"estado_contrato in({estados_str})")
        else:
            condiciones.append(f"estado_contrato='{estado}'")

    # Palabra clave en objeto del contrato
    if palabra_clave:
        condiciones.append(f"upper(objeto_del_contrato) like upper('%{palabra_clave}%')")

    # Rango de fechas
    if fecha_inicio:
        # Convertir dd/MM/yyyy → yyyy-MM-ddT00:00:00
        parts = fecha_inicio.split("/")
        if len(parts) == 3:
            iso = f"{parts[2]}-{parts[1]}-{parts[0]}T00:00:00"
            condiciones.append(f"fecha_de_inicio_del_contrato >= '{iso}'")

    if fecha_fin:
        parts = fecha_fin.split("/")
        if len(parts) == 3:
            iso = f"{parts[2]}-{parts[1]}-{parts[0]}T23:59:59"
            condiciones.append(f"fecha_de_inicio_del_contrato <= '{iso}'")

    return " AND ".join(condiciones) if condiciones else ""


def _fetch_page(
    dataset: str,
    where: str,
    select: str,
    limit: int = API_PAGE_SIZE,
    offset: int = 0,
    order: str = "fecha_de_inicio_del_contrato DESC",
) -> list[dict]:
    """Realiza una consulta paginada a la API Socrata.

    Returns:
        Lista de diccionarios con los registros.
    """
    params = {"$limit": str(limit), "$offset": str(offset)}

    if where:
        params["$where"] = where
    if select:
        params["$select"] = select
    if order:
        params["$order"] = order

    url = f"{BASE_URL}/{dataset}.json?{urllib.parse.urlencode(params)}"
    logger.debug("API request: %s", url)

    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def contar_registros(
    departamento: Optional[str] = None,
    modalidad: Optional[str] = None,
    estado: Optional[str] = None,
    palabra_clave: Optional[str] = None,
) -> int:
    """Cuenta el total de registros que coinciden con los filtros.

    Returns:
        Número total de registros.
    """
    where = _construir_where(departamento, modalidad, estado, palabra_clave)
    data = _fetch_page(
        DATASET_CONTRATOS,
        where=where,
        select="count(*) as total",
        limit=1,
        order="",
    )
    return int(data[0]["total"]) if data else 0


def consultar_contratos(
    departamento: Optional[str] = None,
    modalidad: Optional[str] = None,
    estado: Optional[str] = None,
    palabra_clave: Optional[str] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    max_registros: int = API_MAX_RECORDS,
) -> pd.DataFrame:
    """Consulta contratos de SECOP II vía la API de Datos Abiertos.

    Maneja paginación automática para obtener todos los registros.

    Args:
        departamento: Código SECOP I (ej. '668000') o nombre.
        modalidad:    Código SECOP I (ej. '13') o nombre.
        estado:       'Celebrado' u otro estado; se traduce automáticamente.
        palabra_clave: Filtro por texto en el objeto del contrato.
        fecha_inicio:  Fecha desde (dd/MM/yyyy).
        fecha_fin:     Fecha hasta (dd/MM/yyyy).
        max_registros: Máximo de registros a obtener.

    Returns:
        DataFrame con los contratos encontrados.
    """
    where = _construir_where(
        departamento, modalidad, estado, palabra_clave, fecha_inicio, fecha_fin
    )
    select = ",".join(COLUMNAS_API)

    total = contar_registros(departamento, modalidad, estado, palabra_clave)
    logger.info("Total de registros que coinciden: %d", total)

    if total == 0:
        logger.warning("La consulta no retornó registros.")
        return pd.DataFrame(columns=COLUMNAS_API)

    registros_a_obtener = min(total, max_registros)
    logger.info("Obteniendo %d de %d registros...", registros_a_obtener, total)

    all_records: list[dict] = []
    offset = 0

    while offset < registros_a_obtener:
        page_size = min(API_PAGE_SIZE, registros_a_obtener - offset)
        page = _fetch_page(
            DATASET_CONTRATOS,
            where=where,
            select=select,
            limit=page_size,
            offset=offset,
        )

        if not page:
            break

        all_records.extend(page)
        offset += len(page)
        logger.info(
            "  Página obtenida: %d registros (acumulado: %d / %d)",
            len(page), len(all_records), registros_a_obtener,
        )

        if len(page) < page_size:
            break  # Última página

    df = pd.DataFrame(all_records)
    logger.info("Consulta API completada: %d registros obtenidos.", len(df))
    return df


def consultar_desde_params(params) -> pd.DataFrame:
    """Consulta usando un objeto SearchParams de config.py.

    Args:
        params: Instancia de SearchParams.

    Returns:
        DataFrame con los contratos.
    """
    return consultar_contratos(
        departamento=params.departamento,
        modalidad=params.modalidad,
        estado=params.estado,
        palabra_clave=params.palabra_clave,
        fecha_inicio=params.fecha_inicio,
        fecha_fin=params.fecha_fin,
        max_registros=params.max_pages * 25,  # ~25 registros por página SECOP I
    )


# ────────────────────────────────────────────────────────────
# EJECUCIÓN DIRECTA
# ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from config import setup_logging, SEARCH_SANTANDER_MINIMA_CELEBRADO
    from cleaning import limpiar_dataframe

    setup_logging()

    logger.info("=" * 70)
    logger.info("CONSULTA API — Santander / Mínima Cuantía / Celebrado")
    logger.info("=" * 70)

    df = consultar_desde_params(SEARCH_SANTANDER_MINIMA_CELEBRADO)

    if df.empty:
        logger.warning("No se obtuvieron registros.")
        print("⚠️  Sin resultados.")
    else:
        # Limpieza
        df_limpio = limpiar_dataframe(df)

        # Exportar
        ruta = OUTPUT_DIR / "secop_santander_minima_celebrado.csv"
        df_limpio.to_csv(ruta, index=False, sep=CSV_SEPARATOR, encoding=CSV_ENCODING)

        logger.info("Archivo exportado: %s", ruta)

        print("\n" + "=" * 70)
        print("RESULTADOS — Santander / Mínima Cuantía / Celebrado")
        print("=" * 70)
        print(f"Total registros: {len(df_limpio)}")
        print(f"Archivo: {ruta}")
        print("\nVista previa (primeros 10):")
        cols_preview = [c for c in [
            "nombre_entidad", "ciudad", "estado_contrato",
            "tipo_de_contrato", "valor_del_contrato", "objeto_del_contrato",
        ] if c in df_limpio.columns]
        print(df_limpio[cols_preview].head(10).to_string(index=False))
