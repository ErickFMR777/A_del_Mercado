"""
cleaning.py — Limpieza y tipificación del DataFrame de resultados SECOP I.

Responsabilidades:
  1. Normalizar strings (strip, colapsar espacios, eliminar saltos de línea).
  2. Convertir columnas monetarias (formato colombiano) a ``float``.
  3. Parsear columnas de fecha a ``datetime``.
  4. Eliminar filas completamente vacías.
  5. Renombrar columnas según convención canónica.
  6. Validar el esquema final y generar reporte de calidad.

Principios:
  • Inmutabilidad: todas las funciones retornan un **nuevo** DataFrame.
  • Nunca pierde datos: valores no convertibles se mantienen como ``NaN``
    en vez de descartarse.
  • Logging detallado de cada transformación.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import pandas as pd

from config import (
    COLUMNAS_FECHA,
    COLUMNAS_MONETARIAS,
    COLUMNAS_RESULTADO,
)

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# 1. NORMALIZACIÓN DE STRINGS
# ════════════════════════════════════════════════════════════


def _normalizar_string(valor: object) -> str:
    """Normaliza un valor a string limpio.

    • Convierte a ``str``.
    • Elimina saltos de línea, tabuladores y retornos de carro.
    • Colapsa espacios múltiples.
    • Aplica ``strip()``.
    """
    if pd.isna(valor):
        return ""
    s = str(valor)
    s = re.sub(r"[\n\r\t]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


def normalizar_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica ``_normalizar_string`` a todas las columnas tipo ``object``.

    Returns:
        Nuevo DataFrame con strings normalizados.
    """
    df = df.copy()
    cols_str = df.select_dtypes(include=["object"]).columns
    for col in cols_str:
        df[col] = df[col].map(_normalizar_string)
    logger.info("Strings normalizados en %d columnas.", len(cols_str))
    return df


# ════════════════════════════════════════════════════════════
# 2. CONVERSIÓN DE VALORES MONETARIOS
# ════════════════════════════════════════════════════════════


def _convertir_moneda_colombiana(valor: str) -> Optional[float]:
    """Convierte un string de moneda colombiana a ``float``.

    Formato colombiano típico:
      • ``$1.234.567.890``   → ``1234567890.0``
      • ``$1.234.567,89``    → ``1234567.89``
      • ``1234567``          → ``1234567.0``
      • ``COP 1.234.567``    → ``1234567.0``
      • vacío / no numérico  → ``None``

    Lógica:
      1. Eliminar prefijos (``$``, ``COP``, espacios).
      2. Si hay exactamente una coma, es separador decimal.
         → eliminar puntos (miles), reemplazar coma por punto.
      3. Si solo hay puntos, el último podría ser decimal
         si le siguen ≤2 dígitos; de lo contrario, todos son miles.
    """
    if not valor or valor.strip() == "":
        return None

    # Limpiar prefijos y caracteres no numéricos (excepto . , -)
    s = valor.strip()
    s = re.sub(r"^[^\d\-]*", "", s)  # Quitar prefijos no numéricos
    s = s.replace(" ", "")

    if not s:
        return None

    # Caso: tiene coma → el formato es 1.234.567,89
    if "," in s:
        partes = s.split(",")
        entero = partes[0].replace(".", "")
        decimal = partes[1] if len(partes) > 1 else "0"
        try:
            return float(f"{entero}.{decimal}")
        except ValueError:
            return None

    # Caso: solo puntos → determinar si el último es decimal
    if "." in s:
        partes = s.split(".")
        ultima = partes[-1]
        if len(ultima) <= 2 and len(partes) > 1:
            # Último punto es separador decimal: 1.234.56 → 1234.56
            entero = "".join(partes[:-1])
            try:
                return float(f"{entero}.{ultima}")
            except ValueError:
                return None
        else:
            # Todos los puntos son separadores de miles: 1.234.567 → 1234567
            try:
                return float(s.replace(".", ""))
            except ValueError:
                return None

    # Caso: solo dígitos y posible signo negativo
    try:
        return float(s)
    except ValueError:
        return None


def convertir_columnas_monetarias(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas monetarias de string a ``float64``.

    Solo procesa columnas que existen en el DataFrame **y** están listadas
    en ``config.COLUMNAS_MONETARIAS``.

    Returns:
        Nuevo DataFrame con columnas monetarias convertidas.
    """
    df = df.copy()
    cols_presentes = [c for c in COLUMNAS_MONETARIAS if c in df.columns]

    for col in cols_presentes:
        antes_nulos = df[col].isna().sum()
        df[col] = df[col].astype(str).map(_convertir_moneda_colombiana)
        despues_nulos = df[col].isna().sum()
        nuevos_nulos = despues_nulos - antes_nulos

        if nuevos_nulos > 0:
            logger.warning(
                "Columna '%s': %d valores no convertibles a float.", col, nuevos_nulos
            )
        logger.debug("Columna '%s' convertida a float64.", col)

    if cols_presentes:
        logger.info("Columnas monetarias convertidas: %s", cols_presentes)
    else:
        logger.debug("No se encontraron columnas monetarias para convertir.")

    return df


# ════════════════════════════════════════════════════════════
# 3. PARSEO DE COLUMNAS DE FECHA
# ════════════════════════════════════════════════════════════

# Formatos de fecha que usa el portal SECOP I
_FORMATOS_FECHA: list[str] = [
    "%d/%m/%Y",          # 31/01/2025
    "%d-%m-%Y",          # 31-01-2025
    "%Y-%m-%d",          # 2025-01-31
    "%d/%m/%Y %H:%M",   # 31/01/2025 14:30
    "%d/%m/%Y %H:%M:%S", # 31/01/2025 14:30:00
    "%Y-%m-%dT%H:%M:%S", # ISO 8601
]


def _parsear_fecha(valor: str) -> Optional[pd.Timestamp]:
    """Intenta parsear un string a ``Timestamp`` probando múltiples formatos.

    Returns:
        ``pd.Timestamp`` si se logra, ``None`` en caso contrario.
    """
    if not valor or valor.strip() == "":
        return None

    s = valor.strip()

    for fmt in _FORMATOS_FECHA:
        try:
            return pd.Timestamp(pd.to_datetime(s, format=fmt))
        except (ValueError, TypeError):
            continue

    # Último intento: inferencia automática de pandas
    try:
        return pd.Timestamp(pd.to_datetime(s, dayfirst=True))
    except (ValueError, TypeError):
        return None


def convertir_columnas_fecha(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas de fecha de string a ``datetime64``.

    Solo procesa columnas presentes en el DataFrame que coincidan con
    ``config.COLUMNAS_FECHA``.

    Returns:
        Nuevo DataFrame con columnas de fecha convertidas.
    """
    df = df.copy()
    cols_presentes = [c for c in COLUMNAS_FECHA if c in df.columns]

    for col in cols_presentes:
        antes_nulos = df[col].isna().sum()
        df[col] = df[col].astype(str).map(_parsear_fecha)
        despues_nulos = df[col].isna().sum()
        nuevos_nulos = despues_nulos - antes_nulos

        if nuevos_nulos > 0:
            logger.warning(
                "Columna '%s': %d valores no parseables como fecha.", col, nuevos_nulos
            )
        logger.debug("Columna '%s' convertida a datetime.", col)

    if cols_presentes:
        logger.info("Columnas de fecha convertidas: %s", cols_presentes)

    return df


# ════════════════════════════════════════════════════════════
# 4. ELIMINACIÓN DE FILAS VACÍAS
# ════════════════════════════════════════════════════════════


def eliminar_filas_vacias(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas donde todas las columnas son vacías o NaN.

    También elimina filas donde todas las columnas de texto son strings
    vacíos (que ``dropna`` no detecta).

    Returns:
        Nuevo DataFrame sin filas vacías.
    """
    df = df.copy()
    antes = len(df)

    # Paso 1: dropna clásico
    df.dropna(how="all", inplace=True)

    # Paso 2: filas donde todos los strings son vacíos
    cols_str = df.select_dtypes(include=["object"]).columns
    if len(cols_str) > 0:
        mask_vacios = df[cols_str].apply(
            lambda row: all(str(v).strip() == "" for v in row), axis=1
        )
        df = df[~mask_vacios]

    df.reset_index(drop=True, inplace=True)
    eliminadas = antes - len(df)

    if eliminadas > 0:
        logger.info("Filas vacías eliminadas: %d", eliminadas)

    return df


# ════════════════════════════════════════════════════════════
# 5. RENOMBRAR COLUMNAS (MAPEO OPCIONAL)
# ════════════════════════════════════════════════════════════


def renombrar_columnas(
    df: pd.DataFrame, mapeo: Optional[dict[str, str]] = None
) -> pd.DataFrame:
    """Renombra columnas del DataFrame según un mapeo explícito.

    Si no se provee mapeo, intenta alinear con ``COLUMNAS_RESULTADO``
    por posición.

    Args:
        df:    DataFrame a renombrar.
        mapeo: ``{nombre_actual: nombre_nuevo}`` (opcional).

    Returns:
        Nuevo DataFrame con columnas renombradas.
    """
    df = df.copy()

    if mapeo:
        df.rename(columns=mapeo, inplace=True)
        logger.info("Columnas renombradas con mapeo explícito: %s", mapeo)
    elif list(df.columns) != COLUMNAS_RESULTADO:
        # Solo renombrar si las columnas son genéricas (col_0, col_1, ...)
        if all(str(c).startswith("col_") for c in df.columns):
            nuevas = COLUMNAS_RESULTADO[: len(df.columns)]
            df.columns = nuevas
            logger.info("Columnas renombradas por posición a: %s", nuevas)

    return df


# ════════════════════════════════════════════════════════════
# 6. REPORTE DE CALIDAD DE DATOS
# ════════════════════════════════════════════════════════════


def generar_reporte_calidad(df: pd.DataFrame) -> dict:
    """Genera un reporte resumido de la calidad del DataFrame.

    Returns:
        Diccionario con métricas por columna y globales.
    """
    reporte: dict = {
        "total_filas": len(df),
        "total_columnas": len(df.columns),
        "columnas": {},
    }

    for col in df.columns:
        info_col = {
            "dtype": str(df[col].dtype),
            "nulos": int(df[col].isna().sum()),
            "pct_nulos": round(df[col].isna().mean() * 100, 2),
            "unicos": int(df[col].nunique()),
        }

        # Para strings: contar vacíos
        if df[col].dtype == "object":
            vacios = int((df[col].astype(str).str.strip() == "").sum())
            info_col["vacios"] = vacios

        reporte["columnas"][col] = info_col

    total_nulos = int(df.isna().sum().sum())
    total_celdas = len(df) * len(df.columns)
    reporte["pct_completitud"] = round(
        (1 - total_nulos / total_celdas) * 100, 2
    ) if total_celdas > 0 else 0.0

    logger.info(
        "Reporte de calidad: %d filas, completitud %.1f%%.",
        reporte["total_filas"],
        reporte["pct_completitud"],
    )
    return reporte


# ════════════════════════════════════════════════════════════
# 7. PIPELINE DE LIMPIEZA COMPLETO
# ════════════════════════════════════════════════════════════


def limpiar_dataframe(
    df: pd.DataFrame,
    mapeo_columnas: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    """Ejecuta el pipeline completo de limpieza sobre un DataFrame crudo.

    Orden de operaciones:
      1. Normalizar strings.
      2. Eliminar filas vacías.
      3. Renombrar columnas.
      4. Convertir columnas monetarias.
      5. Convertir columnas de fecha.

    Args:
        df:              DataFrame crudo del parser.
        mapeo_columnas:  Mapeo opcional ``{col_actual: col_nueva}``.

    Returns:
        DataFrame limpio y tipificado.
    """
    logger.info("Iniciando pipeline de limpieza (%d filas)...", len(df))

    df = normalizar_strings(df)
    df = eliminar_filas_vacias(df)
    df = renombrar_columnas(df, mapeo_columnas)
    df = convertir_columnas_monetarias(df)
    df = convertir_columnas_fecha(df)

    reporte = generar_reporte_calidad(df)
    logger.info(
        "Pipeline de limpieza completado: %d filas, completitud %.1f%%.",
        reporte["total_filas"],
        reporte["pct_completitud"],
    )

    return df
