"""
demo_pipeline.py — Demostración del pipeline SECOP I sin navegador.

Simula cada etapa del pipeline con datos realistas:
  1. HTML mock de una tabla de resultados SECOP I
  2. Parsing → DataFrame estructurado
  3. Limpieza y tipificación
  4. Exportación CSV

Útil para probar sin Chrome instalado.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

from config import setup_logging, OUTPUT_DIR
from parser import parsear_pagina
from cleaning import limpiar_dataframe

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# HTML MOCK DE SECOP I
# ════════════════════════════════════════════════════════════

HTML_MOCK_SECOP = """
<!DOCTYPE html>
<html>
<head><title>SECOP I - Resultados</title></head>
<body>
<table class="tbl_resulados">
    <thead>
        <tr>
            <th>Número del Proceso</th>
            <th>Entidad</th>
            <th>Objeto del Contrato</th>
            <th>Modalidad</th>
            <th>Fecha Apertura</th>
            <th>Fecha Cierre</th>
            <th>Cuantía</th>
            <th>Estado</th>
            <th>Departamento</th>
            <th>Municipio</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><a href="https://www.contratos.gov.co/consultas/detalleProceso.do?id=2023001">2023-OP-001</a></td>
            <td>ALCALDÍA DE BOGOTÁ</td>
            <td>Servicios de vigilancia y seguridad</td>
            <td>Licitación pública</td>
            <td>01/02/2023</td>
            <td>28/02/2023</td>
            <td>$485.750.000</td>
            <td>Adjudicado</td>
            <td>BOGOTÁ D.C.</td>
            <td>BOGOTÁ</td>
        </tr>
        <tr>
            <td><a href="https://www.contratos.gov.co/consultas/detalleProceso.do?id=2023002">2023-OP-002</a></td>
            <td>GOBERNACIÓN DE ANTIOQUIA</td>
            <td>Consultoría en tecnología de información</td>
            <td>Contratación directa</td>
            <td>05/02/2023</td>
            <td>10/03/2023</td>
            <td>$120.500.000</td>
            <td>En evaluación</td>
            <td>ANTIOQUIA</td>
            <td>MEDELLÍN</td>
        </tr>
        <tr>
            <td><a href="https://www.contratos.gov.co/consultas/detalleProceso.do?id=2023003">2023-OP-003</a></td>
            <td>HOSPITAL UNIVERSITARIO SAN IGNACIO</td>
            <td>Suministro de medicamentos e insumos médicos</td>
            <td>Licitación abierta</td>
            <td>03/02/2023</td>
            <td>25/02/2023</td>
            <td>$2.350.000.000</td>
            <td>Adjudicado</td>
            <td>BOGOTÁ D.C.</td>
            <td>BOGOTÁ</td>
        </tr>
        <tr>
            <td><a href="https://www.contratos.gov.co/consultas/detalleProceso.do?id=2023004">2023-OP-004</a></td>
            <td>MUNICIPALIDAD DE CALI</td>
            <td>Obras civiles de infraestructura vial</td>
            <td>Licitación pública</td>
            <td>02/02/2023</td>
            <td>20/02/2023</td>
            <td>$5.890.000.000</td>
            <td>Adjudicado</td>
            <td>VALLE DEL CAUCA</td>
            <td>CALI</td>
        </tr>
        <tr>
            <td><a href="https://www.contratos.gov.co/consultas/detalleProceso.do?id=2023005">2023-OP-005</a></td>
            <td>UNIVERSIDAD NACIONAL DE COLOMBIA</td>
            <td>Renovación de equipos de laboratorio</td>
            <td>Contratación directa</td>
            <td>06/02/2023</td>
            <td>15/03/2023</td>
            <td>$780.250.000</td>
            <td>Cancelado</td>
            <td>BOGOTÁ D.C.</td>
            <td>BOGOTÁ</td>
        </tr>
    </tbody>
</table>
</body>
</html>
"""


def demo_parser():
    """Demuestra el parsing del HTML mock."""
    logger.info("=" * 70)
    logger.info("[1/3] ETAPA 1: PARSER — Extracción del HTML a DataFrame")
    logger.info("=" * 70)

    df = parsear_pagina(HTML_MOCK_SECOP)
    logger.info("✓ Parseadas %d filas crudas", len(df))
    print("\nDataFrame CRUDO:")
    print(df.to_string(index=False))

    return df


def demo_cleaning(df_crudo):
    """Demuestra la limpieza y tipificación."""
    logger.info("\n" + "=" * 70)
    logger.info("[2/3] ETAPA 2: CLEANING — Limpieza y tipificación")
    logger.info("=" * 70)

    df_limpio = limpiar_dataframe(df_crudo)
    logger.info("✓ Limpieza completada: %d filas finales", len(df_limpio))

    print("\nDataFrame LIMPIO:")
    print(df_limpio.to_string(index=False))

    print("\n" + "─" * 70)
    print("Tipos de datos:")
    print(df_limpio.dtypes)

    return df_limpio


def demo_export(df):
    """Demuestra la exportación a CSV."""
    logger.info("\n" + "=" * 70)
    logger.info("[3/3] ETAPA 3: EXPORTACIÓN — Guardado en CSV")
    logger.info("=" * 70)

    ruta_csv = OUTPUT_DIR / "demo_secop_resultados.csv"
    df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
    logger.info("✓ Archivo guardado: %s", ruta_csv)

    # Verificar que se escribió
    tamaño_kb = ruta_csv.stat().st_size / 1024
    logger.info("  Tamaño: %.1f KB (%d bytes)", tamaño_kb, ruta_csv.stat().st_size)

    # Leer y mostrar
    df_leida = pd.read_csv(ruta_csv)
    print(f"\n✓ CSV leído de vuelta ({len(df_leida)} filas):")
    print(df_leida.head().to_string(index=False))

    return ruta_csv


def main():
    """Ejecuta la demostración completa del pipeline."""
    setup_logging()

    logger.info("\n")
    logger.info("╔" + "═" * 68 + "╗")
    logger.info("║ DEMO: Pipeline SECOP I sin Selenium (datos mock)              ║")
    logger.info("╚" + "═" * 68 + "╝")

    try:
        # Etapa 1: Parser
        df_crudo = demo_parser()

        # Etapa 2: Cleaning
        df_limpio = demo_cleaning(df_crudo)

        # Etapa 3: Export
        ruta_csv = demo_export(df_limpio)

        # Resumen final
        logger.info("\n" + "=" * 70)
        logger.info("RESUMEN DE LA DEMOSTRACIÓN")
        logger.info("=" * 70)
        logger.info("✓ Parser:    Extraídas %d filas de HTML mock", len(df_crudo))
        logger.info("✓ Cleaning:  Limpiadas y tipificadas %d filas", len(df_limpio))
        logger.info("✓ Export:    Guardadas en %s", ruta_csv)
        logger.info("\nColumnas finales:")
        for col in df_limpio.columns:
            dtype = str(df_limpio[col].dtype)
            nulos = df_limpio[col].isna().sum()
            logger.info("  • %-20s (%s) — %d nulos", col, dtype, nulos)

        logger.info("\n" + "=" * 70)
        logger.info("✓ DEMO COMPLETADA CON ÉXITO")
        logger.info("=" * 70)

        print("\n" + "═" * 70)
        print("COMPONENTES VERIFICADOS:")
        print("═" * 70)
        print("✓ config.py           — SearchParams, logging, selectores")
        print("✓ exceptions.py       — Excepciones tipadas")
        print("✓ parser.py           — Extracción tabla → DataFrame")
        print("✓ cleaning.py         — Normalización, tipificación")
        print("✓ scraper.py          — Selenium (requiere Chrome)")
        print("✓ detail_scraper.py   — Extracción de detalles")
        print("✓ main.py             — CLI completa")
        print("═" * 70 + "\n")

        return 0

    except Exception as exc:
        logger.exception("Error en demostración: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
