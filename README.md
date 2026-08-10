# Adel_Sector — Contratación pública colombiana: SECOP I + SECOP II

Extrae datos de contratación pública de los **dos** portales del Estado
colombiano, los unifica en un solo esquema y genera el **Estudio del
Sector** con la estructura que exige Colombia Compra Eficiente.

| Fuente | Qué es | Cómo se consulta |
|---|---|---|
| **SECOP I** | Portal [contratos.gov.co](https://www.contratos.gov.co) | HTTP directo, sin navegador |
| **SECOP II** | API de [datos.gov.co](https://www.datos.gov.co) (Socrata) | Consultas SoQL con filtros en servidor |

Incluye un dashboard en Streamlit que consulta ambos portales **en vivo**
y exporta el Estudio del Sector a Word y PDF.

---

## Arquitectura

Dos rutas de extracción independientes, con esquemas de columnas
distintos, que convergen en la capa de limpieza:

```
Ruta A — SECOP I                        Ruta B — SECOP II

scraper.py                              api_scraper.py
  · sesión HTTP + cookies                 · consultas SoQL
  · resultadosConsulta.do (GET)           · paginación estable ($order=:id)
  · paginación por paginaObjetivo         · sin tope de registros
  · manejo del WAF (403 + backoff)        · filtros en el servidor
  · Selenium solo como respaldo                    │
        │                                          │
parser.py                                          │
  · 9 columnas reales de la tabla                  │
  · id_proceso desde consultaProceso()             │
  · departamento/municipio y fecha                 │
        │                                          │
        └──────────► cleaning.py ◄─────────────────┘
                       · moneda (dos convenciones)
                       · fechas (dd-mm-yyyy e ISO)
                       · filtro local por palabra clave
                            │
                    ┌───────┴────────┐
              CSV en output/     consulta.py ──► app.py ──► estudio_sector.py
                                (consulta en vivo)            (Word / PDF)
```

`catalogos.py` es la única fuente de verdad de los valores de filtro: los
dos portales **no nombran igual las mismas cosas** y una diferencia de
una letra devuelve cero registros sin ningún error.

## Estructura del proyecto

```
Adel_Sector/
├── config.py             # Endpoints, códigos, selectores, SearchParams, logging
├── catalogos.py          # Departamentos, modalidades, tipos y estados por portal
├── exceptions.py         # Excepciones tipadas del pipeline
├── scraper.py            # SECOP I: transporte HTTP (+ Selenium de respaldo)
├── parser.py             # HTML de SECOP I → DataFrame estructurado
├── api_scraper.py        # SECOP II: API de Datos Abiertos
├── cleaning.py           # Limpieza y tipificación (única capa que convierte tipos)
├── detail_scraper.py     # Ficha de detalle de un proceso de SECOP I
├── consulta.py           # Consulta en vivo y unificación de esquemas
├── estudio_sector.py     # Estudio del Sector en Word y PDF (Guía V3)
├── main.py               # CLI (punto de entrada)
├── app.py                # Dashboard Streamlit
├── verificar_fuentes.py  # Chequeo de salud de ambas fuentes
├── demo_pipeline.py      # Smoke test sin red
├── Dockerfile            # Despliegue en contenedor
├── output/               # CSV generados (auto-creado, ignorado por git)
└── logs/                 # Logs rotativos (auto-creado, ignorado por git)
```

## Instalación

```bash
git clone https://github.com/ErickFMR777/Adel_Sector.git
cd Adel_Sector

python -m venv .venv
source .venv/bin/activate      # Linux/Mac
# .venv\Scripts\activate       # Windows

pip install -r requirements.txt
```

> **No hace falta Chrome.** La ruta principal de SECOP I usa HTTP
> directo. Chrome solo se necesita si fuerzas `--selenium`, y en ese caso
> se detecta automáticamente según el sistema operativo (o se indica con
> `CHROME_BINARY`).

## Uso desde la línea de comandos

```bash
# SECOP II (API): rápido, filtros en el servidor
python main.py --fuente api \
    --departamento Santander \
    --modalidad "Mínima cuantía" \
    --tipo-contrato Obra \
    --fecha-inicio 01/01/2026 --fecha-fin 31/03/2026

# SECOP I (portal): datos en tiempo real
python main.py --fuente secop1 \
    --departamento Santander \
    --modalidad "Mínima Cuantía" --estado Celebrado \
    --max-paginas 5

# auto (por defecto): intenta SECOP I y cae a la API si falla
python main.py --palabra-clave vigilancia

# Consulta nacional: sin departamento
python main.py --fuente api --tipo-contrato Interventoría

# Enriquecer con la ficha de detalle (usa la columna url_detalle)
python main.py --modo detalle --entrada output/resultados.csv

# Dashboard
streamlit run app.py
```

> **SECOP I no tiene búsqueda por texto libre.** Su formulario solo
> filtra por código UNSPSC, entidad, fechas, modalidad, estado, ubicación
> y cuantía. Por eso `--palabra-clave` se aplica en local sobre lo
> descargado; en la API sí viaja al servidor.

### Argumentos

| Argumento | Alias | Descripción |
|---|---|---|
| `--fuente` | | `auto` (default), `secop1` o `api` |
| `--modo` | | `busqueda` (default) o `detalle` |
| `--palabra-clave` | `-k` | Texto en el objeto del contrato |
| `--departamento` | `-d` | Código o nombre (ej. `668000` o `Santander`) |
| `--modalidad` | `-m` | Código o nombre de cualquiera de los dos portales |
| `--tipo-contrato` | | Solo API: `Obra`, `Consultoría`, `Interventoría`… |
| `--estado` | | ID o nombre (ej. `4` o `Celebrado`) |
| `--fecha-inicio` | `-fi` | Desde (`dd/MM/yyyy`) |
| `--fecha-fin` | `-ff` | Hasta (`dd/MM/yyyy`) |
| `--entidad` | | Nombre parcial de la entidad |
| `--numero-proceso` | | Número específico de proceso |
| `--objeto` | | Código UNSPSC del segmento (ej. `80000000`) |
| `--municipio` | | Código de municipio |
| `--cuantia` | | Código del rango de cuantía |
| `--max-paginas` | | Páginas de SECOP I (100 procesos cada una) |
| `--max-registros` | | Tope de registros de la API (default: todos) |
| `--selenium` | | Forzar navegador en SECOP I |
| `--entrada` | `-i` | CSV de entrada (modo detalle) |
| `--salida` | `-o` | Ruta del archivo de salida |
| `--historica` | | Base histórica incremental |
| `--delay-detalle` | | Segundos entre fichas de detalle (default: 1.5) |
| `--debug` | | Logging nivel DEBUG |

### Variables de entorno

| Variable | Efecto |
|---|---|
| `SECOP_DELAY` | Segundos entre páginas de SECOP I (default 2.5) |
| `SECOP_DEBUG` | `1` para logging DEBUG |
| `SECOP_HEADLESS` | `1` para Chrome sin ventana (ruta Selenium) |
| `CHROME_BINARY` | Ruta al ejecutable de Chrome |
| `SOCRATA_APP_TOKEN` | Evita el throttling de la API |
| `SOCRATA_PAGE_SIZE` | Registros por página de la API (default 20000) |
| `SECOP_CSV` | CSV concreto a abrir en el dashboard |
| `PDF_FONT_DIR` | Carpeta con una TTF Unicode para exportar a PDF |

## Campos extraídos

### SECOP I (`scraper` + `parser`)

| Columna | Descripción |
|---|---|
| `numero_proceso` | Número del proceso |
| `id_proceso` | Identificador interno (`numConstancia`) |
| `entidad` | Entidad compradora |
| `objeto_contrato` | Objeto a contratar |
| `modalidad` | Tipo de proceso |
| `estado` | Estado del proceso |
| `departamento` / `municipio` | Ubicación de ejecución |
| `cuantia` | Valor en COP, ya tipado a `float` |
| `fecha_apertura` | Fecha (`datetime`) |
| `fecha_etiqueta` | Qué fecha es: celebración, apertura, liquidación… |
| `url_detalle` | Ficha del proceso |

### SECOP II (`api_scraper`)

`nombre_entidad`, `nit_entidad`, `departamento`, `ciudad`,
`modalidad_de_contratacion`, `estado_contrato`, `tipo_de_contrato`,
`objeto_del_contrato`, `valor_del_contrato`, `valor_pagado`,
`fecha_de_inicio_del_contrato`, `fecha_de_fin_del_contrato`,
`fecha_de_firma`, `documento_proveedor`, `proveedor_adjudicado`,
`proceso_de_compra`, `id_contrato`, `urlproceso`.

`consulta.normalizar_esquema()` traduce el esquema de SECOP I al de la
API, que es contra el que está escrito el dashboard.

### Ficha de detalle (`--modo detalle`)

Añade `valor_estimado`, `valor_adjudicado`, `valor_contrato`,
`numero_contrato`, `tipo_contrato`, `estado_contrato`, `proveedor`,
`nit_proveedor`, `fecha_cierre` y `fecha_adjudicacion`.

## Manejo de errores

Excepciones tipadas en `exceptions.py`, todas con un dict `context` que
se serializa en los logs:

| Excepción | Cuándo se lanza |
|---|---|
| `SecopBlockedError` | El WAF del portal bloqueó la IP (403) |
| `SecopTimeoutError` | Se agotaron los reintentos de red o de espera |
| `SecopRecaptchaError` | reCAPTCHA **visible** (el v3 invisible no cuenta) |
| `SecopIframeError` | No se localizó el iframe de resultados |
| `SecopEmptyTableError` | La consulta no devolvió registros |
| `SecopFormError` | Error al interactuar con el formulario |
| `SecopParsingError` | Cambió la estructura del HTML |
| `SecopPaginationError` | Error navegando entre páginas |
| `SecopExportError` | Error al guardar el archivo |

## Logging

Los logs van a consola y a `logs/secop_pipeline.log` (rotativo, 5 MB × 5).

```
2026-07-30 10:22 | INFO | scraper  | ejecutar_scraping_http        | [HTTP] 5.065 registros encontrados → 51 páginas (se descargarán 2).
2026-07-30 10:22 | INFO | cleaning | convertir_columnas_monetarias | Columnas monetarias convertidas: ['cuantia']
2026-07-30 10:22 | INFO | cleaning | convertir_columnas_fecha      | Columnas de fecha convertidas: ['fecha_apertura']
```

## Uso como biblioteca

```python
# Barrido por departamento contra ambos portales
from consulta import consultar_en_vivo

for departamento in ["Bogotá D.C.", "Antioquia", "Valle del Cauca"]:
    df, informe = consultar_en_vivo(
        fuentes=("SECOP II", "SECOP I"),
        departamento=departamento,
        modalidad="Mínima cuantía",
        fecha_inicio="01/01/2026",
        fecha_fin="31/03/2026",
        max_paginas_secop1=3,
    )
    print(departamento, len(df), informe["por_fuente"])
    df.to_csv(f"output/{departamento}.csv", index=False, encoding="utf-8-sig")
```

Los nombres se resuelven contra `catalogos.py`, así que `"Bogotá D.C."`
funciona aunque la API lo llame `"Distrito Capital de Bogotá"`.

## El dashboard consulta en vivo

`app.py` **no muestra un archivo viejo**: consulta los portales en el
momento de la búsqueda. Se define el alcance en la barra lateral
(portales, departamento, modalidad, estado, fechas) y se pulsa
**🔎 Buscar en SECOP**.

| | SECOP II (API) | SECOP I (portal) |
|---|---|---|
| Velocidad | segundos | ~4 s por página de 100 procesos |
| Filtros | en el servidor, incluido el texto libre | en local sobre lo descargado |
| Frescura | rezago de publicación de unos días | tiempo real |
| Límite | ninguno relevante | `Páginas de SECOP I` en la barra lateral |

Consultar los dos a la vez da la imagen más completa: SECOP II aporta el
detalle de los contratos ya formalizados y SECOP I los procesos más
recientes. La columna `fuente` indica de dónde viene cada fila.

### Filtros disponibles

Todos son desplegables, no campos de texto: se elige de una lista y la
aplicación envía a cada portal el valor exacto que ese portal espera.
Así no hay forma de fallar por una tilde o una mayúscula.

| Filtro | Opciones | Alcance |
|---|---|---|
| Departamento | los 33 departamentos + **Todo el país** | ambos portales |
| Modalidad | 27, anotadas si solo existen en un portal | ambos |
| Tipo de contrato | 24 (Obra, Consultoría, Interventoría…) | solo SECOP II |
| Estado | 19 | ambos |
| Fechas, palabra clave | libres | ambos |

La consulta **nacional** (sin departamento) da el panorama completo del
país. Como son casi 6 millones de contratos, hay un tope configurable de
descarga; si la consulta lo supera, la aplicación avisa cuántos
coincidían en total para que puedas acotar.

> Los dos portales no nombran igual las mismas cosas: la API llama
> "Distrito Capital de Bogotá" a lo que SECOP I llama "Bogotá D.C.".
> `catalogos.py` guarda esas equivalencias; por eso los filtros son
> desplegables y no texto libre.

**Por qué la consulta va con botón y no automática:** Streamlit reejecuta
el script con cada interacción. Si la descarga colgara del flujo normal,
mover un filtro dispararía una petición al portal y el WAF de
contratos.gov.co bloquearía la IP en minutos. Por eso la descarga solo
ocurre al pulsar el botón, hay una caché de 5 minutos por combinación de
filtros, y los controles de "Refinar resultados" trabajan en local.

El modo **Archivo CSV** sigue disponible en la barra lateral para abrir
descargas previas sin tocar la red.

## Estudio del Sector (Guía V3 de Colombia Compra Eficiente)

La pestaña **📑 Estudio del Sector** genera el documento con la
estructura del apartado 5.2 de la [Guía para la Elaboración de Estudios
del Sector V3 (2025)](https://www.colombiacompra.gov.co/wp-content/uploads/2025/09/Guia-para-la-Elaboracion-de-Estudios-del-Sector-V3.pdf)
de la ANCP–CCE, exportable a **Word** y **PDF**:

| Numeral de la guía | Contenido generado |
|---|---|
| 5.2.1 Aspectos generales | Encabezado y contextos (guiados) |
| 5.2.3 Gasto histórico — **demanda** | Modalidades, tipos de contrato, entidades, comportamiento anual y estacionalidad |
| 5.2.4 Estudio de la **oferta** | Proveedores identificados y concentración del mercado |
| 5.2.5 Estudio de **mercado** | Análisis de precios completo (ver abajo) |
| 5.2.6 **Conclusiones** | Precio de referencia, rango, oferentes, modalidad predominante |
| Anexo | Relación contrato por contrato (proceso, contratista, objeto, valor, plazo, enlace) |

El análisis estadístico sigue el apartado 8 de la guía: tendencia
central, dispersión (incluido el coeficiente de variación), medidas de
posición, **identificación de datos atípicos por rango intercuartílico**
y **estadísticas descriptivas ajustadas**.

Ese último punto no es un adorno. En una prueba real sobre 459 contratos
de obra en Santander:

```
Media sin ajustar : $1.092.848.668
Media ajustada    :   $180.556.942   ← tras excluir 59 atípicos (12,9 %)
Coef. de variación: 396 %
```

Tomar el promedio simple habría inflado el precio de referencia seis
veces. Por eso la guía exige el ajuste y por eso el documento propone
como precio de referencia la media ajustada, dejando constancia del
criterio.

El anexo detalla los 50 contratos de mayor valor: el documento lleva una
ficha por contrato, y sin ese tope una consulta de 20.000 registros
tardaría minutos y pesaría decenas de MB. El CSV de la pestaña de
resultados sí incluye todos.

> Los apartados que dependen del criterio de la entidad —contexto
> técnico y regulatorio, presupuesto oficial, requisitos habilitantes,
> riesgos— se emiten señalados como *«Por completar por la Entidad
> Estatal»*. No se inventan.

## Despliegue del dashboard

> **Vercel no sirve para esta aplicación.** Streamlit necesita un proceso
> servidor de larga vida que mantiene una conexión WebSocket con cada
> navegador; Vercel ejecuta funciones serverless de vida corta y sin
> WebSockets persistentes. No existe una forma soportada de alojar
> Streamlit ahí. Usa cualquiera de las opciones de abajo.

### Opción A — Streamlit Community Cloud (recomendada, gratis)

1. Sube el repositorio a GitHub.
2. Entra en [share.streamlit.io](https://share.streamlit.io) → *New app*.
3. Selecciona el repo, la rama y `app.py` como archivo principal.
4. *Deploy*.

El repositorio ya trae lo que necesita esa plataforma:

| Archivo | Para qué |
|---|---|
| `requirements.txt` | dependencias de Python |
| `packages.txt` | `fonts-dejavu-core`, necesario para exportar a PDF |
| `.streamlit/config.toml` | tema oscuro y ajustes del servidor |

Como `output/` está en `.gitignore`, la instancia arranca sin datos y
muestra una pestaña **"Descargar de SECOP"** para traerlos desde la API
en el momento. También puedes subir un CSV a mano.

### Opción B — Contenedor (Render, Railway, Fly.io, HF Spaces, Cloud Run)

```bash
docker build -t adel-sector .
docker run -p 8501:8501 adel-sector
```

La imagen instala `fonts-dejavu-core` y respeta la variable `PORT` que
inyectan Render y Railway. En esos servicios basta con apuntar al
`Dockerfile`; no hace falta configurar el comando de arranque.

### Variables de entorno del dashboard

| Variable | Efecto |
|---|---|
| `SECOP_CSV` | Ruta a un CSV concreto en vez de autodetectar el más reciente de `output/` |
| `PDF_FONT_DIR` | Carpeta con `DejaVuSans.ttf` si el sistema no trae ninguna fuente TrueType |
| `SOCRATA_APP_TOKEN` | Evita el *throttling* de la API al descargar desde la app |

### Persistencia de los datos

El sistema de archivos de Streamlit Cloud y de la mayoría de PaaS es
**efímero**: los CSV descargados desde la app se pierden al reiniciar el
contenedor. Para un panel que deba mantenerse actualizado sin
intervención, las opciones son:

- Programar `python main.py --fuente api ...` en una máquina propia y
  publicar el CSV en un almacenamiento persistente (S3, un volumen), y
  apuntar `SECOP_CSV` ahí.
- O versionar un CSV base en el repositorio y usar el botón
  *"Actualizar desde SECOP"* de la barra lateral cuando haga falta.

## Verificación de las fuentes

Los portales cambian sin avisar. El riesgo real no es que el scraper
falle con estrépito —eso se nota— sino que siga corriendo y devuelva
datos vacíos o sin filtrar. Para detectarlo:

```bash
python verificar_fuentes.py            # informe completo
python verificar_fuentes.py --rapido   # solo la API (no toca SECOP I)
```

Comprueba que el formulario conserve sus campos, que los filtros se
apliquen de verdad, que los códigos de modalidad y departamento sigan
alineados con los del portal, y cuánto tiempo hace que se actualizó cada
fuente. Devuelve código de salida 1 si algo se rompió, así que se puede
programar en cron o en GitHub Actions.

## Licencia

MIT