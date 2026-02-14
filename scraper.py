"""
scraper.py — Automatización Selenium del portal SECOP I (contratos.gov.co).

Responsabilidades:
  1. Inicializar y configurar el WebDriver de Chrome.
  2. Navegar al formulario de consulta y rellenar campos dinámicos.
  3. Manejar la transición al iframe de resultados.
  4. Detectar y manejar reCAPTCHA.
  5. Iterar sobre todas las páginas de resultados (paginación automática).
  6. Recopilar el HTML crudo de cada página para entregarlo al parser.

Principios de diseño:
  • Cada función tiene una sola responsabilidad.
  • Las esperas usan WebDriverWait explícito — nunca ``time.sleep`` fijo.
  • Los errores se convierten en excepciones tipadas (ver ``exceptions.py``).
  • El módulo NO interpreta el HTML; eso le corresponde a ``parser.py``.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from config import (
    CHROME_ARGUMENTS,
    CHROME_HEADLESS,
    CHROME_PREFS,
    DEFAULT_TIMEOUT,
    IFRAME_NAME,
    IFRAME_XPATH,
    MAX_RETRIES,
    PAGE_LOAD_WAIT,
    RECAPTCHA_WAIT,
    RETRY_BACKOFF,
    SECOP_CONSULTA_URL,
    SEL_BTN_BUSCAR,
    SEL_DEPARTAMENTO,
    SEL_ENTIDAD,
    SEL_ESTADO,
    SEL_FAMILIA,
    SEL_FECHA_FIN,
    SEL_FECHA_INICIO,
    SEL_KEYWORD_INPUT,
    SEL_LINK_DETALLE,
    SEL_MODALIDAD,
    SEL_MUNICIPIO,
    SEL_NUMERO_PROCESO,
    SEL_OBJETO,
    SEL_PAGINA_SIGUIENTE,
    SEL_TABLA_RESULTADOS,
    SEL_TABLA_RESULTADOS_FALLBACK,
    SEL_TOTAL_REGISTROS,
    SearchParams,
)
from exceptions import (
    SecopEmptyTableError,
    SecopFormError,
    SecopIframeError,
    SecopPaginationError,
    SecopRecaptchaError,
    SecopTimeoutError,
)

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# 1. INICIALIZACIÓN DEL DRIVER
# ════════════════════════════════════════════════════════════


def crear_driver() -> WebDriver:
    """Crea y retorna una instancia configurada de Chrome WebDriver.

    Aplica las opciones definidas en ``config.py``, incluyendo el modo
    headless si la variable de entorno ``SECOP_HEADLESS=1`` está activa.

    Returns:
        Instancia activa de ``webdriver.Chrome``.

    Raises:
        WebDriverException: Si Chrome o ChromeDriver no están disponibles.
    """
    options = ChromeOptions()

    for arg in CHROME_ARGUMENTS:
        options.add_argument(arg)

    if CHROME_HEADLESS:
        options.add_argument("--headless=new")
        logger.info("Modo headless activado.")

    options.add_experimental_option("prefs", CHROME_PREFS)

    # Ocultar indicadores de automatización
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Inyectar JavaScript para camuflar navigator.webdriver
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )

    logger.info("WebDriver de Chrome inicializado correctamente.")
    return driver


def cerrar_driver(driver: WebDriver) -> None:
    """Cierra el WebDriver de forma segura.

    Intenta ``quit()`` y captura cualquier excepción para que el
    pipeline nunca falle en el bloque ``finally``.
    """
    try:
        driver.quit()
        logger.info("WebDriver cerrado correctamente.")
    except WebDriverException as exc:
        logger.warning("Error al cerrar el WebDriver: %s", exc)


# ════════════════════════════════════════════════════════════
# 2. DETECCIÓN DE reCAPTCHA
# ════════════════════════════════════════════════════════════


def _detectar_recaptcha(driver: WebDriver) -> bool:
    """Detecta si el portal muestra un desafío reCAPTCHA.

    Busca indicadores comunes: iframes de Google reCAPTCHA,
    elementos con class ``g-recaptcha``, o texto indicativo.

    Returns:
        ``True`` si se detecta un CAPTCHA visible.
    """
    indicadores = [
        "//iframe[contains(@src, 'recaptcha')]",
        "//*[contains(@class, 'g-recaptcha')]",
        "//*[contains(@class, 'captcha')]",
        "//*[contains(text(), 'No soy un robot')]",
    ]
    for xpath in indicadores:
        try:
            driver.find_element(By.XPATH, xpath)
            return True
        except NoSuchElementException:
            continue
    return False


def manejar_recaptcha(driver: WebDriver, timeout: int = RECAPTCHA_WAIT) -> None:
    """Pausa la ejecución para resolución manual de reCAPTCHA.

    Si se detecta un CAPTCHA, lanza ``SecopRecaptchaError`` después del
    timeout si no fue resuelto.  En un entorno con monitor, el operador
    puede resolverlo manualmente dentro de la ventana de tiempo.

    Args:
        driver:  WebDriver activo.
        timeout: Segundos máximos de espera para resolución manual.

    Raises:
        SecopRecaptchaError: Si el CAPTCHA persiste tras el timeout.
    """
    if not _detectar_recaptcha(driver):
        return

    logger.warning(
        "⚠️  reCAPTCHA detectado. Esperando resolución manual (%d s)...", timeout
    )

    inicio = time.monotonic()
    while time.monotonic() - inicio < timeout:
        time.sleep(2)
        if not _detectar_recaptcha(driver):
            logger.info("reCAPTCHA resuelto.")
            return

    raise SecopRecaptchaError(
        f"reCAPTCHA no resuelto tras {timeout} segundos.",
        context={"url": driver.current_url},
    )


# ════════════════════════════════════════════════════════════
# 3. HELPERS DE FORMULARIO
# ════════════════════════════════════════════════════════════


def _esperar_elemento(
    driver: WebDriver,
    by: By,
    value: str,
    timeout: int = DEFAULT_TIMEOUT,
    clickable: bool = False,
) -> WebElement:
    """Espera a que un elemento esté presente o clickable.

    Args:
        driver:    WebDriver activo.
        by:        Estrategia de localización (``By.CSS_SELECTOR``, etc.).
        value:     Selector / XPath / nombre del elemento.
        timeout:   Segundos máximos de espera.
        clickable: Si ``True``, espera ``element_to_be_clickable``.

    Returns:
        El WebElement localizado.

    Raises:
        SecopTimeoutError: Si el elemento no aparece en el plazo.
    """
    condition = (
        EC.element_to_be_clickable((by, value))
        if clickable
        else EC.presence_of_element_located((by, value))
    )
    try:
        return WebDriverWait(driver, timeout).until(condition)
    except TimeoutException as exc:
        raise SecopTimeoutError(
            f"Timeout esperando elemento: {value}",
            context={"by": str(by), "selector": value, "timeout": timeout},
        ) from exc


def _rellenar_campo(driver: WebDriver, css: str, valor: Optional[str]) -> None:
    """Rellena un campo de texto si el valor no es None.

    Limpia el campo antes de escribir para evitar texto residual.
    """
    if valor is None:
        return

    try:
        elemento = _esperar_elemento(driver, By.CSS_SELECTOR, css)
        elemento.clear()
        elemento.send_keys(valor)
        logger.debug("Campo '%s' → '%s'", css, valor)
    except SecopTimeoutError:
        logger.warning("Campo '%s' no encontrado, se omite.", css)
    except WebDriverException as exc:
        raise SecopFormError(
            f"Error escribiendo en campo '{css}'",
            context={"selector": css, "valor": valor, "error": str(exc)},
        ) from exc


def _seleccionar_dropdown(
    driver: WebDriver, css: str, texto_visible: Optional[str]
) -> None:
    """Selecciona una opción de un ``<select>`` por su texto visible.

    Si el texto no coincide exactamente, intenta coincidencia parcial
    (``contains``).  Si el dropdown no existe o la opción no se encuentra,
    se registra una advertencia y se sigue — no se aborta el pipeline.

    Args:
        driver:         WebDriver activo.
        css:            Selector CSS del elemento ``<select>``.
        texto_visible:  Texto de la opción a seleccionar (``None`` = omitir).
    """
    if texto_visible is None:
        return

    try:
        elemento = _esperar_elemento(driver, By.CSS_SELECTOR, css)
        select = Select(elemento)

        # Intento 1: coincidencia exacta
        try:
            select.select_by_visible_text(texto_visible)
            logger.debug("Dropdown '%s' → '%s' (exacto)", css, texto_visible)
            return
        except NoSuchElementException:
            pass

        # Intento 2: coincidencia parcial (case-insensitive)
        texto_lower = texto_visible.lower()
        for opcion in select.options:
            if texto_lower in opcion.text.lower():
                select.select_by_visible_text(opcion.text)
                logger.debug(
                    "Dropdown '%s' → '%s' (parcial de '%s')",
                    css, opcion.text, texto_visible,
                )
                return

        logger.warning(
            "Opción '%s' no encontrada en dropdown '%s'. Opciones disponibles: %s",
            texto_visible,
            css,
            [o.text for o in select.options[:10]],
        )

    except SecopTimeoutError:
        logger.warning("Dropdown '%s' no encontrado, se omite.", css)
    except WebDriverException as exc:
        raise SecopFormError(
            f"Error seleccionando en dropdown '{css}'",
            context={"selector": css, "texto": texto_visible, "error": str(exc)},
        ) from exc


def _seleccionar_dropdown_por_valor(
    driver: WebDriver, css: str, valor: Optional[str]
) -> None:
    """Selecciona una opción de un ``<select>`` por su atributo ``value``.

    Más confiable que ``select_by_visible_text`` cuando se conocen los
    valores de los ``<option>`` del DOM.

    Args:
        driver: WebDriver activo.
        css:    Selector CSS del elemento ``<select>``.
        valor:  Atributo ``value`` de la opción a seleccionar (``None`` = omitir).
    """
    if valor is None:
        return

    try:
        elemento = _esperar_elemento(driver, By.CSS_SELECTOR, css)
        select = Select(elemento)
        select.select_by_value(valor)
        logger.debug("Dropdown '%s' → value='%s'", css, valor)
    except NoSuchElementException:
        logger.warning(
            "Value '%s' no encontrado en dropdown '%s'. Opciones: %s",
            valor, css,
            [(o.get_attribute("value"), o.text.strip()) for o in Select(
                driver.find_element(By.CSS_SELECTOR, css)
            ).options[:10]],
        )
    except SecopTimeoutError:
        logger.warning("Dropdown '%s' no encontrado, se omite.", css)
    except WebDriverException as exc:
        raise SecopFormError(
            f"Error seleccionando valor en dropdown '{css}'",
            context={"selector": css, "valor": valor, "error": str(exc)},
        ) from exc


def _esperar_opciones_estado(
    driver: WebDriver, css: str, timeout: int = DEFAULT_TIMEOUT
) -> bool:
    """Espera a que el dropdown de Estado cargue opciones dinámicamente.

    El portal SECOP I carga las opciones del select de Estado mediante
    AJAX después de interactuar con otros campos.  Esta función espera
    hasta que haya más de 1 opción (descartando el placeholder).

    Args:
        driver:  WebDriver activo.
        css:     Selector CSS del ``<select>``.
        timeout: Segundos máximos de espera.

    Returns:
        ``True`` si se cargaron opciones; ``False`` si expiró el timeout.
    """
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(
                Select(d.find_element(By.CSS_SELECTOR, css)).options
            ) > 1
        )
        logger.debug("Opciones de '%s' cargadas dinámicamente.", css)
        return True
    except (TimeoutException, NoSuchElementException):
        logger.warning(
            "Timeout esperando opciones dinámicas en '%s' (%d s).", css, timeout
        )
        return False


# ════════════════════════════════════════════════════════════
# 4. RELLENAR FORMULARIO COMPLETO
# ════════════════════════════════════════════════════════════


def rellenar_formulario(driver: WebDriver, params: SearchParams) -> None:
    """Rellena todos los campos del formulario de búsqueda de SECOP I.

    Navega a la URL de consulta, espera la carga del formulario y
    rellena únicamente los campos cuyos parámetros no sean ``None``.

    Los dropdowns de Modalidad y Departamento se seleccionan por **value**
    (atributo ``value`` del ``<option>``), lo cual es más fiable que por
    texto visible.  El dropdown de Estado se carga dinámicamente por
    AJAX, por lo que se espera a que aparezcan opciones antes de seleccionar.

    Args:
        driver: WebDriver activo.
        params: Instancia de ``SearchParams`` con los filtros deseados.

    Raises:
        SecopFormError:      Si un campo crítico falla.
        SecopRecaptchaError: Si se detecta CAPTCHA al cargar el formulario.
    """
    logger.info("Navegando a %s", SECOP_CONSULTA_URL)
    driver.get(SECOP_CONSULTA_URL)

    # Esperar a que el formulario esté disponible
    _esperar_elemento(driver, By.CSS_SELECTOR, SEL_BTN_BUSCAR, clickable=True)
    logger.info("Formulario de búsqueda cargado.")

    # Verificar CAPTCHA antes de interactuar
    manejar_recaptcha(driver)

    # --- Campos de texto ---
    _rellenar_campo(driver, SEL_KEYWORD_INPUT, params.palabra_clave)
    _rellenar_campo(driver, SEL_NUMERO_PROCESO, params.numero_proceso)
    _rellenar_campo(driver, SEL_ENTIDAD, params.entidad)

    # --- Campos de fecha ---
    _rellenar_campo(driver, SEL_FECHA_INICIO, params.fecha_inicio)
    _rellenar_campo(driver, SEL_FECHA_FIN, params.fecha_fin)

    # --- Producto o Servicio (select#objeto) — por value ---
    _seleccionar_dropdown_por_valor(driver, SEL_OBJETO, params.objeto)

    # --- Modalidad de Contratación (select#tipoProceso) — por value ---
    _seleccionar_dropdown_por_valor(driver, SEL_MODALIDAD, params.modalidad)

    # --- Departamento (select#selDepartamento) — por value ---
    _seleccionar_dropdown_por_valor(driver, SEL_DEPARTAMENTO, params.departamento)

    # Si se selecciona departamento, puede que se cargue municipio dinámicamente
    if params.departamento and params.municipio:
        time.sleep(2.0)  # Espera para carga AJAX del municipio
        _seleccionar_dropdown(driver, SEL_MUNICIPIO, params.municipio)

    # --- Estado (carga dinámica por AJAX) ---
    if params.estado:
        # El dropdown de estado carga sus opciones dinámicamente.
        # Esperamos a que tenga más de 1 opción (el placeholder "Seleccione Estado").
        _esperar_opciones_estado(driver, SEL_ESTADO, timeout=10)
        _seleccionar_dropdown(driver, SEL_ESTADO, params.estado)

    # --- Familia UNSPSC ---
    _seleccionar_dropdown(driver, SEL_FAMILIA, params.familia)

    logger.info(
        "Formulario rellenado: palabra_clave=%r, fechas=%s→%s, "
        "objeto=%r, modalidad=%r, departamento=%r, estado=%r",
        params.palabra_clave,
        params.fecha_inicio,
        params.fecha_fin,
        params.objeto,
        params.modalidad,
        params.departamento,
        params.estado,
    )


# ════════════════════════════════════════════════════════════
# 5. ENVIAR FORMULARIO Y ACCEDER AL IFRAME
# ════════════════════════════════════════════════════════════


def enviar_formulario(driver: WebDriver) -> None:
    """Hace clic en el botón *Buscar* y valida la respuesta.

    Post-condiciones comprobadas:
      • No aparece reCAPTCHA.
      • La página no muestra mensaje de "sin resultados".

    Raises:
        SecopFormError:      Si el botón no existe o el clic falla.
        SecopRecaptchaError: Si aparece CAPTCHA tras el envío.
    """
    try:
        boton = _esperar_elemento(
            driver, By.CSS_SELECTOR, SEL_BTN_BUSCAR, clickable=True
        )
        boton.click()
        logger.info("Formulario enviado (botón Buscar).")
    except SecopTimeoutError as exc:
        raise SecopFormError(
            "No se encontró el botón Buscar.",
            context={"selector": SEL_BTN_BUSCAR},
        ) from exc

    # Pausa para que el servidor procese la consulta
    time.sleep(PAGE_LOAD_WAIT)

    # Verificar CAPTCHA post-envío
    manejar_recaptcha(driver)


def cambiar_a_iframe(driver: WebDriver) -> None:
    """Cambia el contexto del driver al iframe que contiene los resultados.

    Intenta primero por nombre del iframe (``IFRAME_NAME``).  Si falla,
    busca por XPath como fallback.

    Raises:
        SecopIframeError: Si ningún método logra acceder al iframe.
    """
    wait = WebDriverWait(driver, DEFAULT_TIMEOUT)

    # Asegurarse de estar en el contexto principal
    driver.switch_to.default_content()

    # Intento 1: por nombre
    try:
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, IFRAME_NAME)))
        logger.info("Cambio a iframe '%s' exitoso (por nombre).", IFRAME_NAME)
        return
    except TimeoutException:
        logger.debug("Iframe por nombre '%s' no encontrado, intentando XPath.", IFRAME_NAME)

    # Intento 2: por XPath
    try:
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, IFRAME_XPATH)))
        logger.info("Cambio a iframe exitoso (por XPath).")
        return
    except TimeoutException:
        pass

    # Intento 3: primer iframe disponible
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            driver.switch_to.frame(iframes[0])
            logger.warning(
                "Cambio a primer iframe genérico (total iframes: %d).", len(iframes)
            )
            return
    except WebDriverException:
        pass

    raise SecopIframeError(
        "No se pudo acceder a ningún iframe de resultados.",
        context={"url": driver.current_url, "iframe_name": IFRAME_NAME},
    )


# ════════════════════════════════════════════════════════════
# 6. VERIFICAR QUE EXISTAN RESULTADOS
# ════════════════════════════════════════════════════════════


def verificar_resultados(driver: WebDriver) -> int:
    """Verifica que la página de resultados contenga registros.

    Intenta leer el indicador de "Total registros".  Si no lo encuentra,
    comprueba la existencia de la tabla de resultados.

    Returns:
        Número total de registros reportados por el portal (0 si no
        se puede determinar).

    Raises:
        SecopEmptyTableError: Si la página indica explícitamente 0 resultados.
    """
    # Intentar leer total de registros
    try:
        elem_total = driver.find_element(By.CSS_SELECTOR, SEL_TOTAL_REGISTROS)
        texto = elem_total.text.strip().replace(".", "").replace(",", "")
        total = int(texto) if texto.isdigit() else 0
        if total == 0:
            raise SecopEmptyTableError(
                "La consulta retornó 0 registros.",
                context={"url": driver.current_url},
            )
        logger.info("Total de registros reportados: %d", total)
        return total
    except NoSuchElementException:
        logger.debug("Elemento '%s' no encontrado.", SEL_TOTAL_REGISTROS)

    # Fallback: verificar existencia de tabla
    try:
        driver.find_element(By.CSS_SELECTOR, SEL_TABLA_RESULTADOS)
        logger.info("Tabla de resultados encontrada (total desconocido).")
        return 0
    except NoSuchElementException:
        pass

    try:
        driver.find_element(By.CSS_SELECTOR, SEL_TABLA_RESULTADOS_FALLBACK)
        logger.info("Tabla genérica encontrada (total desconocido).")
        return 0
    except NoSuchElementException:
        pass

    # Comprobar mensajes de "sin resultados"
    page_text = driver.page_source.lower()
    mensajes_vacios = [
        "no se encontraron resultados",
        "sin resultados",
        "no records found",
        "0 registro",
    ]
    for msg in mensajes_vacios:
        if msg in page_text:
            raise SecopEmptyTableError(
                f"Sin resultados: '{msg}' detectado en la página.",
                context={"url": driver.current_url},
            )

    logger.warning("No se pudo determinar si hay resultados; continuando.")
    return 0


# ════════════════════════════════════════════════════════════
# 7. PAGINACIÓN AUTOMÁTICA
# ════════════════════════════════════════════════════════════


def _hay_pagina_siguiente(driver: WebDriver) -> bool:
    """Retorna True si existe un enlace de 'página siguiente'."""
    try:
        link = driver.find_element(By.CSS_SELECTOR, SEL_PAGINA_SIGUIENTE)
        return link.is_displayed()
    except NoSuchElementException:
        return False


def _ir_pagina_siguiente(driver: WebDriver) -> bool:
    """Hace clic en el enlace de página siguiente.

    Returns:
        ``True`` si el clic fue exitoso; ``False`` si no hay más páginas.
    """
    try:
        link = driver.find_element(By.CSS_SELECTOR, SEL_PAGINA_SIGUIENTE)
        driver.execute_script("arguments[0].click();", link)
        time.sleep(PAGE_LOAD_WAIT)
        return True
    except (NoSuchElementException, StaleElementReferenceException):
        return False
    except WebDriverException as exc:
        logger.warning("Error al navegar a página siguiente: %s", exc)
        return False


def recopilar_html_paginas(
    driver: WebDriver, max_pages: int
) -> list[str]:
    """Itera sobre todas las páginas de resultados y recopila el HTML de cada una.

    La lógica de paginación es resiliente:
      • Si el enlace "siguiente" desaparece, se detiene.
      • Si una página no carga, reintenta con backoff.
      • Respeta ``max_pages`` como límite de seguridad.

    Args:
        driver:    WebDriver posicionado en el iframe de resultados.
        max_pages: Número máximo de páginas a recorrer.

    Returns:
        Lista de strings HTML, uno por cada página de resultados.
    """
    paginas_html: list[str] = []
    pagina_actual = 1

    while pagina_actual <= max_pages:
        logger.info("Recopilando página %d...", pagina_actual)

        # Capturar HTML de la página actual
        html = driver.page_source
        paginas_html.append(html)

        # ¿Hay siguiente página?
        if not _hay_pagina_siguiente(driver):
            logger.info("Última página alcanzada (%d).", pagina_actual)
            break

        # Navegar a la siguiente
        exito = False
        for intento in range(1, MAX_RETRIES + 1):
            if _ir_pagina_siguiente(driver):
                exito = True
                break
            espera = RETRY_BACKOFF ** intento
            logger.warning(
                "Reintento %d/%d de paginación (espera %.1f s).",
                intento, MAX_RETRIES, espera,
            )
            time.sleep(espera)

        if not exito:
            logger.error(
                "No se pudo avanzar a la página %d tras %d reintentos.",
                pagina_actual + 1, MAX_RETRIES,
            )
            break

        pagina_actual += 1

        # Verificar CAPTCHA entre páginas
        try:
            manejar_recaptcha(driver)
        except SecopRecaptchaError:
            logger.error("reCAPTCHA bloqueó la paginación en página %d.", pagina_actual)
            break

    logger.info("Total de páginas recopiladas: %d", len(paginas_html))
    return paginas_html


# ════════════════════════════════════════════════════════════
# 8. EXTRAER URLs DE DETALLE DE PROCESOS
# ════════════════════════════════════════════════════════════


def extraer_urls_detalle(driver: WebDriver) -> list[str]:
    """Extrae todas las URLs de detalle de proceso de la página actual.

    Estas URLs se usan en ``detail_scraper.py`` para ingresar a cada
    proceso individual y extraer datos completos.

    Returns:
        Lista de URLs absolutas de detalle.
    """
    urls: list[str] = []
    try:
        links = driver.find_elements(By.CSS_SELECTOR, SEL_LINK_DETALLE)
        for link in links:
            href = link.get_attribute("href")
            if href:
                urls.append(href)
    except WebDriverException as exc:
        logger.warning("Error extrayendo URLs de detalle: %s", exc)

    logger.debug("URLs de detalle encontradas en página actual: %d", len(urls))
    return urls


# ════════════════════════════════════════════════════════════
# 9. PIPELINE DE SCRAPING COMPLETO
# ════════════════════════════════════════════════════════════


def ejecutar_scraping(
    params: SearchParams,
    driver: Optional[WebDriver] = None,
    cerrar_al_final: bool = True,
) -> tuple[list[str], list[str]]:
    """Ejecuta el pipeline completo de scraping para una búsqueda dada.

    Flujo:
      1. Crear driver (si no se provee uno).
      2. Rellenar y enviar formulario.
      3. Cambiar a iframe de resultados.
      4. Verificar que hay registros.
      5. Recopilar HTML de todas las páginas.
      6. Recopilar URLs de detalle de cada página.
      7. Cerrar driver (si ``cerrar_al_final`` es True).

    Args:
        params:          Filtros de búsqueda.
        driver:          WebDriver existente (opcional, para reutilización).
        cerrar_al_final: Si cerrar el driver al terminar.

    Returns:
        Tupla con:
          - Lista de HTML de cada página de resultados.
          - Lista consolidada de URLs de detalle.

    Raises:
        SecopTimeoutError:      Si la página no carga.
        SecopRecaptchaError:    Si aparece CAPTCHA irresolvible.
        SecopIframeError:       Si no se accede al iframe.
        SecopEmptyTableError:   Si no hay resultados.
        SecopFormError:         Si falla la interacción con el formulario.
    """
    driver_propio = driver is None
    if driver_propio:
        driver = crear_driver()

    todas_html: list[str] = []
    todas_urls: list[str] = []

    try:
        # Paso 1: Rellenar formulario
        rellenar_formulario(driver, params)

        # Paso 2: Enviar formulario
        enviar_formulario(driver)

        # Paso 3: Cambiar a iframe
        cambiar_a_iframe(driver)

        # Paso 4: Verificar resultados
        total = verificar_resultados(driver)
        logger.info("Registros estimados: %d", total)

        # Paso 5: Recopilar HTML de todas las páginas
        todas_html = recopilar_html_paginas(driver, params.max_pages)

        # Paso 6: Recopilar URLs de detalle (de la última página visible)
        # Para obtener de todas las páginas, el parser las extrae del HTML
        todas_urls = extraer_urls_detalle(driver)

        return todas_html, todas_urls

    finally:
        if driver_propio and cerrar_al_final:
            cerrar_driver(driver)
