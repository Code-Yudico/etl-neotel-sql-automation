# -*- coding: utf-8 -*-
"""
==============================================================================
ETL NEOTEL - Call Center Reports (VERSIÓN OPTIMIZADA)
==============================================================================

Dependencias adicionales:
    pip install python-dotenv tenacity

Archivo .env requerido:
    NEOTEL_USER=xxxx
    NEOTEL_PASS=xxxx
    SQL_SERVER=xxxx
    SQL_DATABASE=xxxx
    SQL_USER=xxxx
    SQL_PASSWORD=xxxx
==============================================================================
"""

import os
import sys
import time
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    StaleElementReferenceException,
    WebDriverException
)

# Importar dotenv para variables de entorno
from dotenv import load_dotenv

# Importar tenacity para reintentos automáticos
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# =============================================================================
# CARGA DE VARIABLES DE ENTORNO
# =============================================================================

# Cargar variables desde archivo .env en el mismo directorio del script
load_dotenv(Path(__file__).parent / '.env')

def obtener_variable_entorno(nombre: str, obligatoria: bool = True) -> str:
    """
    Obtiene una variable de entorno de forma segura.
    
    Args:
        nombre: Nombre de la variable
        obligatoria: Si es True, lanza error si no existe
        
    Returns:
        Valor de la variable o string vacío si no es obligatoria
    """
    valor = os.getenv(nombre)
    if obligatoria and not valor:
        raise EnvironmentError(
            f"Variable de entorno '{nombre}' no encontrada. "
            f"Asegúrate de crear el archivo .env con las credenciales."
        )
    return valor or ""


# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Credenciales desde variables de entorno
NEOTEL_URL = "https://web.neoteluin.mx/callcenter/default.htm"
NEOTEL_USER = obtener_variable_entorno('NEOTEL_USER')
NEOTEL_PASS = obtener_variable_entorno('NEOTEL_PASS')

SQL_SERVER = obtener_variable_entorno('SQL_SERVER')
SQL_DATABASE = obtener_variable_entorno('SQL_DATABASE')
SQL_USER = obtener_variable_entorno('SQL_USER')
SQL_PASSWORD = obtener_variable_entorno('SQL_PASSWORD')
SQL_DRIVER = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')

# Directorio de descargas temporales
DOWNLOAD_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "temp_neotel_downloads"

# Reportes a descargar
REPORTES = [
    {"id": "23", "nombre": "Conducta - Agentes"},
    {"id": "24", "nombre": "Estados operativos - Agentes"}
]

# Whitelist de tablas permitidas para prevenir SQL injection
TABLAS_PERMITIDAS = {'tbl_neotel_conducta', 'tbl_neotel_estados_operativos'}

# Timeouts
WAIT_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 20              #Valor orignal 50, decrecido para pruebas 
DOWNLOAD_CHECK_INTERVAL_MIN = 1    #  Intervalo mínimo (segundos)
DOWNLOAD_CHECK_INTERVAL_MAX = 6    #  Intervalo máximo (segundos)

# MAPEOS DE COLUMNAS COMO CONSTANTES GLOBALES

MAPEO_CONDUCTA = {
    'agente': 'agente',
    'fecha': 'fecha',
    'id': 'id',
    'campana': 'campana',
    'in': 'in_total',  # 'in' es palabra reservada SQL
    'pct_in': 'pct_in',
    'in_rechazadas_ignoradas': 'in_rechazadas_ignoradas',
    'pct_in_rechazadas_ignoradas': 'pct_in_rechazadas_ignoradas',
    'in_atendidas': 'in_atendidas',
    'pct_in_atendidas': 'pct_in_atendidas',
    'out': 'out_total',  # 'out' es palabra reservada SQL
    'pct_out': 'pct_out',
    'out_rechazadas_ignoradas': 'out_rechazadas_ignoradas',
    'pct_out_rechazadas_ignoradas': 'pct_out_rechazadas_ignoradas',
    'out_atendidas': 'out_atendidas',
    'pct_out_atendidas': 'pct_out_atendidas',
    'out_dialing': 'out_dialing',
    'pct_out_dialing': 'pct_out_dialing',
    'llamados_con_hold': 'llamados_con_hold',
    'pct_llamados_con_hold': 'pct_llamados_con_hold',
    'tiempo_medio_de_respuesta_in': 'tiempo_medio_respuesta_in',
    'tiempo_medio_de_respuesta_out': 'tiempo_medio_respuesta_out'
}

COLUMNAS_FINALES_CONDUCTA = [
    'agente', 'fecha', 'id', 'campana', 
    'in_total', 'pct_in', 
    'in_rechazadas_ignoradas', 'pct_in_rechazadas_ignoradas',
    'in_atendidas', 'pct_in_atendidas',
    'out_total', 'pct_out',
    'out_rechazadas_ignoradas', 'pct_out_rechazadas_ignoradas',
    'out_atendidas', 'pct_out_atendidas',
    'out_dialing', 'pct_out_dialing',
    'llamados_con_hold', 'pct_llamados_con_hold',
    'tiempo_medio_respuesta_in', 'tiempo_medio_respuesta_out'
]

MAPEO_ESTADOS = {
    'fecha': 'fecha',
    'intervalo': 'intervalo',
    'id': 'id',
    'agente': 'agente',
    'id_campana': 'id_campana',
    'campana': 'campana',
    't_login': 't_login',
    't_login_neto': 't_login_neto',
    't_available': 't_available',
    't_preview': 't_preview',
    't_dialing': 't_dialing',
    't_ringing': 't_ringing',
    't_talking': 't_talking',
    't_talking_in': 't_talking_in',
    't_talking_out': 't_talking_out',
    't_hold': 't_hold',
    't_acw': 't_acw',
    't_other_crm': 't_other_crm',
    't_pause': 't_pause',
    't_diario_login': 't_diario_login',
    't_diario_login_neto': 't_diario_login_neto',
    't_diario_available': 't_diario_available',
    't_diario_preview': 't_diario_preview',
    't_diario_dialing': 't_diario_dialing',
    't_diario_ringing': 't_diario_ringing',
    't_diario_talking': 't_diario_talking',
    't_diario_talking_in': 't_diario_talking_in',
    't_diario_talking_out': 't_diario_talking_out',
    't_diario_hold': 't_diario_hold',
    't_diario_acw': 't_diario_acw',
    't_diario_other_crm': 't_diario_other_crm',
    't_diario_pause': 't_diario_pause'
}

COLUMNAS_FINALES_ESTADOS = list(MAPEO_ESTADOS.values())

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl_neotel.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def calcular_fecha_ayer() -> str:
    """
    Returns:
        str: Fecha de ayer formateada DD/MM/YYYY
    """
    ayer = datetime.now() - timedelta(days=1)  
    fecha_formateada = ayer.strftime("%d/%m/%Y")
    logger.info(f"Fecha calculada (ayer): {fecha_formateada}")
    return fecha_formateada


def limpiar_directorio_descargas(directorio: Path) -> None:
    """
    Limpia el directorio de descargas eliminando todos los archivos.
    Crea el directorio si no existe.
    
    Args:
        directorio: Path del directorio a limpiar
    """
    if directorio.exists():
        logger.info(f"Limpiando directorio de descargas: {directorio}")
        for archivo in directorio.iterdir():
            try:
                if archivo.is_file():
                    archivo.unlink()
                elif archivo.is_dir():
                    shutil.rmtree(archivo)
            except Exception as e:
                logger.warning(f"No se pudo eliminar {archivo}: {e}")
    else:
        logger.info(f"Creando directorio de descargas: {directorio}")
        directorio.mkdir(parents=True, exist_ok=True)


def configurar_chrome_options(download_dir: Path) -> Options:
    """
    Configura las opciones de Chrome para automatización.
    
    Activado modo headless para ejecución sin interfaz gráfica.
    Esto permite ejecutar el script con la sesión bloqueada.
    
    Args:
        download_dir: Directorio para descargas automáticas
        
    Returns:
        Options: Configuración de Chrome
    """
    chrome_options = Options()
    
    # Preferencias para descargas automáticas sin diálogo
    prefs = {
        "download.default_directory": str(download_dir.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # HEADLESS ACTIVADO Permite ejecución sin interfaz gráfica (sesión bloqueada, servidor, etc.)
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-software-rasterizer")
    
    # Opciones de estabilidad
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Evitar detección de automatización
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    logger.info("Chrome configurado en modo HEADLESS para automatización")
    return chrome_options


def obtener_archivos_en_directorio(directorio: Path) -> set:
    """
    Obtiene el conjunto de nombres de archivos en un directorio.
    
    Args:
        directorio: Path del directorio
        
    Returns:
        set: Conjunto de nombres de archivo
    """
    if not directorio.exists():
        return set()
    return {f.name for f in directorio.iterdir() if f.is_file()}


def esperar_descarga_completa(download_dir: Path, archivos_previos: set, 
                               timeout: int = DOWNLOAD_TIMEOUT) -> str:
    """
    Espera hasta que se complete una descarga en el directorio.
    
    Implementado polling adaptativo:
    - Comienza revisando cada 1 segundo (respuesta rápida)
    - Incrementa gradualmente hasta 6 segundos (reduce CPU en descargas largas)
    
    Args:
        download_dir: Directorio de descargas
        archivos_previos: Set de archivos antes de iniciar descarga
        timeout: Tiempo máximo de espera
        
    Returns:
        str: Nombre del archivo descargado
        
    Raises:
        TimeoutError: Si la descarga no completa en el tiempo límite
    """
    logger.info("Esperando descarga completa...")
    tiempo_inicio = time.time()
    
    # Intervalo adaptativo: empieza rápido, incrementa gradualmente
    intervalo = DOWNLOAD_CHECK_INTERVAL_MIN
    
    while time.time() - tiempo_inicio < timeout:
        archivos_actuales = obtener_archivos_en_directorio(download_dir)
        archivos_nuevos = archivos_actuales - archivos_previos
        
        # Filtrar archivos temporales de descarga incompleta
        archivos_completos = [
            f for f in archivos_nuevos 
            if not f.endswith('.crdownload')  # Chrome
            and not f.endswith('.part')        # Firefox
            and not f.endswith('.tmp')         # Genérico
        ]
        
        if archivos_completos:
            archivo_descargado = archivos_completos[0]
            archivo_path = download_dir / archivo_descargado
            
            # Verificar que el archivo tenga contenido
            if archivo_path.stat().st_size > 0:
                logger.info(f"✓ Descarga completada: {archivo_descargado}")
                return archivo_descargado
        
        time.sleep(intervalo)
        
        # Incrementar intervalo gradualmente (factor 1.5x, máximo 6s)
        intervalo = min(intervalo * 1.5, DOWNLOAD_CHECK_INTERVAL_MAX)
    
    raise TimeoutError(f"La descarga no se completó en {timeout} segundos")


def esperar_spinner(driver: webdriver.Chrome, timeout: int = 30) -> None:
    """
    Espera a que los indicadores de carga desaparezcan.
       
    Args:
        driver: Instancia de Chrome WebDriver
        timeout: Tiempo máximo de espera por spinner
    """
    spinners = [
        "//img[contains(@src, 'waiting')]",
        "//img[contains(@src, 'loading')]",
        "//div[contains(@class, 'loading')]",
        "//div[contains(@class, 'spinner')]",
        "//*[contains(@id, 'loading')]"
    ]
    
    for spinner_xpath in spinners:
        try:
            WebDriverWait(driver, 2).until(
                EC.invisibility_of_element_located((By.XPATH, spinner_xpath))
            )
        except TimeoutException:
            # Esperado si el spinner no existe o ya desapareció
            pass
        except Exception as e:
            # Log de debug para otros errores inesperados
            logger.debug(f"Error inesperado buscando spinner '{spinner_xpath}': {e}")
    
    # Pausa mínima de seguridad (el doble: 2s)
    time.sleep(2)


# =============================================================================
# FUNCIONES REUTILIZABLES DE TRANSFORMACIÓN
# =============================================================================

def limpiar_nombre_columna(nombre: str) -> str:
    """
    Normaliza el nombre de una columna para compatibilidad con SQL.
    
    Transformaciones:
    - Elimina acentos (á→a, ñ→n)
    - Convierte % a 'pct_'
    - Reemplaza caracteres especiales por guión bajo
    - Convierte a minúsculas
    
    Args:
        nombre: Nombre original de la columna
        
    Returns:
        str: Nombre normalizado
    """
    if not isinstance(nombre, str):
        return str(nombre)
        
    import unicodedata
    import re
    
    # Normalizar unicode (ñ → n, á → a, etc.)
    nombre = unicodedata.normalize('NFKD', nombre)
    nombre = nombre.encode('ASCII', 'ignore').decode('ASCII')
    
    # Reemplazar % por 'pct_' al inicio de palabras
    nombre = re.sub(r'%\s*', 'pct_', nombre)
    
    # Reemplazar caracteres no alfanuméricos por guión bajo
    nombre = re.sub(r'[^a-zA-Z0-9]', '_', nombre)
    
    # Eliminar guiones bajos múltiples y en extremos
    nombre = re.sub(r'_+', '_', nombre)
    nombre = nombre.strip('_')
    
    return nombre.lower()


def convertir_tiempo_a_minutos(valor) -> float:
    """
    Convierte un valor de tiempo a minutos decimales.
    
    Formatos soportados:
    - HH:MM:SS → minutos decimales
    - HH:MM → minutos decimales
    - Valores numéricos → se retornan tal cual
    - Valores vacíos/nulos → 0.0
    
    Args:
        valor: Valor de tiempo en cualquier formato
        
    Returns:
        float: Tiempo en minutos
    """
    # Manejar valores nulos o vacíos
    if pd.isna(valor) or str(valor).strip() in ['-', '', 'nan', 'None']:
        return 0.0
    
    try:
        valor_str = str(valor).strip()
        
        # Si contiene ':', es formato HH:MM:SS o HH:MM
        if ':' in valor_str:
            parts = valor_str.split(':')
            if len(parts) == 3:
                h, m, s = map(int, parts)
                return float(h * 60 + m + (s / 60))
            elif len(parts) == 2:
                h, m = map(int, parts)
                return float(h * 60 + m)
        
        # Si no contiene ':', intentar como número
        return float(valor_str)
        
    except (ValueError, TypeError):
        return 0.0


def procesar_columnas_tiempo(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    """
    
    Detecta automáticamente si los valores están en formato HH:MM:SS
    o ya son numéricos, y los convierte a minutos decimales.
    
    Args:
        df: DataFrame a procesar
        columnas: Lista de nombres de columnas de tiempo
        
    Returns:
        pd.DataFrame: DataFrame con columnas de tiempo procesadas
    """
    for col in columnas:
        if col not in df.columns:
            continue
            
        try:
            # Verificar si hay valores con formato HH:MM:SS
            muestra = df[col].astype(str)
            tiene_formato_tiempo = muestra.str.contains(':').any()
            
            if tiene_formato_tiempo:
                # Aplicar conversión de tiempo
                df[col] = df[col].apply(convertir_tiempo_a_minutos)
            else:
                # Ya es numérico, solo asegurar tipo float
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
        except Exception as e:
            logger.warning(f"Error procesando columna de tiempo '{col}': {e}")
            df[col] = 0.0
            
    return df


def procesar_columnas_enteros(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    """ 
    Args:
        df: DataFrame a procesar
        columnas: Lista de nombres de columnas
        
    Returns:
        pd.DataFrame: DataFrame con columnas convertidas a entero
    """
    for col in columnas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df


def procesar_columnas_flotantes(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    """
    Args:
        df: DataFrame a procesar
        columnas: Lista de nombres de columnas
        
    Returns:
        pd.DataFrame: DataFrame con columnas convertidas a float
    """
    for col in columnas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    return df


# =============================================================================
# FUNCIONES DE NAVEGACIÓN Y LOGIN
# =============================================================================

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=40),  # Valor original: 8-60s modificado para pruebas
    retry=retry_if_exception_type((WebDriverException, TimeoutException)),
    before_sleep=lambda retry_state: logger.warning(
        f"Reintentando login... intento {retry_state.attempt_number + 1}"
    )
)
def login_neotel(driver: webdriver.Chrome, url: str, usuario: str, password: str) -> None:
    """
    Realiza el login en el sistema Neotel.
    
    Decorador @retry agregado:
    - 3 intentos máximo
    - Espera exponencial entre intentos (8-60 segundos)
    - Reintenta en errores de WebDriver o Timeout
    
    Args:
        driver: Instancia de Chrome WebDriver
        url: URL de login
        usuario: Nombre de usuario
        password: Contraseña
    """
    logger.info(f"Navegando a {url}")
    driver.get(url)
    wait = WebDriverWait(driver, 15)

    try:
        driver.switch_to.default_content()
        
        # Intentar cambiar al frame de login (múltiples estrategias)
        frame_encontrado = False
        
        # Estrategia 1: Por nombre
        try:
            driver.switch_to.frame("main")
            logger.info("Entramos al frame 'main'")
            frame_encontrado = True
        except NoSuchElementException:
            pass
            
        # Estrategia 2: Por índice
        if not frame_encontrado:
            try:
                driver.switch_to.frame(1)
                logger.info("Entramos al frame índice 1")
                frame_encontrado = True
            except (NoSuchElementException, IndexError):
                pass
        
        # Estrategia 3: Búsqueda por atributos
        if not frame_encontrado:
            frames = driver.find_elements(By.TAG_NAME, "frame")
            for i, f in enumerate(frames):
                frame_id = f.get_attribute("id")
                frame_name = f.get_attribute("name")
                if frame_id == "srcMain" or frame_name == "main":
                    driver.switch_to.frame(f)
                    logger.info(f"Entramos al frame srcMain/main (índice {i})")
                    frame_encontrado = True
                    break
        
        if not frame_encontrado:
            logger.warning("No se encontró frame de login, continuando en contexto principal")
        
        # Ingresar credenciales
        input_user = wait.until(EC.visibility_of_element_located((By.ID, "txtUsuario")))
        input_user.clear()
        input_user.send_keys(usuario)
        
        input_pass = driver.find_element(By.ID, "txtClave")
        input_pass.clear()
        input_pass.send_keys(password)
        logger.info("Credenciales ingresadas")

        # Click en botón de login
        btn_login = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='submit']")))
        btn_login.click()
        logger.info("Botón de login presionado")

        # Espera para carga post-login (originalmente: 10s)
        time.sleep(6)
        driver.switch_to.default_content()
        logger.info("Login completado exitosamente")
        
    except Exception as e:
        logger.error(f"Error crítico en login: {str(e)}")
        driver.save_screenshot("error_login_debug.png")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=30),  # El doble: 4-30s
    retry=retry_if_exception_type((WebDriverException, TimeoutException)),
    before_sleep=lambda retry_state: logger.warning(
        f"Reintentando navegación a reportes... intento {retry_state.attempt_number + 1}"
    )
)
def navegar_a_reportes(driver: webdriver.Chrome) -> None:
    """
    Navega a la página de reportes de Neotel.
    
    Decorador @retry agregado para manejar fallos de red.
    
    Args:
        driver: Instancia de Chrome WebDriver
    """
    url_reportes = "https://web.neoteluin.mx/callcenter/ProcessAndReports.aspx?tipo=Reporte"
    logger.info(f"Navegando a: {url_reportes}")
    
    driver.get(url_reportes)
    wait = WebDriverWait(driver, 20)
    
    # Esperar a que cargue la tabla principal del grid
    wait.until(EC.visibility_of_element_located((By.ID, "dgdProcedimientos_main")))
    esperar_spinner(driver)
    logger.info("Página de reportes cargada y grid visible")
    
    # Pausa de estabilización (el doble: 4s)
    time.sleep(4)


# =============================================================================
# FUNCIONES DE DESCARGA DE REPORTES
# =============================================================================

def seleccionar_fila_reporte(driver: webdriver.Chrome, reporte_id: str) -> bool:
    """
    Busca y selecciona la fila del reporte en el grid de Infragistics.
       
    Estrategias de selección:
    1. API de Infragistics (JavaScript)
    2. Fallback: Búsqueda DOM directa
    
    Args:
        driver: Instancia de Chrome WebDriver
        reporte_id: ID del reporte a seleccionar
        
    Returns:
        bool: True si se seleccionó correctamente
    """
    logger.info(f"Intentando seleccionar reporte ID: {reporte_id} usando Infragistics API")
    wait = WebDriverWait(driver, 10)
    
    try:
        # Script JavaScript para interactuar con grid Infragistics
        js_script = f"""
        try {{
            var grid = igtbl_getGridById('dgdProcedimientos');
            if (!grid) return "GRID_NOT_FOUND";
            
            for (var i = 0; i < grid.Rows.length; i++) {{
                var row = grid.Rows.getRow(i);
                var cellId = row.getCell(0).getValue(); 
                
                // Comparación laxa (string o entero)
                if (cellId == '{reporte_id}') {{
                    row.activate();
                    row.setSelected(true);
                    row.Element.scrollIntoView({{block: 'center', inline: 'nearest'}});
                    return "OK";
                }}
            }}
            return "ID_NOT_FOUND";
        }} catch(e) {{
            return "ERROR: " + e.message;
        }}
        """
        
        resultado = driver.execute_script(js_script)
        logger.info(f"Resultado selección JS: {resultado}")
        
        if resultado == "OK":
            # Espera para reacción de UI (el doble: 2s)
            time.sleep(2)
            return True
        elif resultado == "GRID_NOT_FOUND":
            logger.error("No se encontró el objeto grid Infragistics")
            return False
            
        # Fallback: Búsqueda DOM directa
        logger.warning(f"Infragistics API no encontró ID {reporte_id}, intentando búsqueda DOM...")
        xpath_fallback = f"//tr[td//nobr[normalize-space(text())='{reporte_id}']]"
        fila = wait.until(EC.presence_of_element_located((By.XPATH, xpath_fallback)))
        driver.execute_script("arguments[0].click();", fila)
        return True

    except Exception as e:
        logger.error(f"Error crítico en selección: {e}")
        return False


def click_boton_ejecutar_inferior(driver: webdriver.Chrome) -> bool:
    """
    Hace clic en el botón Ejecutar de la barra inferior.
    
    Args:
        driver: Instancia de Chrome WebDriver
        
    Returns:
        bool: True si el clic fue exitoso
    """
    logger.info("Intentando clic en botón Ejecutar (inferior)")
    wait = WebDriverWait(driver, 10)
    
    try:
        # Esperar a que desaparezca cualquier modal previo
        try:
            wait.until(EC.invisibility_of_element_located((By.ID, "modal")))
        except TimeoutException:
            pass  # No había modal

        # El ID del botón es btnEjecutarParent
        boton = wait.until(EC.element_to_be_clickable((By.ID, "btnEjecutarParent")))
        
        try:
            boton.click()
            logger.info("Botón Ejecutar presionado correctamente")
        except Exception:
            # Fallback: Click vía JavaScript
            logger.warning("Click estándar interceptado, usando JavaScript...")
            driver.execute_script("arguments[0].click();", boton)
            logger.info("Botón Ejecutar presionado vía JS")
            
        return True
        
    except Exception as e:
        logger.error(f"Fallo click botón Ejecutar: {e}")
        return False


def manejar_modal_parametros(driver: webdriver.Chrome, wait: WebDriverWait, 
                              fecha_ayer: str) -> bool:
    """
    Maneja el modal de parámetros del reporte.
    
    Flujo:
    1. Detectar ventana nueva o iframe
    2. Cargar últimos parámetros (si disponible)
    3. Establecer fechas de inicio y fin
    4. Click en botón Ejecutar del modal
    
    Args:
        driver: Instancia de Chrome WebDriver
        wait: WebDriverWait configurado
        fecha_ayer: Fecha en formato DD/MM/YYYY
        
    Returns:
        bool: True si el modal se manejó correctamente
    """
    logger.info("Manejando modal de parámetros...")
    
    handle_original = driver.current_window_handle
    handles_iniciales = set(driver.window_handles)
    
    # Espera para aparición de ventana (el doble: 6s)
    time.sleep(6)
    
    # Verificar si hay nueva ventana
    handles_actuales = set(driver.window_handles)
    nuevas_ventanas = handles_actuales - handles_iniciales
    
    if nuevas_ventanas:
        nueva_ventana = nuevas_ventanas.pop()
        driver.switch_to.window(nueva_ventana)
        logger.info("Cambiado a nueva ventana popup")
    else:
        # Buscar iframe del modal
        try:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            if iframes:
                driver.switch_to.frame(iframes[-1])
                logger.info("Cambiado a iframe modal")
        except Exception:
            logger.info("No se encontró iframe, continuando en contexto actual")
    
    esperar_spinner(driver)
    
    # Espera adicional para carga de modal (el doble: 4s)
    time.sleep(4)
    
    modal_wait = WebDriverWait(driver, 15)
    
    # =========================================================================
    # PASO 1: Cargar últimos parámetros
    # =========================================================================
    
    logger.info("Intentando cargar últimos parámetros vía botón 'btnLoadOldParameters'...")
    usar_ultimos_params = False
    
    for intento in range(3):
        try:
            btn_params = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "btnLoadOldParameters"))
            )
            
            # Intentar ejecutar función JS nativa
            try:
                driver.execute_script("btnLoadOldParameters_Click();")
                logger.info("✓ Función JS btnLoadOldParameters_Click() ejecutada.")
            except Exception:
                driver.execute_script("arguments[0].click();", btn_params)
                logger.info("✓ Click JS en botón btnLoadOldParameters ejecutado.")
                
            usar_ultimos_params = True
            
            # Espera para recarga de parámetros (el doble: 10s)
            logger.info("Esperando recarga de parámetros...")
            time.sleep(10)
            esperar_spinner(driver)
            break
            
        except Exception as e:
            logger.warning(f"Intento {intento+1}/3 falló para btnLoadOldParameters: {e}")
            time.sleep(2)
            continue
    
    if not usar_ultimos_params:
        logger.warning("No se pudo activar btnLoadOldParameters")

    # =========================================================================
    # PASO 2: Establecer Fechas
    # =========================================================================
    
    # Espera adicional (el doble: 4s)
    time.sleep(4)
    
    fechas_establecidas = False
    
    try:
        # IDs de campos de fecha
        campo_inicio = driver.find_element(By.ID, "Inicio_input")
        campo_fin = driver.find_element(By.ID, "Fin_input")
        
        # Desbloquear campos
        driver.execute_script("""
            arguments[0].removeAttribute('readonly');
            arguments[0].removeAttribute('disabled');
            arguments[1].removeAttribute('readonly');
            arguments[1].removeAttribute('disabled');
        """, campo_inicio, campo_fin)
        
        time.sleep(0.5)
        
        # Establecer Fecha Inicio
        driver.execute_script("arguments[0].focus();", campo_inicio)
        campo_inicio.clear()
        campo_inicio.send_keys(fecha_ayer)
        
        # Forzar valor en hidden input y disparar eventos
        driver.execute_script("""
            var visibleInput = arguments[0];
            var dateValue = arguments[1];
            var hiddenInput = document.getElementById('Inicio_hidden');
            if (hiddenInput) hiddenInput.value = dateValue;
            visibleInput.value = dateValue;
            visibleInput.dispatchEvent(new Event('change', { bubbles: true }));
            visibleInput.dispatchEvent(new Event('blur', { bubbles: true }));
        """, campo_inicio, fecha_ayer)
        
        campo_inicio.send_keys(Keys.TAB)
        logger.info(f"Fecha inicio establecida: {fecha_ayer}")
        
        # Espera entre campos (el doble: 2s)
        time.sleep(2)
        
        # Establecer Fecha Fin
        driver.execute_script("arguments[0].focus();", campo_fin)
        campo_fin.clear()
        campo_fin.send_keys(fecha_ayer)
        
        driver.execute_script("""
            var visibleInput = arguments[0];
            var dateValue = arguments[1];
            var hiddenInput = document.getElementById('Fin_hidden');
            if (hiddenInput) hiddenInput.value = dateValue;
            visibleInput.value = dateValue;
            visibleInput.dispatchEvent(new Event('change', { bubbles: true }));
            visibleInput.dispatchEvent(new Event('blur', { bubbles: true }));
        """, campo_fin, fecha_ayer)
        
        campo_fin.send_keys(Keys.TAB)
        logger.info(f"Fecha fin establecida: {fecha_ayer}")
        
        # Click fuera para validación
        time.sleep(1)
        driver.find_element(By.TAG_NAME, "body").click()
        
        fechas_establecidas = True
        
    except NoSuchElementException:
        logger.warning("No se encontraron campos Inicio_input/Fin_input")
    except Exception as e:
        logger.warning(f"Error estableciendo fechas: {e}")
    
    if not fechas_establecidas:
        logger.warning("⚠ No se pudieron establecer las fechas automáticamente")
    
    # Pausa pre-ejecución (el doble: 2s)
    time.sleep(2)
    
    # =========================================================================
    # PASO 3: Click en botón Ejecutar del modal
    # =========================================================================
    
    selectores_ejecutar = [
        "//input[@id='btnEjecutar']",
        "//button[@id='btnEjecutar']",
        "//input[@value='Ejecutar']",
        "//button[contains(text(), 'Ejecutar')]",
        "//a[contains(text(), 'Ejecutar')]",
        "//input[@type='submit']",
        "//input[@type='button'][contains(@value, 'jecuta')]",
        "//*[contains(@onclick, 'Ejecutar')]",
    ]
    
    boton_clickeado = False
    
    for selector in selectores_ejecutar:
        try:
            btn = modal_wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            time.sleep(1)
            
            try:
                btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", btn)
            
            logger.info(f"✓ Click en botón Ejecutar del modal: {selector[:50]}")
            boton_clickeado = True
            break
            
        except TimeoutException:
            continue
        except Exception as e:
            logger.debug(f"Selector '{selector}' falló: {e}")
            continue
    
    if not boton_clickeado:
        logger.error("No se pudo hacer click en el botón Ejecutar del modal")
        return False
    
    logger.info("Esperando procesamiento del reporte...")
    esperar_spinner(driver, timeout=60)
    
    return True


def cerrar_modal_y_volver(driver: webdriver.Chrome, handle_original: str) -> None:
    """
    Cierra el modal/ventana popup y vuelve al contexto original.
    
    Args:
        driver: Instancia de Chrome WebDriver
        handle_original: Handle de la ventana principal
    """
    try:
        handles_actuales = driver.window_handles
        
        if len(handles_actuales) > 1:
            # Cerrar ventanas adicionales
            for handle in handles_actuales:
                if handle != handle_original:
                    driver.switch_to.window(handle)
                    driver.close()
            
            driver.switch_to.window(handle_original)
            logger.info("Cerrada ventana popup, vuelto al contexto original")
        else:
            # Era un iframe
            driver.switch_to.default_content()
            logger.info("Vuelto al contexto principal (default_content)")
        
        # Esperar desaparición de overlay modal
        try:
            WebDriverWait(driver, 5).until(
                EC.invisibility_of_element_located((By.ID, "modal"))
            )
        except TimeoutException:
            pass
        
        # Pausa de limpieza (el doble: 2s)
        time.sleep(2)
            
    except Exception as e:
        logger.warning(f"Error al cerrar modal: {e}")
        try:
            driver.switch_to.default_content()
            driver.switch_to.window(driver.window_handles[0])
        except Exception:
            pass


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=2, min=10, max=60),
    retry=retry_if_exception_type((WebDriverException, TimeoutException, TimeoutError)),
    before_sleep=lambda retry_state: logger.warning(
        f"Reintentando descarga de reporte... intento {retry_state.attempt_number + 1}"
    )
)
def descargar_reporte(driver: webdriver.Chrome, reporte_info: dict, 
                      fecha_ayer: str, download_dir: Path) -> str:
    """
    Proceso completo de descarga de un reporte.
    
    Decorador @retry agregado:
    - 2 intentos máximo
    - Espera exponencial (10-60 segundos)
    
    Flujo:
    1. Seleccionar fila del reporte
    2. Click en botón Ejecutar
    3. Manejar modal de parámetros
    4. Esperar descarga
    5. Cerrar modal y volver
    
    Args:
        driver: Instancia de Chrome WebDriver
        reporte_info: Diccionario con 'id' y 'nombre' del reporte
        fecha_ayer: Fecha en formato DD/MM/YYYY
        download_dir: Directorio de descargas
        
    Returns:
        str: Ruta completa del archivo descargado
    """
    id_reporte = reporte_info['id']
    nombre_reporte = reporte_info['nombre']
    
    logger.info("=" * 50)
    logger.info(f"DESCARGANDO: ID {id_reporte} - {nombre_reporte}")
    logger.info("=" * 50)
    
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    handle_original = driver.current_window_handle
    
    try:
        # PASO 1: Seleccionar fila
        if not seleccionar_fila_reporte(driver, id_reporte):
            raise Exception(f"No se pudo seleccionar la fila del reporte {id_reporte}")
        
        # PASO 2: Click en Ejecutar
        if not click_boton_ejecutar_inferior(driver):
            raise Exception("No se pudo hacer click en el botón Ejecutar")
        
        # Registrar archivos antes de la descarga
        archivos_previos = obtener_archivos_en_directorio(download_dir)
        
        # PASO 3: Manejar modal de parámetros
        if not manejar_modal_parametros(driver, wait, fecha_ayer):
            raise Exception("Error manejando el modal de parámetros")
        
        # PASO 4: Esperar descarga
        try:
            archivo_descargado = esperar_descarga_completa(download_dir, archivos_previos)
        except TimeoutError:
            logger.warning("Timeout esperando descarga, verificando archivos existentes...")
            archivos_actuales = obtener_archivos_en_directorio(download_dir)
            nuevos = archivos_actuales - archivos_previos
            if nuevos:
                archivo_descargado = list(nuevos)[0]
            else:
                raise
        
        # PASO 5: Cerrar modal y volver
        cerrar_modal_y_volver(driver, handle_original)
        
        # Espera antes del siguiente reporte
        time.sleep(4)
        
        return str(download_dir / archivo_descargado)
        
    except Exception as e:
        logger.error(f"Error descargando reporte {nombre_reporte}: {e}")
        driver.save_screenshot(f"error_reporte_{id_reporte}.png")
        
        # Intentar recuperar el contexto
        cerrar_modal_y_volver(driver, handle_original)
        raise


# =============================================================================
# FASE 1: EXTRACCIÓN
# =============================================================================

def ejecutar_extraccion(fecha_ayer: str) -> list:
    """
    Ejecuta la fase de extracción completa.
    
    Args:
        fecha_ayer: Fecha en formato DD/MM/YYYY
        
    Returns:
        list: Lista de rutas de archivos descargados
    """
    logger.info("=" * 60)
    logger.info("FASE 1: EXTRACCIÓN")
    logger.info("=" * 60)
    
    limpiar_directorio_descargas(DOWNLOAD_DIR)
    chrome_options = configurar_chrome_options(DOWNLOAD_DIR)
    
    archivos_descargados = []
    driver = None
    
    try:
        logger.info("Iniciando navegador Chrome (modo headless)...")
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(10)
        
        # Login
        login_neotel(driver, NEOTEL_URL, NEOTEL_USER, NEOTEL_PASS)
        
        # Navegar a Reportes
        navegar_a_reportes(driver)
        
        # Descargar cada reporte
        for reporte in REPORTES:
            try:
                archivo = descargar_reporte(driver, reporte, fecha_ayer, DOWNLOAD_DIR)
                archivos_descargados.append(archivo)
                logger.info(f"✓ Reporte descargado: {archivo}")
            except Exception as e:
                logger.error(f"✗ Error con reporte '{reporte['nombre']}': {e}")
                # Verificar estado antes de recargar
                try:
                    if "ProcessAndReports" not in driver.current_url:
                        navegar_a_reportes(driver)
                except Exception:
                    navegar_a_reportes(driver)
                continue
        
        logger.info(f"✓ Extracción completada. Archivos: {len(archivos_descargados)}")
        return archivos_descargados
        
    except Exception as e:
        logger.error(f"Error en extracción: {e}")
        if driver:
            driver.save_screenshot("error_extraccion_general.png")
        raise
        
    finally:
        if driver:
            driver.quit()
            logger.info("Navegador cerrado")


# =============================================================================
# FASE 2: TRANSFORMACIÓN
# =============================================================================

def identificar_tipo_archivo(nombre_archivo: str) -> str:
    """
    Identifica el tipo de archivo basado en su nombre.
    
    Args:
        nombre_archivo: Nombre del archivo
        
    Returns:
        str: 'conducta', 'estados_operativos', o 'desconocido'
    """
    nombre_lower = nombre_archivo.lower()
    
    if 'conducta' in nombre_lower:
        return 'conducta'
    elif 'estados' in nombre_lower or 'operativo' in nombre_lower:
        return 'estados_operativos'
    return 'desconocido'


def transformar_conducta_agentes(df: pd.DataFrame, fecha_ayer: str) -> pd.DataFrame:
    """
    Transforma el reporte de Conducta - Agentes.
       
    Args:
        df: DataFrame con datos crudos
        fecha_ayer: Fecha de referencia
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    logger.info("Transformando reporte de Conducta...")
    logger.info(f"Columnas originales (ejemplo): {list(df.columns)[:5]} ...")
    
    # 1. Limpieza inicial de nombres
    df.columns = [limpiar_nombre_columna(c) for c in df.columns]
    
    # 2. Aplicar mapeo de columnas (usa constante global)
    df = df.rename(columns=MAPEO_CONDUCTA)
    
    # 3. Rellenar columnas faltantes
    for col in COLUMNAS_FINALES_CONDUCTA:
        if col not in df.columns:
            df[col] = None

    # 4. Filtrar solo columnas destino
    df = df[[c for c in COLUMNAS_FINALES_CONDUCTA if c in df.columns]]
    
    # 5. Conversiones de tipo
    
    # Fecha
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', dayfirst=True).dt.date
    
    # Usar función reutilizable para enteros
    columnas_enteras = [
        'id', 'in_total', 'in_rechazadas_ignoradas', 'in_atendidas',
        'out_total', 'out_rechazadas_ignoradas', 'out_atendidas',
        'out_dialing', 'llamados_con_hold'
    ]
    df = procesar_columnas_enteros(df, columnas_enteras)
    
    # Usar función reutilizable para flotantes (porcentajes)
    columnas_pct = [c for c in df.columns if 'pct' in c]
    df = procesar_columnas_flotantes(df, columnas_pct)
    
    # Usar función reutilizable para tiempos
    columnas_tiempo = ['tiempo_medio_respuesta_in', 'tiempo_medio_respuesta_out']
    df = procesar_columnas_tiempo(df, columnas_tiempo)

    # Eliminar filas sin datos clave
    df = df.dropna(subset=['id', 'fecha'])
    
    logger.info(f"  → Conducta: {len(df)} registros listos.")
    return df


def transformar_estados_operativos(df: pd.DataFrame, fecha_ayer: str) -> pd.DataFrame:
    """
    Transforma el reporte de Estados Operativos.
    
    Args:
        df: DataFrame con datos crudos
        fecha_ayer: Fecha de referencia
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    logger.info("Transformando reporte de Estados Operativos...")
    
    # 1. Limpieza inicial
    df.columns = [limpiar_nombre_columna(c) for c in df.columns]
    
    # 2. Aplicar mapeo (usa constante global)
    df = df.rename(columns=MAPEO_ESTADOS)
    
    # 3. Rellenar faltantes
    for col in COLUMNAS_FINALES_ESTADOS:
        if col not in df.columns:
            df[col] = None
            
    # 4. Filtrar columnas
    df = df[[c for c in COLUMNAS_FINALES_ESTADOS if c in df.columns]]
    
    # 5. Conversiones
    
    # Fecha
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', dayfirst=True).dt.date
    
    # Usar función reutilizable para enteros
    df = procesar_columnas_enteros(df, ['id', 'id_campana'])
    
    # Usar función reutilizable para tiempos
    # Todas las columnas que empiezan con 't_' son tiempos
    columnas_tiempo = [c for c in df.columns if c.startswith('t_')]
    df = procesar_columnas_tiempo(df, columnas_tiempo)
            
    df = df.dropna(subset=['id', 'fecha'])
    
    logger.info(f"  → Estados: {len(df)} registros listos.")
    return df


def ejecutar_transformacion(archivos: list, fecha_ayer: str) -> dict:
    """
    Ejecuta la fase de transformación para todos los archivos.
    
    Args:
        archivos: Lista de rutas de archivos descargados
        fecha_ayer: Fecha de referencia
        
    Returns:
        dict: Diccionario con DataFrames transformados
    """
    logger.info("=" * 60)
    logger.info("FASE 2: TRANSFORMACIÓN")
    logger.info("=" * 60)
    
    resultados = {'conducta': None, 'estados_operativos': None}
    
    for archivo in archivos:
        nombre = os.path.basename(archivo)
        tipo = identificar_tipo_archivo(nombre)
        logger.info(f"Procesando: {nombre} (Tipo: {tipo})")
        
        try:
            # Intentar leer CSV con diferentes encodings
            df = None
            
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(archivo, encoding=encoding, sep=None, engine='python')
                    break
                except Exception:
                    continue
            
            # Fallback: intentar como Excel
            if df is None and archivo.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(archivo)
            
            if df is None:
                logger.error(f"No se pudo leer: {nombre}")
                continue
            
            df = df.dropna(how='all')
            logger.info(f"  → {len(df)} filas, {len(df.columns)} columnas")
            
            if tipo == 'conducta':
                resultados['conducta'] = transformar_conducta_agentes(df, fecha_ayer)
            elif tipo == 'estados_operativos':
                resultados['estados_operativos'] = transformar_estados_operativos(df, fecha_ayer)
            else:
                logger.warning(f"  → Tipo desconocido: {nombre}")
                
        except Exception as e:
            logger.error(f"Error procesando {nombre}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            continue
            
    return resultados


# =============================================================================
# FASE 3: CARGA (PYODBC con Transacciones)
# =============================================================================

DDL_CONDUCTA = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='tbl_neotel_conducta' AND xtype='U')
CREATE TABLE tbl_neotel_conducta (
    agente NVARCHAR(200),
    fecha DATE,
    id INT,
    campana NVARCHAR(150),
    in_total INT,
    pct_in FLOAT,
    in_rechazadas_ignoradas INT,
    pct_in_rechazadas_ignoradas FLOAT,
    in_atendidas INT,
    pct_in_atendidas FLOAT,
    out_total INT,
    pct_out FLOAT,
    out_rechazadas_ignoradas INT,
    pct_out_rechazadas_ignoradas FLOAT,
    out_atendidas INT,
    pct_out_atendidas FLOAT,
    out_dialing INT,
    pct_out_dialing FLOAT,
    llamados_con_hold INT,
    pct_llamados_con_hold FLOAT,
    tiempo_medio_respuesta_in FLOAT,
    tiempo_medio_respuesta_out FLOAT,
    load_date DATETIME DEFAULT GETDATE()
);
"""

DDL_ESTADOS = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='tbl_neotel_estados_operativos' AND xtype='U')
CREATE TABLE tbl_neotel_estados_operativos (
    fecha DATE,
    intervalo NVARCHAR(50),
    id INT,
    agente NVARCHAR(200),
    id_campana INT,
    campana NVARCHAR(150),
    t_login FLOAT,
    t_login_neto FLOAT,
    t_available FLOAT,
    t_preview FLOAT,
    t_dialing FLOAT,
    t_ringing FLOAT,
    t_talking FLOAT,
    t_talking_in FLOAT,
    t_talking_out FLOAT,
    t_hold FLOAT,
    t_acw FLOAT,
    t_other_crm FLOAT,
    t_pause FLOAT,
    t_diario_login FLOAT,
    t_diario_login_neto FLOAT,
    t_diario_available FLOAT,
    t_diario_preview FLOAT,
    t_diario_dialing FLOAT,
    t_diario_ringing FLOAT,
    t_diario_talking FLOAT,
    t_diario_talking_in FLOAT,
    t_diario_talking_out FLOAT,
    t_diario_hold FLOAT,
    t_diario_acw FLOAT,
    t_diario_other_crm FLOAT,
    t_diario_pause FLOAT,
    load_date DATETIME DEFAULT GETDATE()
);
"""


def obtener_conexion_pyodbc():
    """
    Crea una conexión a SQL Server usando pyodbc.
    
    Usa credenciales de variables de entorno.
    
    Returns:
        pyodbc.Connection: Conexión a la base de datos
    """
    import pyodbc
    
    conn_str = (
        f"Driver={{{SQL_DRIVER}}};"
        f"Server={SQL_SERVER};"
        f"Database={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
    )
    
    return pyodbc.connect(conn_str)


def crear_tablas_si_no_existen(conn) -> None:
    """
    Crea las tablas de destino si no existen.
    
    Args:
        conn: Conexión pyodbc
    """
    logger.info("Verificando/creando tablas...")
    cursor = conn.cursor()
    cursor.execute(DDL_CONDUCTA)
    cursor.execute(DDL_ESTADOS)
    conn.commit()
    logger.info("✓ Tablas verificadas/creadas")


def validar_tabla_permitida(tabla: str) -> None:
    """
    Valida que la tabla esté en la whitelist.
    
    Previene SQL injection al validar que solo se usen tablas conocidas.
    
    Args:
        tabla: Nombre de la tabla
        
    Raises:
        ValueError: Si la tabla no está permitida
    """
    if tabla not in TABLAS_PERMITIDAS:
        raise ValueError(
            f"Tabla '{tabla}' no está en la lista de tablas permitidas. "
            f"Tablas válidas: {TABLAS_PERMITIDAS}"
        )


def cargar_dataframe_pyodbc(conn, df: pd.DataFrame, tabla: str, fecha_filtro: str) -> int:
    """
    Carga un DataFrame a SQL Server usando pyodbc.
    
    [FIX] Usa conn.commit() en lugar de cursor.execute("COMMIT")
    porque pyodbc maneja transacciones internamente.
    
    Args:
        conn: Conexión pyodbc
        df: DataFrame a cargar
        tabla: Nombre de la tabla destino
        fecha_filtro: Fecha para el DELETE previo (DD/MM/YYYY)
        
    Returns:
        int: Número de registros insertados
    """
    if df is None or df.empty:
        logger.warning(f"DataFrame vacío para {tabla}, omitiendo carga.")
        return 0
    
    # Validar tabla contra whitelist
    validar_tabla_permitida(tabla)
    
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    # Convertir fecha de DD/MM/YYYY a YYYY-MM-DD para SQL
    try:
        dt_obj = datetime.strptime(fecha_filtro, "%d/%m/%Y")
        fecha_iso = dt_obj.strftime("%Y-%m-%d")
    except ValueError:
        fecha_iso = fecha_filtro
    
    # Borrar datos existentes de esa fecha
    if 'fecha' in df.columns:
        delete_query = f"DELETE FROM {tabla} WHERE CAST(fecha AS DATE) = ?"
        cursor.execute(delete_query, (fecha_iso,))
        logger.info(f"  Borrados registros de {tabla} para fecha {fecha_iso}")
    
    # Construir INSERT dinámico
    cols = list(df.columns)
    cols_sql = ', '.join([f'[{c}]' for c in cols])
    placeholders = ', '.join(['?' for _ in cols])
    insert_query = f"INSERT INTO {tabla} ({cols_sql}) VALUES ({placeholders})"
    
    logger.info(f"  Insertando {len(df)} registros en {tabla}...")
    
    # Convertir DataFrame a lista de tuplas
    records = [tuple(row) for row in df.itertuples(index=False)]
    
    # Ejecutar en lotes
    batch_size = 100
    inserted = 0
    failed_batches = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            cursor.executemany(insert_query, batch)
            inserted += len(batch)
        except Exception as e:
            failed_batches += 1
            logger.warning(f"  Batch {i//batch_size} falló, insertando uno por uno...")
            # Intentar insertar uno por uno
            for rec in batch:
                try:
                    cursor.execute(insert_query, rec)
                    inserted += 1
                except Exception:
                    pass
    
    # [FIX] Usar conn.commit() - el método correcto de pyodbc
    conn.commit()
    
    if failed_batches > 0:
        logger.warning(f"  ⚠ {failed_batches} batches fallaron, se insertaron individualmente.")
    
    logger.info(f"  ✓ {inserted} registros insertados en {tabla}")
    return inserted


def ejecutar_carga(datos: dict, fecha_ayer: str) -> dict:
    """
    Ejecuta la fase de carga a SQL Server.
    
    Args:
        datos: Diccionario con DataFrames transformados
        fecha_ayer: Fecha en formato DD/MM/YYYY
        
    Returns:
        dict: Resumen de registros cargados por tabla
    """
    logger.info("=" * 60)
    logger.info("FASE 3: CARGA")
    logger.info("=" * 60)
    
    resumen = {
        'tbl_neotel_conducta': 0, 
        'tbl_neotel_estados_operativos': 0
    }
    
    conn = None
    
    try:
        conn = obtener_conexion_pyodbc()
        logger.info("Conexión a SQL Server establecida")
        
        crear_tablas_si_no_existen(conn)
        
        # Cargar Conducta
        if datos.get('conducta') is not None:
            resumen['tbl_neotel_conducta'] = cargar_dataframe_pyodbc(
                conn, datos['conducta'], 'tbl_neotel_conducta', fecha_ayer
            )
        
        # Cargar Estados Operativos
        if datos.get('estados_operativos') is not None:
            resumen['tbl_neotel_estados_operativos'] = cargar_dataframe_pyodbc(
                conn, datos['estados_operativos'], 'tbl_neotel_estados_operativos', fecha_ayer
            )
        
        logger.info("✓ Carga completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error en fase de carga: {e}")
        raise
        
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a SQL Server cerrada")
        
    return resumen


# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    """
    Función principal que orquesta las 3 fases del ETL.
    
    Fases:
    1. EXTRACCIÓN: Descarga reportes de Neotel vía Selenium
    2. TRANSFORMACIÓN: Limpia y normaliza datos con Pandas
    3. CARGA: Inserta datos en SQL Server con transacciones
    """
    logger.info("=" * 70)
    logger.info("ETL NEOTEL - INICIANDO (VERSIÓN OPTIMIZADA)")
    logger.info(f"Fecha ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    # Log de configuración (sin mostrar contraseñas)
    logger.info(f"Servidor SQL: {SQL_SERVER}")
    logger.info(f"Base de datos: {SQL_DATABASE}")
    logger.info(f"Modo: HEADLESS (automatización)")
    
    tiempo_inicio = time.time()
    
    try:
        # Calcular fecha objetivo
        fecha_ayer = calcular_fecha_ayer()
        
        # FASE 1: EXTRACCIÓN
        archivos_descargados = ejecutar_extraccion(fecha_ayer)
        
        if not archivos_descargados:
            logger.error("No se descargaron archivos. Abortando proceso.")
            sys.exit(1)
        
        # FASE 2: TRANSFORMACIÓN
        datos_transformados = ejecutar_transformacion(archivos_descargados, fecha_ayer)
        
        # Validar que hay datos para cargar
        # [FIX] Verificar explícitamente None o empty para evitar "ambiguous truth value"
        hay_datos = any(
            v is not None and not v.empty 
            for v in datos_transformados.values()
        )
        if not hay_datos:
            logger.error("No hay datos transformados para cargar. Abortando.")
            sys.exit(1)
        
        # FASE 3: CARGA
        resumen_carga = ejecutar_carga(datos_transformados, fecha_ayer)
        
        # Resumen final
        tiempo_total = time.time() - tiempo_inicio
        
        logger.info("=" * 70)
        logger.info("ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 70)
        logger.info(f"Tiempo total de ejecución: {tiempo_total:.2f} segundos")
        logger.info("Resumen de carga:")
        for tabla, registros in resumen_carga.items():
            logger.info(f"  • {tabla}: {registros} registros")
        logger.info("=" * 70)
        
    except EnvironmentError as e:
        # Error de configuración (variables de entorno faltantes)
        logger.error(f"ERROR DE CONFIGURACIÓN: {e}")
        sys.exit(2)
        
    except Exception as e:
        logger.error(f"ERROR CRÍTICO: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    main()