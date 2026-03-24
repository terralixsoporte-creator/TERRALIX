# Script Control:
# - Role: SII scraping and PDF download stage.
# - Track file: docs/SCRIPT_CONTROL.md
import os
import random
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


def _set_playwright_browsers_path():
    """
    Prioriza navegadores embebidos en el ejecutable.
    Si no existen, usa cache global (%LOCALAPPDATA%\\ms-playwright) o fallback "0".
    """
    current = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if current and Path(current).exists():
        return

    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
        candidates = [
            base / "_internal" / "ms-playwright",
            base / "ms-playwright",
            base / "_internal" / "_internal" / "ms-playwright",
            base / "_internal" / ".local-browsers",
            base / "_internal" / "playwright" / "driver" / "package" / ".local-browsers",
            Path(sys.executable).parent / "_internal" / "ms-playwright",
            Path(sys.executable).parent / "_internal" / "_internal" / "ms-playwright",
        ]
        for p in candidates:
            if p.exists():
                os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(p)
                return
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
        return

    local = os.getenv("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
    ms_pw = Path(local) / "ms-playwright"
    if ms_pw.exists():
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(ms_pw)
    else:
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")


_set_playwright_browsers_path()
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent 
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))
    
def resource_path(relative_path: str) -> str:
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        base_path = sys._MEIPASS  
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

ENV_PATH = resource_path(BASE_DIR / "data/config.env")

load_dotenv(resource_path(str(ENV_PATH)))

# --- CREDENCIALES ---
RUT = os.getenv("RUT")
CLAVE = os.getenv("CLAVE")
RUT2 = os.getenv("RUT2")
CLAVE2 = os.getenv("CLAVE2")
RUTA_PDF= os.getenv("RUTA_PDF_DTE_RECIBIDOS")

# --- CONFIGURACIÃ“N ---
PROXY = os.getenv("PROXY", "")
STORAGE_PATH = resource_path(BASE_DIR / "data/storage_state.json")
SLOW_MO = 0
HEADLESS = True
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.6 Safari/605.1.15",
]

def random_sleep(base=1.0, var=1.5):
    """Simula comportamiento humano con esperas variables"""
    time.sleep(base + random.random() * var)

def login_generico(page, rut, clave, descripcion=""):
    """Completa el formulario de login genÃ©rico del SII"""
    try:
        page.wait_for_selector("input#rutcntr", timeout=8000)
        print(f"ðŸ” Iniciando sesiÃ³n {descripcion}...")
        page.fill("input#rutcntr", rut)
        random_sleep(0.5, 1.0)
        page.fill("input#clave", clave)
        random_sleep(0.5, 1.0)
        page.press("input#clave", "Enter")
        page.wait_for_load_state("networkidle")
        print("âœ… Login exitoso")
        random_sleep(1.0, 2.0)
    except Exception as e:
        print("âš ï¸ No se encontrÃ³ formulario de login:", e)

def obtener_texto_seguro(cell):
    """Extrae texto limpio de una celda"""
    try:
        return (cell.inner_text() or "").strip()
    except:
        return ""

def existe_factura(rut_emisor, folio, carpeta=None):
    """Verifica si un PDF ya fue descargado, buscando coincidencias exactas o parciales"""
    if carpeta is None:
        carpeta = RUTA_PDF
    # Evita fallos si la carpeta no estÃ¡ configurada o no existe
    if not carpeta or not os.path.isdir(carpeta):
        return False
    for f in os.listdir(carpeta):
        if not f.lower().endswith(".pdf"):
            continue
        if rut_emisor in f and str(folio) in f:
            return True
    return False

def procesar_tabla(dte_page, context, ruta, progress_cb=None):
    """Procesa una pÃ¡gina de la tabla, descarga los PDFs y detecta si hay mÃ¡s pÃ¡ginas"""
    os.makedirs(ruta, exist_ok=True)
    errores_descarga = []
    pagina = 1

    while True:
        print(f"\nðŸ“„ Procesando pÃ¡gina {pagina}...")
        dte_page.wait_for_selector("table", timeout=15000)
        rows = dte_page.query_selector_all("table tr")[1:]  # omitir encabezado
        print(f"ðŸ§¾ Filas detectadas en esta pÃ¡gina: {len(rows)}")
        # Progreso por pÃ¡gina (total conocido por pÃ¡gina)
        total_en_pagina = len(rows)
        descargados_en_pagina = 0

        for i, row in enumerate(rows, start=1):
            try:
                cells = row.query_selector_all("td")
                if len(cells) < 5:
                    continue

                tipo_doc = obtener_texto_seguro(cells[3])
                rut_emisor = obtener_texto_seguro(cells[1])
                folio = obtener_texto_seguro(cells[4])

                nombre_pdf = f"{tipo_doc}_{rut_emisor}_{folio}.pdf".replace("/", "_").replace(" ", "_")
                ruta_pdf = os.path.join(ruta, nombre_pdf)

                # âœ… VerificaciÃ³n de duplicados restaurada (exacta + parcial)
                if os.path.exists(ruta_pdf) or existe_factura(rut_emisor, folio, ruta):
                    print(f"â­ï¸ ({i}) Ya existe: {nombre_pdf}, saltando.")
                    descargados_en_pagina += 1
                    if progress_cb:
                        try:
                            progress_cb(descargados_en_pagina, total_en_pagina, f"(pÃ¡g. {pagina})")
                        except Exception:
                            pass
                    continue

                ver_link = row.query_selector("a[href*='mipeGesDocRcp.cgi']")
                if not ver_link:
                    continue

                href_detalle = ver_link.get_attribute("href")
                if href_detalle.startswith("/"):
                    href_detalle = "https://www1.sii.cl" + href_detalle

                print(f"âž¡ï¸ ({i}) Abriendo detalle: {tipo_doc} | {rut_emisor} | Folio {folio}")

                # Abrir detalle en nueva pestaÃ±a
                with context.expect_page() as nueva_pagina_info:
                    dte_page.evaluate(f"window.open('{href_detalle}', '_blank');")
                detalle_page = nueva_pagina_info.value
                detalle_page.wait_for_load_state("networkidle", timeout=15000)
                random_sleep(1.0, 2.0)

                # Expandir secciÃ³n â€œOtros detalles documentoâ€
                print("ðŸ“‚ Expandiendo 'Otros detalles documento'...")
                try:
                    boton_otro = detalle_page.query_selector("a[href='#collapseOtros']")
                    if boton_otro:
                        boton_otro.click(force=True)
                        detalle_page.wait_for_selector(
                            "#collapseOtros a[href*='mipeShowPdf.cgi']", timeout=8000
                        )
                        print("âœ… SecciÃ³n desplegada.")
                    else:
                        print("âš ï¸ No se encontrÃ³ botÃ³n de detalles.")
                except Exception as e:
                    print(f"âš ï¸ No se pudo expandir la secciÃ³n: {e}")

                # Buscar enlace PDF
                pdf_link = detalle_page.query_selector("#collapseOtros a[href*='mipeShowPdf.cgi']")
                if not pdf_link:
                    pdf_link = detalle_page.query_selector("a[href*='mipeShowPdf.cgi']")

                if not pdf_link:
                    print("âš ï¸ No se encontrÃ³ enlace al PDF.")
                    detalle_page.close()
                    errores_descarga.append((tipo_doc, rut_emisor, folio, "Sin enlace PDF"))
                    continue

                pdf_href = pdf_link.get_attribute("href")
                if pdf_href.startswith("/"):
                    pdf_href = "https://www1.sii.cl" + pdf_href
                print(f"ðŸ“„ Enlace PDF detectado: {pdf_href}")

                # Descargar PDF en carpeta local
                try:
                    response = detalle_page.request.get(pdf_href)
                    pdf_bytes = response.body()
                    with open(ruta_pdf, "wb") as f:
                        f.write(pdf_bytes)
                    print(f"âœ… PDF guardado: {ruta_pdf}")
                    descargados_en_pagina += 1
                    if progress_cb:
                        try:
                            progress_cb(descargados_en_pagina, total_en_pagina, f"(pÃ¡g. {pagina})")
                        except Exception:
                            pass
                except Exception as e:
                    print(f"âš ï¸ Error al descargar PDF: {e}")
                    errores_descarga.append((tipo_doc, rut_emisor, folio, str(e)))

                detalle_page.close()
                random_sleep(1.0, 2.0)

            except Exception as e:
                print(f"âš ï¸ Error en fila {i}: {e}")
                errores_descarga.append(("Desconocido", "Desconocido", "Desconocido", str(e)))
                continue

        # ðŸ” Pasar a la siguiente pÃ¡gina (flecha)
        next_btn = dte_page.query_selector("a#pagina_siguiente.paginate_button")

        if next_btn:
            href_next = next_btn.get_attribute("href")
            if href_next and not href_next.strip().endswith("="):
                if href_next.startswith("/"):
                    href_next = "https://www1.sii.cl" + href_next
                print(f"âž¡ï¸ Pasando a la siguiente pÃ¡gina.....")
                dte_page.goto(href_next)
                dte_page.wait_for_load_state("networkidle", timeout=15000)
                random_sleep(2.0, 3.0)
                pagina += 1
                continue

        print("ðŸ No hay mÃ¡s pÃ¡ginas disponibles. Scraping finalizado.")
        break

    # --- Resumen de errores ---
    if errores_descarga:
        print("\nâš ï¸ Descargas incompletas detectadas:")
        for err in errores_descarga:
            print(f"  - {err[0]} | {err[1]} | Folio {err[2]} â†’ {err[3]}")
        print(f"\nâŒ Total de documentos no descargados: {len(errores_descarga)}")
        print("âž¡ï¸ Ejecuta nuevamente el script para reintentar los faltantes.")
        return True
    else:
        print("\nâœ… Todas las facturas fueron descargadas correctamente.")
        return False

def scrapear(ruta_pdf, progress_cb=None):
    with sync_playwright() as p:
        proxy_conf = {"server": PROXY} if PROXY else None
        launch_args = {"headless": HEADLESS, "slow_mo": SLOW_MO}
        if proxy_conf:
            launch_args["proxy"] = proxy_conf

        browser = p.chromium.launch(**launch_args)
        context_args = {
            "user_agent": random.choice(USER_AGENTS),
            "timezone_id": "America/Santiago",
            "locale": "es-CL",
        }

        if os.path.exists(STORAGE_PATH):
            print("â™»ï¸ Reusando sesiÃ³n previa...")
            context_args["storage_state"] = STORAGE_PATH

        context = browser.new_context(**context_args)
        page = context.new_page()

        # 1ï¸âƒ£ Login inicial
        if not os.path.exists(STORAGE_PATH):
            print("ðŸ”¹ Paso 1: Login inicial en Mi SII")
            page.goto(
                "https://zeusr.sii.cl/AUT2000/InicioAutenticacion/IngresoRutClave.html?https://misiir.sii.cl/cgi_misii/siihome.cgi"
            )
            login_generico(page, RUT, CLAVE, "en Mi SII")
            context.storage_state(path=STORAGE_PATH)
            print("ðŸ’¾ SesiÃ³n guardada para futuros accesos")

        # 2ï¸âƒ£ Ir a Servicios Online - DTE
        print("ðŸ”¹ Paso 2: Ir a Servicios Online - DTE")
        page.goto("https://www.sii.cl/servicios_online/1039-1183.html")
        page.wait_for_load_state("networkidle")
        random_sleep(1.5, 2.5)

        # 3ï¸âƒ£ Expandir menÃº â€œHistorial de DTEâ€
        try:
            page.locator("a[href='#collapseTwo']").click(force=True)
        except:
            pass

        # 4ï¸âƒ£ Buscar enlace al portal DTE
        link = page.query_selector("a[href*='mipeLaunchPage.cgi?OPCION=1']")
        href = (
            link.get_attribute("href")
            if link
            else "https://www1.sii.cl/cgi-bin/Portal001/mipeLaunchPage.cgi?OPCION=1&TIPO=4"
        )

        # 5ï¸âƒ£ Ir al portal
        page.goto(href)
        page.wait_for_load_state("networkidle")
        random_sleep(2, 3)

        # 6ï¸âƒ£ Segundo login
        login_generico(page, RUT2, CLAVE2, "en Portal DTE")

        # 7ï¸âƒ£ Ingresar al historial
        panel = page.locator("a[href='#collapseAdm']")
        if panel.count() > 0:
            panel.first.click(force=True)
        random_sleep(1.5)

        links = page.query_selector_all("a[href*='mipeLaunchPage.cgi']")
        href_final = None
        for l in links:
            text_temp = (l.inner_text() or "").strip().lower()
            href_temp = l.get_attribute("href") or ""
            if "ver documentos recibidos" in text_temp or "respuesta al emisor" in text_temp:
                href_final = href_temp
                break

        if href_final and href_final.startswith("/"):
            href_final = "https://www1.sii.cl" + href_final

        page.goto(href_final)
        page.wait_for_load_state("networkidle")
        random_sleep(2.5)
        print("âœ… Historial de DTE cargado correctamente.")

        descargas_incompletas = procesar_tabla(page, context, ruta_pdf, progress_cb=progress_cb)

        if descargas_incompletas:
            print("âš ï¸ Algunas descargas fallaron. Reejecuta el script.")
            return True
        else:
            print("âœ… Todo descargado con Ã©xito.")

        browser.close()

if __name__ == "__main__":
    scrapear(RUTA_PDF)




