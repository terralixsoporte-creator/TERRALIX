# Script Control:
# - Role: SII scraping and PDF download stage.
# - Track file: docs/SCRIPT_CONTROL.md
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime
from html import unescape
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

# Asegurar que la raiz del proyecto este en sys.path
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

from app.core.paths import get_env_path, get_storage_state_path, get_user_data_dir

ENV_PATH     = str(get_env_path())
STORAGE_PATH = str(get_storage_state_path())

load_dotenv(ENV_PATH, override=False)

# --- CREDENCIALES ---
RUT      = os.getenv("RUT")
CLAVE    = os.getenv("CLAVE")
RUT2     = os.getenv("RUT2")
CLAVE2   = os.getenv("CLAVE2")
RUTA_PDF = os.getenv("RUTA_PDF_DTE_RECIBIDOS")

# --- CONFIGURACION ---
PROXY = os.getenv("PROXY", "")
SLOW_MO = 0
HEADLESS = True
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.6 Safari/605.1.15",
]
DATE_RE = re.compile(r"(?<!\d)(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})(?!\d)")
PDF_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']*mipeShowPdf\.cgi[^"\']*)["\']', re.IGNORECASE)
FEC_EMI_RE = re.compile(
    r'for\s*=\s*["\']FEC_EMI["\'][\s\S]{0,1200}?'
    r'<span[^>]*class\s*=\s*["\'][^"\']*form-control[^"\']*["\'][^>]*>\s*([^<]+?)\s*</span>',
    re.IGNORECASE,
)


def _env_int(name, default, min_value=1):
    try:
        value = int(str(os.getenv(name, str(default))).strip())
    except Exception:
        value = default
    return max(min_value, value)


def _clean_env_path(raw):
    return (raw or "").strip().strip('"').strip("'")


def _resolve_db_path():
    db_path = _clean_env_path(os.getenv("DB_PATH_DTE_RECIBIDOS", ""))
    if db_path:
        return db_path
    return str(get_user_data_dir() / "DteRecibidos_db.db")


def _to_abs_sii_url(href):
    if not href:
        return ""
    url = str(href).strip()
    if not url:
        return ""
    if url.startswith("/"):
        return "https://www1.sii.cl" + url
    return url


def _slug_for_doc_id(s):
    s = " ".join(str(s).split())
    s = s.replace("/", ":").replace("\\", "_")
    s = re.sub(r"\s+", "_", s)
    return s


def _normalize_rut_for_doc_id(raw):
    s = str(raw or "").strip().upper().replace(".", "").replace(" ", "")
    s = re.sub(r"[^0-9K-]", "", s)
    if not s:
        return ""
    if "-" not in s and len(s) > 1:
        s = f"{s[:-1]}-{s[-1]}"
    return s


def _normalize_folio_for_doc_id(raw):
    s = str(raw or "").strip()
    m = re.search(r"\d+", s)
    return m.group(0) if m else s


def _build_doc_id(tipo_doc, rut_emisor, folio):
    return f"{_slug_for_doc_id(tipo_doc)}_{_normalize_rut_for_doc_id(rut_emisor)}_{_normalize_folio_for_doc_id(folio)}"


def _parse_iso_date_from_text(raw):
    text = str(raw or "").strip()
    if not text:
        return None

    # Primero intenta formato ISO puro de DB: YYYY-MM-DD
    try:
        return datetime.fromisoformat(text[:10]).date().isoformat()
    except Exception:
        pass

    # Fallback: DD/MM/YYYY, DD-MM-YYYY o DD.MM.YYYY (a veces con año corto).
    m = DATE_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if year < 100:
        year += 2000
    try:
        return datetime(year, month, day).date().isoformat()
    except Exception:
        return None


def _extract_fecha_emision_iso(cells):
    for cell in cells:
        fecha_iso = _parse_iso_date_from_text(obtener_texto_seguro(cell))
        if fecha_iso:
            return fecha_iso
    return None


def _extract_fecha_emision_iso_from_detail_html(html_text):
    if not html_text:
        return None
    try:
        txt = unescape(str(html_text))
    except Exception:
        txt = str(html_text)
    m = FEC_EMI_RE.search(txt)
    if m:
        return _parse_iso_date_from_text(m.group(1))
    return _parse_iso_date_from_text(txt)


def _extract_pdf_href_from_detail_html(html_text):
    if not html_text:
        return ""
    try:
        txt = unescape(str(html_text))
    except Exception:
        txt = str(html_text)
    m = PDF_HREF_RE.search(txt)
    if not m:
        return ""
    return _to_abs_sii_url(m.group(1))


def _extract_fecha_emision_iso_from_detail_page(detalle_page):
    selectors = [
        "label[for='FEC_EMI'] + span.form-control",
        "#collapseOtros label[for='FEC_EMI'] + span.form-control",
        "span.form-control",
    ]
    for sel in selectors:
        try:
            node = detalle_page.query_selector(sel)
            if node:
                fecha_iso = _parse_iso_date_from_text(node.inner_text() or "")
                if fecha_iso:
                    return fecha_iso
        except Exception:
            pass
    try:
        return _extract_fecha_emision_iso_from_detail_html(detalle_page.content())
    except Exception:
        return None


def _load_db_index(db_path):
    existing_doc_ids = set()
    existing_emission_dates = set()
    existing_rut_folios = set()
    if not db_path or not os.path.isfile(db_path):
        return existing_doc_ids, existing_emission_dates, existing_rut_folios

    try:
        with sqlite3.connect(db_path) as con:
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='documentos'")
            if not cur.fetchone():
                return existing_doc_ids, existing_emission_dates, existing_rut_folios

            cur.execute(
                "SELECT id_doc FROM documentos "
                "WHERE id_doc IS NOT NULL AND TRIM(id_doc) <> ''"
            )
            existing_doc_ids = {str(r[0]).strip() for r in cur.fetchall() if r and r[0]}

            cur.execute(
                "SELECT DISTINCT fecha_emision FROM documentos "
                "WHERE fecha_emision IS NOT NULL AND TRIM(fecha_emision) <> ''"
            )
            for (raw_fecha,) in cur.fetchall():
                fecha_iso = _parse_iso_date_from_text(raw_fecha)
                if fecha_iso:
                    existing_emission_dates.add(fecha_iso)

            cur.execute(
                "SELECT rut_emisor, folio FROM documentos "
                "WHERE rut_emisor IS NOT NULL AND TRIM(rut_emisor) <> '' "
                "AND folio IS NOT NULL AND TRIM(folio) <> ''"
            )
            for rut_raw, folio_raw in cur.fetchall():
                rut = _normalize_rut_for_doc_id(rut_raw)
                folio = _normalize_folio_for_doc_id(folio_raw)
                if rut and folio:
                    existing_rut_folios.add((rut, folio))
    except Exception as e:
        print(f"[WARN] No se pudo cargar indice desde DB ({db_path}): {e}")

    return existing_doc_ids, existing_emission_dates, existing_rut_folios

def random_sleep(base=1.0, var=1.5):
    """Simula comportamiento humano con esperas variables"""
    time.sleep(base + random.random() * var)

def login_generico(page, rut, clave, descripcion=""):
    """Completa el formulario de login generico del SII"""
    try:
        page.wait_for_selector("input#rutcntr", timeout=8000)
        print(f"Iniciando sesion {descripcion}...")
        page.fill("input#rutcntr", rut)
        random_sleep(0.5, 1.0)
        page.fill("input#clave", clave)
        random_sleep(0.5, 1.0)
        page.press("input#clave", "Enter")
        page.wait_for_load_state("networkidle")
        print("Login exitoso")
        random_sleep(1.0, 2.0)
    except Exception as e:
        print("No se encuentra el formulario de login:", e)

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
    if not carpeta or not os.path.isdir(carpeta):
        return False
    for f in os.listdir(carpeta):
        if not f.lower().endswith(".pdf"):
            continue
        if rut_emisor in f and str(folio) in f:
            return True
    return False

def procesar_tabla(
    dte_page,
    context,
    ruta,
    progress_cb=None,
    existing_doc_ids=None,
    existing_emission_dates=None,
    existing_rut_folios=None,
    stop_after_known_date_pages=1,
):
    """Procesa una pagina de la tabla, descarga los PDFs y detecta si hay mas paginas"""
    os.makedirs(ruta, exist_ok=True)
    errores_descarga = []
    pagina = 1
    existing_doc_ids = existing_doc_ids if isinstance(existing_doc_ids, set) else set()
    existing_emission_dates = (
        existing_emission_dates if isinstance(existing_emission_dates, set) else set()
    )
    existing_rut_folios = existing_rut_folios if isinstance(existing_rut_folios, set) else set()
    stop_after_known_date_pages = max(1, int(stop_after_known_date_pages or 1))
    consecutive_historic_pages = 0
    historic_tolerance_rows = 2

    while True:
        print(f"Procesando pagina {pagina}...")
        dte_page.wait_for_selector("table", timeout=15000)
        rows = dte_page.query_selector_all("table tr")[1:]  # omitir encabezado
        fecha_col_idx = None
        try:
            header_row = dte_page.query_selector("table tr")
            if header_row:
                header_cells = header_row.query_selector_all("th, td")
                for idx, h in enumerate(header_cells):
                    t = (obtener_texto_seguro(h) or "").lower()
                    if ("fecha" in t and ("emis" in t or "doc" in t)) or "emision" in t or "emisión" in t:
                        fecha_col_idx = idx
                        break
        except Exception:
            fecha_col_idx = None
        print(f"Filas detectadas en esta pagina: {len(rows)}")
        total_en_pagina = len(rows)
        descargados_en_pagina = 0
        candidatos_en_pagina = 0
        filas_con_fecha = 0
        filas_con_fecha_ya_en_db = 0
        filas_validas = 0
        filas_skip_conocidas = 0

        for i, row in enumerate(rows, start=1):
            try:
                cells = row.query_selector_all("td")
                if len(cells) < 5:
                    continue
                filas_validas += 1

                tipo_doc   = obtener_texto_seguro(cells[3])
                rut_emisor = obtener_texto_seguro(cells[1])
                folio      = obtener_texto_seguro(cells[4])
                rut_norm = _normalize_rut_for_doc_id(rut_emisor)
                folio_norm = _normalize_folio_for_doc_id(folio)
                key_rut_folio = (rut_norm, folio_norm) if rut_norm and folio_norm else None
                fecha_cells = (
                    [cells[fecha_col_idx]]
                    if isinstance(fecha_col_idx, int) and 0 <= fecha_col_idx < len(cells)
                    else cells
                )
                fecha_emision_iso = _extract_fecha_emision_iso(fecha_cells)
                fecha_contabilizada = False
                if fecha_emision_iso:
                    filas_con_fecha += 1
                    if fecha_emision_iso in existing_emission_dates:
                        filas_con_fecha_ya_en_db += 1
                    fecha_contabilizada = True

                doc_id = _build_doc_id(tipo_doc, rut_emisor, folio)
                exists_by_doc = bool(doc_id and doc_id in existing_doc_ids)
                exists_by_rut_folio = bool(key_rut_folio and key_rut_folio in existing_rut_folios)
                if exists_by_doc or exists_by_rut_folio:
                    reason = doc_id if exists_by_doc else f"{rut_norm}_{folio_norm}"
                    print(f"({i}) Ya existe en DB: {reason}, saltando.")
                    descargados_en_pagina += 1
                    filas_skip_conocidas += 1
                    if progress_cb:
                        try:
                            progress_cb(descargados_en_pagina, total_en_pagina, f"(pag. {pagina})")
                        except Exception:
                            pass
                    continue

                nombre_pdf = f"{tipo_doc}_{rut_emisor}_{folio}.pdf".replace("/", "_").replace(" ", "_")
                ruta_pdf   = os.path.join(ruta, nombre_pdf)

                if os.path.exists(ruta_pdf) or existe_factura(rut_emisor, folio, ruta):
                    print(f"({i}) Ya existe: {nombre_pdf}, saltando.")
                    descargados_en_pagina += 1
                    filas_skip_conocidas += 1
                    if progress_cb:
                        try:
                            progress_cb(descargados_en_pagina, total_en_pagina, f"(pag. {pagina})")
                        except Exception:
                            pass
                    continue

                if fecha_emision_iso and fecha_emision_iso in existing_emission_dates:
                    print(f"({i}) Fecha emision ya existe en DB ({fecha_emision_iso}), saltando.")
                    descargados_en_pagina += 1
                    filas_skip_conocidas += 1
                    if progress_cb:
                        try:
                            progress_cb(descargados_en_pagina, total_en_pagina, f"(pag. {pagina})")
                        except Exception:
                            pass
                    continue

                ver_link = row.query_selector("a[href*='mipeGesDocRcp.cgi']")
                if not ver_link:
                    continue

                href_detalle = _to_abs_sii_url(ver_link.get_attribute("href"))
                if not href_detalle:
                    continue

                # Fast-path: leer HTML detalle y usar FEC_EMI / link PDF sin abrir pestaña.
                detalle_html = ""
                pdf_href = ""
                try:
                    resp_det = dte_page.request.get(href_detalle)
                    if resp_det and resp_det.ok:
                        detalle_html = resp_det.text() or ""
                        fecha_det_iso = _extract_fecha_emision_iso_from_detail_html(detalle_html)
                        if fecha_det_iso and not fecha_emision_iso:
                            fecha_emision_iso = fecha_det_iso
                        if fecha_emision_iso and not fecha_contabilizada:
                            filas_con_fecha += 1
                            if fecha_emision_iso in existing_emission_dates:
                                filas_con_fecha_ya_en_db += 1
                            fecha_contabilizada = True

                        if fecha_emision_iso and fecha_emision_iso in existing_emission_dates:
                            print(
                                f"({i}) Fecha emision ya existe en DB "
                                f"(FEC_EMI={fecha_emision_iso}), saltando."
                            )
                            descargados_en_pagina += 1
                            filas_skip_conocidas += 1
                            if progress_cb:
                                try:
                                    progress_cb(descargados_en_pagina, total_en_pagina, f"(pag. {pagina})")
                                except Exception:
                                    pass
                            continue

                        pdf_href = _extract_pdf_href_from_detail_html(detalle_html)
                except Exception as e:
                    print(f"[OPT] No se pudo consultar detalle por request en fila {i}: {e}")

                candidatos_en_pagina += 1
                if pdf_href:
                    print(f"({i}) Descarga directa via HTML detalle: {tipo_doc} | {rut_emisor} | Folio {folio}")
                    print(f"Enlace PDF detectado: {pdf_href}")
                    try:
                        response = dte_page.request.get(pdf_href)
                        pdf_bytes = response.body()
                        with open(ruta_pdf, "wb") as f:
                            f.write(pdf_bytes)
                        print(f"PDF guardado: {ruta_pdf}")
                        if doc_id:
                            existing_doc_ids.add(doc_id)
                        if key_rut_folio:
                            existing_rut_folios.add(key_rut_folio)
                        if fecha_emision_iso:
                            existing_emission_dates.add(fecha_emision_iso)
                        descargados_en_pagina += 1
                        if progress_cb:
                            try:
                                progress_cb(descargados_en_pagina, total_en_pagina, f"(pag. {pagina})")
                            except Exception:
                                pass
                        random_sleep(0.6, 1.2)
                        continue
                    except Exception as e:
                        print(f"[OPT] Descarga directa fallo, se usa fallback UI: {e}")

                print(f"({i}) Abriendo detalle: {tipo_doc} | {rut_emisor} | Folio {folio}")

                with context.expect_page() as nueva_pagina_info:
                    dte_page.evaluate(f"window.open('{href_detalle}', '_blank');")
                detalle_page = nueva_pagina_info.value
                detalle_page.wait_for_load_state("networkidle", timeout=15000)
                random_sleep(1.0, 2.0)

                if not fecha_emision_iso:
                    fecha_det_page = _extract_fecha_emision_iso_from_detail_page(detalle_page)
                    if fecha_det_page:
                        fecha_emision_iso = fecha_det_page
                if fecha_emision_iso and not fecha_contabilizada:
                    filas_con_fecha += 1
                    if fecha_emision_iso in existing_emission_dates:
                        filas_con_fecha_ya_en_db += 1
                    fecha_contabilizada = True
                if fecha_emision_iso and fecha_emision_iso in existing_emission_dates:
                    print(
                        f"({i}) Fecha emision ya existe en DB "
                        f"(FEC_EMI={fecha_emision_iso}), saltando."
                    )
                    candidatos_en_pagina = max(0, candidatos_en_pagina - 1)
                    detalle_page.close()
                    descargados_en_pagina += 1
                    filas_skip_conocidas += 1
                    if progress_cb:
                        try:
                            progress_cb(descargados_en_pagina, total_en_pagina, f"(pag. {pagina})")
                        except Exception:
                            pass
                    continue

                print("Expandiendo 'Otros detalles documento'...")
                try:
                    boton_otro = detalle_page.query_selector("a[href='#collapseOtros']")
                    if boton_otro:
                        boton_otro.click(force=True)
                        detalle_page.wait_for_selector(
                            "#collapseOtros a[href*='mipeShowPdf.cgi']", timeout=8000
                        )
                        print("Seccion desplegada.")
                    else:
                        print("No se encontro boton de detalles.")
                except Exception as e:
                    print(f"No se pudo expandir la seccion: {e}")

                pdf_link = detalle_page.query_selector("#collapseOtros a[href*='mipeShowPdf.cgi']")
                if not pdf_link:
                    pdf_link = detalle_page.query_selector("a[href*='mipeShowPdf.cgi']")

                if not pdf_link:
                    print("No se encontro enlace al PDF.")
                    detalle_page.close()
                    errores_descarga.append((tipo_doc, rut_emisor, folio, "Sin enlace PDF"))
                    continue

                pdf_href = _to_abs_sii_url(pdf_link.get_attribute("href"))
                if not pdf_href:
                    print("No se pudo resolver URL del PDF.")
                    detalle_page.close()
                    errores_descarga.append((tipo_doc, rut_emisor, folio, "URL PDF invalida"))
                    continue
                print(f"Enlace PDF detectado: {pdf_href}")

                try:
                    response  = detalle_page.request.get(pdf_href)
                    pdf_bytes = response.body()
                    with open(ruta_pdf, "wb") as f:
                        f.write(pdf_bytes)
                    print(f"PDF guardado: {ruta_pdf}")
                    if doc_id:
                        existing_doc_ids.add(doc_id)
                    if key_rut_folio:
                        existing_rut_folios.add(key_rut_folio)
                    if fecha_emision_iso:
                        existing_emission_dates.add(fecha_emision_iso)
                    descargados_en_pagina += 1
                    if progress_cb:
                        try:
                            progress_cb(descargados_en_pagina, total_en_pagina, f"(pag. {pagina})")
                        except Exception:
                            pass
                except Exception as e:
                    print(f"Error al descargar PDF: {e}")
                    errores_descarga.append((tipo_doc, rut_emisor, folio, str(e)))

                detalle_page.close()
                random_sleep(1.0, 2.0)

            except Exception as e:
                print(f"Error en fila {i}: {e}")
                errores_descarga.append(("Desconocido", "Desconocido", "Desconocido", str(e)))
                continue

        print(
            "[OPT] Resumen pagina "
            f"{pagina}: validas={filas_validas}, skip_conocidas={filas_skip_conocidas}, "
            f"candidatas={candidatos_en_pagina}, fechas_detectadas={filas_con_fecha}, "
            f"fechas_ya_db={filas_con_fecha_ya_en_db}"
        )
        threshold = max(1, filas_validas - historic_tolerance_rows)
        page_is_historic = (
            filas_validas > 0
            and candidatos_en_pagina == 0
            and filas_skip_conocidas >= threshold
        )
        if page_is_historic:
            consecutive_historic_pages += 1
            print(
                "[OPT] Pagina historica detectada: "
                f"candidatos=0 | skip_conocidas={filas_skip_conocidas}/{filas_validas} "
                f"(tolerancia={historic_tolerance_rows})."
            )
        else:
            consecutive_historic_pages = 0

        if consecutive_historic_pages >= stop_after_known_date_pages:
            print(
                "[OPT] Deteniendo paginacion por redundancia temporal: "
                f"{consecutive_historic_pages} pagina(s) consecutiva(s) con fechas ya conocidas."
            )
            break

        next_btn = dte_page.query_selector("a#pagina_siguiente.paginate_button")

        if next_btn:
            href_next = next_btn.get_attribute("href")
            if href_next and not href_next.strip().endswith("="):
                if href_next.startswith("/"):
                    href_next = "https://www1.sii.cl" + href_next
                print("Pasando a la siguiente pagina.....")
                dte_page.goto(href_next)
                dte_page.wait_for_load_state("networkidle", timeout=15000)
                random_sleep(2.0, 3.0)
                pagina += 1
                continue

        print("No hay mas paginas disponibles. Scraping finalizado.")
        break

    if errores_descarga:
        print("\nDescargas incompletas detectadas:")
        for err in errores_descarga:
            print(f"  - {err[0]} | {err[1]} | Folio {err[2]} -> {err[3]}")
        print(f"\nTotal de documentos no descargados: {len(errores_descarga)}")
        print("Ejecuta nuevamente el script para reintentar los faltantes.")
        return True
    else:
        print("\nTodas las facturas fueron descargadas correctamente.")
        return False

def scrapear(ruta_pdf, progress_cb=None):
    stop_after_known_date_pages = _env_int("SII_STOP_AFTER_KNOWN_DATE_PAGES", 1, min_value=1)
    db_path = _resolve_db_path()
    existing_doc_ids, existing_emission_dates, existing_rut_folios = _load_db_index(db_path)
    print(
        "[OPT] Indice de DB cargado: "
        f"db='{db_path}' | id_doc={len(existing_doc_ids)} | "
        f"rut_folio={len(existing_rut_folios)} | fechas_emision={len(existing_emission_dates)} | "
        f"stop_after_known_date_pages={stop_after_known_date_pages}"
    )

    with sync_playwright() as p:
        proxy_conf  = {"server": PROXY} if PROXY else None
        launch_args = {"headless": HEADLESS, "slow_mo": SLOW_MO}
        if proxy_conf:
            launch_args["proxy"] = proxy_conf

        browser      = p.chromium.launch(**launch_args)
        context_args = {
            "user_agent":  random.choice(USER_AGENTS),
            "timezone_id": "America/Santiago",
            "locale":      "es-CL",
        }

        if os.path.exists(STORAGE_PATH):
            print("Reusando sesion previa...")
            context_args["storage_state"] = STORAGE_PATH

        context = browser.new_context(**context_args)
        page    = context.new_page()

        if not os.path.exists(STORAGE_PATH):
            print("Paso 1: Login inicial en Mi SII")
            page.goto(
                "https://zeusr.sii.cl/AUT2000/InicioAutenticacion/IngresoRutClave.html"
                "?https://misiir.sii.cl/cgi_misii/siihome.cgi"
            )
            login_generico(page, RUT, CLAVE, "en Mi SII")
            context.storage_state(path=STORAGE_PATH)
            print("Sesion guardada para futuros accesos")

        print("Paso 2: Ir a Servicios Online - DTE")
        page.goto("https://www.sii.cl/servicios_online/1039-1183.html")
        page.wait_for_load_state("networkidle")
        random_sleep(1.5, 2.5)

        try:
            page.locator("a[href='#collapseTwo']").click(force=True)
        except:
            pass

        link = page.query_selector("a[href*='mipeLaunchPage.cgi?OPCION=1']")
        href = (
            link.get_attribute("href")
            if link
            else "https://www1.sii.cl/cgi-bin/Portal001/mipeLaunchPage.cgi?OPCION=1&TIPO=4"
        )

        page.goto(href)
        page.wait_for_load_state("networkidle")
        random_sleep(2, 3)

        login_generico(page, RUT2, CLAVE2, "en Portal DTE")

        panel = page.locator("a[href='#collapseAdm']")
        if panel.count() > 0:
            panel.first.click(force=True)
        random_sleep(1.5)

        links      = page.query_selector_all("a[href*='mipeLaunchPage.cgi']")
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
        print("Historial de DTE cargado correctamente.")

        descargas_incompletas = procesar_tabla(
            page,
            context,
            ruta_pdf,
            progress_cb=progress_cb,
            existing_doc_ids=existing_doc_ids,
            existing_emission_dates=existing_emission_dates,
            existing_rut_folios=existing_rut_folios,
            stop_after_known_date_pages=stop_after_known_date_pages,
        )

        if descargas_incompletas:
            print("Algunas descargas fallaron. Reejecuta el script.")
            return True
        else:
            print("Todo descargado con exito.")

        browser.close()

if __name__ == "__main__":
    scrapear(RUTA_PDF)
